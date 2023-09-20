/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.baidu.bifromq.apiserver.http.handler;

import static com.baidu.bifromq.apiserver.Headers.HEADER_CLIENT_ID;
import static com.baidu.bifromq.apiserver.Headers.HEADER_CLIENT_TYPE;
import static com.baidu.bifromq.apiserver.Headers.HEADER_USER_ID;
import static com.baidu.bifromq.apiserver.http.handler.HTTPHeaderUtils.getClientMeta;
import static com.baidu.bifromq.apiserver.http.handler.HTTPHeaderUtils.getHeader;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.baidu.bifromq.apiserver.http.IHTTPRequestHandler;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import com.baidu.bifromq.sessiondict.rpc.proto.KillReply;
import com.baidu.bifromq.type.ClientInfo;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/kill")
public final class HTTPKickHandler implements IHTTPRequestHandler {
    private final ISessionDictClient sessionDictClient;

    public HTTPKickHandler(ISessionDictClient sessionDictClient) {
        this.sessionDictClient = sessionDictClient;
    }

    @DELETE
    @Operation(summary = "Disconnect a MQTT client connection")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER, description = "optional caller provided request id", schema = @Schema(implementation = Long.class)),
        @Parameter(name = "tenant_id", in = ParameterIn.HEADER, required = true, description = "the tenant id"),
        @Parameter(name = "user_id", in = ParameterIn.HEADER, required = true, description = "the user id of the MQTT client connection to be disconnected"),
        @Parameter(name = "client_type", in = ParameterIn.HEADER, required = true, description = "the client type"),
        @Parameter(name = "client_meta_*", in = ParameterIn.HEADER, description = "the metadata header about the kicker client, must be started with client_meta_"),
    })
    @RequestBody(required = false)
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Success"),
        @ApiResponse(responseCode = "404", description = "Not Found")
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(@Parameter(hidden = true) long reqId,
                                                      @Parameter(hidden = true) String tenantId,
                                                      @Parameter(hidden = true) FullHttpRequest req) {
        try {
            String userId = getHeader(HEADER_USER_ID, req, true);
            String clientId = getHeader(HEADER_CLIENT_ID, req, true);
            String clientType = getHeader(HEADER_CLIENT_TYPE, req, true);
            Map<String, String> clientMeta = getClientMeta(req);

            log.trace(
                "Handling http kill request: reqId={}, tenantId={}, userId={}, clientId={}, clientType={}, clientMeta={}",
                reqId, tenantId, userId, clientId, clientType, clientMeta);
            return sessionDictClient.kill(reqId, tenantId, userId, clientId, ClientInfo.newBuilder()
                    .setTenantId(tenantId)
                    .setType(clientType)
                    .putAllMetadata(clientMeta)
                    .build())
                .thenApply(v -> new DefaultFullHttpResponse(req.protocolVersion(), v.getResult() == KillReply.Result.OK
                        ? OK : NOT_FOUND, Unpooled.EMPTY_BUFFER));
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
