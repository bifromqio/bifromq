/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

import static com.baidu.bifromq.apiserver.Headers.HEADER_EXPIRY_SECONDS;
import static com.baidu.bifromq.apiserver.http.handler.HeaderUtils.getHeader;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.rpc.proto.ExpireAllReply;
import com.baidu.bifromq.inbox.rpc.proto.ExpireAllRequest;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
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
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/session")
public final class ExpireSessionHandler extends TenantAwareHandler {
    private final IInboxClient inboxClient;

    public ExpireSessionHandler(ISettingProvider settingProvider, IInboxClient inboxClient) {
        super(settingProvider);
        this.inboxClient = inboxClient;
    }

    @DELETE
    @Operation(summary = "Expire inactive persistent session using given expiry time")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER, description = "optional caller provided request id", schema = @Schema(implementation = Long.class)),
        @Parameter(name = "tenant_id", in = ParameterIn.HEADER, required = true, description = "the tenant id"),
        @Parameter(name = "expiry_seconds", in = ParameterIn.HEADER, required = true, description = "the overridden session expiry time in seconds"),
    })
    @RequestBody(required = false)
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Success"),
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(@Parameter(hidden = true) long reqId,
                                                      @Parameter(hidden = true) String tenantId,
                                                      @Parameter(hidden = true) FullHttpRequest req) {
        try {
            int expirySeconds = Integer.parseInt(getHeader(HEADER_EXPIRY_SECONDS, req, true));
            log.trace("Handling http expiry inbox request: {}", req);
            ExpireAllRequest.Builder reqBuilder = ExpireAllRequest.newBuilder()
                .setReqId(reqId)
                .setExpirySeconds(expirySeconds)
                .setNow(HLC.INST.getPhysical());
            if (tenantId != null) {
                reqBuilder.setTenantId(tenantId);
            }
            return inboxClient.expireAll(reqBuilder.build())
                .thenApply(r -> new DefaultFullHttpResponse(req.protocolVersion(),
                    r.getCode() == ExpireAllReply.Code.OK ? OK : INTERNAL_SERVER_ERROR, Unpooled.EMPTY_BUFFER));
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
