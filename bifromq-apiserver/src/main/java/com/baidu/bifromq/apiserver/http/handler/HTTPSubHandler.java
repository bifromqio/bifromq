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

import static com.baidu.bifromq.apiserver.Headers.HEADER_DELIVERER_KEY;
import static com.baidu.bifromq.apiserver.Headers.HEADER_INBOX_ID;
import static com.baidu.bifromq.apiserver.Headers.HEADER_SUB_QOS;
import static com.baidu.bifromq.apiserver.Headers.HEADER_TOPIC_FILTER;
import static com.baidu.bifromq.apiserver.http.handler.HTTPHeaderUtils.getHeader;
import static com.baidu.bifromq.apiserver.http.handler.HTTPHeaderUtils.getRequiredSubBrokerId;
import static com.baidu.bifromq.apiserver.http.handler.HTTPHeaderUtils.getRequiredSubQoS;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.baidu.bifromq.apiserver.http.IHTTPRequestHandler;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.type.QoS;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/sub")
public final class HTTPSubHandler implements IHTTPRequestHandler {
    private final IDistClient distClient;

    public HTTPSubHandler(IDistClient distClient) {
        this.distClient = distClient;
    }

    @PUT
    @Operation(summary = "Add a topic subscription to an inbox")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER, description = "optional caller provided request id", schema = @Schema(implementation = Long.class)),
        @Parameter(name = "tenant_id", in = ParameterIn.HEADER, required = true, description = "the tenant id"),
        @Parameter(name = "topic_filter", in = ParameterIn.HEADER, required = true, description = "the topic filter to add"),
        @Parameter(name = "sub_qos", in = ParameterIn.HEADER, schema = @Schema(allowableValues = {"0", "1",
            "2"}), required = true, description = "the qos of the subscription"),
        @Parameter(name = "inbox_id", in = ParameterIn.HEADER, required = true, description = "the inbox for receiving subscribed messages"),
        @Parameter(name = "deliverer_key", in = ParameterIn.HEADER, required = true, description = "the key of the deliverer for the inbox"),
        @Parameter(name = "subbroker_id", in = ParameterIn.HEADER, required = true, schema = @Schema(implementation = Integer.class), description = "the id of the subbroker hosting the inbox"),
    })
    @RequestBody(required = false)
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Success", headers = {
            @Header(name = "sub_qos", description = "the sub qos granted", schema = @Schema(allowableValues = {"0", "1",
                "2"}))}),
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(@Parameter(hidden = true) long reqId,
                                                      @Parameter(hidden = true) String tenantId,
                                                      @Parameter(hidden = true) FullHttpRequest req) {
        try {
            String topicFilter = getHeader(HEADER_TOPIC_FILTER, req, true);
            QoS subQoS = getRequiredSubQoS(req);
            String inboxId = getHeader(HEADER_INBOX_ID, req, true);
            String delivererKey = getHeader(HEADER_DELIVERER_KEY, req, true);
            int subBrokerId = getRequiredSubBrokerId(req);
            log.trace(
                "Handling http sub request: reqId={}, tenantId={}, topicFilter={}, subQoS={}, inboxId={}, delivererKey={}, subBrokerId={}",
                reqId, tenantId, topicFilter, subQoS, inboxId, delivererKey, subBrokerId);
            return distClient.sub(reqId, tenantId, topicFilter, subQoS, inboxId, delivererKey, subBrokerId)
                .thenApply((v) -> {
                    DefaultFullHttpResponse resp =
                        new DefaultFullHttpResponse(req.protocolVersion(), OK, Unpooled.EMPTY_BUFFER);
                    resp.headers().set(HEADER_SUB_QOS.header, v);
                    return resp;
                });
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}