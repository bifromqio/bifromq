/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.baidu.bifromq.apiserver.http.IHTTPRequestHandler;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficGovernor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.PUT;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class SetTrafficRulesHandler implements IHTTPRequestHandler {
    protected abstract IRPCServiceTrafficGovernor trafficGovernor();

    @PUT
    @Operation(summary = "Set the traffic directive")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER,
            description = "optional caller provided request id", schema = @Schema(implementation = Long.class))
    })
    @RequestBody(required = false)
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Success"),
        @ApiResponse(responseCode = "400", description = "Bad Request"),
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(long reqId, FullHttpRequest req) {
        Map<String, Map<String, Integer>> td = new HashMap<>();
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectNode tdObject =
                (ObjectNode) objectMapper.readTree(req.content().toString(io.netty.util.CharsetUtil.UTF_8));
            tdObject.fields().forEachRemaining(entry -> {
                String key = entry.getKey();
                ObjectNode innerObject = (ObjectNode) entry.getValue();
                Map<String, Integer> innerMap = new HashMap<>();

                innerObject.fields()
                    .forEachRemaining(innerEntry -> innerMap.put(innerEntry.getKey(), innerEntry.getValue().asInt()));
                if (key.isEmpty()) {
                    throw new IllegalArgumentException("Empty tenantIdPrefix is not allowed");
                }
                td.put(key, innerMap);
            });
        } catch (Exception e) {
            return CompletableFuture.completedFuture(
                new DefaultFullHttpResponse(req.protocolVersion(), HttpResponseStatus.BAD_REQUEST,
                    Unpooled.copiedBuffer(e.getMessage().getBytes())));
        }
        log.trace("Handling http set traffic directive request: {}", req);
        return CompletableFuture.allOf(td.entrySet()
                .stream()
                .map(e -> trafficGovernor().setTrafficRules(e.getKey(), e.getValue()))
                .toArray(CompletableFuture[]::new))
            .thenApply(
                trafficDirective -> new DefaultFullHttpResponse(req.protocolVersion(), OK, Unpooled.EMPTY_BUFFER));
    }
}
