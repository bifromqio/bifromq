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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.reactivex.rxjava3.core.Single;
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
import javax.ws.rs.GET;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class GetTrafficRulesHandler implements IHTTPRequestHandler {
    protected abstract Single<Map<String, Map<String, Integer>>> trafficRules();

    @GET
    @Operation(summary = "Get the traffic rules")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER,
            description = "optional caller provided request id", schema = @Schema(implementation = Long.class))
    })
    @RequestBody(required = false)
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Success"),
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(long reqId, FullHttpRequest req) {
        log.trace("Handling http get traffic rules request: {}", req);
        return trafficRules()
            .toCompletionStage()
            .toCompletableFuture()
            .thenApply(trafficDirective -> {
                DefaultFullHttpResponse
                    resp = new DefaultFullHttpResponse(req.protocolVersion(), OK,
                    Unpooled.wrappedBuffer(toJSON(trafficDirective).getBytes()));
                resp.headers().set("Content-Type", "application/json");
                return resp;
            });
    }

    private String toJSON(Map<String, Map<String, Integer>> trafficDirective) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode rootObject = mapper.createObjectNode();
        for (String tenantIdPrefix : trafficDirective.keySet()) {
            Map<String, Integer> groupWeights = trafficDirective.get(tenantIdPrefix);
            ObjectNode groupWeightsObject = mapper.createObjectNode();
            for (String group : groupWeights.keySet()) {
                groupWeightsObject.put(group, groupWeights.get(group));
            }
            rootObject.set(tenantIdPrefix, groupWeightsObject);
        }
        return rootObject.toString();
    }
}
