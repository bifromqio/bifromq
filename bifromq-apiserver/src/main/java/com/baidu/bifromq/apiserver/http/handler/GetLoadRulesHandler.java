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

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.util.Collections.emptyMap;

import com.baidu.bifromq.apiserver.Headers;
import com.baidu.bifromq.apiserver.http.IHTTPRequestHandler;
import com.baidu.bifromq.basekv.metaservice.IBaseKVClusterMetadataManager;
import com.baidu.bifromq.basekv.metaservice.IBaseKVMetaService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/rules/load")
public class GetLoadRulesHandler implements IHTTPRequestHandler {
    private final Map<String, IBaseKVClusterMetadataManager> metadataManagers = new ConcurrentHashMap<>();

    protected GetLoadRulesHandler(IBaseKVMetaService metaService) {
        metaService.clusterIds().subscribe(clusterIds -> {
            metadataManagers.keySet().removeIf(clusterId -> !clusterIds.contains(clusterId));
            for (String clusterId : clusterIds) {
                metadataManagers.computeIfAbsent(clusterId, metaService::metadataManager);
            }
        });
    }


    @GET
    @Operation(summary = "Get the load rules")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER,
            description = "optional caller provided request id", schema = @Schema(implementation = Long.class)),
        @Parameter(name = "service_name", in = ParameterIn.HEADER, required = true,
            description = "the service name", schema = @Schema(implementation = String.class))
    })
    @RequestBody(required = false)
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Success"),
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(long reqId, FullHttpRequest req) {
        log.trace("Handling http get load rules request: {}", req);
        String serviceName = HeaderUtils.getHeader(Headers.HEADER_SERVICE_NAME, req, true);
        IBaseKVClusterMetadataManager metadataManager = metadataManagers.get(serviceName);
        if (metadataManager == null) {
            return CompletableFuture.completedFuture(new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND,
                Unpooled.copiedBuffer(("Service not found: " + serviceName).getBytes())));
        }
        return metadataManager.loadRules()
            .timeout(1, TimeUnit.SECONDS)
            .firstElement()
            .toCompletionStage()
            .toCompletableFuture()
            .handle((loadRules, e) -> {
                if (e != null) {
                    if (e instanceof TimeoutException) {
                        DefaultFullHttpResponse resp =
                            new DefaultFullHttpResponse(req.protocolVersion(), OK,
                                Unpooled.wrappedBuffer(toJson(emptyMap()).getBytes()));
                        resp.headers().set("Content-Type", "application/json");
                        return resp;
                    } else {
                        return new DefaultFullHttpResponse(req.protocolVersion(), INTERNAL_SERVER_ERROR,
                            Unpooled.copiedBuffer(e.getMessage().getBytes()));
                    }
                } else {
                    DefaultFullHttpResponse resp = new DefaultFullHttpResponse(req.protocolVersion(), OK,
                        Unpooled.wrappedBuffer(toJson(loadRules).getBytes()));
                    resp.headers().set("Content-Type", "application/json");
                    return resp;
                }
            });
    }

    @SneakyThrows
    private String toJson(Map<String, Struct> balancerLoadRules) {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode rootNode = objectMapper.createObjectNode();
        for (Map.Entry<String, Struct> entry : balancerLoadRules.entrySet()) {
            String key = entry.getKey();
            Struct value = entry.getValue();
            String jsonValue = JsonFormat.printer().print(value);
            rootNode.set(key, objectMapper.readTree(jsonValue));
        }
        return objectMapper.writeValueAsString(rootNode);
    }
}
