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

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.baidu.bifromq.apiserver.Headers;
import com.baidu.bifromq.apiserver.http.IHTTPRequestHandler;
import com.baidu.bifromq.basekv.IBaseKVClusterMetadataManager;
import com.baidu.bifromq.basekv.IBaseKVMetaService;
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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
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
        @Parameter(name = "cluster_id", in = ParameterIn.HEADER,
            description = "optional caller provided request id", schema = @Schema(implementation = Long.class))
    })
    @RequestBody(required = false)
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Success"),
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(long reqId, FullHttpRequest req) {
        log.trace("Handling http get load rules request: {}", req);
        String clusterId = HeaderUtils.getHeader(Headers.HEADER_CLUSTER_ID, req, true);
        IBaseKVClusterMetadataManager metadataManager = metadataManagers.get(clusterId);
        if (metadataManager == null) {
            return CompletableFuture.completedFuture(
                new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND, Unpooled.EMPTY_BUFFER));
        }
        return metadataManager.loadRules()
            .first("")
            .toCompletionStage()
            .toCompletableFuture()
            .thenApply(loadRules -> {
                DefaultFullHttpResponse
                    resp = new DefaultFullHttpResponse(req.protocolVersion(), OK,
                    Unpooled.wrappedBuffer(loadRules.getBytes()));
                resp.headers().set("Content-Type", "application/json");
                return resp;
            });
    }
}
