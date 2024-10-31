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
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficDirector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
import java.util.Base64;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.GET;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class GetServerLandscapeHandler implements IHTTPRequestHandler {

    protected abstract Single<Set<IRPCServiceTrafficDirector.Server>> landscapeSingle();

    @GET
    @Operation(summary = "Get the server landscape information")
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
        log.trace("Handling http get server landscape request: {}", req);
        return landscapeSingle()
            .toCompletionStage()
            .toCompletableFuture()
            .thenApply(landscape -> {
                DefaultFullHttpResponse
                    resp = new DefaultFullHttpResponse(req.protocolVersion(), OK,
                    Unpooled.wrappedBuffer(toJSON(landscape).getBytes()));
                resp.headers().set("Content-Type", "application/json");
                return resp;
            });
    }

    private String toJSON(Set<IRPCServiceTrafficDirector.Server> landscape) {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode rootObject = mapper.createArrayNode();
        for (IRPCServiceTrafficDirector.Server server : landscape) {
            ObjectNode serverObject = mapper.createObjectNode();
            serverObject.put("agentHostId", Base64.getEncoder().encodeToString(server.agentHostId.toByteArray()));
            serverObject.put("id", server.id);
            serverObject.put("address", server.address);
            serverObject.put("port", server.port);

            ObjectNode attrsObject = mapper.createObjectNode();
            for (String attrName : server.attrs.keySet()) {
                attrsObject.put(attrName, server.attrs.get(attrName));
            }
            serverObject.set("attributes", attrsObject);
            ArrayNode groupTagsArray = mapper.createArrayNode();
            for (String groupTag : server.groupTags) {
                groupTagsArray.add(groupTag);
            }
            serverObject.set("groups", groupTagsArray);
            rootObject.add(serverObject);
        }
        return rootObject.toString();
    }

}
