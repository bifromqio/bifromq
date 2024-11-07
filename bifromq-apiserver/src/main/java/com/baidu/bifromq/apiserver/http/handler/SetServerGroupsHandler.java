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

import static com.baidu.bifromq.apiserver.Headers.HEADER_SERVER_ID;
import static com.baidu.bifromq.apiserver.http.handler.HeaderUtils.getHeader;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.baidu.bifromq.apiserver.http.IHTTPRequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.PUT;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class SetServerGroupsHandler implements IHTTPRequestHandler {

    protected abstract CompletableFuture<Void> setServerGroups(String serverId, Set<String> groupTags);

    @PUT
    @Operation(summary = "Set server groups")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER,
            description = "optional caller provided request id", schema = @Schema(implementation = Long.class)),
        @Parameter(name = "server_id", in = ParameterIn.HEADER, required = true,
            description = "the id of server", schema = @Schema(implementation = String.class)),
    })
    @RequestBody(required = true)
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Success"),
        @ApiResponse(responseCode = "400", description = "Bad Request"),
        @ApiResponse(responseCode = "404", description = "Server not found"),
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(long reqId, FullHttpRequest req) {
        String serverId = getHeader(HEADER_SERVER_ID, req, true);
        Set<String> groupTags = new HashSet<>();
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            ArrayNode groupTagsArray =
                (ArrayNode) objectMapper.readTree(req.content().toString(io.netty.util.CharsetUtil.UTF_8));
            groupTagsArray.forEach(tagNode -> groupTags.add(tagNode.asText()));
        } catch (Exception e) {
            return CompletableFuture.completedFuture(
                new DefaultFullHttpResponse(req.protocolVersion(), HttpResponseStatus.BAD_REQUEST,
                    Unpooled.EMPTY_BUFFER));
        }
        log.trace("Handling http set server group tags request: {}", req);
        return setServerGroups(serverId, groupTags)
            .handle((v, e) -> {
                if (e != null) {
                    return new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND, Unpooled.EMPTY_BUFFER);
                } else {
                    return new DefaultFullHttpResponse(req.protocolVersion(), OK, Unpooled.EMPTY_BUFFER);
                }
            });
    }
}
