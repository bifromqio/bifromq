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

import static com.baidu.bifromq.apiserver.Headers.HEADER_CLIENT_TYPE;
import static com.baidu.bifromq.apiserver.Headers.HEADER_PUB_QOS;
import static com.baidu.bifromq.apiserver.http.handler.HTTPHeaderUtils.getClientMeta;
import static com.baidu.bifromq.apiserver.http.handler.HTTPHeaderUtils.getHeader;
import static com.baidu.bifromq.apiserver.http.handler.HTTPHeaderUtils.getRetain;
import static com.baidu.bifromq.dist.client.ByteBufUtil.toRetainedByteBuffer;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.util.concurrent.CompletableFuture.allOf;

import com.baidu.bifromq.apiserver.Headers;
import com.baidu.bifromq.apiserver.http.IHTTPRequestHandler;
import com.baidu.bifromq.apiserver.utils.TopicUtil;
import com.baidu.bifromq.dist.client.DistResult;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.google.protobuf.ByteString;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/pub")
public final class HTTPPubHandler implements IHTTPRequestHandler {
    private final IDistClient distClient;
    private final IRetainClient retainClient;
    private final ISettingProvider settingProvider;

    public HTTPPubHandler(IDistClient distClient, IRetainClient retainClient, ISettingProvider settingProvider) {
        this.distClient = distClient;
        this.retainClient = retainClient;
        this.settingProvider = settingProvider;
    }

    @POST
    @Operation(summary = "Publish a message to given topic")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER, description = "optional caller provided request id", schema = @Schema(implementation = Long.class)),
        @Parameter(name = "tenant_id", in = ParameterIn.HEADER, required = true, description = "the tenant id"),
        @Parameter(name = "topic", in = ParameterIn.HEADER, required = true, description = "the message topic"),
        @Parameter(name = "client_type", in = ParameterIn.HEADER, required = true, description = "the client type"),
        @Parameter(name = "pub_qos", in = ParameterIn.HEADER, required = true, description = "QoS of the message to be distributed"),
        @Parameter(name = "retain", in = ParameterIn.HEADER, description = "the message should be retained"),
        @Parameter(name = "client_meta_*", in = ParameterIn.HEADER, description = "the metadata header about the kicker client, must be started with client_meta_"),
    })
    @RequestBody(required = true, description = "Message payload will be treated as binary", content = @Content(mediaType = "application/octet-stream"))
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Success"),
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(@Parameter(hidden = true) long reqId,
                                                      @Parameter(hidden = true) String tenantId,
                                                      @Parameter(hidden = true) FullHttpRequest req) {
        try {
            String topic = getHeader(Headers.HEADER_TOPIC, req, true);
            String clientType = getHeader(HEADER_CLIENT_TYPE, req, true);
            int qos = Integer.parseInt(getHeader(HEADER_PUB_QOS, req, true));
            boolean isRetain = getRetain(req);
            Map<String, String> clientMeta = getClientMeta(req);
            log.trace("Handling http pub request: reqId={}, tenantId={}, topic={}, clientType={}, clientMeta={}",
                reqId, tenantId, topic, clientType, clientMeta);
            if (!TopicUtil.checkTopicFilter(topic, tenantId, settingProvider)) {
                return CompletableFuture.completedFuture(
                    new DefaultFullHttpResponse(req.protocolVersion(), FORBIDDEN, Unpooled.EMPTY_BUFFER));
            }
            if (qos < 0 || qos > 2) {
                return CompletableFuture.completedFuture(
                    new DefaultFullHttpResponse(req.protocolVersion(), BAD_REQUEST, Unpooled.EMPTY_BUFFER));
            }
            ClientInfo clientInfo = ClientInfo.newBuilder()
                .setTenantId(tenantId)
                .setType(clientType)
                .putAllMetadata(clientMeta)
                .build();
            CompletableFuture<DistResult> distFuture = distClient.pub(reqId, topic,
                Message.newBuilder()
                    .setPubQoS(QoS.forNumber(qos))
                    .setPayload(toRetainedByteBuffer(req.content()))
                    .setExpiryInterval(Integer.MAX_VALUE)
                    .build(),
                clientInfo);
            CompletableFuture<Boolean> retainFuture = isRetain ?
                retainMessage(reqId, topic, toRetainedByteBuffer(req.content()), clientInfo) :
                CompletableFuture.completedFuture(true);
            return allOf(retainFuture, distFuture)
                .thenApply((v) -> new DefaultFullHttpResponse(req.protocolVersion(), OK, Unpooled.EMPTY_BUFFER));
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<Boolean> retainMessage(long reqId, String topic,
                                                     ByteString payload, ClientInfo clientInfo) {
        return retainClient.retain(reqId, topic, QoS.AT_MOST_ONCE, payload, Integer.MAX_VALUE, clientInfo)
            .thenApply(v -> {
                if (v.getResult() != RetainReply.Result.ERROR) {
                    return true;
                }
                return false;
            });
    }
}
