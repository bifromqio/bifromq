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

import static com.baidu.bifromq.apiserver.Headers.HEADER_CLIENT_TYPE;
import static com.baidu.bifromq.apiserver.http.handler.HTTPHeaderUtils.getClientMeta;
import static com.baidu.bifromq.apiserver.http.handler.HTTPHeaderUtils.getHeader;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.baidu.bifromq.apiserver.Headers;
import com.baidu.bifromq.apiserver.http.IHTTPRequestHandler;
import com.baidu.bifromq.apiserver.http.annotation.Method;
import com.baidu.bifromq.apiserver.http.annotation.Route;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Route(contextPath = "/pub", method = Method.POST)
public final class HTTPPubHandler implements IHTTPRequestHandler {
    private final IDistClient distClient;

    public HTTPPubHandler(IDistClient distClient) {
        this.distClient = distClient;
    }

    @Override
    public CompletableFuture<FullHttpResponse> handle(long reqId, String tenantId, FullHttpRequest req) {
        try {
            String topic = getHeader(Headers.HEADER_TOPIC, req, true);
            String clientType = getHeader(HEADER_CLIENT_TYPE, req, true);
            Map<String, String> clientMeta = getClientMeta(req);
            log.trace("Handling http pub request: reqId={}, tenantId={}, topic={}, clientType={}, clientMeta={}",
                reqId, tenantId, topic, clientType, clientMeta);
            return distClient.pub(reqId, topic, QoS.AT_LEAST_ONCE, req.content().duplicate().nioBuffer(),
                    Integer.MAX_VALUE, ClientInfo.newBuilder()
                        .setTenantId(tenantId)
                        .setType(clientType)
                        .putAllMetadata(clientMeta)
                        .build())
                .thenApply((v) -> new DefaultFullHttpResponse(req.protocolVersion(), OK, Unpooled.EMPTY_BUFFER));
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
