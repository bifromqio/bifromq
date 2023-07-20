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
import static com.baidu.bifromq.apiserver.Headers.HEADER_TOPIC_FILTER;
import static com.baidu.bifromq.apiserver.http.handler.HTTPHeaderUtils.getHeader;
import static com.baidu.bifromq.apiserver.http.handler.HTTPHeaderUtils.getRequiredSubBrokerId;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.baidu.bifromq.apiserver.http.IHTTPRequestHandler;
import com.baidu.bifromq.apiserver.http.annotation.Method;
import com.baidu.bifromq.apiserver.http.annotation.Route;
import com.baidu.bifromq.dist.client.IDistClient;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Route(contextPath = "/sub", method = Method.DELETE)
public final class HTTPUnsubHandler implements IHTTPRequestHandler {
    private final IDistClient distClient;

    public HTTPUnsubHandler(IDistClient distClient) {
        this.distClient = distClient;
    }

    @Override
    public CompletableFuture<FullHttpResponse> handle(long reqId, String tenantId, FullHttpRequest req) {
        try {
            String topicFilter = getHeader(HEADER_TOPIC_FILTER, req, true);
            String inboxId = getHeader(HEADER_INBOX_ID, req, true);
            String delivererKey = getHeader(HEADER_DELIVERER_KEY, req, true);
            int subBrokerId = getRequiredSubBrokerId(req);
            log.trace(
                "Handling http unsub request: reqId={}, tenantId={}, topicFilter={}, inboxId={}, delivererKey={}, subBrokerId={}",
                reqId, tenantId, topicFilter, inboxId, delivererKey, subBrokerId);
            return distClient.unsub(reqId, tenantId, topicFilter, inboxId, delivererKey, subBrokerId)
                .thenApply(
                    v -> new DefaultFullHttpResponse(req.protocolVersion(), v ? OK : NOT_FOUND, Unpooled.EMPTY_BUFFER));
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
