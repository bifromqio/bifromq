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
import static com.baidu.bifromq.apiserver.Headers.HEADER_SUBBROKER_ID;
import static com.baidu.bifromq.apiserver.Headers.HEADER_SUB_QOS;
import static com.baidu.bifromq.apiserver.Headers.HEADER_TOPIC_FILTER;
import static com.baidu.bifromq.apiserver.http.handler.HTTPHeaderUtils.getRequiredSubBrokerId;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.baidu.bifromq.dist.client.IDistClient;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class HTTPUnsubHandlerTest extends AbstractHTTPRequestHandlerTest<HTTPUnsubHandler> {
    @Mock
    private IDistClient distClient;

    @Override
    protected Class<HTTPUnsubHandler> handlerClass() {
        return HTTPUnsubHandler.class;
    }

    @Test
    public void missingHeaders() {
        DefaultFullHttpRequest req = buildRequest();
        HTTPUnsubHandler handler = new HTTPUnsubHandler(distClient);
        assertThrows(() -> handler.handle(123, "fakeTenant", req).join());
    }

    @Test
    public void unsub() {
        DefaultFullHttpRequest req = buildRequest();

        req.headers().set(HEADER_TOPIC_FILTER.header, "/greeting/#");
        req.headers().set(HEADER_INBOX_ID.header, "greeting_inbox");
        req.headers().set(HEADER_DELIVERER_KEY.header, "postman_no1");
        req.headers().set(HEADER_SUBBROKER_ID.header, "0");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        HTTPUnsubHandler handler = new HTTPUnsubHandler(distClient);
        handler.handle(reqId, tenantId, req);
        ArgumentCaptor<Long> reqIdCap = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<String> tenantIdCap = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> topicFilterCap = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> inboxIdCap = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> delivererKeyCap = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> subBrokerIdCap = ArgumentCaptor.forClass(Integer.class);
        verify(distClient).unsub(reqIdCap.capture(),
            tenantIdCap.capture(),
            topicFilterCap.capture(),
            inboxIdCap.capture(),
            delivererKeyCap.capture(),
            subBrokerIdCap.capture());

        assertEquals(reqIdCap.getValue(), reqId);
        assertEquals(tenantIdCap.getValue(), tenantId);
        assertEquals(topicFilterCap.getValue(), req.headers().get(HEADER_TOPIC_FILTER.header));
        assertEquals(inboxIdCap.getValue(), req.headers().get(HEADER_INBOX_ID.header));
        assertEquals(delivererKeyCap.getValue(), req.headers().get(HEADER_DELIVERER_KEY.header));
        assertEquals(subBrokerIdCap.getValue(), getRequiredSubBrokerId(req));
    }

    @Test
    public void unsubSucceed() {
        DefaultFullHttpRequest req = buildRequest();

        req.headers().set(HEADER_TOPIC_FILTER.header, "/greeting/#");
        req.headers().set(HEADER_SUB_QOS.header, "1");
        req.headers().set(HEADER_INBOX_ID.header, "greeting_inbox");
        req.headers().set(HEADER_DELIVERER_KEY.header, "postman_no1");
        req.headers().set(HEADER_SUBBROKER_ID.header, "0");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        HTTPUnsubHandler handler = new HTTPUnsubHandler(distClient);

        when(distClient.unsub(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(true));
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.OK);
        assertEquals(response.content().readableBytes(), 0);
    }

    @Test
    public void unsubNothing() {
        DefaultFullHttpRequest req = buildRequest();

        req.headers().set(HEADER_TOPIC_FILTER.header, "/greeting/#");
        req.headers().set(HEADER_SUB_QOS.header, "1");
        req.headers().set(HEADER_INBOX_ID.header, "greeting_inbox");
        req.headers().set(HEADER_DELIVERER_KEY.header, "postman_no1");
        req.headers().set(HEADER_SUBBROKER_ID.header, "0");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        HTTPUnsubHandler handler = new HTTPUnsubHandler(distClient);

        when(distClient.unsub(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(false));
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.NOT_FOUND);
        assertEquals(response.content().readableBytes(), 0);
    }

    private DefaultFullHttpRequest buildRequest() {
        return buildRequest(HttpMethod.DELETE);
    }
}
