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
import static com.baidu.bifromq.apiserver.http.handler.HTTPHeaderUtils.getRequiredSubQoS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.baidu.bifromq.apiserver.http.annotation.Route;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.type.QoS;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class HTTPSubHandlerTest extends AbstractHTTPRequestHandlerTest<HTTPSubHandler> {
    @Mock
    private IDistClient distClient;

    @Override
    protected Class<HTTPSubHandler> handlerClass() {
        return HTTPSubHandler.class;
    }

    @Test
    public void missingHeaders() {
        Route route = HTTPUnsubHandler.class.getAnnotation(Route.class);
        DefaultFullHttpRequest req =
            new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, route.method().method, route.contextPath());

        HTTPSubHandler handler = new HTTPSubHandler(distClient);
        assertThrows(() -> handler.handle(123, "fakeTenant", req).join());
    }

    @Test
    public void sub() {
        Route route = HTTPSubHandler.class.getAnnotation(Route.class);
        DefaultFullHttpRequest req =
            new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, route.method().method, route.contextPath());
        req.headers().set(HEADER_TOPIC_FILTER.header, "/greeting/#");
        req.headers().set(HEADER_SUB_QOS.header, "1");
        req.headers().set(HEADER_INBOX_ID.header, "greeting_inbox");
        req.headers().set(HEADER_DELIVERER_KEY.header, "postman_no1");
        req.headers().set(HEADER_SUBBROKER_ID.header, "0");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        HTTPSubHandler handler = new HTTPSubHandler(distClient);
        handler.handle(reqId, tenantId, req);
        ArgumentCaptor<Long> reqIdCap = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<String> tenantIdCap = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> topicFilterCap = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<QoS> subQosCap = ArgumentCaptor.forClass(QoS.class);
        ArgumentCaptor<String> inboxIdCap = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> delivererKeyCap = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> subBrokerIdCap = ArgumentCaptor.forClass(Integer.class);
        verify(distClient).sub(reqIdCap.capture(),
            tenantIdCap.capture(),
            topicFilterCap.capture(),
            subQosCap.capture(),
            inboxIdCap.capture(),
            delivererKeyCap.capture(),
            subBrokerIdCap.capture());

        assertEquals(reqIdCap.getValue(), reqId);
        assertEquals(tenantIdCap.getValue(), tenantId);
        assertEquals(topicFilterCap.getValue(), req.headers().get(HEADER_TOPIC_FILTER.header));
        assertEquals(subQosCap.getValue(), getRequiredSubQoS(req));
        assertEquals(inboxIdCap.getValue(), req.headers().get(HEADER_INBOX_ID.header));
        assertEquals(delivererKeyCap.getValue(), req.headers().get(HEADER_DELIVERER_KEY.header));
        assertEquals(subBrokerIdCap.getValue(), getRequiredSubBrokerId(req));
    }

    @Test
    public void subSucceed() {
        Route route = HTTPSubHandler.class.getAnnotation(Route.class);
        DefaultFullHttpRequest req =
            new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, route.method().method, route.contextPath());
        req.headers().set(HEADER_TOPIC_FILTER.header, "/greeting/#");
        req.headers().set(HEADER_SUB_QOS.header, "1");
        req.headers().set(HEADER_INBOX_ID.header, "greeting_inbox");
        req.headers().set(HEADER_DELIVERER_KEY.header, "postman_no1");
        req.headers().set(HEADER_SUBBROKER_ID.header, "0");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        HTTPSubHandler handler = new HTTPSubHandler(distClient);

        when(distClient.sub(anyLong(), anyString(), anyString(), any(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(1));
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.OK);
        assertEquals(response.headers().get(HEADER_SUB_QOS.header), "1");
        assertEquals(response.content().readableBytes(), 0);
    }
}
