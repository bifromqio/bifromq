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

import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.client.MatchResult;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.client.InboxSubResult;
import com.baidu.bifromq.mqtt.inbox.IMqttBrokerClient;
import com.baidu.bifromq.mqtt.inbox.MqttSubResult;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.type.QoS;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;

import static com.baidu.bifromq.apiserver.Headers.*;
import static com.baidu.bifromq.apiserver.http.handler.HTTPHeaderUtils.getRequiredSubQoS;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class HTTPSubHandlerTest extends AbstractHTTPRequestHandlerTest<HTTPSubHandler> {
    @Mock
    private IMqttBrokerClient mqttBrokerClient;
    @Mock
    private IInboxClient inboxClient;
    @Mock
    private IDistClient distClient;
    private ISettingProvider settingProvider = Setting::current;

    @Override
    protected Class<HTTPSubHandler> handlerClass() {
        return HTTPSubHandler.class;
    }

    @Test
    public void missingHeaders() {
        DefaultFullHttpRequest req = buildRequest();
        HTTPSubHandler handler = new HTTPSubHandler(mqttBrokerClient, inboxClient, distClient, settingProvider);
        assertThrows(() -> handler.handle(123, "fakeTenant", req).join());
    }

    @Test
    public void sub() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_TOPIC_FILTER.header, "/greeting/#");
        req.headers().set(HEADER_SUB_QOS.header, "1");
        req.headers().set(HEADER_INBOX_ID.header, "greeting_inbox");
        req.headers().set(HEADER_SUBBROKER_ID.header, "0");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        HTTPSubHandler handler = new HTTPSubHandler(mqttBrokerClient, inboxClient, distClient, settingProvider);
        when(mqttBrokerClient.sub(anyLong(), anyString(), anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(MqttSubResult.OK));
        handler.handle(reqId, tenantId, req);
        ArgumentCaptor<Long> reqIdCap = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<String> tenantIdCap = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> topicFilterCap = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<QoS> subQosCap = ArgumentCaptor.forClass(QoS.class);
        ArgumentCaptor<String> inboxIdCap = ArgumentCaptor.forClass(String.class);
        verify(mqttBrokerClient).sub(reqIdCap.capture(), tenantIdCap.capture(),
                inboxIdCap.capture(), topicFilterCap.capture(), subQosCap.capture());

        assertEquals(reqIdCap.getValue(), reqId);
        assertEquals(tenantIdCap.getValue(), tenantId);
        assertEquals(topicFilterCap.getValue(), req.headers().get(HEADER_TOPIC_FILTER.header));
        assertEquals(subQosCap.getValue(), getRequiredSubQoS(req));
        assertEquals(inboxIdCap.getValue(), req.headers().get(HEADER_INBOX_ID.header));
    }

    @Test
    public void subTransientInboxSucceed() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_TOPIC_FILTER.header, "/greeting/#");
        req.headers().set(HEADER_SUB_QOS.header, "1");
        req.headers().set(HEADER_INBOX_ID.header, "greeting_inbox");
        req.headers().set(HEADER_SUBBROKER_ID.header, "0");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        HTTPSubHandler handler = new HTTPSubHandler(mqttBrokerClient, inboxClient, distClient, settingProvider);

        when(mqttBrokerClient.sub(anyLong(), anyString(), anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(MqttSubResult.OK));

        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.OK);
        assertEquals(response.headers().get(HEADER_SUB_QOS.header), "1");
        assertEquals(response.content().readableBytes(), 0);
    }

    @Test
    public void subPersistentInboxSucceed() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_TOPIC_FILTER.header, "/greeting/#");
        req.headers().set(HEADER_SUB_QOS.header, "0");
        req.headers().set(HEADER_INBOX_ID.header, "greeting_inbox");
        req.headers().set(HEADER_SUBBROKER_ID.header, "1");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        HTTPSubHandler handler = new HTTPSubHandler(mqttBrokerClient, inboxClient, distClient, settingProvider);

        when(inboxClient.sub(anyLong(), anyString(), anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(InboxSubResult.OK));

        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.OK);
        assertEquals(response.headers().get(HEADER_SUB_QOS.header), "0");
        assertEquals(response.content().readableBytes(), 0);
    }

    @Test
    public void subTransientInboxFailed() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_TOPIC_FILTER.header, "/greeting/#");
        req.headers().set(HEADER_SUB_QOS.header, "1");
        req.headers().set(HEADER_INBOX_ID.header, "greeting_inbox");
        req.headers().set(HEADER_SUBBROKER_ID.header, "0");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        HTTPSubHandler handler = new HTTPSubHandler(mqttBrokerClient, inboxClient, distClient, settingProvider);

        when(mqttBrokerClient.sub(anyLong(), anyString(), anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(MqttSubResult.ERROR));

        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.OK);
        assertEquals(response.headers().get(HEADER_SUB_QOS.header), "128");
        assertEquals(response.content().readableBytes(), 0);
    }

    @Test
    public void subPersistentInboxFailed() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_TOPIC_FILTER.header, "/greeting/#");
        req.headers().set(HEADER_SUB_QOS.header, "0");
        req.headers().set(HEADER_INBOX_ID.header, "greeting_inbox");
        req.headers().set(HEADER_SUBBROKER_ID.header, "1");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        HTTPSubHandler handler = new HTTPSubHandler(mqttBrokerClient, inboxClient, distClient, settingProvider);

        when(inboxClient.sub(anyLong(), anyString(), anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(InboxSubResult.NO_INBOX));

        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.OK);
        assertEquals(response.headers().get(HEADER_SUB_QOS.header), "128");
        assertEquals(response.content().readableBytes(), 0);
    }

    @Test
    public void subOtherSubBroker() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_TOPIC_FILTER.header, "/greeting/#");
        req.headers().set(HEADER_SUB_QOS.header, "0");
        req.headers().set(HEADER_INBOX_ID.header, "greeting_inbox");
        req.headers().set(HEADER_SUBBROKER_ID.header, "3");
        req.headers().set(HEADER_DELIVERER_KEY.header, "delivererKey");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        HTTPSubHandler handler = new HTTPSubHandler(mqttBrokerClient, inboxClient, distClient, settingProvider);

        when(distClient.match(anyLong(), anyString(), anyString(), any(), anyString(), anyString(), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));

        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.OK);
        assertEquals(response.headers().get(HEADER_SUB_QOS.header), "0");
        assertEquals(response.content().readableBytes(), 0);
    }

    private DefaultFullHttpRequest buildRequest() {
        return buildRequest(HttpMethod.PUT);
    }
}
