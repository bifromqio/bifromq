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

import static com.baidu.bifromq.apiserver.Headers.HEADER_CLIENT_META_PREFIX;
import static com.baidu.bifromq.apiserver.Headers.HEADER_CLIENT_TYPE;
import static com.baidu.bifromq.apiserver.Headers.HEADER_PUB_QOS;
import static com.baidu.bifromq.apiserver.Headers.HEADER_RETAIN;
import static com.baidu.bifromq.apiserver.Headers.HEADER_TOPIC;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.baidu.bifromq.dist.client.DistResult;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import com.baidu.bifromq.type.QoS;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class HTTPPubHandlerTest extends AbstractHTTPRequestHandlerTest<HTTPPubHandler> {
    @Mock
    private IDistClient distClient;
    @Mock
    private IRetainClient retainClient;
    private ISettingProvider settingProvider = Setting::current;

    @Override
    protected Class<HTTPPubHandler> handlerClass() {
        return HTTPPubHandler.class;
    }

    @Test
    public void missingHeaders() {
        DefaultFullHttpRequest req = buildRequest();

        HTTPPubHandler handler = new HTTPPubHandler(distClient, retainClient, settingProvider);
        assertThrows(() -> handler.handle(123, "fakeTenant", req).join());
    }

    @Test
    public void pub() {

        ByteBuf content = Unpooled.wrappedBuffer("Hello BifroMQ".getBytes());
        DefaultFullHttpRequest req = buildRequest(HttpMethod.POST, content);
        req.headers().set(HEADER_TOPIC.header, "admin_user");
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_PUB_QOS.header, "1");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        HTTPPubHandler handler = new HTTPPubHandler(distClient, retainClient, settingProvider);
        handler.handle(reqId, tenantId, req);
        verify(distClient).pub(eq(reqId),
            eq(req.headers().get(HEADER_TOPIC.header)),
            argThat(m -> m.getPubQoS().equals(QoS.AT_LEAST_ONCE) &&
                m.getPayload().equals(ByteString.copyFrom(content.nioBuffer())) &&
                m.getExpiryInterval() == Integer.MAX_VALUE),
            argThat(killer -> killer.getType().equals(req.headers().get(HEADER_CLIENT_TYPE.header)) &&
                killer.getMetadataCount() == 1 &&
                killer.getMetadataMap().get("age").equals("4")));
    }

    @Test
    public void pubSucceed() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_TOPIC.header, "/greeting");
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_PUB_QOS.header, "1");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        HTTPPubHandler handler = new HTTPPubHandler(distClient, retainClient, settingProvider);

        when(distClient.pub(anyLong(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(DistResult.OK));
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.OK);
        assertEquals(response.content().readableBytes(), 0);
    }

    @Test
    public void pubWithWrongQoS() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_TOPIC.header, "/greeting");
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_PUB_QOS.header, "3");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        HTTPPubHandler handler = new HTTPPubHandler(distClient, retainClient, settingProvider);

        when(distClient.pub(anyLong(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(DistResult.OK));
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.BAD_REQUEST);
        assertEquals(response.content().readableBytes(), 0);
    }

    @Test
    public void pubWithRetain() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_TOPIC.header, "/greeting");
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_PUB_QOS.header, "2");
        req.headers().set(HEADER_RETAIN.header, "true");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        HTTPPubHandler handler = new HTTPPubHandler(distClient, retainClient, settingProvider);

        when(distClient.pub(anyLong(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(DistResult.OK));
        when(retainClient.retain(anyLong(), anyString(), any(), any(), anyInt(), any()))
            .thenReturn(CompletableFuture.completedFuture(RetainReply.getDefaultInstance()));
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.OK);
        assertEquals(response.content().readableBytes(), 0);
    }

    private DefaultFullHttpRequest buildRequest() {
        return buildRequest(HttpMethod.POST);
    }
}
