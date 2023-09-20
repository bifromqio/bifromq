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

package com.baidu.bifromq.apiserver.http;

import static com.baidu.bifromq.apiserver.Headers.HEADER_REQ_ID;
import static com.baidu.bifromq.apiserver.Headers.HEADER_TENANT_ID;
import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.apiserver.MockableTest;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class HTTPRequestRouterTest extends MockableTest {
    private EmbeddedChannel channel;
    @Mock
    private IHTTPRouteMap routeMap;
    @Mock
    private IHTTPRequestHandler requestHandler;
    @Mock
    private ISettingProvider settingProvider;

    @BeforeMethod
    public void setup() {
        super.setup();
        channel = new EmbeddedChannel(true, true, new ChannelInitializer<EmbeddedChannel>() {
            @Override
            protected void initChannel(EmbeddedChannel ch) {
                ch.pipeline().addLast(new HTTPRequestRouter(routeMap, settingProvider));
            }
        });
        channel.freezeTime();
    }

    @Test
    public void badRequest() {
        DefaultHttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/fake");
        channel.writeInbound(req);
        HttpResponse resp = channel.readOutbound();
        assertEquals(resp.protocolVersion(), req.protocolVersion());
        assertEquals(resp.status(), HttpResponseStatus.BAD_REQUEST);
        assertEquals(resp.headers().get(CONTENT_TYPE), "text/plain; charset=UTF-8");
        assertEquals(resp.headers().getInt(CONTENT_LENGTH), 0);
    }

    @Test
    public void keepAliveHeaderForHTTP_1_0() {
        DefaultHttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, "/fake");
        req.headers().set(CONNECTION, KEEP_ALIVE);
        channel.writeInbound(req);
        HttpResponse resp = channel.readOutbound();
        assertEquals(resp.protocolVersion(), req.protocolVersion());
        assertEquals(resp.headers().get(CONNECTION), KEEP_ALIVE.toString());
    }

    @Test
    public void closeConnectionForHTTP_1_0() {
        DefaultHttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, "/fake");
        channel.writeInbound(req);
        HttpResponse resp = channel.readOutbound();
        assertEquals(resp.protocolVersion(), req.protocolVersion());
        assertEquals(resp.headers().get(CONNECTION), CLOSE.toString());
        assertFalse(channel.isOpen());
    }

    @Test
    public void closeConnectionByRequest() {
        DefaultHttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/fake");
        req.headers().set(CONNECTION, CLOSE);
        channel.writeInbound(req);
        HttpResponse resp = channel.readOutbound();
        assertEquals(resp.protocolVersion(), req.protocolVersion());
        assertEquals(resp.headers().get(CONNECTION), CLOSE.toString());
        assertFalse(channel.isOpen());
    }

    @Test
    public void setupRouterForTenant() {
        String tenantId = "bifromq-dev";
        DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/fake");
        req.headers().set(HEADER_TENANT_ID.header, tenantId);
        req.headers().set(CONTENT_LENGTH, 0);
        when(settingProvider.provide(Setting.MaxUserPayloadBytes, tenantId)).thenReturn(
            Setting.MaxUserPayloadBytes.current(tenantId));
        when(routeMap.getHandler(any())).thenReturn(requestHandler);
        when(requestHandler.handle(anyLong(), anyString(), any())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Mocked Exception")));
        channel.writeInbound(req);
        HttpResponse resp = channel.readOutbound();
        assertEquals(resp.protocolVersion(), req.protocolVersion());
        assertTrue(resp.headers().contains(HEADER_REQ_ID.header));
        assertEquals(req.refCnt(), 0);
    }

    @Test
    public void reuseExistingRouter() {
        String tenantId = "bifromq-dev";
        DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/fake");
        req.headers().set(HEADER_TENANT_ID.header, tenantId);
        req.headers().set(CONTENT_LENGTH, 0);
        when(settingProvider.provide(Setting.MaxUserPayloadBytes, tenantId)).thenReturn(
            Setting.MaxUserPayloadBytes.current(tenantId));
        when(routeMap.getHandler(any())).thenReturn(requestHandler);
        when(requestHandler.handle(anyLong(), anyString(), any())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Mocked Exception")));

        channel.writeInbound(req);

        req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/fake");
        req.headers().set(HEADER_TENANT_ID.header, tenantId);
        req.headers().set(CONTENT_LENGTH, 0);
        channel.writeInbound(req);
        verify(settingProvider, times(1)).provide(Setting.MaxUserPayloadBytes, tenantId);
    }

    @Test
    public void routerSwitch() {
        String tenantA = "bifromq-dev";
        String tenantB = "bifromq-ops";
        DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/fake");
        req.headers().set(HEADER_TENANT_ID.header, tenantA);
        req.headers().set(CONTENT_LENGTH, 0);
        when(settingProvider.provide(Setting.MaxUserPayloadBytes, tenantA)).thenReturn(
            Setting.MaxUserPayloadBytes.current(tenantA));
        when(settingProvider.provide(Setting.MaxUserPayloadBytes, tenantB)).thenReturn(
            Setting.MaxUserPayloadBytes.current(tenantB));
        when(routeMap.getHandler(any())).thenReturn(requestHandler);
        when(requestHandler.handle(anyLong(), anyString(), any())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Mocked Exception")));

        channel.writeInbound(req);

        req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/fake");
        req.headers().set(HEADER_TENANT_ID.header, tenantB);
        req.headers().set(CONTENT_LENGTH, 0);
        channel.writeInbound(req);
        verify(settingProvider, times(2)).provide(any(), anyString());
    }
}
