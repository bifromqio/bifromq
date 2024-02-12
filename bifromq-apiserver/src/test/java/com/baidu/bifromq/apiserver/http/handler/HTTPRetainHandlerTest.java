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
import static com.baidu.bifromq.apiserver.Headers.HEADER_QOS;
import static com.baidu.bifromq.apiserver.Headers.HEADER_RETAIN;
import static com.baidu.bifromq.apiserver.Headers.HEADER_TOPIC;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class HTTPRetainHandlerTest extends AbstractHTTPRequestHandlerTest<HTTPRetainHandler> {
    @Mock
    private IRetainClient retainClient;
    private ISettingProvider settingProvider = Setting::current;

    @Override
    protected Class<HTTPRetainHandler> handlerClass() {
        return HTTPRetainHandler.class;
    }

    @Test
    public void missingHeaders() {
        DefaultFullHttpRequest req = buildRequest();

        HTTPRetainHandler handler = new HTTPRetainHandler(retainClient, settingProvider);
        assertThrows(() -> handler.handle(123, "fakeTenant", req).join());
    }

    @Test
    public void retainResultHandling() {
        retain(RetainReply.Result.RETAINED, HttpResponseStatus.OK);
        retain(RetainReply.Result.CLEARED, HttpResponseStatus.OK);
        retain(RetainReply.Result.ERROR, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

    private void retain(RetainReply.Result result, HttpResponseStatus expectedStatus) {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_TOPIC.header, "/greeting");
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_QOS.header, "2");
        req.headers().set(HEADER_RETAIN.header, "true");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        HTTPRetainHandler handler = new HTTPRetainHandler(retainClient, settingProvider);

        when(retainClient.retain(anyLong(), anyString(), any(), any(), anyInt(), any()))
            .thenReturn(CompletableFuture.completedFuture(RetainReply.newBuilder()
                .setResult(result).build()));
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), expectedStatus);
    }

    private DefaultFullHttpRequest buildRequest() {
        return buildRequest(HttpMethod.POST);
    }
}
