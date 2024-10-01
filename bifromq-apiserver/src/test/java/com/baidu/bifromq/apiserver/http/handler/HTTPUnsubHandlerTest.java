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

import static com.baidu.bifromq.apiserver.Headers.HEADER_CLIENT_ID;
import static com.baidu.bifromq.apiserver.Headers.HEADER_TOPIC_FILTER;
import static com.baidu.bifromq.apiserver.Headers.HEADER_USER_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import com.baidu.bifromq.sessiondict.rpc.proto.UnsubReply;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class HTTPUnsubHandlerTest extends AbstractHTTPRequestHandlerTest<HTTPUnsubHandler> {
    @Mock
    private ISessionDictClient sessionDictClient;

    @Override
    protected Class<HTTPUnsubHandler> handlerClass() {
        return HTTPUnsubHandler.class;
    }

    @Test
    public void missingHeaders() {
        DefaultFullHttpRequest req = buildRequest();
        HTTPUnsubHandler handler = new HTTPUnsubHandler(settingProvider, sessionDictClient);
        assertThrows(() -> handler.handle(123, "fakeTenant", req).join());
    }

    @Test
    public void unsub() {
        unsub(UnsubReply.Result.OK, HttpResponseStatus.OK);
        unsub(UnsubReply.Result.NO_SUB, HttpResponseStatus.OK);
        unsub(UnsubReply.Result.NO_SESSION, HttpResponseStatus.NOT_FOUND);
        unsub(UnsubReply.Result.NOT_AUTHORIZED, HttpResponseStatus.UNAUTHORIZED);
        unsub(UnsubReply.Result.TOPIC_FILTER_INVALID, HttpResponseStatus.BAD_REQUEST);
        unsub(UnsubReply.Result.ERROR, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

    private void unsub(UnsubReply.Result result, HttpResponseStatus expectedStatus) {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_USER_ID.header, "user");
        req.headers().set(HEADER_CLIENT_ID.header, "greeting_inbox");
        req.headers().set(HEADER_TOPIC_FILTER.header, "/greeting/#");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        HTTPUnsubHandler handler = new HTTPUnsubHandler(settingProvider, sessionDictClient);
        when(sessionDictClient.unsub(any()))
            .thenReturn(CompletableFuture.completedFuture(UnsubReply.newBuilder()
                .setResult(result)
                .build()));
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), expectedStatus);
        verify(sessionDictClient).unsub(argThat(r -> r.getReqId() == reqId
            && r.getTenantId().equals(tenantId)
            && r.getUserId().equals(req.headers().get(HEADER_USER_ID.header))
            && r.getTopicFilter().equals(req.headers().get(HEADER_TOPIC_FILTER.header))
            && r.getClientId().equals(req.headers().get(HEADER_CLIENT_ID.header))));
        Mockito.reset(sessionDictClient);
    }

    private DefaultFullHttpRequest buildRequest() {
        return buildRequest(HttpMethod.DELETE);
    }
}
