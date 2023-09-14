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

import static com.baidu.bifromq.apiserver.http.AnnotationUtil.getHTTPMethod;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.apiserver.MockableTest;
import com.baidu.bifromq.apiserver.http.handler.HTTPPubHandler;
import com.baidu.bifromq.dist.client.IDistClient;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.Collection;
import java.util.Collections;
import javax.ws.rs.Path;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class HTTPRouteMapTest extends MockableTest {
    @Mock
    private IDistClient distClient;
    @Mock
    private IHTTPRequestHandlersFactory handlersFactory;

    @Test
    public void fallbackHandler() {
        when(handlersFactory.build()).thenReturn(Collections.emptyList());
        HTTPRouteMap routeMap = new HTTPRouteMap(handlersFactory);
        FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/fake");
        IHTTPRequestHandler handler =
            routeMap.getHandler(httpRequest);
        FullHttpResponse response = handler.handle(123, "fakeTenant", httpRequest).join();
        assertEquals(response.protocolVersion(), httpRequest.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.BAD_REQUEST);
        assertEquals(response.content().readableBytes(), 0);
    }

    @Test
    public void getHandler() {
        HTTPPubHandler pubHandler = new HTTPPubHandler(distClient);
        Collection<IHTTPRequestHandler> ret = Collections.singleton(pubHandler);
        when(handlersFactory.build()).thenReturn(ret);
        HTTPRouteMap routeMap = new HTTPRouteMap(handlersFactory);
        Path route = HTTPPubHandler.class.getAnnotation(Path.class);
        FullHttpRequest httpRequest =
            new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, getHTTPMethod(HTTPPubHandler.class), route.value());
        IHTTPRequestHandler handler = routeMap.getHandler(httpRequest);
        assertEquals(handler, pubHandler);
    }
}
