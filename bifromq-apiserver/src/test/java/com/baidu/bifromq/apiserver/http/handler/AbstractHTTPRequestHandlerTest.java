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

import static org.testng.Assert.assertNotNull;

import com.baidu.bifromq.apiserver.MockableTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import javax.ws.rs.Path;
import org.testng.annotations.Test;

public abstract class AbstractHTTPRequestHandlerTest<T> extends MockableTest {
    protected abstract Class<T> handlerClass();

    @Test
    public final void annotationAttached() {
        assertNotNull(handlerClass().getAnnotation(Path.class));
    }

    protected DefaultFullHttpRequest buildRequest(HttpMethod method) {
        return buildRequest(method, Unpooled.EMPTY_BUFFER);
    }

    protected DefaultFullHttpRequest buildRequest(HttpMethod method, ByteBuf content) {
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method,
            handlerClass().getAnnotation(Path.class).value(), content);
    }
}
