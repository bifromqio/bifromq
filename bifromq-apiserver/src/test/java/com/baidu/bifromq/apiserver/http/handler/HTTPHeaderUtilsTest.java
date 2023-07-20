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

import static com.baidu.bifromq.apiserver.http.handler.HTTPHeaderUtils.getHeader;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.apiserver.Headers;
import com.baidu.bifromq.type.QoS;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.util.Map;
import org.testng.annotations.Test;

public class HTTPHeaderUtilsTest {
    @Test
    public void getOptionalReqId() {
        DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/fake");
        assertTrue(HTTPHeaderUtils.getOptionalReqId(req) != 0);

        req.headers().set(Headers.HEADER_REQ_ID.header, "123");
        assertEquals(HTTPHeaderUtils.getOptionalReqId(req), 123L);

        req.headers().set(Headers.HEADER_REQ_ID.header, "int_unparsable");
        assertTrue(HTTPHeaderUtils.getOptionalReqId(req) != 0);
    }

    @Test
    public void getRequiredSubQoS() {
        DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/fake");
        assertThrows(() -> HTTPHeaderUtils.getRequiredSubQoS(req));

        req.headers().set(Headers.HEADER_SUB_QOS.header, "0");
        assertEquals(HTTPHeaderUtils.getRequiredSubQoS(req), QoS.AT_MOST_ONCE);

        req.headers().set(Headers.HEADER_SUB_QOS.header, "3");
        assertThrows(() -> HTTPHeaderUtils.getRequiredSubQoS(req));
    }

    @Test
    public void getRequiredSubBrokerId() {
        DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/fake");
        assertThrows(() -> HTTPHeaderUtils.getRequiredSubBrokerId(req));

        req.headers().set(Headers.HEADER_SUBBROKER_ID.header, "0");
        assertEquals(HTTPHeaderUtils.getRequiredSubBrokerId(req), 0);

        req.headers().set(Headers.HEADER_SUBBROKER_ID.header, "int-unparsable");
        assertThrows(() -> HTTPHeaderUtils.getRequiredSubBrokerId(req));
    }

    @Test
    public void getClientMeta() {
        DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/fake");
        assertTrue(HTTPHeaderUtils.getClientMeta(req).isEmpty());

        String name_field = "name";
        String age_field = "age";
        req.headers().set(Headers.HEADER_CLIENT_META_PREFIX + name_field, "BifroMQ");
        req.headers().set(Headers.HEADER_CLIENT_META_PREFIX + age_field, "4");
        Map<String, String> clientMeta = HTTPHeaderUtils.getClientMeta(req);
        assertEquals(clientMeta.get(name_field), req.headers().get(Headers.HEADER_CLIENT_META_PREFIX + name_field));
        assertEquals(clientMeta.get(age_field), req.headers().get(Headers.HEADER_CLIENT_META_PREFIX + age_field));
    }

    @Test
    public void getHttpHeader() {
        DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/fake");
        assertNull(getHeader(Headers.HEADER_USER_ID, req, false));
        assertThrows(() -> getHeader(Headers.HEADER_USER_ID, req, true));
    }
}
