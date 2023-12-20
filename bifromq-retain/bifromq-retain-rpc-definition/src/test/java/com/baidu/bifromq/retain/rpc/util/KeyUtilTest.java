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

package com.baidu.bifromq.retain.rpc.util;

import static com.baidu.bifromq.retain.utils.KeyUtil.isTenantNS;
import static com.baidu.bifromq.retain.utils.KeyUtil.parseTopic;
import static com.baidu.bifromq.retain.utils.KeyUtil.parseTenantNS;
import static com.baidu.bifromq.retain.utils.KeyUtil.retainKey;
import static com.baidu.bifromq.retain.utils.KeyUtil.retainKeyPrefix;
import static com.baidu.bifromq.retain.utils.KeyUtil.tenantNS;
import static com.baidu.bifromq.retain.utils.TopicUtil.parse;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import org.testng.annotations.Test;

public class KeyUtilTest {
    @Test
    public void testParseTenantNS() {
        ByteString tenantNS = tenantNS("tenantA");
        assertEquals(parseTenantNS(tenantNS), tenantNS);

        assertEquals(parseTenantNS(retainKey(tenantNS, "/a/b/c")), tenantNS);
        assertEquals(parseTenantNS(retainKeyPrefix(tenantNS, parse("/a/b/c", false))), tenantNS);
    }

    @Test
    public void testIsTenantNS() {
        ByteString tenantNS = tenantNS("tenantA");
        assertTrue(isTenantNS(tenantNS));
        assertFalse(isTenantNS(retainKey(tenantNS, "/a/b/c")));
        assertFalse(isTenantNS(retainKeyPrefix(tenantNS, parse("/a/b/c", false))));
    }

    @Test
    public void testParseTopic() {
        ByteString tenantNS = tenantNS("tenantA");
        String topic = "/a/b/c";
        assertEquals(parse(topic, false), parseTopic(retainKey(tenantNS, topic)));
    }
}
