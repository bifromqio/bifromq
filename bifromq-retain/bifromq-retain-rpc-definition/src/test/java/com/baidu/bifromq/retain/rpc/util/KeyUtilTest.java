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

package com.baidu.bifromq.retain.rpc.util;

import static com.baidu.bifromq.retain.utils.KeyUtil.isTrafficNS;
import static com.baidu.bifromq.retain.utils.KeyUtil.parseTopic;
import static com.baidu.bifromq.retain.utils.KeyUtil.parseTrafficNS;
import static com.baidu.bifromq.retain.utils.KeyUtil.retainKey;
import static com.baidu.bifromq.retain.utils.KeyUtil.retainKeyPrefix;
import static com.baidu.bifromq.retain.utils.KeyUtil.trafficNS;
import static com.baidu.bifromq.retain.utils.TopicUtil.parse;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import org.testng.annotations.Test;

public class KeyUtilTest {
    @Test
    public void testParseTrafficNS() {
        ByteString trafficNS = trafficNS("trafficId");
        assertEquals(trafficNS, parseTrafficNS(trafficNS));

        assertEquals(trafficNS, parseTrafficNS(retainKey(trafficNS, "/a/b/c")));
        assertEquals(trafficNS, parseTrafficNS(retainKeyPrefix(trafficNS, parse("/a/b/c", false))));
    }

    @Test
    public void testIsTrafficNS() {
        ByteString trafficNS = trafficNS("trafficId");
        assertTrue(isTrafficNS(trafficNS));
        assertFalse(isTrafficNS(retainKey(trafficNS, "/a/b/c")));
        assertFalse(isTrafficNS(retainKeyPrefix(trafficNS, parse("/a/b/c", false))));
    }

    @Test
    public void testParseTopic() {
        ByteString trafficNS = trafficNS("trafficId");
        String topic = "/a/b/c";
        assertEquals(parseTopic(retainKey(trafficNS, topic)), parse(topic, false));
    }
}
