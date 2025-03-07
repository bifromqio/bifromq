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

package com.baidu.bifromq.retain.utils;

import static com.baidu.bifromq.retain.utils.KeyUtil.filterPrefix;
import static com.baidu.bifromq.retain.utils.KeyUtil.isTenantNS;
import static com.baidu.bifromq.retain.utils.KeyUtil.parseTenantId;
import static com.baidu.bifromq.retain.utils.KeyUtil.parseTenantNS;
import static com.baidu.bifromq.retain.utils.KeyUtil.parseTopic;
import static com.baidu.bifromq.retain.utils.KeyUtil.retainKey;
import static com.baidu.bifromq.retain.utils.KeyUtil.retainKeyPrefix;
import static com.baidu.bifromq.retain.utils.KeyUtil.tenantNS;
import static com.baidu.bifromq.util.BSUtil.toByteString;
import static com.baidu.bifromq.util.TopicUtil.isMultiWildcardTopicFilter;
import static com.baidu.bifromq.util.TopicUtil.parse;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.util.List;
import org.testng.annotations.Test;

public class KeyUtilTest {

    private static ByteString levelByte(int value) {
        return toByteString((short) value);
    }

    @Test
    public void testRetainKeyPrefix() {
        String tenantId = "tenantA";
        ByteString tenantNS = tenantNS(tenantId);
        assertRetainKeyPrefix(tenantId, "#", tenantNS.concat(levelByte(0)));
        assertRetainKeyPrefix(tenantId, "/#", tenantNS.concat(levelByte(1)).concat(LevelHash.hash(List.of(""))));
        assertRetainKeyPrefix(tenantId, "+", tenantNS.concat(levelByte(1)));
        assertRetainKeyPrefix(tenantId, "+/#", tenantNS.concat(levelByte(1)));
        assertRetainKeyPrefix(tenantId, "a/#", tenantNS.concat(levelByte(1).concat(LevelHash.hash(List.of("a")))));
        assertRetainKeyPrefix(tenantId, "/a",
            tenantNS.concat(levelByte(2).concat(LevelHash.hash(List.of("", "a")))));
        assertRetainKeyPrefix(tenantId, "a/+",
            tenantNS.concat(levelByte(2).concat(LevelHash.hash(List.of("a")))));

        assertRetainKeyPrefix(tenantId, "a/b", tenantNS.concat(levelByte(2).concat(LevelHash.hash(List.of("a", "b")))));

        assertRetainKeyPrefix(tenantId, "/a/#",
            tenantNS.concat(levelByte(2).concat(LevelHash.hash(List.of("", "a")))));

        assertRetainKeyPrefix(tenantId, "/a/+", tenantNS.concat(
            levelByte(3).concat(LevelHash.hash(List.of("", "a")))));

        assertRetainKeyPrefix(tenantId, "/a/+/+", tenantNS.concat(
            levelByte(4).concat(LevelHash.hash(List.of("", "a")))));

        assertRetainKeyPrefix(tenantId, "/+/b/", tenantNS.concat(levelByte(4).concat(LevelHash.hash(List.of("")))));
        assertRetainKeyPrefix(tenantId, "/+/b/+/", tenantNS.concat(levelByte(5).concat(LevelHash.hash(List.of("")))));
    }

    private void assertRetainKeyPrefix(String tenantId, String topicFilter, ByteString bytes) {
        assertEquals(toRetainKeyPrefix(tenantId, topicFilter), bytes);
    }

    private ByteString toRetainKeyPrefix(String tenantId, String topicFilter) {
        List<String> filterLevels = parse(topicFilter, false);
        List<String> filterPrefix = filterPrefix(filterLevels);
        short levels =
            (short) (isMultiWildcardTopicFilter(topicFilter) ? filterLevels.size() - 1 : filterLevels.size());
        return retainKeyPrefix(tenantId, levels, filterPrefix);
    }

    @Test
    public void testParseTenantNS() {
        String tenantId = "tenantA";
        ByteString tenantNS = tenantNS(tenantId);
        assertEquals(parseTenantNS(tenantNS), tenantNS);
        assertEquals(parseTenantId(tenantNS), tenantId);

        assertEquals(parseTenantNS(retainKey(tenantId, "/a/b/c")), tenantNS);
        assertEquals(parseTenantNS(toRetainKeyPrefix(tenantId, "/a/b/c")), tenantNS);
        assertEquals(parseTenantId(toRetainKeyPrefix(tenantId, "/a/b/c")), tenantId);
    }

    @Test
    public void testIsTenantNS() {
        ByteString tenantNS = tenantNS("tenantA");
        assertTrue(isTenantNS(tenantNS));
        assertFalse(isTenantNS(retainKey("tenantA", "/a/b/c")));
        assertFalse(isTenantNS(toRetainKeyPrefix("tenantA", "/a/b/c")));
    }

    @Test
    public void testParseTopic() {
        String topic = "/a/b/c";
        assertEquals(parse(topic, false), parseTopic(retainKey("tenantA", topic)));
    }

    @Test
    public void testFilterPrefix() {
        List<String> filterLevels = parse("/a/b/+", false);
        assertEquals(filterPrefix(filterLevels), filterLevels.subList(0, 3));

        filterLevels = parse("/a/b/c", false);
        assertEquals(filterPrefix(filterLevels), filterLevels);

        filterLevels = parse("/", false);
        assertEquals(filterPrefix(filterLevels), filterLevels);

        filterLevels = parse("/#", false);
        assertEquals(filterPrefix(filterLevels), filterLevels.subList(0, filterLevels.size() - 1));

        filterLevels = parse("#", false);
        assertTrue(filterPrefix(filterLevels).isEmpty());

        filterLevels = parse("+", false);
        assertTrue(filterPrefix(filterLevels).isEmpty());

        filterLevels = parse("+/#", false);
        assertTrue(filterPrefix(filterLevels).isEmpty());
    }
}
