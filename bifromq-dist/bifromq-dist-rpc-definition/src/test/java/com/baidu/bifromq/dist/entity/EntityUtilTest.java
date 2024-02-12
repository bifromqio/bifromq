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

package com.baidu.bifromq.dist.entity;

import static com.baidu.bifromq.dist.entity.EntityUtil.parseMatchRecord;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseTopicFilter;
import static com.baidu.bifromq.dist.entity.EntityUtil.toMatchRecordKey;
import static com.baidu.bifromq.dist.entity.EntityUtil.toQInboxId;
import static com.baidu.bifromq.dist.util.TopicUtil.escape;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.dist.rpc.proto.GroupMatchRecord;
import com.baidu.bifromq.type.MatchInfo;
import com.google.protobuf.ByteString;
import org.testng.annotations.Test;

public class EntityUtilTest {
    private static final int MqttBroker = 0;
    private static final int InboxService = 1;
    private static final int RuleEngine = 2;

    @Test
    public void testParseTopicFilter() {
        String scopedInboxId = toQInboxId(MqttBroker, "inbox1", "delivererKey1");
        String topicFilter = "/a/b/c";
        ByteString key = toMatchRecordKey("tenantId", topicFilter, scopedInboxId);
        assertEquals(parseTopicFilter(key.toStringUtf8()), topicFilter);
    }

    @Test
    public void testParseNormalMatchRecord() {
        String scopedInboxId = toQInboxId(MqttBroker, "inbox1", "delivererKey1");
        ByteString key = toMatchRecordKey("tenantId", "/a/b/c", scopedInboxId);
        Matching matching = parseMatchRecord(key, ByteString.EMPTY);
        assertEquals(matching.tenantId, "tenantId");
        assertEquals(matching.escapedTopicFilter, escape("/a/b/c"));
        assertEquals(matching.originalTopicFilter(), "/a/b/c");
        assertTrue(matching instanceof NormalMatching);
        assertEquals(((NormalMatching) matching).scopedInboxId, scopedInboxId);

        MatchInfo matchInfo = ((NormalMatching) matching).matchInfo;
        assertEquals(matchInfo.getTenantId(), "tenantId");
        assertEquals(matchInfo.getReceiverId(), "inbox1");
        assertEquals(matchInfo.getTopicFilter(), "/a/b/c");

        assertEquals(((NormalMatching) matching).subBrokerId, MqttBroker);
        assertEquals(((NormalMatching) matching).delivererKey, "delivererKey1");
    }

    @Test
    public void testParseGroupMatchRecord() {
        String scopedReceiverId = toQInboxId(MqttBroker, "inbox1", "server1");
        ByteString key = toMatchRecordKey("tenantId", "$share/group//a/b/c", scopedReceiverId);
        GroupMatchRecord record = GroupMatchRecord.newBuilder()
            .addQReceiverId(scopedReceiverId)
            .build();
        Matching matching = parseMatchRecord(key, record.toByteString());
        assertEquals(matching.tenantId, "tenantId");
        assertEquals(matching.escapedTopicFilter, escape("/a/b/c"));
        assertEquals(matching.originalTopicFilter(), "$share/group//a/b/c");
        assertTrue(matching instanceof GroupMatching);
        assertEquals(((GroupMatching) matching).group, "group");
        assertEquals(((GroupMatching) matching).receiverList.get(0).scopedInboxId, scopedReceiverId);

        MatchInfo matchInfo = ((GroupMatching) matching).receiverList.get(0).matchInfo;
        assertEquals(matchInfo.getReceiverId(), "inbox1");
        assertEquals(((GroupMatching) matching).receiverList.get(0).subBrokerId, MqttBroker);
        assertEquals(((GroupMatching) matching).receiverList.get(0).delivererKey, "server1");

    }
}
