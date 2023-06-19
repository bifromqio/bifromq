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

package com.baidu.bifromq.dist.entity;

import static com.baidu.bifromq.dist.entity.EntityUtil.matchRecordKey;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseMatchRecord;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseTopicFilter;
import static com.baidu.bifromq.dist.entity.EntityUtil.toQualifiedInboxId;
import static com.baidu.bifromq.dist.util.TopicUtil.escape;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import com.baidu.bifromq.dist.rpc.proto.GroupMatchRecord;
import com.baidu.bifromq.dist.rpc.proto.MatchRecord;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.google.protobuf.ByteString;
import org.testng.annotations.Test;

public class EntityUtilTest {
    private static final int MqttBroker = 0;
    private static final int InboxService = 1;
    private static final int RuleEngine = 2;

    @Test
    public void testParseTopicFilter() {
        String scopedInboxId = toQualifiedInboxId(MqttBroker, "inbox1", "inboxGroupKey1");
        String topicFilter = "/a/b/c";
        ByteString key = matchRecordKey("trafficId", topicFilter, scopedInboxId);
        assertEquals(topicFilter, parseTopicFilter(key.toStringUtf8()));
    }

    @Test
    public void testParseNormalMatchRecord() {
        String scopedInboxId = toQualifiedInboxId(MqttBroker, "inbox1", "inboxGroupKey1");
        ByteString key = matchRecordKey("trafficId", "/a/b/c", scopedInboxId);
        MatchRecord normal = MatchRecord.newBuilder()
            .setNormal(QoS.AT_MOST_ONCE).build();
        Matching matching = parseMatchRecord(key, normal.toByteString());
        assertEquals("trafficId", matching.trafficId);
        assertEquals(escape("/a/b/c"), matching.escapedTopicFilter);
        assertEquals("/a/b/c", matching.originalTopicFilter());
        assertTrue(matching instanceof NormalMatching);
        assertEquals(scopedInboxId, ((NormalMatching) matching).scopedInboxId);

        SubInfo subInfo = ((NormalMatching) matching).subInfo;
        assertEquals("trafficId", subInfo.getTrafficId());
        assertEquals(normal.getNormal(), subInfo.getSubQoS());
        assertEquals("inbox1", subInfo.getInboxId());
        assertEquals("/a/b/c", subInfo.getTopicFilter());

        assertEquals(MqttBroker, ((NormalMatching) matching).brokerId);
        assertEquals("inboxGroupKey1", ((NormalMatching) matching).inboxGroupKey);
    }

    @Test
    public void testParseGroupMatchRecord() {
        String scopedInboxId = toQualifiedInboxId(MqttBroker, "inbox1", "server1");
        ByteString key = matchRecordKey("trafficId", "$share/group//a/b/c", scopedInboxId);
        MatchRecord record = MatchRecord.newBuilder()
            .setGroup(GroupMatchRecord.newBuilder()
                .putEntry(scopedInboxId, QoS.AT_MOST_ONCE)
                .build())
            .build();
        Matching matching = parseMatchRecord(key, record.toByteString());
        assertEquals("trafficId", matching.trafficId);
        assertEquals(escape("/a/b/c"), matching.escapedTopicFilter);
        assertEquals("$share/group//a/b/c", matching.originalTopicFilter());
        assertTrue(matching instanceof GroupMatching);
        assertEquals("group", ((GroupMatching) matching).group);
        assertEquals(scopedInboxId, ((GroupMatching) matching).inboxList.get(0).scopedInboxId);

        SubInfo subInfo = ((GroupMatching) matching).inboxList.get(0).subInfo;
        assertEquals(record.getGroup().getEntryMap().get(scopedInboxId), subInfo.getSubQoS());
        assertEquals("inbox1", subInfo.getInboxId());
        assertEquals(MqttBroker, ((GroupMatching) matching).inboxList.get(0).brokerId);
        assertEquals("server1", ((GroupMatching) matching).inboxList.get(0).inboxGroupKey);

    }
}
