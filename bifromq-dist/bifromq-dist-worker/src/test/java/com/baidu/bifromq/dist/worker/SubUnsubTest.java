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

package com.baidu.bifromq.dist.worker;

import static com.baidu.bifromq.dist.entity.EntityUtil.matchRecordKey;
import static com.baidu.bifromq.dist.entity.EntityUtil.subInfoKey;
import static com.baidu.bifromq.dist.entity.EntityUtil.toQualifiedInboxId;
import static com.baidu.bifromq.type.QoS.AT_LEAST_ONCE;
import static com.baidu.bifromq.type.QoS.AT_MOST_ONCE;
import static com.baidu.bifromq.type.QoS.EXACTLY_ONCE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.dist.rpc.proto.AddTopicFilterReply;
import com.baidu.bifromq.dist.rpc.proto.ClearSubInfoReply;
import com.baidu.bifromq.dist.rpc.proto.InsertMatchRecordReply;
import com.baidu.bifromq.dist.rpc.proto.JoinMatchGroupReply;
import com.baidu.bifromq.dist.rpc.proto.RemoveTopicFilterReply;
import com.baidu.bifromq.type.QoS;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class SubUnsubTest extends DistWorkerTest {
    @Test(groups = "integration")
    public void testAddTopicFilter() {
        String subInfoKeyUtf8 = subInfoKey(tenantA,
            toQualifiedInboxId(MqttBroker, "inbox1", "server1")).toStringUtf8();
        AddTopicFilterReply reply = addTopicFilter(tenantA, "/a/b/c", AT_MOST_ONCE, MqttBroker, "inbox1",
            "server1");
        assertEquals(reply.getResultMap().get(subInfoKeyUtf8).getResultsMap().get("/a/b/c"),
            AddTopicFilterReply.Result.OK);

        reply = addTopicFilter(tenantA, "/a/b/c", AT_LEAST_ONCE, MqttBroker, "inbox1", "server1");
        assertEquals(reply.getResultMap().get(subInfoKeyUtf8).getResultsMap().get("/a/b/c"),
            AddTopicFilterReply.Result.OK);

        reply = addTopicFilter(tenantA, "/a/b/c", AT_LEAST_ONCE, MqttBroker, "inbox1", "server1");
        assertEquals(reply.getResultMap().get(subInfoKeyUtf8).getResultsMap().get("/a/b/c"),
            AddTopicFilterReply.Result.OK);

        reply = addTopicFilter(tenantA, "/a/b/c", EXACTLY_ONCE, MqttBroker, "inbox1", "server1");
        assertEquals(reply.getResultMap().get(subInfoKeyUtf8).getResultsMap().get("/a/b/c"),
            AddTopicFilterReply.Result.OK);

        removeTopicFilter(tenantA, "/a/b/c", MqttBroker, "inbox1", "server1");
    }

    @Test(groups = "integration")
    public void testRemoveTopicFilter() {
        String subInfoKeyUtf8_1 = subInfoKey(tenantA,
            toQualifiedInboxId(MqttBroker, "inbox1", "server1")).toStringUtf8();
        String subInfoKeyUtf8_2 = subInfoKey(tenantA,
            toQualifiedInboxId(MqttBroker, "inbox1", "server2")).toStringUtf8();
        String topicFilter = "/a/b/c";
        RemoveTopicFilterReply reply = removeTopicFilter(tenantA, topicFilter, MqttBroker, "inbox1",
            "server2");
        assertEquals(reply.getResultMap().get(subInfoKeyUtf8_2).getResultMap().get(topicFilter), false);

        addTopicFilter(tenantA, topicFilter, AT_MOST_ONCE, MqttBroker, "inbox1", "server1");

        reply = removeTopicFilter(tenantA, topicFilter, MqttBroker, "inbox1", "server2");
        assertEquals(reply.getResultMap().get(subInfoKeyUtf8_2).getResultMap().get(topicFilter), false);

        reply = removeTopicFilter(tenantA, topicFilter, MqttBroker, "inbox1", "server1");
        assertEquals(reply.getResultMap().get(subInfoKeyUtf8_1).getResultMap().get(topicFilter), true);

        reply = removeTopicFilter(tenantA, topicFilter, MqttBroker, "inbox1", "server1");
        assertEquals(reply.getResultMap().get(subInfoKeyUtf8_1).getResultMap().get(topicFilter), false);

        addTopicFilter(tenantA, topicFilter, AT_LEAST_ONCE, MqttBroker, "inbox1", "server1");

        reply = removeTopicFilter(tenantA, topicFilter, MqttBroker, "inbox1", "server2");
        assertEquals(reply.getResultMap().get(subInfoKeyUtf8_2).getResultMap().get(topicFilter), false);

        reply = removeTopicFilter(tenantA, topicFilter, MqttBroker, "inbox1", "server1");
        assertEquals(reply.getResultMap().get(subInfoKeyUtf8_1).getResultMap().get(topicFilter), true);

        reply = removeTopicFilter(tenantA, topicFilter, MqttBroker, "inbox1", "server1");
        assertEquals(reply.getResultMap().get(subInfoKeyUtf8_1).getResultMap().get(topicFilter), false);

        addTopicFilter(tenantA, topicFilter, EXACTLY_ONCE, MqttBroker, "inbox1", "server1");

        reply = removeTopicFilter(tenantA, topicFilter, MqttBroker, "inbox1", "server2");
        assertEquals(reply.getResultMap().get(subInfoKeyUtf8_2).getResultMap().get(topicFilter), false);

        reply = removeTopicFilter(tenantA, topicFilter, MqttBroker, "inbox1", "server1");
        assertEquals(reply.getResultMap().get(subInfoKeyUtf8_1).getResultMap().get(topicFilter), true);

        reply = removeTopicFilter(tenantA, topicFilter, MqttBroker, "inbox1", "server1");
        assertEquals(reply.getResultMap().get(subInfoKeyUtf8_1).getResultMap().get(topicFilter), false);
    }

    @Test(groups = "integration")
    public void testInsertMatchRecord() {
        InsertMatchRecordReply reply = insertMatchRecord("tenantA", "/a/b/c", AT_MOST_ONCE, MqttBroker,
            "inbox1", "server1");

        reply = insertMatchRecord("tenantA", "/a/b/c", QoS.AT_LEAST_ONCE, MqttBroker, "inbox1", "server1");

        reply = insertMatchRecord("tenantA", "/a/b/c", EXACTLY_ONCE, MqttBroker, "inbox1", "server1");
    }

    @Test(groups = "integration")
    public void testJoinMatchGroup() {
        String topicFilter = "$share/group/a/b/c";

        String qInboxId = toQualifiedInboxId(MqttBroker, "inbox1", "server1");
        String matchRecordKeyUtf8 = matchRecordKey(tenantA, topicFilter, qInboxId).toStringUtf8();

        JoinMatchGroupReply reply = joinMatchGroup(tenantA, topicFilter, AT_MOST_ONCE, MqttBroker, "inbox1",
            "server1");
        assertEquals(reply.getResultMap().get(matchRecordKeyUtf8).getResultMap().get(qInboxId),
            JoinMatchGroupReply.Result.OK);

        qInboxId = toQualifiedInboxId(MqttBroker, "inbox1", "server2");
        matchRecordKeyUtf8 = matchRecordKey(tenantA, topicFilter, qInboxId).toStringUtf8();
        reply = joinMatchGroup(tenantA, topicFilter, AT_LEAST_ONCE, MqttBroker, "inbox1",
            "server2");
        assertEquals(reply.getResultMap().get(matchRecordKeyUtf8).getResultMap().get(qInboxId),
            JoinMatchGroupReply.Result.OK);

        qInboxId = toQualifiedInboxId(MqttBroker, "inbox1", "server3");
        matchRecordKeyUtf8 = matchRecordKey(tenantA, topicFilter, qInboxId).toStringUtf8();
        reply = joinMatchGroup(tenantA, topicFilter, EXACTLY_ONCE, MqttBroker, "inbox1",
            "server3");
        assertEquals(reply.getResultMap().get(matchRecordKeyUtf8).getResultMap().get(qInboxId),
            JoinMatchGroupReply.Result.OK);
    }

    @Test(groups = "integration")
    public void testDeleteNormalMatchRecord() {
        deleteMatchRecord(tenantB, "/a/b/c", MqttBroker, "inbox1", "server1");
        insertMatchRecord(tenantA, "/a/b/c", AT_MOST_ONCE, MqttBroker, "inbox1", "server1");
        deleteMatchRecord(tenantB, "/a/b/c", MqttBroker, "inbox1", "server1");

        insertMatchRecord(tenantA, "/a/b/c", AT_LEAST_ONCE, MqttBroker, "inbox1", "server2");
        deleteMatchRecord(tenantB, "/a/b/c", MqttBroker, "inbox1", "server2");

        insertMatchRecord(tenantA, "/a/b/c", EXACTLY_ONCE, MqttBroker, "inbox1", "server3");
        deleteMatchRecord(tenantB, "/a/b/c", MqttBroker, "inbox1", "server3");
    }

    @Test(groups = "integration")
    public void testLeaveMatchGroup() {
        leaveMatchGroup(tenantB, "$share/group/a/b/c", MqttBroker, "inbox1", "server1");
        joinMatchGroup(tenantA, "$share/group/a/b/c", AT_MOST_ONCE, MqttBroker, "inbox1", "server1");
        leaveMatchGroup(tenantB, "$share/group/a/b/c", MqttBroker, "inbox1", "server1");

        joinMatchGroup(tenantA, "$share/group/a/b/c", AT_LEAST_ONCE, MqttBroker, "inbox1", "server2");
        leaveMatchGroup(tenantB, "$share/group/a/b/c", MqttBroker, "inbox1", "server2");

        joinMatchGroup(tenantA, "$share/group/a/b/c", EXACTLY_ONCE, MqttBroker, "inbox1", "server3");
        leaveMatchGroup(tenantB, "$share/group/a/b/c", MqttBroker, "inbox1", "server3");
    }

    @Test(groups = "integration")
    public void testClearSubInfo() {
        ClearSubInfoReply reply = clearSubInfo(tenantA, MqttBroker, "inbox1", "server1");
        assertTrue(reply.getSubInfo(0).getTopicFiltersMap().isEmpty());

        addTopicFilter(tenantA, "/a/b/c", AT_MOST_ONCE, MqttBroker, "inbox1", "server1");
        addTopicFilter(tenantA, "/a/b/c", AT_LEAST_ONCE, MqttBroker, "inbox1", "server1");
        addTopicFilter(tenantA, "$share/group/a/b/c", EXACTLY_ONCE, MqttBroker, "inbox1", "server1");

        reply = clearSubInfo(tenantA, MqttBroker, "inbox1", "server1");
        assertEquals(reply.getSubInfo(0).getTopicFiltersMap(),
            Map.of("/a/b/c", AT_LEAST_ONCE, "$share/group/a/b/c", EXACTLY_ONCE));

        reply = clearSubInfo(tenantA, MqttBroker, "inbox1", "server1");
        assertTrue(reply.getSubInfo(0).getTopicFiltersMap().isEmpty());
    }
}
