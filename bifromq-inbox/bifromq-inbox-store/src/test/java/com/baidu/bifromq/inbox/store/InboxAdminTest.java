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

package com.baidu.bifromq.inbox.store;

import static com.baidu.bifromq.inbox.util.KeyUtil.scopedInboxId;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.store.proto.KVRangeROReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.inbox.storage.proto.HasReply;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;

import java.io.IOException;
import java.time.Clock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.mockito.Mock;

public class InboxAdminTest extends InboxStoreTest {
    @Mock
    private Clock clock;

    @BeforeMethod(groups = "integration")
    public void setup() throws IOException {
        super.setup();
        when(clock.millis()).thenReturn(0L);
    }

    @Override
    protected Clock getClock() {
        return clock;
    }

    @Test(groups = "integration")
    public void createAndHasCheck() {
        String trafficId = "trafficId";
        String inboxId = "inboxId";
        HasReply has = requestHas(trafficId, inboxId);
        assertFalse(has.getExistsMap().get(scopedInboxId(trafficId, inboxId).toStringUtf8()));
        requestCreate(trafficId, inboxId, 10, 100, false);
        has = requestHas(trafficId, inboxId);
        assertTrue(has.getExistsMap().get(scopedInboxId(trafficId, inboxId).toStringUtf8()));
    }

    @Test(groups = "integration")
    public void expireAndHasCheck() {
        String trafficId = "trafficId";
        String inboxId = "inboxId";
        HasReply has = requestHas(trafficId, inboxId);
        assertFalse(has.getExistsMap().get(scopedInboxId(trafficId, inboxId).toStringUtf8()));
        requestCreate(trafficId, inboxId, 10, 2, false);
        has = requestHas(trafficId, inboxId);
        assertTrue(has.getExistsMap().get(scopedInboxId(trafficId, inboxId).toStringUtf8()));
        when(clock.millis()).thenReturn(2100L);
        has = requestHas(trafficId, inboxId);
        assertFalse(has.getExistsMap().get(scopedInboxId(trafficId, inboxId).toStringUtf8()));
    }

    @Test(groups = "integration")
    public void createAndDelete() {
        String trafficId = "trafficId";
        String inboxId1 = "inboxId1";
        String inboxId2 = "inboxId2";
        requestCreate(trafficId, inboxId1, 10, 10, false);
        requestCreate(trafficId, inboxId2, 10, 10, false);
        assertTrue(requestHas(trafficId, inboxId1).getExistsMap()
            .get(scopedInboxId(trafficId, inboxId1).toStringUtf8()));
        requestDelete(trafficId, inboxId1);

        assertFalse(requestHas(trafficId, inboxId1).getExistsMap()
            .get(scopedInboxId(trafficId, inboxId1).toStringUtf8()));
        assertTrue(requestHas(trafficId, inboxId2).getExistsMap()
            .get(scopedInboxId(trafficId, inboxId2).toStringUtf8()));
    }

    @Test(groups = "integration")
    public void deleteNonExist() {
        String trafficId = "trafficId";
        String inboxId = "inboxId";
        requestDelete(trafficId, inboxId);
        HasReply has = requestHas(trafficId, inboxId);
        assertFalse(has.getExistsMap().get(scopedInboxId(trafficId, inboxId).toStringUtf8()));
    }

    @Test(groups = "integration")
    public void testGC() {
        String trafficId = "trafficId";
        String inboxId = "inboxId";
        String topic = "greeting";
        TopicMessagePack.SenderMessagePack msg0 = message(QoS.AT_MOST_ONCE, "hello");
        TopicMessagePack.SenderMessagePack msg1 = message(QoS.AT_LEAST_ONCE, "world");
        TopicMessagePack.SenderMessagePack msg2 = message(QoS.EXACTLY_ONCE, "!!!!!");
        requestCreate(trafficId, inboxId, 3, 1, true);
        SubInfo subInfo = SubInfo.newBuilder()
            .setTrafficId(trafficId)
            .setInboxId(inboxId)
            .setSubQoS(QoS.AT_MOST_ONCE)
            .setTopicFilter("greeting")
            .build();
        requestInsert(subInfo, topic, msg0, msg1, msg2);
        // advance to gc'able
        when(clock.millis()).thenReturn(1100L);
        KVRangeSetting s = storeClient.findByKey(scopedInboxId(trafficId, inboxId)).get();
        testStore.gcRange(s).join();

        KVRangeROReply reply = storeClient.query(s.leader, KVRangeRORequest.newBuilder()
            .setReqId(System.nanoTime())
            .setKvRangeId(s.id)
            .setVer(s.ver)
            .setExistKey(scopedInboxId(trafficId, inboxId))
            .build()).join();
        assertFalse(reply.getExistResult());
    }
}
