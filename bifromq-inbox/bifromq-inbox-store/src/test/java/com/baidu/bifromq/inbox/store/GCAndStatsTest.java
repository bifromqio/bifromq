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
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.store.proto.KVRangeROReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import org.testng.annotations.Test;

public class GCAndStatsTest extends InboxStoreTest {
    @Test(groups = "integration")
    public void gc() {
        String tenantId = "tenantId";
        String inboxId = "inboxId";
        String topic = "greeting";
        TopicMessagePack.PublisherPack msg0 = message(QoS.AT_MOST_ONCE, "hello");
        TopicMessagePack.PublisherPack msg1 = message(QoS.AT_LEAST_ONCE, "world");
        TopicMessagePack.PublisherPack msg2 = message(QoS.EXACTLY_ONCE, "!!!!!");
        requestCreate(tenantId, inboxId, 3, 1, true);
        SubInfo subInfo = SubInfo.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setSubQoS(QoS.AT_MOST_ONCE)
            .setTopicFilter("greeting")
            .build();
        requestInsert(subInfo, topic, msg0, msg1, msg2);
        assertTrue(exist(tenantId, inboxId));
        await().until(() -> !exist(tenantId, inboxId));
    }

    private boolean exist(String tenantId, String inboxId) {
        KVRangeSetting s = storeClient.findByKey(scopedInboxId(tenantId, inboxId)).get();
        KVRangeROReply reply = storeClient.query(s.leader, KVRangeRORequest.newBuilder()
            .setReqId(System.nanoTime())
            .setKvRangeId(s.id)
            .setVer(s.ver)
            .setExistKey(scopedInboxId(tenantId, inboxId))
            .build()).join();
        return reply.getExistResult();
    }
}
