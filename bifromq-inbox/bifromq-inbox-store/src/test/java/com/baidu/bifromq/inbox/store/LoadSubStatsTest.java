/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertNotSame;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basekv.utils.BoundaryUtil;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchSubRequest;
import com.baidu.bifromq.type.ClientInfo;
import io.micrometer.core.instrument.Gauge;
import org.testng.annotations.Test;

public class LoadSubStatsTest extends InboxStoreTest {
    @Test(groups = "integration")
    public void collectAfterRestart() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        requestCreate(BatchCreateRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setClient(client)
            .setNow(now)
            .build());

        BatchSubRequest.Params subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        requestSub(subParams);
        Gauge subCountGauge = getSubCountGauge(tenantId);
        Gauge pSessionGauge = getPSessionGauge(tenantId);
        Gauge pSessionSpaceGauge = getPSessionSpaceGauge(tenantId);
        await().until(() -> subCountGauge.value() == 1);
        await().until(() -> pSessionGauge.value() == 1);
//        await().until(() -> pSessionSpaceGauge.value() > 0);

        restartStoreServer();

        await().until(() -> BoundaryUtil.isValidSplitSet(storeClient.latestEffectiveRouter().keySet()));
        Gauge newSubCountGauge = getSubCountGauge(tenantId);
        Gauge newPSessionGauge = getPSessionGauge(tenantId);
        Gauge newPSessionSpaceGauge = getPSessionSpaceGauge(tenantId);
        assertNotSame(subCountGauge, newSubCountGauge);
        assertNotSame(pSessionGauge, newPSessionGauge);
        assertNotSame(pSessionSpaceGauge, newPSessionSpaceGauge);
        await().until(() -> newSubCountGauge.value() == 1);
        await().until(() -> newPSessionGauge.value() == 1);
        await().until(() -> newPSessionSpaceGauge.value() > 0);
    }
}
