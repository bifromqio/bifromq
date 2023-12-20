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

package com.baidu.bifromq.basekv.store;

import static com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus.Candidate;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.basekv.annotation.Cluster;
import com.baidu.bifromq.basekv.proto.KVRangeId;

import java.time.Duration;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

@Slf4j
public class KVRangeStoreClusterRecoveryTest extends KVRangeStoreClusterTestTemplate {

    @Cluster(initNodes = 2)
    @Test(groups = "integration")
    public void recoveryFromTwoToOne() {
        KVRangeId genesisKVRangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSetting = cluster.awaitAllKVRangeReady(genesisKVRangeId, 2, 40);
        String leader = rangeSetting.leader;
        List<String> storeIds = cluster.allStoreIds();
        assertEquals(storeIds.size(), 2);
        storeIds.remove(leader);
        cluster.shutdownStore(storeIds.get(0));
        await().ignoreExceptions().atMost(Duration.ofSeconds(30)).until(() -> {
            KVRangeConfig s = cluster.kvRangeSetting(genesisKVRangeId);
            return s != null && cluster.getKVRange(leader, genesisKVRangeId).getRole() == Candidate;
        });

        cluster.recover(leader).toCompletableFuture().join();
        await().until(() -> {
            KVRangeConfig s = cluster.kvRangeSetting(genesisKVRangeId);
            return s != null && followStores(s).isEmpty() && s.leader.equals(leader);
        });
    }

    @Cluster(initNodes = 3)
    @Test(groups = "integration")
    public void recoveryFromThreeToOne() {
        KVRangeId genesisKVRangeId = cluster.genesisKVRangeId();
        KVRangeConfig setting = cluster.awaitAllKVRangeReady(genesisKVRangeId, 2, 40);
        String leader = setting.leader;
        List<String> storeIds = cluster.allStoreIds();
        assertEquals(storeIds.size(), 3);
        storeIds.remove(leader);
        cluster.shutdownStore(storeIds.get(0));
        cluster.shutdownStore(storeIds.get(1));
        log.info("Wait for becoming candidate");
        await().ignoreExceptions().atMost(Duration.ofSeconds(30)).until(() -> {
            KVRangeConfig s = cluster.kvRangeSetting(genesisKVRangeId);
            return s != null && cluster.getKVRange(leader, genesisKVRangeId).getRole() == Candidate;
        });

        cluster.recover(leader).toCompletableFuture().join();
        await().until(() -> {
            KVRangeConfig s = cluster.kvRangeSetting(genesisKVRangeId);
            return s != null && followStores(s).isEmpty() && s.leader.equals(leader);
        });
    }
}
