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

package com.baidu.bifromq.basekv.store;

import static com.baidu.bifromq.basekv.proto.State.StateType.Merged;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.compare;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.awaitility.Awaitility.await;

import com.baidu.bifromq.basekv.annotation.Cluster;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class KVRangeStoreClusterMergeTest extends KVRangeStoreClusterTestTemplate {
    @Test(groups = "integration")
    public void mergeFromLeaderStore() {
        KVRangeId genesisKVRangeId = cluster.genesisKVRangeId();
        KVRangeConfig genesisKVRangeSettings = cluster.awaitAllKVRangeReady(genesisKVRangeId, 1, 40);
        log.info("Splitting range");
        cluster.split(genesisKVRangeSettings.leader,
                genesisKVRangeSettings.ver,
                genesisKVRangeId,
                copyFromUtf8("e"))
            .toCompletableFuture().join();
        await().atMost(Duration.ofSeconds(100)).until(() -> cluster.allKVRangeIds().size() == 2);
        KVRangeConfig range0 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig range1 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        AtomicReference<KVRangeConfig> merger;
        AtomicReference<KVRangeConfig> mergee;
        if (range0.boundary.hasEndKey() &&
            compare(range0.boundary.getEndKey(), range1.boundary.getStartKey()) <= 0) {
            merger = new AtomicReference<>(range0);
            mergee = new AtomicReference<>(range1);
        } else {
            merger = new AtomicReference<>(range1);
            mergee = new AtomicReference<>(range0);
        }
        while (!merger.get().leader.equals(mergee.get().leader)) {
            cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);
            log.info("Transfer mergee {} leader from {} to {}", KVRangeIdUtil.toString(mergee.get().id),
                mergee.get().leader,
                merger.get().leader);
            cluster.transferLeader(mergee.get().leader,
                    mergee.get().ver,
                    mergee.get().id,
                    merger.get().leader)
                .toCompletableFuture().join();
            try {
                await().atMost(Duration.ofSeconds(5)).until(() ->
                    cluster.kvRangeSetting(mergee.get().id).leader.equals(merger.get().leader));
                break;
            } catch (Throwable e) {
                log.info("Transfer failed, try again");
                merger.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(0)));
                mergee.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(1)));
            }
        }
        log.info("Merge KVRange[{}] to KVRange[{}] from leader store[{}]",
            KVRangeIdUtil.toString(mergee.get().id),
            KVRangeIdUtil.toString(merger.get().id),
            merger.get().leader);
        cluster.awaitKVRangeReady(merger.get().leader, merger.get().id);
        cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);
        cluster.merge(merger.get().leader,
                merger.get().ver,
                merger.get().id,
                mergee.get().id)
            .toCompletableFuture().join();

        KVRangeConfig setting = cluster.awaitAllKVRangeReady(merger.get().id, 3, 40);
        log.info("Merged settings {}", setting);
        await().atMost(Duration.ofSeconds(400)).until(() -> {
            for (String storeId : cluster.allStoreIds()) {
                KVRangeDescriptor mergeeDesc = cluster.getKVRange(storeId, mergee.get().id);
                if (mergeeDesc.getState() != Merged) {
                    return false;
                }
            }
            return true;
        });
        log.info("Merge done");
        // TODO: simulate graceful quit

    }

    @Test(groups = "integration")
    public void mergeUnderOnlyQuorumAvailable() {
        KVRangeId genesisKVRangeId = cluster.genesisKVRangeId();
        KVRangeConfig genesisKVRangeSettings = cluster.awaitAllKVRangeReady(genesisKVRangeId, 1, 40);
        log.info("Splitting range");
        cluster.split(genesisKVRangeSettings.leader,
                genesisKVRangeSettings.ver,
                genesisKVRangeId,
                copyFromUtf8("e"))
            .toCompletableFuture().join();
        await().atMost(Duration.ofSeconds(100)).until(() -> cluster.allKVRangeIds().size() == 2);
        KVRangeConfig range0 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig range1 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        AtomicReference<KVRangeConfig> merger;
        AtomicReference<KVRangeConfig> mergee;
        if (range0.boundary.hasEndKey() &&
            compare(range0.boundary.getEndKey(), range1.boundary.getStartKey()) <= 0) {
            merger = new AtomicReference<>(range0);
            mergee = new AtomicReference<>(range1);
        } else {
            merger = new AtomicReference<>(range1);
            mergee = new AtomicReference<>(range0);
        }
        while (!merger.get().leader.equals(mergee.get().leader)) {
            cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);
            log.info("Transfer mergee {} leader from {} to {}", KVRangeIdUtil.toString(mergee.get().id),
                mergee.get().leader,
                merger.get().leader);
            cluster.transferLeader(mergee.get().leader,
                    mergee.get().ver,
                    mergee.get().id,
                    merger.get().leader)
                .toCompletableFuture().join();
            try {
                await().atMost(Duration.ofSeconds(5)).until(() ->
                    cluster.kvRangeSetting(mergee.get().id).leader == merger.get().leader);
                break;
            } catch (Throwable e) {
                log.info("Transfer failed, try again");
                merger.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(0)));
                mergee.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(1)));
            }
        }
        cluster.awaitKVRangeReady(merger.get().leader, merger.get().id);
        cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);

        String followerStoreId = cluster.allStoreIds().stream()
            .filter(s -> !s.equals(merger.get().leader)).collect(Collectors.toList()).get(0);
        log.info("Shutdown one store {}", followerStoreId);
        cluster.shutdownStore(followerStoreId);

        log.info("Merge KVRange {} to {} from leader store {}",
            KVRangeIdUtil.toString(mergee.get().id),
            KVRangeIdUtil.toString(merger.get().id),
            merger.get().leader);

        cluster.merge(merger.get().leader, merger.get().ver, merger.get().id, mergee.get().id)
            .toCompletableFuture().join();

        KVRangeConfig mergedSettings = cluster.awaitAllKVRangeReady(merger.get().id, 3, 40);
        log.info("Merged settings {}", mergedSettings);
        await().atMost(Duration.ofSeconds(40))
            .until(() -> cluster.kvRangeSetting(merger.get().id).boundary.equals(FULL_BOUNDARY));
        log.info("Merge done");
    }

    @Cluster(installSnapshotTimeoutTick = 10)
    @Test(groups = "integration")
    public void mergeWithOneMemberIsolated() {
        KVRangeId genesisKVRangeId = cluster.genesisKVRangeId();
        KVRangeConfig genesisKVRangeSettings = cluster.awaitAllKVRangeReady(genesisKVRangeId, 1, 40);
        log.info("Splitting range");
        cluster.split(genesisKVRangeSettings.leader,
                genesisKVRangeSettings.ver,
                genesisKVRangeId,
                copyFromUtf8("e"))
            .toCompletableFuture().join();
        await().atMost(Duration.ofSeconds(100)).until(() -> cluster.allKVRangeIds().size() == 2);
        KVRangeConfig range0 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig range1 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        AtomicReference<KVRangeConfig> merger;
        AtomicReference<KVRangeConfig> mergee;
        if (range0.boundary.hasEndKey() &&
            compare(range0.boundary.getEndKey(), range1.boundary.getStartKey()) <= 0) {
            merger = new AtomicReference<>(range0);
            mergee = new AtomicReference<>(range1);
        } else {
            merger = new AtomicReference<>(range1);
            mergee = new AtomicReference<>(range0);
        }
        while (!merger.get().leader.equals(mergee.get().leader)) {
            cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);
            log.info("Transfer mergee {} leader from {} to {}",
                KVRangeIdUtil.toString(mergee.get().id),
                mergee.get().leader,
                merger.get().leader);
            cluster.transferLeader(mergee.get().leader,
                    mergee.get().ver,
                    mergee.get().id,
                    merger.get().leader)
                .toCompletableFuture().join();
            try {
                await().atMost(Duration.ofSeconds(5)).until(() ->
                    cluster.kvRangeSetting(mergee.get().id).leader == merger.get().leader);
                break;
            } catch (Throwable e) {
                log.info("Transfer failed, try again");
                merger.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(0)));
                mergee.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(1)));
            }
        }
        cluster.awaitKVRangeReady(merger.get().leader, merger.get().id);
        cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);

        String isolatedStoreId = cluster.allStoreIds().stream()
            .filter(s -> !s.equals(merger.get().leader)).collect(Collectors.toList()).get(0);
        log.info("Isolate one store {}", isolatedStoreId);
        cluster.isolate(isolatedStoreId);

        log.info("Merge KVRange {} to {} from leader store {}",
            KVRangeIdUtil.toString(mergee.get().id),
            KVRangeIdUtil.toString(merger.get().id),
            merger.get().leader);

        cluster.merge(merger.get().leader, merger.get().ver, merger.get().id, mergee.get().id)
            .toCompletableFuture().join();

        KVRangeConfig mergedSettings = cluster.awaitAllKVRangeReady(merger.get().id, 3, 40);
        await().atMost(Duration.ofSeconds(40))
            .until(() -> cluster.kvRangeSetting(merger.get().id).boundary.equals(FULL_BOUNDARY));
        log.info("Merge done {}", mergedSettings);
        log.info("Integrate {} into cluster, and wait for all mergees quited", isolatedStoreId);
        cluster.integrate(isolatedStoreId);
        await().atMost(Duration.ofSeconds(400)).until(() -> {
            for (String storeId : cluster.allStoreIds()) {
                KVRangeDescriptor mergeeDesc = cluster.getKVRange(storeId, mergee.get().id);
                if (mergeeDesc.getState() != Merged) {
                    return false;
                }
            }
            return true;
        });
    }
}
