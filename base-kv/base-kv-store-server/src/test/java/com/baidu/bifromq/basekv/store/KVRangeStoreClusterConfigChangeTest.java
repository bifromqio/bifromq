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

import static java.util.Collections.emptySet;
import static org.awaitility.Awaitility.await;

import com.baidu.bifromq.basekv.annotation.Cluster;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

@Slf4j
public class KVRangeStoreClusterConfigChangeTest extends KVRangeStoreClusterTestTemplate {
    @Test(groups = "integration")
    public void removeNonLeaderReplicaFromNonLeaderStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig setting = await().until(() -> cluster.kvRangeSetting(rangeId), obj ->
            obj != null && obj.clusterConfig.getVotersCount() == 3);
        log.info("Start to change config");
        await().ignoreExceptions().until(() -> {
            KVRangeConfig newSetting = cluster.kvRangeSetting(rangeId);
            String remainStore = nonLeaderStore(setting);
            if (newSetting.clusterConfig.getVotersCount() == 2) {
                return true;
            }
            try {
                cluster.changeReplicaConfig(remainStore, newSetting.ver, rangeId, followStores(setting), emptySet())
                    .toCompletableFuture().join();
                newSetting = cluster.kvRangeSetting(rangeId);
                return newSetting.clusterConfig.getVotersCount() == 2;
            } catch (Throwable e) {
                log.info("Change config failed", e);
                return false;
            }
        });
    }

    @Test(groups = "integration")
    public void removeNonLeaderReplicaFromHostingStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig setting = cluster.awaitAllKVRangeReady(rangeId, 1, 5000);
        String leaderStore = setting.leader;
        String remainStore = nonLeaderStore(setting);
        String removedStore = followStores(setting)
            .stream()
            .filter(storeId -> !storeId.equals(remainStore))
            .collect(Collectors.joining());

        log.info("Remove replica[{}]", removedStore);
        await().ignoreExceptions().until(() -> {
            KVRangeConfig newSetting = cluster.kvRangeSetting(rangeId);
            if (newSetting.clusterConfig.getVotersCount() == 2 &&
                !newSetting.clusterConfig.getVotersList().contains(removedStore)) {
                return true;
            }
            cluster.changeReplicaConfig(remainStore, newSetting.ver, rangeId, Sets.newHashSet(leaderStore, remainStore),
                emptySet()).toCompletableFuture().join();
            newSetting = cluster.kvRangeSetting(rangeId);
            return
                newSetting.clusterConfig.getVotersCount() == 2 &&
                    !newSetting.clusterConfig.getVotersList().contains(removedStore);
        });
    }

    @Test(groups = "integration")
    public void removeLeaderReplicaFromLeaderStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSettings = await().until(() -> cluster.kvRangeSetting(rangeId), Objects::nonNull);
        String leaderStore = rangeSettings.leader;
        Set<String> remainStores = followStores(rangeSettings);
        log.info("Remove: {}, remain: {}", leaderStore, remainStores);

        await().ignoreExceptions().until(() -> {
            KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
            if (remainStores.containsAll(setting.clusterConfig.getVotersList())) {
                return true;
            }
            cluster.changeReplicaConfig(leaderStore, setting.ver, rangeId, Sets.newHashSet(remainStores), emptySet())
                .toCompletableFuture().join();
            setting = cluster.kvRangeSetting(rangeId);
            return remainStores.containsAll(setting.clusterConfig.getVotersList());
        });
    }

    @Test(groups = "integration")
    public void removeFailedReplica() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSettings = cluster.kvRangeSetting(rangeId);
        String leaderStore = rangeSettings.leader;
        String failureStore = nonLeaderStore(rangeSettings);
        log.info("shutdown store {}", failureStore);
        cluster.shutdownStore(failureStore);

        List<String> remainStores = Lists.newArrayList(rangeSettings.clusterConfig.getVotersList());
        remainStores.remove(failureStore);
        log.info("Remain: {}", remainStores);
        await().ignoreExceptions().until(() -> {
            KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
            if (remainStores.containsAll(setting.clusterConfig.getVotersList())) {
                return true;
            }
            cluster.changeReplicaConfig(leaderStore, setting.ver, rangeId, Sets.newHashSet(remainStores), emptySet())
                .toCompletableFuture().join();
            setting = cluster.kvRangeSetting(rangeId);
            return remainStores.containsAll(setting.clusterConfig.getVotersList());
        });
    }

    @Test(groups = "integration")
    public void removeNonLeaderReplicaFromLeaderStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSettings = cluster.awaitAllKVRangeReady(rangeId, 1, 5000);
        String leaderStore = rangeSettings.leader;
        String remainStore = nonLeaderStore(rangeSettings);

        await().ignoreExceptions().until(() -> {
            KVRangeConfig newSetting = cluster.kvRangeSetting(rangeId);
            if (newSetting.clusterConfig.getVotersCount() == 2 &&
                newSetting.clusterConfig.getVotersList().contains(leaderStore) &&
                newSetting.clusterConfig.getVotersList().contains(remainStore)) {
                return true;
            }
            cluster.changeReplicaConfig(remainStore, newSetting.ver, rangeId, Sets.newHashSet(leaderStore, remainStore),
                emptySet()).toCompletableFuture().join();
            newSetting = cluster.kvRangeSetting(rangeId);
            return newSetting.clusterConfig.getVotersCount() == 2 &&
                newSetting.clusterConfig.getVotersList().contains(leaderStore) &&
                newSetting.clusterConfig.getVotersList().contains(remainStore);
        });
    }

    @Test(groups = "integration")
    public void removeLeaderReplicaFromNonLeaderStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSettings = cluster.kvRangeSetting(rangeId);
        String leaderStore = rangeSettings.leader;
        List<String> remainStores = Lists.newArrayList(rangeSettings.clusterConfig.getVotersList());
        remainStores.remove(leaderStore);
        log.info("Remain: {}", remainStores);

        await().ignoreExceptions().until(() -> {
            KVRangeConfig newSetting = cluster.kvRangeSetting(rangeId);
            if (newSetting.clusterConfig.getVotersCount() == 2 &&
                !newSetting.clusterConfig.getVotersList().contains(leaderStore)) {
                return true;
            }
            cluster.changeReplicaConfig(remainStores.get(0), newSetting.ver, rangeId, Sets.newHashSet(remainStores),
                emptySet()).toCompletableFuture().join();
            newSetting = cluster.kvRangeSetting(rangeId);
            return newSetting.clusterConfig.getVotersCount() == 2 &&
                !newSetting.clusterConfig.getVotersList().contains(leaderStore);
        });
    }

    @Cluster(initNodes = 1)
    @Test(groups = "integration")
    public void addReplicaFromLeaderStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        String newStore = cluster.addStore();
        log.info("add replica {}", newStore);

        await().ignoreExceptions().until(() -> {
            KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
            if (setting.clusterConfig.getVotersCount() == 2) {
                return true;
            }
            cluster.changeReplicaConfig(setting.leader, setting.ver, rangeId, Sets.newHashSet(setting.leader, newStore),
                emptySet()).toCompletableFuture().join();
            setting = cluster.kvRangeSetting(rangeId);
            return setting.clusterConfig.getVotersCount() == 2;
        });
    }

    @Cluster(initNodes = 2)
    @Test(groups = "integration")
    public void addReplicaFromNonLeaderStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSettings = cluster.awaitAllKVRangeReady(rangeId, 1, 5000);
        String newStore = cluster.addStore();
        Set<String> newReplicas = Sets.newHashSet(rangeSettings.clusterConfig.getVotersList());
        newReplicas.add(newStore);

        await().ignoreExceptions().until(() -> {
            KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
            if (setting.clusterConfig.getVotersCount() == 3 &&
                setting.clusterConfig.getVotersList().contains(newStore)) {
                return true;
            }
            cluster.changeReplicaConfig(nonLeaderStore(setting), setting.ver, rangeId, newReplicas, emptySet())
                .toCompletableFuture().join();
            setting = cluster.kvRangeSetting(rangeId);
            return setting.clusterConfig.getVotersCount() == 3 &&
                setting.clusterConfig.getVotersList().contains(newStore);
        });
    }

    @Test(groups = "integration")
    public void jointChangeReplicasFromLeaderStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSettings = cluster.awaitAllKVRangeReady(rangeId, 1, 5000);
        String newStore1 = cluster.addStore();
        String newStore2 = cluster.addStore();
        String newStore3 = cluster.addStore();
        Set<String> newReplicas = Sets.newHashSet(newStore1, newStore2, newStore3);
        log.info("Config change from {} to {}", rangeSettings.clusterConfig.getVotersList(), newReplicas);
        await().ignoreExceptions().until(() -> {
            KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
            if (newReplicas.containsAll(setting.clusterConfig.getVotersList())) {
                return true;
            }
            cluster.changeReplicaConfig(setting.leader, setting.ver, rangeId, newReplicas, emptySet())
                .toCompletableFuture().join();
            setting = cluster.kvRangeSetting(rangeId);
            return newReplicas.containsAll(setting.clusterConfig.getVotersList());
        });
        await().until(
            () -> rangeSettings.clusterConfig.getVotersList().stream()
                .noneMatch(storeId -> cluster.isHosting(storeId, rangeId)));
        log.info("Test done");
    }

    @Cluster(initNodes = 1)
    @Test(groups = "integration")
    public void moveHostingStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSettings = cluster.awaitAllKVRangeReady(rangeId, 0, 5000);
        String newStore = cluster.addStore();
        Set<String> newReplicas = Sets.newHashSet(newStore);
        log.info("Config change from {} to {}", rangeSettings.clusterConfig.getVotersList(), newReplicas);
        await().ignoreExceptions().until(() -> {
            KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
            if (newReplicas.containsAll(setting.clusterConfig.getVotersList())) {
                return true;
            }
            cluster.changeReplicaConfig(setting.leader, setting.ver, rangeId, newReplicas, emptySet())
                .toCompletableFuture().join();
            setting = cluster.kvRangeSetting(rangeId);
            return newReplicas.containsAll(setting.clusterConfig.getVotersList());
        });
        await().until(
            () -> rangeSettings.clusterConfig.getVotersList().stream()
                .noneMatch(storeId -> cluster.isHosting(storeId, rangeId)));
        log.info("Test done");
    }


    @Test(groups = "integration")
    public void jointChangeReplicasFromNonLeaderStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSettings = cluster.awaitAllKVRangeReady(rangeId, 1, 5000);
        String newStore1 = cluster.addStore();
        String newStore2 = cluster.addStore();
        String newStore3 = cluster.addStore();
        Set<String> newReplicas = Sets.newHashSet(newStore1, newStore2, newStore3);

        log.info("Current config: {}", rangeSettings.clusterConfig.getVotersList());
        await().ignoreExceptions().until(() -> {
            KVRangeConfig newSettings = cluster.kvRangeSetting(rangeId);
            if (newReplicas.containsAll(newSettings.clusterConfig.getVotersList())) {
                return true;
            }
            log.info("Joint-Config change to {}", newReplicas);
            cluster.changeReplicaConfig(nonLeaderStore(rangeSettings), newSettings.ver, rangeId, newReplicas,
                emptySet()).toCompletableFuture().join();
            newSettings = cluster.kvRangeSetting(rangeId);
            return newReplicas.containsAll(newSettings.clusterConfig.getVotersList());
        });
        await().until(
            () -> rangeSettings.clusterConfig.getVotersList().stream()
                .noneMatch(storeId -> cluster.isHosting(storeId, rangeId)));
        log.info("Test done");

    }
}
