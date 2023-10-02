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

import static com.baidu.bifromq.basekv.TestUtil.isDevEnv;
import static com.baidu.bifromq.basekv.utils.KVRangeIdUtil.toShortString;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.fail;

import com.baidu.bifromq.basekv.annotation.Cluster;
import com.baidu.bifromq.basekv.localengine.memory.InMemKVEngineConfigurator;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.option.KVRangeOptions;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterMethod;

@Slf4j
public abstract class KVRangeStoreClusterTestTemplate {
    protected KVRangeStoreTestCluster cluster;
    private int initNodes = 3;
    private KVRangeStoreOptions options;

    public void createClusterByAnnotation(Method testMethod) {
        Cluster cluster = testMethod.getAnnotation(Cluster.class);
        options = new KVRangeStoreOptions();
        if (!isDevEnv()) {
            options.setWalEngineConfigurator(new InMemKVEngineConfigurator());
            options.setDataEngineConfigurator(new InMemKVEngineConfigurator());
        }
        if (cluster != null) {
            Preconditions.checkArgument(cluster.initNodes() > 0,
                "Init nodes number must be greater than zero");
            initNodes = cluster.initNodes();
            KVRangeOptions rangeOptions = new KVRangeOptions();
            rangeOptions.setWalRaftConfig(rangeOptions.getWalRaftConfig().setAsyncAppend(cluster.asyncAppend()));
            rangeOptions.setWalRaftConfig(rangeOptions.getWalRaftConfig()
                .setInstallSnapshotTimeoutTick(cluster.installSnapshotTimeoutTick()));
            options.setKvRangeOptions(rangeOptions);
        } else {
            initNodes = 3;
        }
        log.info("Starting test: " + testMethod.getName());
        setup();
    }

    public void setup() {
        try {
            log.info("Starting test cluster");
            cluster = new KVRangeStoreTestCluster(options);
            String store0 = cluster.bootstrapStore();
            KVRangeId rangeId = cluster.genesisKVRangeId();
            cluster.awaitKVRangeReady(store0, rangeId);
            Set<String> voters = Sets.newHashSet(store0);
            for (int i = 1; i < initNodes; i++) {
                voters.add(cluster.addStore());
            }
            long start = System.currentTimeMillis();
            log.info("Preparing replica config for testing");
            await().ignoreExceptions().forever().until(() -> {
                KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
                String leader = setting.leader;
                if (setting.clusterConfig.getVotersList().containsAll(voters)) {
                    return true;
                }
                cluster.changeReplicaConfig(leader, setting.ver, rangeId, voters, Sets.newHashSet())
                    .toCompletableFuture().join();
                setting = cluster.kvRangeSetting(rangeId);
                log.info("Config change succeed: {}, {}, {}", setting, voters,
                    setting.clusterConfig.getVotersList().containsAll(voters));
                return setting.clusterConfig.getVotersList().containsAll(voters);
            });
            cluster.awaitAllKVRangeReady(rangeId, voters.size() == 1 ? 0 : 2, 5000);
            log.info("KVRange[{}] ready in {}ms start testing", toShortString(rangeId),
                System.currentTimeMillis() - start);
        } catch (Throwable e) {
            log.error("Failed to setup test cluster", e);
            fail();
        }
    }

    @AfterMethod
    public void teardown() {
        if (cluster != null) {
            log.info("Shutting down test cluster");
            new Thread(() -> cluster.shutdown()).start();
        }
    }

    public String nonLeaderStore(KVRangeConfig setting) {
        Set<String> followerStores = followStores(setting);
        if (followerStores.isEmpty()) {
            throw new RuntimeException("No non-leader store");
        }
        return followerStores.iterator().next();
    }

    public Set<String> followStores(KVRangeConfig setting) {
        Set<String> voters = new HashSet<>(setting.clusterConfig.getVotersList());
        voters.remove(setting.leader);
        return voters;
    }
}
