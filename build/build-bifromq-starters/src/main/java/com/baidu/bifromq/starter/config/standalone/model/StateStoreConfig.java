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

package com.baidu.bifromq.starter.config.standalone.model;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StateStoreConfig {
    private String overrideIdentity;
    private int queryThreads = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);
    private int tickerThreads = Math.max(1, Runtime.getRuntime().availableProcessors() / 20);
    private int bgWorkerThreads = Math.max(1, EnvProvider.INSTANCE.availableProcessors() / 4);

    @JsonSetter(nulls = Nulls.SKIP)
    private DistWorkerConfig distWorkerConfig = new DistWorkerConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    private InboxStoreConfig inboxStoreConfig = new InboxStoreConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    private RetainStoreConfig retainStoreConfig = new RetainStoreConfig();

    @Getter
    @Setter
    public static class BalancerOptions {
        private long scheduleIntervalInMs = 5000;
        @JsonSetter(nulls = Nulls.SKIP)
        private List<String> balancers = new ArrayList<>();
    }

    @Getter
    @Setter
    public static class DistWorkerConfig {
        private int queryPipelinePerStore = 1000;
        private int compactWALThreshold = 2500;
        @JsonSetter(nulls = Nulls.SKIP)
        private StorageEngineConfig dataEngineConfig = new RocksDBEngineConfig();
        @JsonSetter(nulls = Nulls.SKIP)
        private StorageEngineConfig walEngineConfig = new RocksDBEngineConfig()
            .setManualCompaction(true)
            .setCompactMinTombstoneKeys(2500)
            .setCompactMinTombstoneRanges(2);
        @JsonSetter(nulls = Nulls.SKIP)
        private BalancerOptions balanceConfig = new BalancerOptions();

        public DistWorkerConfig() {
            // DO not enable DistWorker split by default
//            balanceConfig.balancers.add("com.baidu.bifromq.dist.worker.balance.DistWorkerSplitBalancerFactory");
            balanceConfig.balancers.add("com.baidu.bifromq.dist.worker.balance.RangeLeaderBalancerFactory");
            balanceConfig.balancers.add("com.baidu.bifromq.dist.worker.balance.ReplicaCntBalancerFactory");
        }
    }

    @Getter
    @Setter
    public static class InboxStoreConfig {
        private int queryPipelinePerStore = 100;
        private int compactWALThreshold = 2500;
        private int gcIntervalSeconds = 600;
        @JsonSetter(nulls = Nulls.SKIP)
        private StorageEngineConfig dataEngineConfig = new RocksDBEngineConfig();
        @JsonSetter(nulls = Nulls.SKIP)
        private StorageEngineConfig walEngineConfig = new RocksDBEngineConfig()
            .setManualCompaction(true)
            .setCompactMinTombstoneKeys(2500)
            .setCompactMinTombstoneRanges(2);
        @JsonSetter(nulls = Nulls.SKIP)
        private BalancerOptions balanceConfig = new BalancerOptions();

        public InboxStoreConfig() {
            balanceConfig.balancers.add("com.baidu.bifromq.inbox.store.balance.ReplicaCntBalancerFactory");
            balanceConfig.balancers.add("com.baidu.bifromq.inbox.store.balance.RangeSplitBalancerFactory");
            balanceConfig.balancers.add("com.baidu.bifromq.inbox.store.balance.RangeLeaderBalancerFactory");
        }
    }

    @Getter
    @Setter
    public static class RetainStoreConfig {
        private int queryPipelinePerStore = 100;
        private int compactWALThreshold = 2500;
        private int gcIntervalSeconds = 600;
        @JsonSetter(nulls = Nulls.SKIP)
        private StorageEngineConfig dataEngineConfig = new RocksDBEngineConfig();
        @JsonSetter(nulls = Nulls.SKIP)
        private StorageEngineConfig walEngineConfig = new RocksDBEngineConfig()
            .setManualCompaction(true)
            .setCompactMinTombstoneKeys(5000)
            .setCompactMinTombstoneRanges(2);
        @JsonSetter(nulls = Nulls.SKIP)
        private BalancerOptions balanceConfig = new BalancerOptions();

        public RetainStoreConfig() {
            balanceConfig.balancers.add("com.baidu.bifromq.retain.store.balance.ReplicaCntBalancerFactory");
        }
    }
}
