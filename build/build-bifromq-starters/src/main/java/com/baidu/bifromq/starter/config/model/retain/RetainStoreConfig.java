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

package com.baidu.bifromq.starter.config.model.retain;

import com.baidu.bifromq.starter.config.model.BalancerOptions;
import com.baidu.bifromq.starter.config.model.RocksDBEngineConfig;
import com.baidu.bifromq.starter.config.model.StorageEngineConfig;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.google.protobuf.Struct;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RetainStoreConfig {
    private boolean enable = true;
    private int workerThreads = Math.max(2, Runtime.getRuntime().availableProcessors() / 8);
        // 0 means use calling thread
    private int tickerThreads = Math.max(1, Runtime.getRuntime().availableProcessors() / 20);
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

    public RetainStoreConfig() {
        balanceConfig.getBalancers().put("com.baidu.bifromq.retain.store.balance.ReplicaCntBalancerFactory",
            Struct.getDefaultInstance());
        balanceConfig.getBalancers().put(
            "com.baidu.bifromq.retain.store.balance.UnreachableReplicaRemovalBalancerFactory",
            Struct.getDefaultInstance());
        balanceConfig.getBalancers().put("com.baidu.bifromq.retain.store.balance.RangeBootstrapBalancerFactory",
            Struct.getDefaultInstance());
        balanceConfig.getBalancers().put("com.baidu.bifromq.retain.store.balance.RedundantEpochRemovalBalancerFactory",
            Struct.getDefaultInstance());
    }
}
