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

package com.baidu.bifromq.dist.worker.balance;

import com.baidu.bifromq.basekv.balance.StoreBalancer;
import com.baidu.bifromq.basekv.balance.impl.UnreachableReplicaRemovalBalancer;
import com.baidu.bifromq.dist.worker.spi.IDistWorkerBalancerFactory;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UnreachableReplicaRemovalBalancerFactory implements IDistWorkerBalancerFactory {
    private static final String PROBE_WAIT_TIME = "probeSeconds";
    private static final long DEFAULT_WAIT_SECONDS = 15;

    private Duration probeTime;

    @Override
    public void init(Struct config) {
        int probeTimeConfig = (int) config.getFieldsOrDefault(PROBE_WAIT_TIME, Value.newBuilder()
            .setNumberValue(DEFAULT_WAIT_SECONDS).build()).getNumberValue();
        if (probeTimeConfig < 5 || probeTimeConfig > 60) {
            probeTimeConfig = 15;
            log.warn("Invalid probe time config {}, use default {}", probeTimeConfig, DEFAULT_WAIT_SECONDS);
        }
        probeTime = Duration.ofSeconds(probeTimeConfig);
    }

    @Override
    public StoreBalancer newBalancer(String clusterId, String localStoreId) {
        return new UnreachableReplicaRemovalBalancer(clusterId, localStoreId, probeTime);
    }
}
