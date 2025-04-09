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

package com.baidu.bifromq.inbox.store.balance;

import com.baidu.bifromq.basekv.balance.StoreBalancer;
import com.baidu.bifromq.basekv.balance.impl.RangeBootstrapBalancer;
import com.baidu.bifromq.inbox.store.spi.IInboxStoreBalancerFactory;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RangeBootstrapBalancerFactory implements IInboxStoreBalancerFactory {
    private static final String BOOTSTRAP_WAIT_TIME = "waitSeconds";
    private static final long DEFAULT_WAIT_SECONDS = 15;

    private Duration waitTime;

    @Override
    public void init(Struct config) {
        int waitTimeConfig = (int) config.getFieldsOrDefault(BOOTSTRAP_WAIT_TIME, Value.newBuilder()
            .setNumberValue(DEFAULT_WAIT_SECONDS).build()).getNumberValue();
        if (waitTimeConfig < 1 || waitTimeConfig > 60) {
            waitTimeConfig = 15;
            log.warn("Invalid wait time config {}, use default {}", waitTimeConfig, DEFAULT_WAIT_SECONDS);
        }
        waitTime = Duration.ofSeconds(waitTimeConfig);
    }

    @Override
    public StoreBalancer newBalancer(String clusterId, String localStoreId) {
        return new RangeBootstrapBalancer(clusterId, localStoreId, waitTime);
    }
}
