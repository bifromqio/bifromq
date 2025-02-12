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

package com.baidu.bifromq.deliverer;

import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.sysprops.props.DataPlaneBurstLatencyMillis;
import com.baidu.bifromq.sysprops.props.DataPlaneTolerableLatencyMillis;
import java.time.Duration;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MessageDeliverer extends BatchCallScheduler<DeliveryCall, DeliveryResult.Code, DelivererKey>
    implements IMessageDeliverer {
    private final IDistClient distClient;
    private final ISubBrokerManager subBrokerManager;

    public MessageDeliverer(ISubBrokerManager subBrokerManager, IDistClient distClient) {
        super("dist_worker_deliver_batcher", Duration.ofMillis(DataPlaneTolerableLatencyMillis.INSTANCE.get()),
            Duration.ofMillis(DataPlaneBurstLatencyMillis.INSTANCE.get()));
        this.distClient = distClient;
        this.subBrokerManager = subBrokerManager;
    }

    @Override
    protected Batcher<DeliveryCall, DeliveryResult.Code, DelivererKey> newBatcher(String name,
                                                                                  long tolerableLatencyNanos,
                                                                                  long burstLatencyNanos,
                                                                                  DelivererKey delivererKey) {
        return new DeliveryCallBatcher(distClient,
            subBrokerManager,
            delivererKey,
            name,
            tolerableLatencyNanos,
            burstLatencyNanos);
    }

    @Override
    protected Optional<DelivererKey> find(DeliveryCall request) {
        return Optional.of(request.delivererKey);
    }

}
