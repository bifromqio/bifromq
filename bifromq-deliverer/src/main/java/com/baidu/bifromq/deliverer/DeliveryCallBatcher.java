/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;

class DeliveryCallBatcher extends Batcher<DeliveryCall, DeliveryResult.Code, DelivererKey> {
    private final IDistClient distClient;
    private final IDeliverer deliverer;

    DeliveryCallBatcher(IDistClient distClient,
                        ISubBrokerManager subBrokerManager,
                        DelivererKey batcherKey,
                        String name,
                        long tolerableLatencyNanos,
                        long burstLatencyNanos) {
        super(batcherKey, name, tolerableLatencyNanos, burstLatencyNanos);
        int brokerId = batcherKey.subBrokerId();
        this.distClient = distClient;
        this.deliverer = subBrokerManager.get(brokerId).open(batcherKey.delivererKey());
    }

    @Override
    protected IBatchCall<DeliveryCall, DeliveryResult.Code, DelivererKey> newBatch() {
        return new DeliveryBatchCall(distClient, deliverer, batcherKey);
    }

    @Override
    public void close() {
        super.close();
        deliverer.close();
    }
}
