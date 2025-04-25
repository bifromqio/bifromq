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
import com.baidu.bifromq.basescheduler.exception.BackPressureException;
import com.baidu.bifromq.basescheduler.exception.RetryTimeoutException;
import com.baidu.bifromq.sysprops.props.DataPlaneMaxBurstLatencyMillis;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MessageDeliverer extends BatchCallScheduler<DeliveryCall, DeliveryCallResult, DelivererKey>
    implements IMessageDeliverer {

    public MessageDeliverer(BatchDeliveryCallBuilderFactory batchDeliveryCallBuilderFactory) {
        super(batchDeliveryCallBuilderFactory,
            Duration.ofMillis(DataPlaneMaxBurstLatencyMillis.INSTANCE.get()).toNanos());
    }

    @Override
    public CompletableFuture<DeliveryCallResult> schedule(DeliveryCall request) {
        return super.schedule(request).exceptionally(e -> {
            if (e instanceof BackPressureException || e.getCause() instanceof BackPressureException) {
                return DeliveryCallResult.BACK_PRESSURE_REJECTED;
            }
            if (e instanceof RetryTimeoutException || e.getCause() instanceof RetryTimeoutException) {
                return DeliveryCallResult.BACK_PRESSURE_REJECTED;
            }
            log.error("Failed to schedule delivery call", e);
            return DeliveryCallResult.ERROR;
        });
    }

    @Override
    protected Optional<DelivererKey> find(DeliveryCall request) {
        return Optional.of(request.delivererKey);
    }
}
