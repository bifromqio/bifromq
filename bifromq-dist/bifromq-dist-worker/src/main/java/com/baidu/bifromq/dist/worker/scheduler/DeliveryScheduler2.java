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

package com.baidu.bifromq.dist.worker.scheduler;

import static com.baidu.bifromq.sysprops.BifroMQSysProp.DIST_SERVER_MAX_TOLERANT_LATENCY_MS;

import com.baidu.bifromq.basescheduler.BatchCall2;
import com.baidu.bifromq.basescheduler.BatchCallScheduler2;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.plugin.subbroker.DeliveryPack;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.type.SubInfo;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.jctools.queues.MpscUnboundedArrayQueue;

@Slf4j
public class DeliveryScheduler2 extends BatchCallScheduler2<DeliveryRequest, DeliveryResult, DelivererKey>
    implements IDeliveryScheduler {
    private final ISubBrokerManager subBrokerManager;

    public DeliveryScheduler2(ISubBrokerManager subBrokerManager) {
        super("dist_worker_deliver_batcher", Duration.ofMillis(DIST_SERVER_MAX_TOLERANT_LATENCY_MS.get()));
        this.subBrokerManager = subBrokerManager;
    }

    @Override
    protected Batcher<DeliveryRequest, DeliveryResult, DelivererKey> newBatcher(String name,
                                                                                long maxTolerantLatencyNanos,
                                                                                DelivererKey delivererKey) {
        return new DeliveryCallBatcher(delivererKey, name, maxTolerantLatencyNanos);
    }

    @Override
    protected Optional<DelivererKey> find(DeliveryRequest request) {
        return Optional.of(request.writerKey);
    }

    private class DeliveryCallBatcher extends Batcher<DeliveryRequest, DeliveryResult, DelivererKey> {
        private final IDeliverer deliverer;

        private class DeliveryBatchCall extends BatchCall2<DeliveryRequest, DeliveryResult> {
            private final Map<MessagePackWrapper, Set<SubInfo>> batch = new ConcurrentHashMap<>();
            private final Queue<CallTask<DeliveryRequest, DeliveryResult>> tasks = new MpscUnboundedArrayQueue<>(128);

            @Override
            public void add(CallTask<DeliveryRequest, DeliveryResult> callTask) {
                batch.computeIfAbsent(callTask.call.msgPackWrapper, k -> ConcurrentHashMap.newKeySet())
                    .add(callTask.call.subInfo);
                tasks.add(callTask);
            }

            @Override
            public CompletableFuture<Void> execute() {
                return deliverer.deliver(batch.entrySet().stream()
                        .map(e -> new DeliveryPack(e.getKey().messagePack, e.getValue()))
                        .collect(Collectors.toList()))
                    .handle((reply, e) -> {
                        if (e != null) {
                            CallTask<DeliveryRequest, DeliveryResult> task;
                            while ((task = tasks.poll()) != null) {
                                task.callResult.completeExceptionally(e);
                            }
                        } else {
                            CallTask<DeliveryRequest, DeliveryResult> task;
                            while ((task = tasks.poll()) != null) {
                                DeliveryResult result = reply.get(task.call.subInfo);
                                if (result != null) {
                                    task.callResult.complete(result);
                                } else {
                                    log.warn("No write result for sub: {}", task.call.subInfo);
                                    task.callResult.complete(DeliveryResult.OK);
                                }
                            }
                        }
                        return null;
                    });
            }
        }

        DeliveryCallBatcher(DelivererKey batcherKey, String name, long maxTolerantLatencyNanos) {
            super(batcherKey, name, maxTolerantLatencyNanos);
            int brokerId = batcherKey.subBrokerId();
            this.deliverer = subBrokerManager.get(brokerId).open(batcherKey.delivererKey());
        }

        @Override
        protected BatchCall2<DeliveryRequest, DeliveryResult> newBatch() {
            return new DeliveryBatchCall();
        }

        @Override
        public void close() {
            super.close();
            deliverer.close();
        }
    }
}
