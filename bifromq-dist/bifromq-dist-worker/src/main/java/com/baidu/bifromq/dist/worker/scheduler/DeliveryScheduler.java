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

import static com.baidu.bifromq.sysprops.BifroMQSysProp.DIST_MAX_BATCH_SEND_MESSAGES;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DIST_WORKER_MAX_INFLIGHT_CALLS_PER_QUEUE;

import com.baidu.bifromq.basescheduler.BatchCallBuilder;
import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.plugin.subbroker.DeliveryPack;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.type.SubInfo;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jctools.queues.MpscUnboundedArrayQueue;

@Slf4j
public class DeliveryScheduler
    extends BatchCallScheduler<DeliveryRequest, DeliveryResult, DeliveryRequest.DelivererKey> {
    private static final int MAX_BATCH_MESSAGES = DIST_MAX_BATCH_SEND_MESSAGES.get();
    private final ISubBrokerManager subBrokerManager;

    public DeliveryScheduler(ISubBrokerManager subBrokerManager) {
        super("dist_worker_deliver_batcher", DIST_WORKER_MAX_INFLIGHT_CALLS_PER_QUEUE.get());
        this.subBrokerManager = subBrokerManager;
    }

    @Override
    protected BatchCallBuilder<DeliveryRequest, DeliveryResult> newBuilder(String name, int maxInflights,
                                                                           DeliveryRequest.DelivererKey inboxWriterKey) {
        return new DeliveryCallBuilder(name, maxInflights, inboxWriterKey);
    }

    @Override
    protected Optional<DeliveryRequest.DelivererKey> find(DeliveryRequest request) {
        return Optional.of(request.writerKey);
    }

    private class DeliveryCallBuilder extends BatchCallBuilder<DeliveryRequest, DeliveryResult> {
        private class BatchInboxWriteCall implements IBatchCall<DeliveryRequest, DeliveryResult> {
            private final AtomicInteger msgCount = new AtomicInteger();
            private final Map<MessagePackWrapper, Set<SubInfo>> batch = new ConcurrentHashMap<>();
            private final Queue<DeliveryTask> tasks = new MpscUnboundedArrayQueue<>(128);

            @Override
            public boolean isEmpty() {
                return batch.isEmpty();
            }

            @Override
            public boolean isEnough() {
                return msgCount.get() > MAX_BATCH_MESSAGES;
            }

            @Override
            public CompletableFuture<DeliveryResult> add(DeliveryRequest request) {
                if (batch.computeIfAbsent(request.msgPackWrapper, k -> ConcurrentHashMap.newKeySet())
                    .add(request.subInfo)) {
                    request.msgPackWrapper.messagePack.getMessageList()
                        .forEach(senderMsgPack -> msgCount.addAndGet(senderMsgPack.getMessageCount()));
                }
                DeliveryTask task = new DeliveryTask(request.subInfo);
                tasks.add(task);
                return task.onDone;
            }

            @Override
            public void reset() {
                msgCount.set(0);
                batch.clear();
            }

            @Override
            public CompletableFuture<Void> execute() {
                msgCountSummary.record(msgCount.get());
                return deliverer.deliver(batch.entrySet().stream()
                        .map(e -> new DeliveryPack(e.getKey().messagePack, e.getValue()))
                        .collect(Collectors.toList()))
                    .handle((reply, e) -> {
                        if (e != null) {
                            DeliveryTask task;
                            while ((task = tasks.poll()) != null) {
                                task.onDone.completeExceptionally(e);
                            }
                        } else {
                            DeliveryTask task;
                            while ((task = tasks.poll()) != null) {
                                DeliveryResult result = reply.get(task.subInfo);
                                if (result != null) {
                                    task.onDone.complete(result);
                                } else {
                                    log.warn("No write result for sub: {}", task.subInfo);
                                    task.onDone.complete(DeliveryResult.OK);
                                }
                            }
                        }
                        return null;
                    });
            }
        }

        private final IDeliverer deliverer;
        private final DistributionSummary msgCountSummary;

        DeliveryCallBuilder(String name, int maxInflights, DeliveryRequest.DelivererKey key) {
            super(name, maxInflights);
            int brokerId = key.subBrokerId();
            this.deliverer = subBrokerManager.get(brokerId).open(key.delivererKey());
            Tags tags = Tags.of("subBrokerId", String.valueOf(brokerId));
            msgCountSummary = DistributionSummary.builder("dist.server.send.messages")
                .tags(tags)
                .register(Metrics.globalRegistry);
        }

        @Override
        public BatchInboxWriteCall newBatch() {
            return new BatchInboxWriteCall();
        }

        @Override
        public void close() {
            deliverer.close();
            Metrics.globalRegistry.remove(msgCountSummary);
        }
    }

    @AllArgsConstructor
    private static class DeliveryTask {
        final SubInfo subInfo;
        final CompletableFuture<DeliveryResult> onDone = new CompletableFuture<>();
    }
}
