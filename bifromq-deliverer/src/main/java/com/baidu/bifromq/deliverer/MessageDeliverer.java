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

import static com.baidu.bifromq.plugin.subbroker.TypeUtil.toMap;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DATA_PLANE_BURST_LATENCY_MS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DATA_PLANE_TOLERABLE_LATENCY_MS;

import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.plugin.subbroker.DeliveryPack;
import com.baidu.bifromq.plugin.subbroker.DeliveryPackage;
import com.baidu.bifromq.plugin.subbroker.DeliveryRequest;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.type.MatchInfo;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MessageDeliverer extends BatchCallScheduler<DeliveryCall, DeliveryResult.Code, DelivererKey>
    implements IMessageDeliverer {
    private final ISubBrokerManager subBrokerManager;

    public MessageDeliverer(ISubBrokerManager subBrokerManager) {
        super("dist_worker_deliver_batcher", Duration.ofMillis(DATA_PLANE_TOLERABLE_LATENCY_MS.get()),
            Duration.ofMillis(DATA_PLANE_BURST_LATENCY_MS.get()));
        this.subBrokerManager = subBrokerManager;
    }

    @Override
    protected Batcher<DeliveryCall, DeliveryResult.Code, DelivererKey> newBatcher(String name,
                                                                                  long tolerableLatencyNanos,
                                                                                  long burstLatencyNanos,
                                                                                  DelivererKey delivererKey) {
        return new DeliveryCallBatcher(delivererKey, name, tolerableLatencyNanos, burstLatencyNanos);
    }

    @Override
    protected Optional<DelivererKey> find(DeliveryCall request) {
        return Optional.of(request.delivererKey);
    }

    private class DeliveryCallBatcher extends Batcher<DeliveryCall, DeliveryResult.Code, DelivererKey> {
        private final IDeliverer deliverer;

        private class DeliveryBatchCall implements IBatchCall<DeliveryCall, DeliveryResult.Code, DelivererKey> {
            private final Queue<CallTask<DeliveryCall, DeliveryResult.Code, DelivererKey>> tasks =
                new ArrayDeque<>(128);
            private Map<String, Map<MessagePackWrapper, Set<MatchInfo>>> batch = new HashMap<>(128);

            @Override
            public void reset() {
                batch = new HashMap<>(128);
            }

            @Override
            public void add(CallTask<DeliveryCall, DeliveryResult.Code, DelivererKey> callTask) {
                batch.computeIfAbsent(callTask.call.tenantId, k -> new LinkedHashMap<>(128))
                    .computeIfAbsent(callTask.call.msgPackWrapper, k -> new HashSet<>())
                    .add(callTask.call.matchInfo);
                tasks.add(callTask);
            }

            @Override
            public CompletableFuture<Void> execute() {
                DeliveryRequest.Builder requestBuilder = DeliveryRequest.newBuilder();
                batch.forEach((tenantId, pack) -> {
                    DeliveryPackage.Builder packageBuilder = DeliveryPackage.newBuilder();
                    pack.forEach((msgPackWrapper, matchInfos) ->
                        packageBuilder.addPack(DeliveryPack.newBuilder()
                            .setMessagePack(msgPackWrapper.messagePack)
                            .addAllMatchInfo(matchInfos)
                            .build()));
                    requestBuilder.putPackage(tenantId, packageBuilder.build());
                });

                return deliverer.deliver(requestBuilder.build())
                    .handle((reply, e) -> {
                        if (e != null) {
                            CallTask<DeliveryCall, DeliveryResult.Code, DelivererKey> task;
                            while ((task = tasks.poll()) != null) {
                                task.callResult.completeExceptionally(e);
                            }
                        } else {
                            CallTask<DeliveryCall, DeliveryResult.Code, DelivererKey> task;
                            Map<String, Map<MatchInfo, DeliveryResult.Code>> resultMap =
                                toMap(reply.getResultMap());
                            while ((task = tasks.poll()) != null) {
                                DeliveryResult.Code result = resultMap
                                    .getOrDefault(task.call.tenantId, Collections.emptyMap())
                                    .get(task.call.matchInfo);
                                if (result != null) {
                                    task.callResult.complete(result);
                                } else {
                                    log.warn("[{}]No deliver result: tenantId={}, route={}, batcherKey={}",
                                        this.hashCode(), task.call.tenantId, task.call.matchInfo,
                                        task.call.delivererKey);
                                    task.callResult.complete(DeliveryResult.Code.OK);
                                }
                            }
                        }
                        return null;
                    });
            }
        }

        DeliveryCallBatcher(DelivererKey batcherKey,
                            String name,
                            long tolerableLatencyNanos,
                            long burstLatencyNanos) {
            super(batcherKey, name, tolerableLatencyNanos, burstLatencyNanos);
            int brokerId = batcherKey.subBrokerId();
            this.deliverer = subBrokerManager.get(brokerId).open(batcherKey.delivererKey());
        }

        @Override
        protected IBatchCall<DeliveryCall, DeliveryResult.Code, DelivererKey> newBatch() {
            return new DeliveryBatchCall();
        }

        @Override
        public void close() {
            super.close();
            deliverer.close();
        }
    }
}
