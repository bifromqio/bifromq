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

import static com.baidu.bifromq.plugin.subbroker.TypeUtil.toMap;

import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.basescheduler.ICallTask;
import com.baidu.bifromq.basescheduler.exception.NeedRetryException;
import com.baidu.bifromq.deliverer.exception.DeliveryException;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.plugin.subbroker.DeliveryPack;
import com.baidu.bifromq.plugin.subbroker.DeliveryPackage;
import com.baidu.bifromq.plugin.subbroker.DeliveryReply;
import com.baidu.bifromq.plugin.subbroker.DeliveryRequest;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.baidu.bifromq.type.MatchInfo;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DeliveryBatchCall implements IBatchCall<DeliveryCall, DeliveryResult.Code, DelivererKey> {
    private final IDistClient distClient;
    private final IDeliverer deliverer;
    private final DelivererKey batcherKey;
    private final Queue<ICallTask<DeliveryCall, DeliveryResult.Code, DelivererKey>> tasks = new ArrayDeque<>(128);
    private Map<String, Map<MessagePackWrapper, Set<MatchInfo>>> batch = new HashMap<>(128);

    DeliveryBatchCall(IDistClient distClient, IDeliverer deliverer, DelivererKey batcherKey) {
        this.distClient = distClient;
        this.deliverer = deliverer;
        this.batcherKey = batcherKey;
    }

    @Override
    public void reset() {
        batch = new HashMap<>(128);
    }

    @Override
    public void add(ICallTask<DeliveryCall, DeliveryResult.Code, DelivererKey> callTask) {
        batch.computeIfAbsent(callTask.call().tenantId, k -> new LinkedHashMap<>(128))
            .computeIfAbsent(callTask.call().msgPackWrapper, k -> new HashSet<>()).add(callTask.call().matchInfo);
        tasks.add(callTask);
    }

    @Override
    public CompletableFuture<Void> execute() {
        DeliveryRequest.Builder requestBuilder = DeliveryRequest.newBuilder();
        batch.forEach((tenantId, pack) -> {
            DeliveryPackage.Builder packageBuilder = DeliveryPackage.newBuilder();
            pack.forEach((msgPackWrapper, matchInfos) -> {
                DeliveryPack.Builder packBuilder = DeliveryPack.newBuilder().setMessagePack(msgPackWrapper.messagePack);
                matchInfos.forEach(packBuilder::addMatchInfo);
                packageBuilder.addPack(packBuilder.build());
            });
            requestBuilder.putPackage(tenantId, packageBuilder.build());
        });

        return deliverer.deliver(requestBuilder.build()).exceptionally(e -> {
            log.error("Unexpected exception", e);
            return DeliveryReply.newBuilder().setCode(DeliveryReply.Code.ERROR).build();
        }).thenAccept(reply -> {
            switch (reply.getCode()) {
                case OK -> {
                    ICallTask<DeliveryCall, DeliveryResult.Code, DelivererKey> task;
                    Map<String, Map<MatchInfo, DeliveryResult.Code>> resultMap = toMap(reply.getResultMap());
                    Map<String, Set<MatchInfo>> staleMatchInfos = new HashMap<>();
                    while ((task = tasks.poll()) != null) {
                        DeliveryResult.Code result =
                            resultMap.getOrDefault(task.call().tenantId, Collections.emptyMap())
                                .get(task.call().matchInfo);
                        if (result != null) {
                            if (result == DeliveryResult.Code.NO_SUB || result == DeliveryResult.Code.NO_RECEIVER) {
                                staleMatchInfos.computeIfAbsent(task.call().tenantId, k -> new HashSet<>())
                                    .add(task.call().matchInfo);
                            }
                            task.resultPromise().complete(result);
                        } else {
                            log.warn("[{}]No deliver result: tenantId={}, route={}, batcherKey={}", this.hashCode(),
                                task.call().tenantId, task.call().matchInfo, task.call().delivererKey);
                            task.resultPromise().complete(DeliveryResult.Code.OK);
                        }
                    }
                    for (Map.Entry<String, Set<MatchInfo>> entry : staleMatchInfos.entrySet()) {
                        String tenantId = entry.getKey();
                        Set<MatchInfo> matchInfos = entry.getValue();
                        for (MatchInfo matchInfo : matchInfos) {
                            log.warn(
                                "Stale match info: tenantId={}, topicFilter={}, receiverId={}, delivererKey={}, subBrokerId={}",
                                tenantId, matchInfo.getMatcher().getMqttTopicFilter(), matchInfo.getReceiverId(),
                                batcherKey.delivererKey(), batcherKey.subBrokerId());
                            distClient.removeRoute(System.nanoTime(), tenantId, matchInfo.getMatcher(),
                                matchInfo.getReceiverId(), batcherKey.delivererKey(), batcherKey.subBrokerId(),
                                matchInfo.getIncarnation());
                        }
                    }
                }
                case TRY_LATER -> throw new NeedRetryException();
                default -> {
                    assert reply.getCode() == DeliveryReply.Code.ERROR;
                    ICallTask<DeliveryCall, DeliveryResult.Code, DelivererKey> task;
                    while ((task = tasks.poll()) != null) {
                        task.resultPromise().completeExceptionally(new DeliveryException("Failed to deliver"));
                    }
                }
            }
        });
    }
}

