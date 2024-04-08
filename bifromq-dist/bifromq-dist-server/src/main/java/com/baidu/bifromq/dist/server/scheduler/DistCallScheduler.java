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

package com.baidu.bifromq.dist.server.scheduler;

import static com.baidu.bifromq.dist.entity.EntityUtil.matchRecordKeyPrefix;
import static com.baidu.bifromq.dist.entity.EntityUtil.tenantUpperBound;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DATA_PLANE_BURST_LATENCY_MS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DATA_PLANE_TOLERABLE_LATENCY_MS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DIST_WORKER_FANOUT_SPLIT_THRESHOLD;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.basescheduler.ICallScheduler;
import com.baidu.bifromq.dist.rpc.proto.DistServiceROCoProcInput;
import com.baidu.bifromq.dist.rpc.proto.TenantDistReply;
import com.baidu.bifromq.dist.rpc.proto.TenantDistRequest;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.common.collect.Iterables;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DistCallScheduler
    extends BatchCallScheduler<DistWorkerCall, Map<String, Integer>, DistCallScheduler.BatcherKey>
    implements IDistCallScheduler {
    public record BatcherKey(String tenantId, Integer callQueueIdx) {
    }

    private final IBaseKVStoreClient distWorkerClient;
    private final Function<String, Integer> tenantFanoutGetter;
    private final int fanoutSplitThreshold = DIST_WORKER_FANOUT_SPLIT_THRESHOLD.get();

    public DistCallScheduler(ICallScheduler<DistWorkerCall> reqScheduler,
                             IBaseKVStoreClient distWorkerClient,
                             Function<String, Integer> tenantFanoutGetter) {
        super("dist_server_dist_batcher", reqScheduler, Duration.ofMillis(DATA_PLANE_TOLERABLE_LATENCY_MS.get()),
            Duration.ofMillis(DATA_PLANE_BURST_LATENCY_MS.get()));
        this.distWorkerClient = distWorkerClient;
        this.tenantFanoutGetter = tenantFanoutGetter;
    }

    @Override
    protected Batcher<DistWorkerCall, Map<String, Integer>, BatcherKey> newBatcher(String name,
                                                                                   long tolerableLatencyNanos,
                                                                                   long burstLatencyNanos,
                                                                                   BatcherKey batchKey) {
        return new DistWorkerCallBatcher(batchKey, name, tolerableLatencyNanos, burstLatencyNanos,
            fanoutSplitThreshold,
            distWorkerClient,
            tenantFanoutGetter);
    }

    @Override
    protected Optional<BatcherKey> find(DistWorkerCall request) {
        return Optional.of(new BatcherKey(request.tenantId, request.callQueueIdx));
    }

    private static class DistWorkerCallBatcher
        extends Batcher<DistWorkerCall, Map<String, Integer>, DistCallScheduler.BatcherKey> {
        private final IBaseKVStoreClient distWorkerClient;
        private final String orderKey = UUID.randomUUID().toString();
        private final Function<String, Integer> tenantFanoutGetter;
        private final int fanoutSplitThreshold;

        protected DistWorkerCallBatcher(DistCallScheduler.BatcherKey batcherKey,
                                        String name,
                                        long tolerableLatencyNanos,
                                        long burstLatencyNanos,
                                        int fanoutSplitThreshold,
                                        IBaseKVStoreClient distWorkerClient,
                                        Function<String, Integer> tenantFanoutGetter) {
            super(batcherKey, name, tolerableLatencyNanos, burstLatencyNanos);
            this.distWorkerClient = distWorkerClient;
            this.tenantFanoutGetter = tenantFanoutGetter;
            this.fanoutSplitThreshold = fanoutSplitThreshold;
        }

        @Override
        protected IBatchCall<DistWorkerCall, Map<String, Integer>, DistCallScheduler.BatcherKey> newBatch() {
            return new BatchDistCall();
        }

        private class BatchDistCall implements IBatchCall<DistWorkerCall, Map<String, Integer>, BatcherKey> {
            private final Queue<CallTask<DistWorkerCall, Map<String, Integer>, BatcherKey>> tasks =
                new ArrayDeque<>(128);
            private Map<String, Map<ClientInfo, Iterable<Message>>> batch = new HashMap<>(128);

            @Override
            public void reset() {
                batch = new HashMap<>(128);
            }

            @Override
            public void add(CallTask<DistWorkerCall, Map<String, Integer>, BatcherKey> callTask) {
                callTask.call.publisherMsgPacks.forEach(senderMsgPack ->
                    senderMsgPack.getMessagePackList().forEach(topicMsgs ->
                        batch.computeIfAbsent(topicMsgs.getTopic(), k -> new HashMap<>())
                            .compute(senderMsgPack.getPublisher(), (k, v) -> {
                                if (v == null) {
                                    v = topicMsgs.getMessageList();
                                } else {
                                    v = Iterables.concat(v, topicMsgs.getMessageList());
                                }
                                return v;
                            })));
                tasks.add(callTask);
            }

            @Override
            public CompletableFuture<Void> execute() {
                String tenantId = batcherKey.tenantId();
                Map<KVRangeReplica, List<TopicMessagePack>> msgPacksByRangeReplica = new HashMap<>();
                batch.forEach((topic, senderMap) -> {
                    TopicMessagePack.Builder topicMsgPackBuilder = TopicMessagePack.newBuilder().setTopic(topic);
                    senderMap.forEach((sender, msgs) ->
                        topicMsgPackBuilder.addMessage(TopicMessagePack.PublisherPack
                            .newBuilder()
                            .setPublisher(sender)
                            .addAllMessage(msgs)
                            .build()));
                    TopicMessagePack topicMsgPack = topicMsgPackBuilder.build();
                    int fanoutScale = tenantFanoutGetter.apply(tenantId);
                    List<KVRangeSetting> ranges = distWorkerClient.findByBoundary(Boundary.newBuilder()
                        .setStartKey(matchRecordKeyPrefix(tenantId))
                        .setEndKey(tenantUpperBound(tenantId))
                        .build());
                    if (fanoutScale > fanoutSplitThreshold) {
                        ranges.forEach(range -> msgPacksByRangeReplica.computeIfAbsent(
                            new KVRangeReplica(range.id, range.ver, range.leader),
                            k -> new LinkedList<>()).add(topicMsgPack));
                    } else {
                        ranges.forEach(range -> msgPacksByRangeReplica.computeIfAbsent(
                            new KVRangeReplica(range.id, range.ver, range.randomReplica()),
                            k -> new LinkedList<>()).add(topicMsgPack));
                    }
                });

                long reqId = System.nanoTime();
                @SuppressWarnings("unchecked")
                CompletableFuture<TenantDistReply>[] distReplyFutures = msgPacksByRangeReplica.entrySet().stream()
                    .map(entry -> {
                        KVRangeReplica rangeReplica = entry.getKey();
                        TenantDistRequest tenantDistRequest = TenantDistRequest.newBuilder()
                            .setReqId(reqId)
                            .setTenantId(tenantId)
                            .addAllMsgPack(entry.getValue())
                            .setOrderKey(orderKey)
                            .build();
                        return distWorkerClient.query(rangeReplica.storeId, KVRangeRORequest.newBuilder()
                                .setReqId(reqId)
                                .setVer(rangeReplica.ver)
                                .setKvRangeId(rangeReplica.id)
                                .setRoCoProc(ROCoProcInput.newBuilder()
                                    .setDistService(DistServiceROCoProcInput.newBuilder()
                                        .setTenantDist(tenantDistRequest)
                                        .build())
                                    .build())
                                .build(), tenantDistRequest.getOrderKey())
                            .thenApply(v -> {
                                if (v.getCode() == ReplyCode.Ok) {
                                    TenantDistReply tenantDistReply = v.getRoCoProcResult()
                                        .getDistService()
                                        .getTenantDist();
                                    assert tenantDistReply.getReqId() == reqId;
                                    return tenantDistReply;
                                }
                                log.warn("Failed to exec ro co-proc[code={}]", v.getCode());
                                throw new RuntimeException("Failed to exec rw co-proc");
                            });
                    })
                    .toArray(CompletableFuture[]::new);
                return CompletableFuture.allOf(distReplyFutures)
                    .handle((v, e) -> {
                        CallTask<DistWorkerCall, Map<String, Integer>, DistCallScheduler.BatcherKey> task;
                        if (e != null) {
                            while ((task = tasks.poll()) != null) {
                                task.callResult.completeExceptionally(e);
                            }
                        } else {
                            // aggregate fanout from each reply
                            Map<String, Integer> allTopicFanouts = new HashMap<>();
                            for (CompletableFuture<TenantDistReply> replyFuture : distReplyFutures) {
                                TenantDistReply reply = replyFuture.join();
                                reply.getResultsMap()
                                    .forEach((topic, result) -> allTopicFanouts.compute(topic, (k, f) -> {
                                        if (f == null) {
                                            f = 0;
                                        }
                                        switch (result.getCode()) {
                                            case OK -> {
                                                if (f >= 0) {
                                                    f += result.getFanout();
                                                }
                                            }
                                            // -1 stands for dist error
                                            case ERROR -> f = -1;
                                        }
                                        return f;
                                    }));
                            }
                            while ((task = tasks.poll()) != null) {
                                Map<String, Integer> topicFanouts = new HashMap<>();
                                task.call.publisherMsgPacks.forEach(clientMessagePack ->
                                    clientMessagePack.getMessagePackList().forEach(topicMessagePack ->
                                        topicFanouts.put(topicMessagePack.getTopic(),
                                            allTopicFanouts.getOrDefault(topicMessagePack.getTopic(), 0))));
                                task.callResult.complete(topicFanouts);
                            }
                        }
                        return null;
                    });
            }
        }

        private record KVRangeReplica(KVRangeId id, long ver, String storeId) {
        }
    }
}
