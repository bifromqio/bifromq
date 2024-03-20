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
import static com.baidu.bifromq.dist.util.MessageUtil.buildBatchDistRequest;
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
import com.baidu.bifromq.dist.rpc.proto.BatchDistReply;
import com.baidu.bifromq.dist.rpc.proto.BatchDistRequest;
import com.baidu.bifromq.dist.rpc.proto.DistPack;
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
public class DistCallScheduler extends BatchCallScheduler<DistWorkerCall, Map<String, Integer>, Integer>
    implements IDistCallScheduler {
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
    protected Batcher<DistWorkerCall, Map<String, Integer>, Integer> newBatcher(String name,
                                                                                long tolerableLatencyNanos,
                                                                                long burstLatencyNanos,
                                                                                Integer batchKey) {
        return new DistWorkerCallBatcher(batchKey, name, tolerableLatencyNanos, burstLatencyNanos,
            fanoutSplitThreshold,
            distWorkerClient,
            tenantFanoutGetter);
    }

    @Override
    protected Optional<Integer> find(DistWorkerCall request) {
        return Optional.of(request.callQueueIdx);
    }

    private static class DistWorkerCallBatcher extends Batcher<DistWorkerCall, Map<String, Integer>, Integer> {
        private final IBaseKVStoreClient distWorkerClient;
        private final String orderKey = UUID.randomUUID().toString();
        private final Function<String, Integer> tenantFanoutGetter;
        private final int fanoutSplitThreshold;

        protected DistWorkerCallBatcher(Integer batcherKey, String name,
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
        protected IBatchCall<DistWorkerCall, Map<String, Integer>, Integer> newBatch() {
            return new BatchDistCall();
        }

        private class BatchDistCall implements IBatchCall<DistWorkerCall, Map<String, Integer>, Integer> {
            private final Queue<CallTask<DistWorkerCall, Map<String, Integer>, Integer>> tasks = new ArrayDeque<>(128);
            private Map<String, Map<String, Map<ClientInfo, Iterable<Message>>>> batch = new HashMap<>(128);

            @Override
            public void reset() {
                batch = new HashMap<>(128);
            }

            @Override
            public void add(CallTask<DistWorkerCall, Map<String, Integer>, Integer> callTask) {
                Map<String, Map<ClientInfo, Iterable<Message>>> clientMsgsByTopic =
                    batch.computeIfAbsent(callTask.call.tenantId, k -> new HashMap<>());
                callTask.call.publisherMsgPacks.forEach(senderMsgPack ->
                    senderMsgPack.getMessagePackList().forEach(topicMsgs ->
                        clientMsgsByTopic.computeIfAbsent(topicMsgs.getTopic(), k -> new HashMap<>())
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
                Map<KVRangeReplica, List<DistPack>> distPacksByRangeReplica = new HashMap<>();
                batch.forEach((tenantId, topicMap) -> {
                    DistPack.Builder distPackBuilder = DistPack.newBuilder().setTenantId(tenantId);
                    topicMap.forEach((topic, senderMap) -> {
                        TopicMessagePack.Builder topicMsgPackBuilder = TopicMessagePack.newBuilder().setTopic(topic);
                        senderMap.forEach((sender, msgs) ->
                            topicMsgPackBuilder.addMessage(TopicMessagePack.PublisherPack
                                .newBuilder()
                                .setPublisher(sender)
                                .addAllMessage(msgs)
                                .build()));
                        distPackBuilder.addMsgPack(topicMsgPackBuilder.build());
                    });
                    DistPack distPack = distPackBuilder.build();
                    int fanoutScale = tenantFanoutGetter.apply(tenantId);
                    List<KVRangeSetting> ranges = distWorkerClient.findByBoundary(Boundary.newBuilder()
                        .setStartKey(matchRecordKeyPrefix(tenantId))
                        .setEndKey(tenantUpperBound(tenantId))
                        .build());
                    if (fanoutScale > fanoutSplitThreshold) {
                        ranges.forEach(range -> distPacksByRangeReplica.computeIfAbsent(
                            new KVRangeReplica(range.id, range.ver, range.leader),
                            k -> new LinkedList<>()).add(distPack));
                    } else {
                        ranges.forEach(range -> distPacksByRangeReplica.computeIfAbsent(
                            new KVRangeReplica(range.id, range.ver, range.randomReplica()),
                            k -> new LinkedList<>()).add(distPack));
                    }
                });

                long reqId = System.nanoTime();
                @SuppressWarnings("unchecked")
                CompletableFuture<BatchDistReply>[] distReplyFutures = distPacksByRangeReplica.entrySet().stream()
                    .map(entry -> {
                        KVRangeReplica rangeReplica = entry.getKey();
                        BatchDistRequest batchDist = BatchDistRequest.newBuilder()
                            .setReqId(reqId)
                            .addAllDistPack(entry.getValue())
                            .setOrderKey(orderKey)
                            .build();
                        return distWorkerClient.query(rangeReplica.storeId, KVRangeRORequest.newBuilder()
                                .setReqId(reqId)
                                .setVer(rangeReplica.ver)
                                .setKvRangeId(rangeReplica.id)
                                .setRoCoProc(ROCoProcInput.newBuilder()
                                    .setDistService(buildBatchDistRequest(batchDist))
                                    .build())
                                .build(), batchDist.getOrderKey())
                            .thenApply(v -> {
                                if (v.getCode() == ReplyCode.Ok) {
                                    BatchDistReply batchDistReply = v.getRoCoProcResult()
                                        .getDistService()
                                        .getBatchDist();
                                    assert batchDistReply.getReqId() == reqId;
                                    return batchDistReply;
                                }
                                log.warn("Failed to exec ro co-proc[code={}]", v.getCode());
                                throw new RuntimeException("Failed to exec rw co-proc");
                            });
                    })
                    .toArray(CompletableFuture[]::new);
                return CompletableFuture.allOf(distReplyFutures)
                    .handle((v, e) -> {
                        CallTask<DistWorkerCall, Map<String, Integer>, Integer> task;
                        if (e != null) {
                            while ((task = tasks.poll()) != null) {
                                task.callResult.completeExceptionally(e);
                            }
                        } else {
                            // aggregate fanout from each reply
                            Map<String, Map<String, Integer>> topicFanoutByTenant = new HashMap<>();
                            for (CompletableFuture<BatchDistReply> replyFuture : distReplyFutures) {
                                BatchDistReply reply = replyFuture.join();
                                reply.getResultMap().forEach((tenantId, topicFanout) -> {
                                    topicFanoutByTenant.computeIfAbsent(tenantId, k -> new HashMap<>());
                                    topicFanout.getFanoutMap()
                                        .forEach((topic, fanout) -> topicFanoutByTenant.get(tenantId)
                                            .compute(topic, (k, val) -> {
                                                if (val == null) {
                                                    val = 0;
                                                }
                                                val += fanout;
                                                return val;
                                            }));
                                });
                            }
                            while ((task = tasks.poll()) != null) {
                                Map<String, Integer> allTopicFanouts =
                                    topicFanoutByTenant.get(task.call.tenantId);
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
