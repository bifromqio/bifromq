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

package com.baidu.bifromq.dist.server.scheduler;

import static com.baidu.bifromq.dist.entity.EntityUtil.matchRecordKeyPrefix;
import static com.baidu.bifromq.dist.entity.EntityUtil.tenantUpperBound;
import static com.baidu.bifromq.dist.util.MessageUtil.buildBatchDistRequest;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DATA_PLANE_BURST_LATENCY_MS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DATA_PLANE_TOLERABLE_LATENCY_MS;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.proto.Range;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.basescheduler.ICallScheduler;
import com.baidu.bifromq.dist.rpc.proto.BatchDistRequest;
import com.baidu.bifromq.dist.rpc.proto.BatchDistReply;
import com.baidu.bifromq.dist.rpc.proto.DistPack;
import com.baidu.bifromq.dist.rpc.proto.DistServiceROCoProcOutput;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DistCallScheduler extends BatchCallScheduler<DistWorkerCall, Map<String, Integer>, Integer>
    implements IDistCallScheduler {
    private final IBaseKVStoreClient distWorkerClient;

    public DistCallScheduler(ICallScheduler<DistWorkerCall> reqScheduler,
                             IBaseKVStoreClient distWorkerClient) {
        super("dist_server_dist_batcher", reqScheduler, Duration.ofMillis(DATA_PLANE_TOLERABLE_LATENCY_MS.get()),
            Duration.ofMillis(DATA_PLANE_BURST_LATENCY_MS.get()));
        this.distWorkerClient = distWorkerClient;
    }

    @Override
    protected Batcher<DistWorkerCall, Map<String, Integer>, Integer> newBatcher(String name,
                                                                                long tolerableLatencyNanos,
                                                                                long burstLatencyNanos,
                                                                                Integer batchKey) {
        return new DistWorkerCallBatcher(batchKey, name, tolerableLatencyNanos, burstLatencyNanos, distWorkerClient);
    }

    @Override
    protected Optional<Integer> find(DistWorkerCall request) {
        return Optional.of(request.callQueueIdx);
    }

    private static class DistWorkerCallBatcher extends Batcher<DistWorkerCall, Map<String, Integer>, Integer> {
        private final IBaseKVStoreClient distWorkerClient;
        private final String orderKey = UUID.randomUUID().toString();

        protected DistWorkerCallBatcher(Integer batcherKey, String name,
                                        long tolerableLatencyNanos,
                                        long burstLatencyNanos,
                                        IBaseKVStoreClient distWorkerClient) {
            super(batcherKey, name, tolerableLatencyNanos, burstLatencyNanos);
            this.distWorkerClient = distWorkerClient;
        }

        @Override
        protected IBatchCall<DistWorkerCall, Map<String, Integer>> newBatch() {
            return new BatchDistCall();
        }

        private class BatchDistCall implements IBatchCall<DistWorkerCall, Map<String, Integer>> {
            private final Queue<CallTask<DistWorkerCall, Map<String, Integer>>> tasks = new ArrayDeque<>(128);
            private Map<String, Map<String, Map<ClientInfo, Iterable<Message>>>> batch = new HashMap<>(128);

            @Override
            public void reset() {
                batch = new HashMap<>(128);
            }

            @Override
            public void add(CallTask<DistWorkerCall, Map<String, Integer>> callTask) {
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
                Map<String, DistPack> distPackMap = buildDistPack();
                Map<KVRangeSetting, List<DistPack>> distPacksByRange = new HashMap<>();
                distPackMap.forEach((tenantId, distPack) -> {
                    List<KVRangeSetting> ranges = distWorkerClient.findByRange(Range.newBuilder()
                        .setStartKey(matchRecordKeyPrefix(tenantId))
                        .setEndKey(tenantUpperBound(tenantId))
                        .build());
                    ranges.forEach(range ->
                        distPacksByRange.computeIfAbsent(range, k -> new LinkedList<>()).add(distPack));
                });

                long reqId = System.nanoTime();
                List<CompletableFuture<BatchDistReply>> distReplyFutures = distPacksByRange.entrySet().stream()
                    .map(entry -> {
                        KVRangeSetting range = entry.getKey();
                        BatchDistRequest batchDist = BatchDistRequest.newBuilder()
                            .setReqId(reqId)
                            .addAllDistPack(entry.getValue())
                            .setOrderKey(orderKey)
                            .build();
                        return distWorkerClient.query(selectStore(range), KVRangeRORequest.newBuilder()
                                .setReqId(reqId)
                                .setVer(range.ver)
                                .setKvRangeId(range.id)
                                .setRoCoProcInput(buildBatchDistRequest(batchDist).toByteString())
                                .build(), batchDist.getOrderKey())
                            .thenApply(v -> {
                                if (v.getCode() == ReplyCode.Ok) {
                                    try {
                                        BatchDistReply batchDistReply =
                                            DistServiceROCoProcOutput.parseFrom(
                                                    v.getRoCoProcResult())
                                                .getBatchDist();
                                        assert batchDistReply.getReqId() == reqId;
                                        return batchDistReply;
                                    } catch (Throwable e) {
                                        log.error("Unable to parse ro co-proc output", e);
                                        throw new RuntimeException("Unable to parse rw co-proc output",
                                            e);
                                    }
                                }
                                log.warn("Failed to exec ro co-proc[code={}]", v.getCode());
                                throw new RuntimeException("Failed to exec rw co-proc");
                            });
                    })
                    .toList();
                return CompletableFuture.allOf(distReplyFutures.toArray(CompletableFuture[]::new))
                    .thenApply(v -> distReplyFutures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()))
                    .handle((replyList, e) -> {
                        CallTask<DistWorkerCall, Map<String, Integer>> task;
                        if (e != null) {
                            while ((task = tasks.poll()) != null) {
                                task.callResult.completeExceptionally(e);
                            }
                        } else {
                            // aggregate fanout from each reply
                            Map<String, Map<String, Integer>> topicFanoutByTenant = new HashMap<>();
                            replyList.forEach(reply -> reply.getResultMap()
                                .forEach((tenantId, topicFanout) -> {
                                    topicFanoutByTenant.computeIfAbsent(tenantId, k -> new HashMap<>());
                                    topicFanout.getFanoutMap()
                                        .forEach((topic, fanout) -> topicFanoutByTenant.get(tenantId)
                                            .compute(topic, (k, v) -> {
                                                if (v == null) {
                                                    v = 0;
                                                }
                                                v += fanout;
                                                return v;
                                            }));
                                }));
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

            private String selectStore(KVRangeSetting setting) {
                return setting.allReplicas.get(ThreadLocalRandom.current().nextInt(setting.allReplicas.size()));
            }

            private Map<String, DistPack> buildDistPack() {
                Map<String, DistPack> distPackMap = new HashMap<>();
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
                    distPackMap.put(tenantId, distPackBuilder.build());
                });
                return distPackMap;
            }
        }
    }
}
