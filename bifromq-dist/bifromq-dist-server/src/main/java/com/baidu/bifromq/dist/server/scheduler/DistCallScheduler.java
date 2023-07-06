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
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DIST_MAX_TOPICS_IN_BATCH;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DIST_SERVER_MAX_INFLIGHT_CALLS_PER_QUEUE;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.proto.Range;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basescheduler.BatchCallBuilder;
import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.basescheduler.ICallScheduler;
import com.baidu.bifromq.dist.entity.EntityUtil;
import com.baidu.bifromq.dist.rpc.proto.BatchDist;
import com.baidu.bifromq.dist.rpc.proto.BatchDistReply;
import com.baidu.bifromq.dist.rpc.proto.DistPack;
import com.baidu.bifromq.dist.rpc.proto.DistServiceROCoProcOutput;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.common.collect.Iterables;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jctools.queues.MpscUnboundedArrayQueue;

@Slf4j
public class DistCallScheduler extends BatchCallScheduler<DistCall, Map<String, Integer>, Integer> {
    private final int maxBatchTopics;
    private final IBaseKVStoreClient kvStoreClient;
    private final DistributionSummary batchTopicSummary;

    public DistCallScheduler(IBaseKVStoreClient kvStoreClient,
                             ICallScheduler<DistCall> callScheduler) {
        super("dist_server_dist_batcher", DIST_SERVER_MAX_INFLIGHT_CALLS_PER_QUEUE.get(), callScheduler);
        maxBatchTopics = DIST_MAX_TOPICS_IN_BATCH.get();
        this.kvStoreClient = kvStoreClient;
        batchTopicSummary = DistributionSummary.builder("dist.server.batch.topics")
            .register(Metrics.globalRegistry);
    }

    @Override
    public void close() {
        super.close();
        Metrics.globalRegistry.remove(batchTopicSummary);
    }

    @Override
    protected BatchCallBuilder<DistCall, Map<String, Integer>> newBuilder(String name, int maxInflights,
                                                                          Integer batchKey) {
        return new DistWorkerCallBuilder(name, maxInflights, kvStoreClient);
    }

    @Override
    protected Optional<Integer> find(DistCall request) {
        return Optional.of(request.callQueueIdx);
    }

    private class DistWorkerCallBuilder extends BatchCallBuilder<DistCall, Map<String, Integer>> {
        private class DistWorkerCall implements IBatchCall<DistCall, Map<String, Integer>> {
            private final AtomicInteger msgCount = new AtomicInteger();
            private final AtomicInteger topicCount = new AtomicInteger();
            private final Queue<DistTask> tasks = new MpscUnboundedArrayQueue<>(128);
            // key: tenantId, subKey: topic
            private final Map<String, Map<String, Map<ClientInfo, Iterable<Message>>>> batch =
                new ConcurrentHashMap<>();

            @Override
            public boolean isEmpty() {
                return tasks.isEmpty();
            }

            @Override
            public boolean isEnough() {
                return topicCount.get() > maxBatchTopics;
            }

            @Override
            public CompletableFuture<Map<String, Integer>> add(DistCall dist) {
                DistTask task = new DistTask(dist);
                Map<String, Map<ClientInfo, Iterable<Message>>> clientMsgsByTopic =
                    batch.computeIfAbsent(dist.tenatId, k -> new ConcurrentHashMap<>());
                dist.senderMsgPacks.forEach(senderMsgPack ->
                    senderMsgPack.getMessagePackList().forEach(topicMsgs ->
                        clientMsgsByTopic.computeIfAbsent(topicMsgs.getTopic(), k -> {
                                topicCount.incrementAndGet();
                                return new ConcurrentHashMap<>();
                            })
                            .compute(senderMsgPack.getSender(), (k, v) -> {
                                if (v == null) {
                                    v = topicMsgs.getMessageList();
                                } else {
                                    v = Iterables.concat(v, topicMsgs.getMessageList());
                                }
                                msgCount.addAndGet(topicMsgs.getMessageCount());
                                return v;
                            })));
                tasks.add(task);
                return task.onDone;
            }

            @Override
            public void reset() {
                msgCount.set(0);
                topicCount.set(0);
                batch.clear();
            }

            @Override
            public CompletableFuture<Void> execute() {
                Map<String, DistPack> distPackMap = buildDistPack();
                Map<KVRangeSetting, List<DistPack>> distPacksByRange = new HashMap<>();
                distPackMap.forEach((tenantId, distPack) -> {
                    List<KVRangeSetting> ranges = kvStoreClient.findByRange(Range.newBuilder()
                        .setStartKey(matchRecordKeyPrefix(tenantId))
                        .setEndKey(tenantUpperBound(tenantId))
                        .build());
                    ranges.forEach(range ->
                        distPacksByRange.computeIfAbsent(range, k -> new LinkedList<>()).add(distPack));
                });
                batchTopicSummary.record(topicCount.get());

                long reqId = System.nanoTime();
                List<CompletableFuture<BatchDistReply>> distReplyFutures = distPacksByRange.entrySet().stream()
                    .map(entry -> {
                        KVRangeSetting range = entry.getKey();
                        BatchDist batchDist = BatchDist.newBuilder()
                            .setReqId(reqId)
                            .addAllDistPack(entry.getValue())
                            .setOrderKey(orderKey)
                            .build();
                        return kvStoreClient.query(selectStore(range), KVRangeRORequest.newBuilder()
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
                                                .getDistReply();
                                        assert batchDistReply.getReqId() == reqId;
                                        return batchDistReply;
                                    } catch (Throwable e) {
                                        log.error("Unable to parse rw co-proc output", e);
                                        throw new RuntimeException("Unable to parse rw co-proc output",
                                            e);
                                    }
                                }
                                log.warn("Failed to exec rw co-proc[code={}]", v.getCode());
                                throw new RuntimeException("Failed to exec rw co-proc");
                            });
                    })
                    .collect(Collectors.toList());
                return CompletableFuture.allOf(distReplyFutures.toArray(CompletableFuture[]::new))
                    .thenApply(v -> distReplyFutures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()))
                    .handle((replyList, e) -> {
                        DistTask task;
                        if (e != null) {
                            while ((task = tasks.poll()) != null) {
                                task.onDone.completeExceptionally(e);
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
                                    topicFanoutByTenant.get(task.distCall.tenatId);
                                Map<String, Integer> topicFanouts = new HashMap<>();
                                task.distCall.senderMsgPacks.forEach(clientMessagePack ->
                                    clientMessagePack.getMessagePackList().forEach(topicMessagePack ->
                                        topicFanouts.put(topicMessagePack.getTopic(),
                                            allTopicFanouts.getOrDefault(topicMessagePack.getTopic(), 0))));
                                task.onDone.complete(topicFanouts);
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
                            topicMsgPackBuilder.addMessage(TopicMessagePack.SenderMessagePack
                                .newBuilder()
                                .setSender(sender)
                                .addAllMessage(msgs)
                                .build()));
                        distPackBuilder.addMsgPack(topicMsgPackBuilder.build());
                    });
                    distPackMap.put(tenantId, distPackBuilder.build());
                });
                return distPackMap;
            }
        }

        private final IBaseKVStoreClient kvStoreClient;
        private final String orderKey;

        DistWorkerCallBuilder(String name, int maxInflights, IBaseKVStoreClient kvStoreClient) {
            super(name, maxInflights);
            this.kvStoreClient = kvStoreClient;
            this.orderKey = UUID.randomUUID().toString();
        }

        @Override
        public DistWorkerCall newBatch() {
            return new DistWorkerCall();
        }
    }

    @AllArgsConstructor
    private static class DistTask {
        final DistCall distCall;
        final CompletableFuture<Map<String, Integer>> onDone = new CompletableFuture<>();
    }
}
