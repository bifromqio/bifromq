/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

import static com.baidu.bifromq.basekv.client.KVRangeRouterUtil.findByBoundary;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static com.baidu.bifromq.dist.worker.schema.KVSchemaUtil.tenantBeginKey;
import static com.baidu.bifromq.util.TopicConst.NUL;
import static com.baidu.bifromq.util.TopicUtil.fastJoin;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.KVRangeSetting;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.basescheduler.ICallTask;
import com.baidu.bifromq.dist.rpc.proto.BatchDistReply;
import com.baidu.bifromq.dist.rpc.proto.BatchDistRequest;
import com.baidu.bifromq.dist.rpc.proto.DistPack;
import com.baidu.bifromq.dist.rpc.proto.DistServiceROCoProcInput;
import com.baidu.bifromq.dist.rpc.proto.Fact;
import com.baidu.bifromq.dist.trie.TopicFilterIterator;
import com.baidu.bifromq.dist.trie.TopicTrieNode;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.TopicMessagePack;
import com.baidu.bifromq.util.TopicUtil;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BatchDistServerCall
    implements IBatchCall<DistServerCall, Map<String, Optional<Integer>>, DistServerCallBatcherKey> {
    private final IBaseKVStoreClient distWorkerClient;
    private final DistServerCallBatcherKey batcherKey;
    private final String orderKey;
    private final Queue<ICallTask<DistServerCall, Map<String, Optional<Integer>>, DistServerCallBatcherKey>> tasks =
        new ArrayDeque<>(128);
    private Map<String, Map<ClientInfo, Iterable<Message>>> batch = new HashMap<>(128);
    private TopicTrieNode.Builder<String> topicTrieBuilder = TopicTrieNode.builder(true);

    BatchDistServerCall(IBaseKVStoreClient distWorkerClient, DistServerCallBatcherKey batcherKey) {
        this.distWorkerClient = distWorkerClient;
        this.batcherKey = batcherKey;
        this.orderKey = batcherKey.tenantId() + batcherKey.batcherId();
    }

    @Override
    public void reset() {
        batch = new HashMap<>(128);
        topicTrieBuilder = TopicTrieNode.builder(true);
    }

    @Override
    public void add(ICallTask<DistServerCall, Map<String, Optional<Integer>>, DistServerCallBatcherKey> callTask) {
        callTask.call().publisherMessagePacks().forEach(publisherMsgPack -> publisherMsgPack.getMessagePackList()
            .forEach(topicMsgs -> batch.computeIfAbsent(topicMsgs.getTopic(), k -> {
                topicTrieBuilder.addTopic(TopicUtil.parse(batcherKey.tenantId(), k, false), k);
                return new HashMap<>();
            }).compute(publisherMsgPack.getPublisher(), (k, v) -> {
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
        long reqId = System.nanoTime();
        Collection<KVRangeSetting> candidates = rangeLookup();
        if (candidates.isEmpty()) {
            // no candidate range
            ICallTask<DistServerCall, Map<String, Optional<Integer>>, DistServerCallBatcherKey> task;
            while ((task = tasks.poll()) != null) {
                Map<String, Optional<Integer>> fanOutResult = new HashMap<>();
                task.call().publisherMessagePacks().forEach(clientMessagePack -> clientMessagePack.getMessagePackList()
                    .forEach(topicMessagePack -> fanOutResult.put(topicMessagePack.getTopic(), Optional.of(0))));
                task.resultPromise().complete(fanOutResult);
            }
            return CompletableFuture.completedFuture(null);
        } else {
            @SuppressWarnings("unchecked") CompletableFuture<BatchDistReply>[] distReplyFutures =
                replicaSelect(candidates).entrySet().stream().map(entry -> {
                    KVRangeReplica rangeReplica = entry.getKey();
                    Map<String, Map<ClientInfo, Iterable<Message>>> replicaBatch = entry.getValue();
                    BatchDistRequest.Builder batchDistBuilder =
                        BatchDistRequest.newBuilder().setReqId(reqId).setOrderKey(orderKey);
                    replicaBatch.forEach((topic, publisherMsgs) -> {
                        String tenantId = batcherKey.tenantId();
                        DistPack.Builder distPackBuilder = DistPack.newBuilder().setTenantId(tenantId);
                        TopicMessagePack.Builder topicMsgPackBuilder = TopicMessagePack.newBuilder().setTopic(topic);
                        publisherMsgs.forEach((publisher, msgs) -> topicMsgPackBuilder.addMessage(
                            TopicMessagePack.PublisherPack.newBuilder().setPublisher(publisher).addAllMessage(msgs)
                                .build()));
                        distPackBuilder.addMsgPack(topicMsgPackBuilder.build());
                        batchDistBuilder.addDistPack(distPackBuilder.build());
                    });
                    return distWorkerClient.query(rangeReplica.storeId,
                            KVRangeRORequest.newBuilder()
                                .setReqId(reqId)
                                .setVer(rangeReplica.ver)
                                .setKvRangeId(rangeReplica.id)
                                .setRoCoProc(ROCoProcInput.newBuilder()
                                    .setDistService(DistServiceROCoProcInput.newBuilder()
                                        .setBatchDist(batchDistBuilder.build())
                                        .build())
                                    .build()).build(), orderKey)
                        .thenApply(v -> {
                            if (v.getCode() == ReplyCode.Ok) {
                                BatchDistReply batchDistReply = v.getRoCoProcResult().getDistService().getBatchDist();
                                assert batchDistReply.getReqId() == reqId;
                                return batchDistReply;
                            }
                            log.warn("Failed to exec ro co-proc[code={}]", v.getCode());
                            throw new RuntimeException("Failed to exec rw co-proc");
                        });
                }).toArray(CompletableFuture[]::new);
            return CompletableFuture.allOf(distReplyFutures).handle((v, e) -> {
                ICallTask<DistServerCall, Map<String, Optional<Integer>>, DistServerCallBatcherKey> task;
                if (e != null) {
                    while ((task = tasks.poll()) != null) {
                        task.resultPromise().completeExceptionally(e);
                    }
                } else {
                    // aggregate fanout from each reply
                    Map<String, Integer> allFanOutByTopic = new HashMap<>();
                    for (CompletableFuture<BatchDistReply> replyFuture : distReplyFutures) {
                        BatchDistReply reply = replyFuture.join();
                        for (String tenantId : reply.getResultMap().keySet()) {
                            assert tenantId.equals(batcherKey.tenantId());
                            Map<String, Integer> topicFanOut = reply.getResultMap().get(tenantId).getFanoutMap();
                            topicFanOut.forEach((topic, fanOut) -> allFanOutByTopic.compute(topic, (k, val) -> {
                                if (val == null) {
                                    val = 0;
                                }
                                val += fanOut;
                                return val;
                            }));

                        }
                    }
                    while ((task = tasks.poll()) != null) {
                        Map<String, Optional<Integer>> fanoutResult = new HashMap<>();
                        task.call().publisherMessagePacks().forEach(
                            clientMessagePack -> clientMessagePack.getMessagePackList().forEach(
                                topicMessagePack -> fanoutResult.put(topicMessagePack.getTopic(),
                                    Optional.ofNullable(allFanOutByTopic.get(topicMessagePack.getTopic())))));
                        task.resultPromise().complete(fanoutResult);
                    }
                }
                return null;
            });
        }
    }

    private Collection<KVRangeSetting> rangeLookup() {
        ByteString tenantStartKey = tenantBeginKey(batcherKey.tenantId());
        Collection<KVRangeSetting> allCandidates =
            findByBoundary(toBoundary(tenantStartKey, upperBound(tenantStartKey)),
                distWorkerClient.latestEffectiveRouter());
        TopicFilterIterator<String> topicFilterIterator = new TopicFilterIterator<>(topicTrieBuilder.build());
        List<KVRangeSetting> finalCandidates = new LinkedList<>();
        for (KVRangeSetting candidate : allCandidates) {
            Optional<Fact> factOpt = candidate.getFact(Fact.class);
            if (factOpt.isEmpty()) {
                finalCandidates.add(candidate);
                continue;
            }
            Fact fact = factOpt.get();
            if (!fact.hasFirstGlobalFilterLevels() || !fact.hasLastGlobalFilterLevels()) {
                // range is empty
                continue;
            }
            List<String> firstFilterLevels = fact.getFirstGlobalFilterLevels().getFilterLevelList();
            List<String> lastFilterLevels = fact.getLastGlobalFilterLevels().getFilterLevelList();
            topicFilterIterator.seek(firstFilterLevels);
            if (topicFilterIterator.isValid()) {
                // firstTopicFilter <= nextTopicFilter
                if (topicFilterIterator.key().equals(firstFilterLevels)
                    || fastJoin(NUL, topicFilterIterator.key()).compareTo(fastJoin(NUL, lastFilterLevels)) <= 0) {
                    // if firstTopicFilter == nextTopicFilter || nextFilterLevels <= lastFilterLevels
                    // add to finalCandidates
                    finalCandidates.add(candidate);
                }
            } else {
                // endTopicFilter < firstTopicFilter, stop
                break;
            }
        }
        return finalCandidates;
    }

    private Map<KVRangeReplica, Map<String, Map<ClientInfo, Iterable<Message>>>> replicaSelect(
        Collection<KVRangeSetting> candidates) {
        Map<KVRangeReplica, Map<String, Map<ClientInfo, Iterable<Message>>>> batchByReplica = new HashMap<>();
        for (KVRangeSetting rangeSetting : candidates) {
            if (rangeSetting.hasInProcReplica() || rangeSetting.allReplicas.size() == 1) {
                // build-in or single replica
                KVRangeReplica replica =
                    new KVRangeReplica(rangeSetting.id, rangeSetting.ver, rangeSetting.randomReplica());
                batchByReplica.put(replica, batch);
            } else {
                for (String topic : batch.keySet()) {
                    // bind replica based on tenantId, topic
                    int hash = Objects.hash(batcherKey.tenantId(), topic);
                    int replicaIdx = Math.abs(hash) % rangeSetting.allReplicas.size();
                    // replica bind
                    KVRangeReplica replica =
                        new KVRangeReplica(rangeSetting.id, rangeSetting.ver, rangeSetting.allReplicas.get(replicaIdx));
                    batchByReplica.computeIfAbsent(replica, k -> new HashMap<>()).put(topic, batch.get(topic));
                }
            }
        }
        return batchByReplica;
    }

    private record KVRangeReplica(KVRangeId id, long ver, String storeId) {
    }
}
