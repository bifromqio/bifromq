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

import static com.baidu.bifromq.basekv.client.KVRangeRouterUtil.findByKey;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static com.baidu.bifromq.dist.entity.EntityUtil.matchRecordKeyPrefix;
import static com.baidu.bifromq.dist.entity.EntityUtil.tenantUpperBound;
import static com.baidu.bifromq.util.TopicConst.NUL;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.KVRangeRouterUtil;
import com.baidu.bifromq.basekv.client.KVRangeSetting;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.basescheduler.ICallScheduler;
import com.baidu.bifromq.basescheduler.ICallTask;
import com.baidu.bifromq.dist.entity.EntityUtil;
import com.baidu.bifromq.dist.rpc.proto.BatchDistReply;
import com.baidu.bifromq.dist.rpc.proto.BatchDistRequest;
import com.baidu.bifromq.dist.rpc.proto.DistPack;
import com.baidu.bifromq.dist.rpc.proto.DistServiceROCoProcInput;
import com.baidu.bifromq.dist.trie.TopicFilterIterator;
import com.baidu.bifromq.dist.trie.TopicTrieNode;
import com.baidu.bifromq.dist.util.TopicUtil;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.sysprops.props.DataPlaneBurstLatencyMillis;
import com.baidu.bifromq.sysprops.props.DataPlaneTolerableLatencyMillis;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.common.collect.Iterables;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DistCallScheduler extends BatchCallScheduler<DistWorkerCall, Map<String, Integer>, Integer>
    implements IDistCallScheduler {
    private final ISettingProvider settingProvider;
    private final IBaseKVStoreClient distWorkerClient;

    public DistCallScheduler(ICallScheduler<DistWorkerCall> reqScheduler,
                             IBaseKVStoreClient distWorkerClient,
                             ISettingProvider settingProvider) {
        super("dist_server_dist_batcher", reqScheduler,
            Duration.ofMillis(DataPlaneTolerableLatencyMillis.INSTANCE.get()),
            Duration.ofMillis(DataPlaneBurstLatencyMillis.INSTANCE.get()));
        this.settingProvider = settingProvider;
        this.distWorkerClient = distWorkerClient;
    }

    @Override
    protected Batcher<DistWorkerCall, Map<String, Integer>, Integer> newBatcher(String name,
                                                                                long tolerableLatencyNanos,
                                                                                long burstLatencyNanos,
                                                                                Integer batchKey) {
        return new DistWorkerCallBatcher(batchKey, name, tolerableLatencyNanos, burstLatencyNanos,
            distWorkerClient, settingProvider);
    }

    @Override
    protected Optional<Integer> find(DistWorkerCall request) {
        return Optional.of(request.callQueueIdx);
    }

    private static class DistWorkerCallBatcher extends Batcher<DistWorkerCall, Map<String, Integer>, Integer> {
        private final ISettingProvider settingProvider;
        private final IBaseKVStoreClient distWorkerClient;
        private final String orderKey = UUID.randomUUID().toString();

        protected DistWorkerCallBatcher(Integer batcherKey, String name,
                                        long tolerableLatencyNanos,
                                        long burstLatencyNanos,
                                        IBaseKVStoreClient distWorkerClient,
                                        ISettingProvider settingProvider) {
            super(batcherKey, name, tolerableLatencyNanos, burstLatencyNanos);
            this.settingProvider = settingProvider;
            this.distWorkerClient = distWorkerClient;
        }

        @Override
        protected IBatchCall<DistWorkerCall, Map<String, Integer>, Integer> newBatch() {
            return new BatchDistCall();
        }

        private class BatchDistCall implements IBatchCall<DistWorkerCall, Map<String, Integer>, Integer> {
            private final Queue<ICallTask<DistWorkerCall, Map<String, Integer>, Integer>> tasks = new ArrayDeque<>(128);
            private Map<GlobalTopic, Map<ClientInfo, Iterable<Message>>> batch = new HashMap<>(128);
            private Map<String, Set<GlobalTopic>> topicsByTenantId = new HashMap<>();

            @Override
            public void reset() {
                batch = new HashMap<>(128);
                topicsByTenantId = new HashMap<>();
            }

            @Override
            public void add(ICallTask<DistWorkerCall, Map<String, Integer>, Integer> callTask) {
                callTask.call().publisherMsgPacks.forEach(publisherMsgPack ->
                    publisherMsgPack.getMessagePackList().forEach(topicMsgs -> {
                        GlobalTopic globalTopic = new GlobalTopic(callTask.call().tenantId, topicMsgs.getTopic());
                        topicsByTenantId.computeIfAbsent(callTask.call().tenantId, k -> new HashSet<>())
                            .add(globalTopic);
                        batch.computeIfAbsent(globalTopic, k -> new HashMap<>())
                            .compute(publisherMsgPack.getPublisher(), (k, v) -> {
                                if (v == null) {
                                    v = topicMsgs.getMessageList();
                                } else {
                                    v = Iterables.concat(v, topicMsgs.getMessageList());
                                }
                                return v;
                            });
                    }));
                tasks.add(callTask);
            }

            @Override
            public CompletableFuture<Void> execute() {
                long reqId = System.nanoTime();
                @SuppressWarnings("unchecked")
                CompletableFuture<BatchDistReply>[] distReplyFutures = replicaSelect(rangeLookup())
                    .entrySet()
                    .stream()
                    .map(entry -> {
                        KVRangeReplica rangeReplica = entry.getKey();
                        Map<GlobalTopic, Map<ClientInfo, Iterable<Message>>> replicaBatch = entry.getValue();
                        BatchDistRequest.Builder batchDistBuilder = BatchDistRequest.newBuilder()
                            .setReqId(reqId)
                            .setOrderKey(orderKey);
                        replicaBatch.forEach((globalTopic, publisherMsgs) -> {
                            String tenantId = globalTopic.tenantId;
                            String topic = globalTopic.topic;
                            DistPack.Builder distPackBuilder = DistPack.newBuilder().setTenantId(tenantId);
                            TopicMessagePack.Builder topicMsgPackBuilder =
                                TopicMessagePack.newBuilder().setTopic(topic);
                            publisherMsgs.forEach((publisher, msgs) ->
                                topicMsgPackBuilder.addMessage(TopicMessagePack.PublisherPack
                                    .newBuilder()
                                    .setPublisher(publisher)
                                    .addAllMessage(msgs)
                                    .build()));
                            distPackBuilder.addMsgPack(topicMsgPackBuilder.build());
                            batchDistBuilder.addDistPack(distPackBuilder.build());
                        });
                        return distWorkerClient.query(rangeReplica.storeId, KVRangeRORequest.newBuilder()
                                .setReqId(reqId)
                                .setVer(rangeReplica.ver)
                                .setKvRangeId(rangeReplica.id)
                                .setRoCoProc(ROCoProcInput.newBuilder()
                                    .setDistService(DistServiceROCoProcInput.newBuilder()
                                        .setBatchDist(batchDistBuilder.build())
                                        .build())
                                    .build())
                                .build(), orderKey)
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
                        ICallTask<DistWorkerCall, Map<String, Integer>, Integer> task;
                        if (e != null) {
                            while ((task = tasks.poll()) != null) {
                                task.resultPromise().completeExceptionally(e);
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
                                    topicFanoutByTenant.get(task.call().tenantId);
                                Map<String, Integer> topicFanouts = new HashMap<>();
                                task.call().publisherMsgPacks.forEach(clientMessagePack ->
                                    clientMessagePack.getMessagePackList().forEach(topicMessagePack ->
                                        topicFanouts.put(topicMessagePack.getTopic(),
                                            allTopicFanouts.getOrDefault(topicMessagePack.getTopic(), 0))));
                                task.resultPromise().complete(topicFanouts);
                            }
                        }
                        return null;
                    });
            }

            private Map<KVRangeSetting, Map<GlobalTopic, Map<ClientInfo, Iterable<Message>>>> rangeLookup() {
                Map<KVRangeSetting, Map<GlobalTopic, Map<ClientInfo, Iterable<Message>>>> batchByRange =
                    new HashMap<>();
                if (distWorkerClient.latestEffectiveRouter().containsKey(FULL_BOUNDARY)) {
                    // no splitting
                    assert distWorkerClient.latestEffectiveRouter().size() == 1;
                    batchByRange.put(distWorkerClient.latestEffectiveRouter().get(FULL_BOUNDARY), batch);
                } else {
                    for (String tenantId : topicsByTenantId.keySet()) {
                        List<KVRangeSetting> coveredRanges = KVRangeRouterUtil.findByBoundary(Boundary.newBuilder()
                            .setStartKey(matchRecordKeyPrefix(tenantId))
                            .setEndKey(tenantUpperBound(tenantId))
                            .build(), distWorkerClient.latestEffectiveRouter());
                        if (coveredRanges.size() == 1) {
                            // one range per tenant mode
                            Map<GlobalTopic, Map<ClientInfo, Iterable<Message>>> topicMsgs =
                                batchByRange.computeIfAbsent(coveredRanges.get(0), k -> new HashMap<>());
                            for (GlobalTopic globalTopic : topicsByTenantId.get(tenantId)) {
                                topicMsgs.put(globalTopic, batch.get(globalTopic));
                            }
                            batchByRange.put(coveredRanges.get(0), topicMsgs);
                        } else {
                            // multi ranges per tenant mode
                            for (GlobalTopic globalTopic : topicsByTenantId.get(tenantId)) {
                                String topic = globalTopic.topic;
                                if (!(boolean) settingProvider.provide(Setting.WildcardSubscriptionEnabled, tenantId)) {
                                    // wildcard disabled
                                    Optional<KVRangeSetting> rangeSetting =
                                        findByKey(matchRecordKeyPrefix(tenantId, topic),
                                            distWorkerClient.latestEffectiveRouter());
                                    assert rangeSetting.isPresent();
                                    batchByRange.computeIfAbsent(rangeSetting.get(), k -> new HashMap<>())
                                        .put(globalTopic, batch.get(globalTopic));
                                } else {
                                    // wildcard disabled, rough screening via 'one-pass' scan
                                    Map<ClientInfo, Iterable<Message>> publisherMsg = batch.get(globalTopic);
                                    TopicFilterIterator<String> topicFilterIterator =
                                        new TopicFilterIterator<>(TopicTrieNode.<String>builder(true)
                                            .addTopic(TopicUtil.parse(tenantId, topic, false), topic)
                                            .build());
                                    for (Boundary boundary : distWorkerClient.latestEffectiveRouter().keySet()) {
                                        KVRangeSetting rangeSetting =
                                            distWorkerClient.latestEffectiveRouter().get(boundary);
                                        if (!boundary.hasStartKey()) {
                                            // left open range, must be the first range
                                            EntityUtil.TenantAndEscapedTopicFilter endTenantAndEscapedTopicFilter =
                                                EntityUtil.parseTenantAndEscapedTopicFilter(boundary.getEndKey());
                                            List<String> globalTopicFilter =
                                                TopicUtil.parse(endTenantAndEscapedTopicFilter.tenantId(),
                                                    endTenantAndEscapedTopicFilter.escapedTopicFilter(), true);
                                            topicFilterIterator.seekPrev(globalTopicFilter);
                                            if (topicFilterIterator.isValid()) {
                                                batchByRange.computeIfAbsent(rangeSetting, k -> new HashMap<>())
                                                    .computeIfAbsent(globalTopic, k -> new HashMap<>())
                                                    .putAll(publisherMsg);
                                            }
                                        } else if (!boundary.hasEndKey()) {
                                            // right open range, must be the last range
                                            EntityUtil.TenantAndEscapedTopicFilter startTenantAndEscapedTopicFilter =
                                                EntityUtil.parseTenantAndEscapedTopicFilter(boundary.getStartKey());
                                            List<String> globalTopicFilter =
                                                TopicUtil.parse(startTenantAndEscapedTopicFilter.tenantId(),
                                                    startTenantAndEscapedTopicFilter.escapedTopicFilter(), true);
                                            topicFilterIterator.seek(globalTopicFilter);
                                            if (topicFilterIterator.isValid()) {
                                                batchByRange.computeIfAbsent(rangeSetting, k -> new HashMap<>())
                                                    .computeIfAbsent(globalTopic, k -> new HashMap<>())
                                                    .putAll(publisherMsg);
                                            }
                                        } else {
                                            EntityUtil.TenantAndEscapedTopicFilter startTenantAndEscapedTopicFilter =
                                                EntityUtil.parseTenantAndEscapedTopicFilter(boundary.getStartKey());
                                            List<String> startGlobalTopicFilter =
                                                TopicUtil.parse(startTenantAndEscapedTopicFilter.tenantId(),
                                                    startTenantAndEscapedTopicFilter.escapedTopicFilter(), true);
                                            topicFilterIterator.seek(startGlobalTopicFilter);
                                            if (topicFilterIterator.isValid()) {
                                                String probeTopicFilter =
                                                    TopicUtil.fastJoin(NUL, topicFilterIterator.key());
                                                EntityUtil.TenantAndEscapedTopicFilter endTenantAndEscapedTopicFilter =
                                                    EntityUtil.parseTenantAndEscapedTopicFilter(boundary.getEndKey());
                                                String endTopicFilter =
                                                    endTenantAndEscapedTopicFilter.toGlobalTopicFilter();
                                                if (probeTopicFilter.compareTo(endTopicFilter) < 0) {
                                                    batchByRange.computeIfAbsent(rangeSetting, k -> new HashMap<>())
                                                        .computeIfAbsent(globalTopic, k -> new HashMap<>())
                                                        .putAll(publisherMsg);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                return batchByRange;
            }

            private Map<KVRangeReplica, Map<GlobalTopic, Map<ClientInfo, Iterable<Message>>>> replicaSelect(
                Map<KVRangeSetting, Map<GlobalTopic, Map<ClientInfo, Iterable<Message>>>> batchByRange) {
                Map<KVRangeReplica, Map<GlobalTopic, Map<ClientInfo, Iterable<Message>>>> batchByReplica =
                    new HashMap<>();
                for (KVRangeSetting rangeSetting : batchByRange.keySet()) {
                    Map<GlobalTopic, Map<ClientInfo, Iterable<Message>>> rangeBatch = batchByRange.get(rangeSetting);
                    if (rangeSetting.hasInProcReplica() || rangeSetting.allReplicas.size() == 1) {
                        // build-in or single replica
                        batchByReplica.put(new KVRangeReplica(
                            rangeSetting.id,
                            rangeSetting.ver,
                            rangeSetting.randomReplica()), rangeBatch);
                    } else {
                        // bind replica based on tenant, topic and publisher
                        for (GlobalTopic globalTopic : rangeBatch.keySet()) {
                            Map<ClientInfo, Iterable<Message>> rangePublisherMsgs = rangeBatch.get(globalTopic);
                            String tenantId = globalTopic.tenantId;
                            String topic = globalTopic.topic;
                            for (ClientInfo publisher : rangePublisherMsgs.keySet()) {
                                int hash = Objects.hash(tenantId, topic, publisher);
                                int replicaIdx = Math.abs(hash) % rangeSetting.allReplicas.size();
                                // replica bind
                                batchByReplica.computeIfAbsent(new KVRangeReplica(
                                        rangeSetting.id,
                                        rangeSetting.ver,
                                        rangeSetting.allReplicas.get(replicaIdx)), k -> new HashMap<>())
                                    .computeIfAbsent(globalTopic, k -> new HashMap<>())
                                    .put(publisher, rangePublisherMsgs.get(publisher));
                            }
                        }
                    }
                }
                return batchByReplica;
            }
        }

        private record GlobalTopic(String tenantId, String topic) {

        }

        private record KVRangeReplica(KVRangeId id, long ver, String storeId) {
        }
    }
}
