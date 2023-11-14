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

package com.baidu.bifromq.dist.worker;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.intersect;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.isEmptyRange;
import static com.baidu.bifromq.dist.entity.EntityUtil.matchRecordKeyPrefix;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseMatchRecord;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseOriginalTopicFilter;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseQInboxIdFromScopedTopicFilter;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseTenantId;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseTenantIdFromScopedTopicFilter;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseTopicFilter;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseTopicFilterFromScopedTopicFilter;
import static com.baidu.bifromq.dist.entity.EntityUtil.tenantPrefix;
import static com.baidu.bifromq.dist.entity.EntityUtil.tenantUpperBound;
import static com.baidu.bifromq.dist.entity.EntityUtil.toGroupMatchRecordKey;
import static com.baidu.bifromq.dist.entity.EntityUtil.toNormalMatchRecordKey;
import static com.baidu.bifromq.dist.entity.EntityUtil.toScopedTopicFilter;
import static com.baidu.bifromq.dist.util.TopicUtil.isNormalTopicFilter;
import static com.baidu.bifromq.dist.util.TopicUtil.isWildcardTopicFilter;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DIST_FAN_OUT_PARALLELISM;
import static java.util.Collections.singletonMap;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProc;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.api.IKVWriter;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ROCoProcOutput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.entity.GroupMatching;
import com.baidu.bifromq.dist.entity.Matching;
import com.baidu.bifromq.dist.rpc.proto.BatchDistReply;
import com.baidu.bifromq.dist.rpc.proto.BatchDistRequest;
import com.baidu.bifromq.dist.rpc.proto.BatchMatchReply;
import com.baidu.bifromq.dist.rpc.proto.BatchMatchRequest;
import com.baidu.bifromq.dist.rpc.proto.BatchUnmatchReply;
import com.baidu.bifromq.dist.rpc.proto.BatchUnmatchRequest;
import com.baidu.bifromq.dist.rpc.proto.CollectMetricsReply;
import com.baidu.bifromq.dist.rpc.proto.DistPack;
import com.baidu.bifromq.dist.rpc.proto.DistServiceROCoProcInput;
import com.baidu.bifromq.dist.rpc.proto.DistServiceROCoProcOutput;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcInput;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcOutput;
import com.baidu.bifromq.dist.rpc.proto.GroupMatchRecord;
import com.baidu.bifromq.dist.rpc.proto.MatchRecord;
import com.baidu.bifromq.dist.rpc.proto.TopicFanout;
import com.baidu.bifromq.dist.worker.scheduler.IDeliveryScheduler;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DistWorkerCoProc implements IKVRangeCoProc {
    private final KVRangeId id;
    private final IDistClient distClient;
    private final ISettingProvider settingProvider;
    private final ISubBrokerManager subBrokerManager;
    private final IDeliveryScheduler scheduler;
    private final SubscriptionCache routeCache;
    private final FanoutExecutorGroup fanoutExecutorGroup;

    public DistWorkerCoProc(KVRangeId id,
                            Supplier<IKVReader> readClientProvider,
                            IEventCollector eventCollector,
                            ISettingProvider settingProvider,
                            IDistClient distClient,
                            ISubBrokerManager subBrokerManager,
                            IDeliveryScheduler scheduler,
                            Executor matchExecutor) {
        this.id = id;
        this.distClient = distClient;
        this.settingProvider = settingProvider;
        this.subBrokerManager = subBrokerManager;
        this.scheduler = scheduler;
        this.routeCache = new SubscriptionCache(id, readClientProvider, matchExecutor);
        fanoutExecutorGroup =
            new FanoutExecutorGroup(scheduler, eventCollector, distClient, DIST_FAN_OUT_PARALLELISM.get());
    }

    @Override
    public CompletableFuture<ROCoProcOutput> query(ROCoProcInput input, IKVReader reader) {
        try {
            DistServiceROCoProcInput coProcInput = input.getDistService();
            switch (coProcInput.getInputCase()) {
                case BATCHDIST -> {
                    return batchDist(coProcInput.getBatchDist(), reader)
                        .thenApply(
                            v -> ROCoProcOutput.newBuilder().setDistService(DistServiceROCoProcOutput.newBuilder()
                                .setBatchDist(v).build()).build());
                }
                case COLLECTMETRICS -> {
                    return collect(coProcInput.getCollectMetrics().getReqId(), reader)
                        .thenApply(
                            v -> ROCoProcOutput.newBuilder().setDistService(DistServiceROCoProcOutput.newBuilder()
                                .setCollectMetrics(v).build()).build());
                }
                default -> {
                    log.error("Unknown co proc type {}", coProcInput.getInputCase());
                    CompletableFuture<ROCoProcOutput> f = new CompletableFuture<>();
                    f.completeExceptionally(
                        new IllegalStateException("Unknown co proc type " + coProcInput.getInputCase()));
                    return f;
                }
            }
        } catch (Throwable e) {
            log.error("Unable to parse ro co-proc", e);
            CompletableFuture<ROCoProcOutput> f = new CompletableFuture<>();
            f.completeExceptionally(new IllegalStateException("Unable to parse ro co-proc", e));
            return f;
        }
    }

    @SneakyThrows
    @Override
    public Supplier<RWCoProcOutput> mutate(RWCoProcInput input, IKVReader reader, IKVWriter writer) {
        DistServiceRWCoProcInput coProcInput = input.getDistService();
        log.trace("Receive rw co-proc request\n{}", coProcInput);
        Set<String> touchedTenants = Sets.newHashSet();
        Set<ScopedTopic> touchedTopicFilters = Sets.newHashSet();
        DistServiceRWCoProcOutput.Builder outputBuilder = DistServiceRWCoProcOutput.newBuilder();
        switch (coProcInput.getTypeCase()) {
            case BATCHMATCH -> outputBuilder.setBatchMatch(
                batchMatch(coProcInput.getBatchMatch(), reader, writer, touchedTenants, touchedTopicFilters));
            case BATCHUNMATCH -> outputBuilder.setBatchUnmatch(
                batchUnmatch(coProcInput.getBatchUnmatch(), reader, writer, touchedTenants, touchedTopicFilters));
        }
        RWCoProcOutput output = RWCoProcOutput.newBuilder().setDistService(outputBuilder.build()).build();
        return () -> {
            touchedTopicFilters.forEach(topicFilter -> {
                routeCache.invalidate(topicFilter);
                fanoutExecutorGroup.invalidate(topicFilter);
            });
            touchedTenants.forEach(routeCache::touch);
            return output;
        };
    }

    public void close() {
        routeCache.close();
        fanoutExecutorGroup.shutdown();
    }

    private BatchMatchReply batchMatch(BatchMatchRequest request,
                                       IKVReader reader,
                                       IKVWriter writer,
                                       Set<String> touchedTenants,
                                       Set<ScopedTopic> touchedTopics) {
        BatchMatchReply.Builder replyBuilder = BatchMatchReply.newBuilder().setReqId(request.getReqId());
        Map<ByteString, Map<String, QoS>> groupMatchRecords = new HashMap<>();
        request.getScopedTopicFilterMap().forEach((scopedTopicFilter, subQoS) -> {
            String tenantId = parseTenantIdFromScopedTopicFilter(scopedTopicFilter);
            String qInboxId = parseQInboxIdFromScopedTopicFilter(scopedTopicFilter);
            String topicFilter = parseTopicFilterFromScopedTopicFilter(scopedTopicFilter);
            if (isNormalTopicFilter(topicFilter)) {
                ByteString normalMatchRecordKey = toNormalMatchRecordKey(tenantId, topicFilter, qInboxId);
                if (!reader.exist(normalMatchRecordKey)) {
                    writer.put(normalMatchRecordKey, MatchRecord.newBuilder().setNormal(subQoS).build().toByteString());
                    if (isWildcardTopicFilter(topicFilter)) {
                        touchedTenants.add(tenantId);
                    }
                    touchedTopics.add(ScopedTopic.builder()
                        .tenantId(tenantId)
                        .topic(topicFilter)
                        .boundary(reader.boundary())
                        .build());
                }
                replyBuilder.putResults(scopedTopicFilter, BatchMatchReply.Result.OK);
            } else {
                ByteString groupMatchRecordKey = toGroupMatchRecordKey(tenantId, topicFilter);
                groupMatchRecords.computeIfAbsent(groupMatchRecordKey, k -> new HashMap<>()).put(qInboxId, subQoS);
            }
        });
        groupMatchRecords.forEach((groupMatchRecordKey, newGroupMembers) -> {
            String tenantId = parseTenantId(groupMatchRecordKey);
            GroupMatchRecord.Builder matchGroup = reader.get(groupMatchRecordKey)
                .map(b -> {
                    try {
                        return MatchRecord.parseFrom(b).getGroup();
                    } catch (InvalidProtocolBufferException e) {
                        log.error("Unable to parse GroupMatchRecord", e);
                        return GroupMatchRecord.getDefaultInstance();
                    }
                })
                .orElse(GroupMatchRecord.getDefaultInstance()).toBuilder();

            boolean updated = false;
            int maxMembers = settingProvider.provide(Setting.MaxSharedGroupMembers, tenantId);
            for (String newQInboxId : newGroupMembers.keySet()) {
                QoS newSubQoS = newGroupMembers.get(newQInboxId);
                QoS oldSubQoS = matchGroup.getEntryMap().get(newQInboxId);
                if (oldSubQoS != newSubQoS) {
                    if (oldSubQoS != null || matchGroup.getEntryCount() < maxMembers) {
                        matchGroup.putEntry(newQInboxId, newSubQoS);
                        replyBuilder.putResults(toScopedTopicFilter(tenantId, newQInboxId,
                                parseOriginalTopicFilter(groupMatchRecordKey.toStringUtf8())),
                            BatchMatchReply.Result.OK);
                        updated = true;
                    } else {
                        replyBuilder.putResults(toScopedTopicFilter(tenantId, newQInboxId,
                                parseOriginalTopicFilter(groupMatchRecordKey.toStringUtf8())),
                            BatchMatchReply.Result.EXCEED_LIMIT);
                    }
                } else {
                    replyBuilder.putResults(toScopedTopicFilter(tenantId, newQInboxId,
                            parseOriginalTopicFilter(groupMatchRecordKey.toStringUtf8())),
                        BatchMatchReply.Result.OK);
                }
            }
            if (updated) {
                writer.put(groupMatchRecordKey, MatchRecord.newBuilder().setGroup(matchGroup).build().toByteString());
                String groupTopicFilter = parseTopicFilter(groupMatchRecordKey.toStringUtf8());
                if (isWildcardTopicFilter(groupTopicFilter)) {
                    touchedTenants.add(parseTenantId(groupMatchRecordKey));
                }
                touchedTopics.add(ScopedTopic.builder()
                    .tenantId(parseTenantId(groupMatchRecordKey))
                    .topic(groupTopicFilter)
                    .boundary(reader.boundary())
                    .build());
            }
        });
        return replyBuilder.build();
    }

    private BatchUnmatchReply batchUnmatch(BatchUnmatchRequest request,
                                           IKVReader reader,
                                           IKVWriter writer,
                                           Set<String> touchedTenants,
                                           Set<ScopedTopic> touchedTopics) {
        BatchUnmatchReply.Builder replyBuilder = BatchUnmatchReply.newBuilder().setReqId(request.getReqId());
        Map<ByteString, Set<String>> delGroupMatchRecords = new HashMap<>();
        for (String scopedTopicFilter : request.getScopedTopicFilterList()) {
            String tenantId = parseTenantIdFromScopedTopicFilter(scopedTopicFilter);
            String qInboxId = parseQInboxIdFromScopedTopicFilter(scopedTopicFilter);
            String topicFilter = parseTopicFilterFromScopedTopicFilter(scopedTopicFilter);
            if (isNormalTopicFilter(topicFilter)) {
                ByteString normalMatchRecordKey = toNormalMatchRecordKey(tenantId, topicFilter, qInboxId);
                Optional<ByteString> value = reader.get(normalMatchRecordKey);
                if (value.isPresent()) {
                    writer.delete(normalMatchRecordKey);
                    if (isWildcardTopicFilter(topicFilter)) {
                        touchedTenants.add(tenantId);
                    }
                    touchedTopics.add(ScopedTopic.builder()
                        .tenantId(tenantId)
                        .topic(topicFilter)
                        .boundary(reader.boundary())
                        .build());
                    replyBuilder.putResults(scopedTopicFilter, BatchUnmatchReply.Result.OK);
                } else {
                    replyBuilder.putResults(scopedTopicFilter, BatchUnmatchReply.Result.NOT_EXISTED);
                }
            } else {
                ByteString groupMatchRecordKey = toGroupMatchRecordKey(tenantId, topicFilter);
                delGroupMatchRecords.computeIfAbsent(groupMatchRecordKey, k -> new HashSet<>()).add(qInboxId);
            }
        }
        delGroupMatchRecords.forEach((groupMatchRecordKey, delGroupMembers) -> {
            String tenantId = parseTenantId(groupMatchRecordKey);
            Optional<ByteString> value = reader.get(groupMatchRecordKey);
            if (value.isPresent()) {
                Matching matching = parseMatchRecord(groupMatchRecordKey, value.get());
                assert matching instanceof GroupMatching;
                GroupMatching groupMatching = (GroupMatching) matching;
                Map<String, QoS> existing = Maps.newHashMap(groupMatching.inboxMap);
                for (String delQInboxId : delGroupMembers) {
                    if (existing.remove(delQInboxId) != null) {
                        replyBuilder.putResults(
                            toScopedTopicFilter(tenantId, delQInboxId, groupMatching.originalTopicFilter()),
                            BatchUnmatchReply.Result.OK);

                    } else {
                        replyBuilder.putResults(
                            toScopedTopicFilter(tenantId, delQInboxId, groupMatching.originalTopicFilter()),
                            BatchUnmatchReply.Result.NOT_EXISTED);
                    }
                }
                if (existing.size() != groupMatching.inboxMap.size()) {
                    if (existing.isEmpty()) {
                        writer.delete(groupMatchRecordKey);
                    } else {
                        writer.put(groupMatchRecordKey, MatchRecord.newBuilder()
                            .setGroup(GroupMatchRecord.newBuilder()
                                .putAllEntry(existing)
                                .build()).build()
                            .toByteString());
                    }
                    String groupTopicFilter = parseTopicFilter(groupMatchRecordKey.toStringUtf8());
                    if (isWildcardTopicFilter(groupTopicFilter)) {
                        touchedTenants.add(parseTenantId(groupMatchRecordKey));
                    }
                    touchedTopics.add(ScopedTopic.builder()
                        .tenantId(parseTenantId(groupMatchRecordKey))
                        .topic(groupTopicFilter)
                        .boundary(reader.boundary())
                        .build());
                }
            } else {
                delGroupMembers.forEach(delQInboxId ->
                    replyBuilder.putResults(toScopedTopicFilter(tenantId, delQInboxId,
                            parseOriginalTopicFilter(groupMatchRecordKey.toStringUtf8())),
                        BatchUnmatchReply.Result.NOT_EXISTED));
            }
        });
        return replyBuilder.build();
    }

    private CompletableFuture<BatchDistReply> batchDist(BatchDistRequest request, IKVReader reader) {
        List<DistPack> distPackList = request.getDistPackList();
        if (distPackList.isEmpty()) {
            return CompletableFuture.completedFuture(BatchDistReply.newBuilder()
                .setReqId(request.getReqId())
                .build());
        }
        List<CompletableFuture<Map<String, Map<String, Integer>>>> distFanOutFutures = new ArrayList<>();
        for (DistPack distPack : distPackList) {
            String tenantId = distPack.getTenantId();
            Boundary boundary = intersect(Boundary.newBuilder()
                .setStartKey(matchRecordKeyPrefix(tenantId))
                .setEndKey(tenantUpperBound(tenantId))
                .build(), reader.boundary());
            if (isEmptyRange(boundary)) {
                continue;
            }
            for (TopicMessagePack topicMsgPack : distPack.getMsgPackList()) {
                String topic = topicMsgPack.getTopic();
                ScopedTopic scopedTopic = ScopedTopic.builder()
                    .tenantId(tenantId)
                    .topic(topic)
                    .boundary(reader.boundary())
                    .build();
                distFanOutFutures.add(routeCache.get(scopedTopic)
                    .thenApply(matchResult -> {
                        fanoutExecutorGroup.submit(matchResult.routes, topicMsgPack);
                        return singletonMap(tenantId, singletonMap(topic, matchResult.routes.size()));
                    }));
            }
        }
        return CompletableFuture.allOf(distFanOutFutures.toArray(CompletableFuture[]::new))
            .thenApply(v -> distFanOutFutures.stream().map(CompletableFuture::join).collect(Collectors.toList()))
            .thenApply(v -> {
                // tenantId -> topic -> fanOut
                Map<String, Map<String, Integer>> tenantfanout = new HashMap<>();
                v.forEach(fanoutMap -> fanoutMap.forEach((tenantId, topicFanout) ->
                    tenantfanout.computeIfAbsent(tenantId, k -> new HashMap<>()).putAll(topicFanout)));
                return BatchDistReply.newBuilder()
                    .setReqId(request.getReqId())
                    .putAllResult(Maps.transformValues(tenantfanout,
                        f -> TopicFanout.newBuilder().putAllFanout(f).build()))
                    .build();
            });
    }

    private CompletableFuture<CollectMetricsReply> collect(long reqId, IKVReader reader) {
        CollectMetricsReply.Builder builder = CollectMetricsReply.newBuilder().setReqId(reqId);
        try {
            IKVIterator itr = reader.iterator();
            for (itr.seekToFirst(); itr.isValid(); ) {
                String tenantId = parseTenantId(itr.key());
                builder.putUsedSpaces(tenantId,
                    reader.size(intersect(reader.boundary(), Boundary.newBuilder()
                        .setStartKey(tenantPrefix(tenantId))
                        .setEndKey(tenantUpperBound(tenantId))
                        .build())));
                itr.seek(tenantUpperBound(tenantId));
            }
        } catch (Exception e) {
            log.error("Unexpected error", e);
        }
        return CompletableFuture.completedFuture(builder.build());
    }
}
