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

import static com.baidu.bifromq.basekv.utils.KeyRangeUtil.intersect;
import static com.baidu.bifromq.basekv.utils.KeyRangeUtil.isEmptyRange;
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

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.Range;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProc;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.api.IKVWriter;
import com.baidu.bifromq.basekv.store.range.ILoadTracker;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.entity.GroupMatching;
import com.baidu.bifromq.dist.entity.Matching;
import com.baidu.bifromq.dist.rpc.proto.BatchDist;
import com.baidu.bifromq.dist.rpc.proto.BatchDistReply;
import com.baidu.bifromq.dist.rpc.proto.BatchSubReply;
import com.baidu.bifromq.dist.rpc.proto.BatchSubRequest;
import com.baidu.bifromq.dist.rpc.proto.BatchUnsubReply;
import com.baidu.bifromq.dist.rpc.proto.BatchUnsubRequest;
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
import com.baidu.bifromq.dist.worker.scheduler.MessagePackWrapper;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
                            Supplier<IKVRangeReader> readClientProvider,
                            IEventCollector eventCollector,
                            ISettingProvider settingProvider,
                            IDistClient distClient,
                            ISubBrokerManager subBrokerManager,
                            IDeliveryScheduler scheduler,
                            Executor matchExecutor,
                            ILoadTracker loadTracker) {
        this.id = id;
        this.distClient = distClient;
        this.settingProvider = settingProvider;
        this.subBrokerManager = subBrokerManager;
        this.scheduler = scheduler;
        this.routeCache = new SubscriptionCache(id, readClientProvider, matchExecutor, loadTracker);
        fanoutExecutorGroup = new FanoutExecutorGroup(this.subBrokerManager, scheduler, eventCollector, distClient,
            DIST_FAN_OUT_PARALLELISM.get());
    }

    @Override
    public CompletableFuture<ByteString> query(ByteString input, IKVReader reader) {
        try {
            CodedInputStream cis = input.newCodedInput();
            cis.enableAliasing(true);
            DistServiceROCoProcInput coProcInput = DistServiceROCoProcInput.parseFrom(cis);
            switch (coProcInput.getInputCase()) {
                case DIST -> {
                    return dist(coProcInput.getDist(), reader)
                        .thenApply(v -> DistServiceROCoProcOutput.newBuilder()
                            .setDistReply(v).build().toByteString());
                }
                case COLLECTMETRICSREQUEST -> {
                    return collect(coProcInput.getCollectMetricsRequest().getReqId(), reader)
                        .thenApply(v -> DistServiceROCoProcOutput.newBuilder()
                            .setCollectMetricsReply(v).build().toByteString());
                }
                default -> {
                    log.error("Unknown co proc type {}", coProcInput.getInputCase());
                    CompletableFuture<ByteString> f = new CompletableFuture<>();
                    f.completeExceptionally(
                        new IllegalStateException("Unknown co proc type " + coProcInput.getInputCase()));
                    return f;
                }
            }
        } catch (Throwable e) {
            log.error("Unable to parse ro co-proc", e);
            CompletableFuture<ByteString> f = new CompletableFuture<>();
            f.completeExceptionally(new IllegalStateException("Unable to parse ro co-proc", e));
            return f;
        }
    }

    @SneakyThrows
    @Override
    public Supplier<ByteString> mutate(ByteString input, IKVReader reader, IKVWriter writer) {
        CodedInputStream cis = input.newCodedInput();
        cis.enableAliasing(true);
        DistServiceRWCoProcInput coProcInput = DistServiceRWCoProcInput.parseFrom(cis);
        log.trace("Receive rw co-proc request\n{}", coProcInput);
        Set<String> touchedTenants = Sets.newHashSet();
        Set<ScopedTopic> touchedTopics = Sets.newHashSet();
        DistServiceRWCoProcOutput.Builder outputBuilder = DistServiceRWCoProcOutput.newBuilder();
        switch (coProcInput.getTypeCase()) {
            case BATCHSUB -> outputBuilder.setBatchSub(
                batchSub(coProcInput.getBatchSub(), reader, writer, touchedTenants, touchedTopics));
            case BATCHUNSUB -> outputBuilder.setBatchUnsub(
                batchUnsub(coProcInput.getBatchUnsub(), reader, writer, touchedTenants, touchedTopics));
        }
        ByteString output = outputBuilder.build().toByteString();
        return () -> {
            touchedTopics.forEach(routeCache::invalidate);
            touchedTenants.forEach(routeCache::touch);
            return output;
        };
    }

    public void close() {
        routeCache.close();
        fanoutExecutorGroup.shutdown();
    }

    private BatchSubReply batchSub(BatchSubRequest request,
                                   IKVReader reader,
                                   IKVWriter writer,
                                   Set<String> touchedTenants,
                                   Set<ScopedTopic> touchedTopics) {
        BatchSubReply.Builder replyBuilder = BatchSubReply.newBuilder()
            .setReqId(request.getReqId());
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
                    } else {
                        touchedTopics.add(ScopedTopic.builder()
                            .tenantId(tenantId)
                            .topic(topicFilter)
                            .range(reader.range())
                            .build());
                    }
                }
                replyBuilder.putResults(scopedTopicFilter, BatchSubReply.Result.OK);
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
                            BatchSubReply.Result.OK);
                        updated = true;
                    } else {
                        replyBuilder.putResults(toScopedTopicFilter(tenantId, newQInboxId,
                                parseOriginalTopicFilter(groupMatchRecordKey.toStringUtf8())),
                            BatchSubReply.Result.EXCEED_LIMIT);
                    }
                } else {
                    replyBuilder.putResults(toScopedTopicFilter(tenantId, newQInboxId,
                            parseOriginalTopicFilter(groupMatchRecordKey.toStringUtf8())),
                        BatchSubReply.Result.OK);
                }
            }
            if (updated) {
                writer.put(groupMatchRecordKey, MatchRecord.newBuilder().setGroup(matchGroup).build().toByteString());
                String groupTopicFilter = parseTopicFilter(groupMatchRecordKey.toStringUtf8());
                if (isWildcardTopicFilter(groupTopicFilter)) {
                    touchedTenants.add(parseTenantId(groupMatchRecordKey));
                } else {
                    touchedTopics.add(ScopedTopic.builder()
                        .tenantId(parseTenantId(groupMatchRecordKey))
                        .topic(groupTopicFilter)
                        .range(reader.range())
                        .build());
                }
            }
        });
        return replyBuilder.build();
    }

    private BatchUnsubReply batchUnsub(BatchUnsubRequest request,
                                       IKVReader reader,
                                       IKVWriter writer,
                                       Set<String> touchedTenants,
                                       Set<ScopedTopic> touchedTopics) {
        BatchUnsubReply.Builder replyBuilder = BatchUnsubReply.newBuilder().setReqId(request.getReqId());
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
                    } else {
                        touchedTopics.add(ScopedTopic.builder()
                            .tenantId(tenantId)
                            .topic(topicFilter)
                            .range(reader.range())
                            .build());
                    }
                    replyBuilder.putResults(scopedTopicFilter, BatchUnsubReply.Result.OK);
                } else {
                    replyBuilder.putResults(scopedTopicFilter, BatchUnsubReply.Result.NOT_EXISTED);
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
                            BatchUnsubReply.Result.OK);

                    } else {
                        replyBuilder.putResults(
                            toScopedTopicFilter(tenantId, delQInboxId, groupMatching.originalTopicFilter()),
                            BatchUnsubReply.Result.NOT_EXISTED);
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
                    } else {
                        touchedTopics.add(ScopedTopic.builder()
                            .tenantId(parseTenantId(groupMatchRecordKey))
                            .topic(groupTopicFilter)
                            .range(reader.range())
                            .build());
                    }
                }
            } else {
                delGroupMembers.forEach(delQInboxId ->
                    replyBuilder.putResults(toScopedTopicFilter(tenantId, delQInboxId,
                            parseOriginalTopicFilter(groupMatchRecordKey.toStringUtf8())),
                        BatchUnsubReply.Result.NOT_EXISTED));
            }
        });
        return replyBuilder.build();
    }

    private CompletableFuture<BatchDistReply> dist(BatchDist request, IKVReader reader) {
        List<DistPack> distPackList = request.getDistPackList();
        if (distPackList.isEmpty()) {
            return CompletableFuture.completedFuture(BatchDistReply.newBuilder()
                .setReqId(request.getReqId())
                .build());
        }
        List<CompletableFuture<Map<String, Map<String, Integer>>>> distFanOutFutures = new ArrayList<>();
        for (DistPack distPack : distPackList) {
            String tenantId = distPack.getTenantId();
            Range range = intersect(Range.newBuilder()
                .setStartKey(matchRecordKeyPrefix(tenantId))
                .setEndKey(tenantUpperBound(tenantId))
                .build(), reader.range());
            if (isEmptyRange(range)) {
                continue;
            }
            for (TopicMessagePack topicMsgPack : distPack.getMsgPackList()) {
                String topic = topicMsgPack.getTopic();
                ScopedTopic scopedTopic = ScopedTopic.builder()
                    .tenantId(tenantId)
                    .topic(topic)
                    .range(reader.range())
                    .build();
                Map<ClientInfo, TopicMessagePack.PublisherPack> senderMsgPackMap = topicMsgPack.getMessageList()
                    .stream().collect(Collectors.toMap(TopicMessagePack.PublisherPack::getPublisher, e -> e));
                distFanOutFutures.add(routeCache.get(scopedTopic, senderMsgPackMap.keySet())
                    .thenApply(routeMap -> {
                        fanoutExecutorGroup.submit(Objects.hash(tenantId, topic, request.getOrderKey()), routeMap,
                            MessagePackWrapper.wrap(topicMsgPack), senderMsgPackMap);
                        return singletonMap(tenantId, singletonMap(topic, routeMap.size()));
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
        try (IKVIterator itr = reader.iterator()) {
            for (itr.seekToFirst(); itr.isValid(); ) {
                String tenantId = parseTenantId(itr.key());
                builder.putUsedSpaces(tenantId,
                    reader.size(intersect(reader.range(), Range.newBuilder()
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
