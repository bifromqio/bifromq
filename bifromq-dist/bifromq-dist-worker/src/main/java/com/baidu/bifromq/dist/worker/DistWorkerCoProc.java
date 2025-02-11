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

package com.baidu.bifromq.dist.worker;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.intersect;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.isNULLRange;
import static com.baidu.bifromq.dist.entity.EntityUtil.getType;
import static com.baidu.bifromq.dist.entity.EntityUtil.matchRecordKeyPrefix;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseMatchRecord;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseOriginalTopicFilter;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseQInboxIdFromScopedTopicFilter;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseTenantId;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseTenantIdFromScopedTopicFilter;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseTopicFilter;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseTopicFilterFromScopedTopicFilter;
import static com.baidu.bifromq.dist.entity.EntityUtil.tenantUpperBound;
import static com.baidu.bifromq.dist.entity.EntityUtil.toGroupMatchRecordKey;
import static com.baidu.bifromq.dist.entity.EntityUtil.toNormalMatchRecordKey;
import static com.baidu.bifromq.dist.entity.EntityUtil.toScopedTopicFilter;
import static com.baidu.bifromq.util.TopicUtil.isNormalTopicFilter;
import static com.baidu.bifromq.util.TopicUtil.isOrderedShared;
import static com.baidu.bifromq.util.TopicUtil.unescape;
import static java.util.Collections.singletonMap;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.api.IKVCloseableReader;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProc;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.api.IKVWriter;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ROCoProcOutput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.dist.entity.GroupMatching;
import com.baidu.bifromq.dist.entity.Matching;
import com.baidu.bifromq.dist.entity.NormalMatching;
import com.baidu.bifromq.dist.rpc.proto.BatchDistReply;
import com.baidu.bifromq.dist.rpc.proto.BatchDistRequest;
import com.baidu.bifromq.dist.rpc.proto.BatchMatchReply;
import com.baidu.bifromq.dist.rpc.proto.BatchMatchRequest;
import com.baidu.bifromq.dist.rpc.proto.BatchUnmatchReply;
import com.baidu.bifromq.dist.rpc.proto.BatchUnmatchRequest;
import com.baidu.bifromq.dist.rpc.proto.DistPack;
import com.baidu.bifromq.dist.rpc.proto.DistServiceROCoProcInput;
import com.baidu.bifromq.dist.rpc.proto.DistServiceROCoProcOutput;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcInput;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcOutput;
import com.baidu.bifromq.dist.rpc.proto.GCReply;
import com.baidu.bifromq.dist.rpc.proto.GCRequest;
import com.baidu.bifromq.dist.rpc.proto.GroupMatchRecord;
import com.baidu.bifromq.dist.rpc.proto.TopicFanout;
import com.baidu.bifromq.dist.worker.cache.ISubscriptionCache;
import com.baidu.bifromq.plugin.subbroker.CheckRequest;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DistWorkerCoProc implements IKVRangeCoProc {
    private final Supplier<IKVCloseableReader> readerProvider;
    private final ISubscriptionCache routeCache;
    private final ITenantsState tenantsState;
    private final IDeliverExecutorGroup deliverExecutorGroup;
    private final ISubscriptionCleaner subscriptionChecker;
    private transient Boundary boundary;

    public DistWorkerCoProc(KVRangeId id,
                            Supplier<IKVCloseableReader> readerProvider,
                            ISubscriptionCache routeCache,
                            ITenantsState tenantsState,
                            IDeliverExecutorGroup deliverExecutorGroup,
                            ISubscriptionCleaner subscriptionChecker) {
        this.readerProvider = readerProvider;
        this.routeCache = routeCache;
        this.tenantsState = tenantsState;
        this.deliverExecutorGroup = deliverExecutorGroup;
        this.subscriptionChecker = subscriptionChecker;
        load();
    }

    @Override
    public CompletableFuture<ROCoProcOutput> query(ROCoProcInput input, IKVReader reader) {
        try {
            DistServiceROCoProcInput coProcInput = input.getDistService();
            switch (coProcInput.getInputCase()) {
                case BATCHDIST -> {
                    return batchDist(coProcInput.getBatchDist())
                        .thenApply(
                            v -> ROCoProcOutput.newBuilder().setDistService(DistServiceROCoProcOutput.newBuilder()
                                .setBatchDist(v).build()).build());
                }
                case GC -> {
                    return gc(coProcInput.getGc(), reader)
                        .thenApply(
                            v -> ROCoProcOutput.newBuilder().setDistService(DistServiceROCoProcOutput.newBuilder()
                                .setGc(v).build()).build());
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
        // tenantId -> topicFilter -> if ordered shared subscription
        Map<String, Map<String, Boolean>> updatedMatches = Maps.newHashMap();
        DistServiceRWCoProcOutput.Builder outputBuilder = DistServiceRWCoProcOutput.newBuilder();
        AtomicReference<Runnable> afterMutate = new AtomicReference<>();
        switch (coProcInput.getTypeCase()) {
            case BATCHMATCH -> {
                BatchMatchReply.Builder replyBuilder = BatchMatchReply.newBuilder();
                afterMutate.set(batchMatch(coProcInput.getBatchMatch(), reader, writer, updatedMatches, replyBuilder));
                outputBuilder.setBatchMatch(replyBuilder.build());
            }
            case BATCHUNMATCH -> {
                BatchUnmatchReply.Builder replyBuilder = BatchUnmatchReply.newBuilder();
                afterMutate.set(
                    batchUnmatch(coProcInput.getBatchUnmatch(), reader, writer, updatedMatches, replyBuilder));
                outputBuilder.setBatchUnmatch(replyBuilder.build());
            }
        }
        RWCoProcOutput output = RWCoProcOutput.newBuilder().setDistService(outputBuilder.build()).build();
        return () -> {
            routeCache.refresh(Maps.transformValues(updatedMatches, Map::keySet));
            updatedMatches.forEach((tenantId, topicFilters) ->
                topicFilters.forEach((topicFilter, isOrderedShareSub) -> {
                    if (isOrderedShareSub) {
                        deliverExecutorGroup.refreshOrderedShareSubRoutes(tenantId, topicFilter);
                    }
                }));
            afterMutate.get().run();
            return output;
        };
    }

    @Override
    public void reset(Boundary boundary) {
        tenantsState.reset();
        load();
    }

    public void close() {
        tenantsState.close();
        routeCache.close();
        deliverExecutorGroup.shutdown();
    }

    private Runnable batchMatch(BatchMatchRequest request,
                                IKVReader reader,
                                IKVWriter writer,
                                Map<String, Map<String, Boolean>> newMatches,
                                BatchMatchReply.Builder replyBuilder) {
        replyBuilder.setReqId(request.getReqId());
        Map<String, AtomicInteger> normalRoutesAdded = new HashMap<>();
        Map<String, AtomicInteger> sharedRoutesAdded = new HashMap<>();
        Map<ByteString, List<String>> groupMatchRecords = new HashMap<>();
        request.getScopedTopicFilterList().forEach(scopedTopicFilter -> {
            String tenantId = parseTenantIdFromScopedTopicFilter(scopedTopicFilter);
            String qInboxId = parseQInboxIdFromScopedTopicFilter(scopedTopicFilter);
            String topicFilter = parseTopicFilterFromScopedTopicFilter(scopedTopicFilter);
            if (isNormalTopicFilter(topicFilter)) {
                ByteString normalMatchRecordKey = toNormalMatchRecordKey(tenantId, topicFilter, qInboxId);
                if (!reader.exist(normalMatchRecordKey)) {
                    writer.put(normalMatchRecordKey, ByteString.EMPTY);
                    normalRoutesAdded.computeIfAbsent(tenantId, k -> new AtomicInteger()).incrementAndGet();
                    newMatches.computeIfAbsent(tenantId, k -> new HashMap<>()).put(topicFilter, false);
                }
                replyBuilder.putResults(scopedTopicFilter, BatchMatchReply.Result.OK);
            } else {
                ByteString groupMatchRecordKey = toGroupMatchRecordKey(tenantId, topicFilter);
                groupMatchRecords.computeIfAbsent(groupMatchRecordKey, k -> new LinkedList<>()).add(qInboxId);
            }
        });
        groupMatchRecords.forEach((groupMatchRecordKey, newGroupMembers) -> {
            String tenantId = parseTenantId(groupMatchRecordKey);
            GroupMatchRecord.Builder newMatch = GroupMatchRecord.newBuilder();
            GroupMatchRecord.Builder matchGroup = reader.get(groupMatchRecordKey)
                .map(b -> {
                    try {
                        return GroupMatchRecord.parseFrom(b).toBuilder();
                    } catch (InvalidProtocolBufferException e) {
                        log.error("Unable to parse GroupMatchRecord", e);
                        return GroupMatchRecord.newBuilder();
                    }
                })
                .orElseGet(() -> {
                    // new shared subscription
                    sharedRoutesAdded.computeIfAbsent(tenantId, k -> new AtomicInteger()).incrementAndGet();
                    return GroupMatchRecord.newBuilder();
                });
            boolean updated = false;
            int maxMembers = request.getOptionsMap().get(tenantId).getMaxReceiversPerSharedSubGroup();
            for (String newQInboxId : newGroupMembers) {
                if (!matchGroup.getQReceiverIdList().contains(newQInboxId)) {
                    if (matchGroup.getQReceiverIdCount() < maxMembers) {
                        matchGroup.addQReceiverId(newQInboxId);
                        newMatch.addQReceiverId(newQInboxId);
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
                writer.put(groupMatchRecordKey, matchGroup.build().toByteString());
                String originalTopicFilter = parseOriginalTopicFilter(groupMatchRecordKey.toStringUtf8());
                String groupTopicFilter = parseTopicFilter(groupMatchRecordKey.toStringUtf8());
                newMatches.computeIfAbsent(tenantId, k -> new HashMap<>())
                    .put(groupTopicFilter, isOrderedShared(originalTopicFilter));
            }
        });
        return () -> {
            normalRoutesAdded.forEach((tenantId, added) -> tenantsState.incNormalRoutes(tenantId, added.get()));
            sharedRoutesAdded.forEach((tenantId, added) -> tenantsState.incSharedRoutes(tenantId, added.get()));
        };
    }

    private Runnable batchUnmatch(BatchUnmatchRequest request,
                                  IKVReader reader,
                                  IKVWriter writer,
                                  Map<String, Map<String, Boolean>> removedMatches,
                                  BatchUnmatchReply.Builder replyBuilder) {
        replyBuilder.setReqId(request.getReqId());
        Map<String, AtomicInteger> normalRoutesRemoved = new HashMap<>();
        Map<String, AtomicInteger> sharedRoutesRemoved = new HashMap<>();
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
                    normalRoutesRemoved.computeIfAbsent(tenantId, k -> new AtomicInteger()).incrementAndGet();
                    removedMatches.computeIfAbsent(tenantId, k -> new HashMap<>()).put(topicFilter, false);
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
                Set<String> existing = Sets.newLinkedHashSet(groupMatching.receiverIds);
                GroupMatchRecord.Builder removedMatch = GroupMatchRecord.newBuilder();

                for (String delQInboxId : delGroupMembers) {
                    if (existing.remove(delQInboxId)) {
                        removedMatch.addQReceiverId(delQInboxId);
                        replyBuilder.putResults(
                            toScopedTopicFilter(tenantId, delQInboxId, groupMatching.originalTopicFilter()),
                            BatchUnmatchReply.Result.OK);

                    } else {
                        replyBuilder.putResults(
                            toScopedTopicFilter(tenantId, delQInboxId, groupMatching.originalTopicFilter()),
                            BatchUnmatchReply.Result.NOT_EXISTED);
                    }
                }
                if (existing.size() != groupMatching.receiverIds.size()) {
                    if (existing.isEmpty()) {
                        writer.delete(groupMatchRecordKey);
                        sharedRoutesRemoved.computeIfAbsent(tenantId, k -> new AtomicInteger()).incrementAndGet();
                    } else {
                        writer.put(groupMatchRecordKey, GroupMatchRecord.newBuilder()
                            .addAllQReceiverId(existing)
                            .build()
                            .toByteString());
                    }
                    String groupTopicFilter = parseTopicFilter(groupMatchRecordKey.toStringUtf8());
                    String originalTopicFilter = parseOriginalTopicFilter(groupMatchRecordKey.toStringUtf8());
                    removedMatches.computeIfAbsent(tenantId, k -> new HashMap<>())
                        .put(groupTopicFilter, isOrderedShared(originalTopicFilter));
                }
            } else {
                delGroupMembers.forEach(delQInboxId ->
                    replyBuilder.putResults(toScopedTopicFilter(tenantId, delQInboxId,
                            parseOriginalTopicFilter(groupMatchRecordKey.toStringUtf8())),
                        BatchUnmatchReply.Result.NOT_EXISTED));
            }
        });
        return () -> {
            normalRoutesRemoved.forEach((tenantId, removed) -> tenantsState.decNormalRoutes(tenantId, removed.get()));
            sharedRoutesRemoved.forEach((tenantId, removed) -> tenantsState.decSharedRoutes(tenantId, removed.get()));
        };
    }

    private CompletableFuture<BatchDistReply> batchDist(BatchDistRequest request) {
        List<DistPack> distPackList = request.getDistPackList();
        if (distPackList.isEmpty()) {
            return CompletableFuture.completedFuture(BatchDistReply.newBuilder()
                .setReqId(request.getReqId())
                .build());
        }
        List<CompletableFuture<Map<String, Map<String, Integer>>>> distFanOutFutures = new ArrayList<>();
        for (DistPack distPack : distPackList) {
            String tenantId = distPack.getTenantId();
            Boundary tenantBoundary = intersect(Boundary.newBuilder()
                .setStartKey(matchRecordKeyPrefix(tenantId))
                .setEndKey(tenantUpperBound(tenantId))
                .build(), boundary);
            if (isNULLRange(tenantBoundary)) {
                continue;
            }
            for (TopicMessagePack topicMsgPack : distPack.getMsgPackList()) {
                String topic = topicMsgPack.getTopic();
                distFanOutFutures.add(routeCache.get(tenantId, topic)
                    .thenApply(routes -> {
                        deliverExecutorGroup.submit(tenantId, routes, topicMsgPack);
                        return singletonMap(tenantId, singletonMap(topic, routes.size()));
                    }));
            }
        }
        return CompletableFuture.allOf(distFanOutFutures.toArray(CompletableFuture[]::new))
            .thenApply(v -> distFanOutFutures.stream().map(CompletableFuture::join).collect(Collectors.toList()))
            .thenApply(fanoutMapList -> {
                // tenantId -> topic -> fanOut
                Map<String, Map<String, Integer>> tenantfanout = new HashMap<>();
                fanoutMapList.forEach(fanoutMap -> fanoutMap.forEach((tenantId, topicFanout) ->
                    tenantfanout.computeIfAbsent(tenantId, k -> new HashMap<>()).putAll(topicFanout)));
                return BatchDistReply.newBuilder()
                    .setReqId(request.getReqId())
                    .putAllResult(Maps.transformValues(tenantfanout,
                        f -> TopicFanout.newBuilder().putAllFanout(f).build()))
                    .build();
            });
    }

    private CompletableFuture<GCReply> gc(GCRequest request, IKVReader reader) {
        reader.refresh();
        // subBrokerId -> delivererKey -> tenantId-> CheckRequest
        Map<Integer, Map<String, Map<String, CheckRequest.Builder>>> checkRequestBuilders = new HashMap<>();
        IKVIterator itr = reader.iterator();
        for (itr.seekToFirst(); itr.isValid(); itr.next()) {
            Matching matching = parseMatchRecord(itr.key(), itr.value());
            switch (matching.type()) {
                case Normal -> {
                    if (!routeCache.isCached(matching.tenantId, matching.originalTopicFilter())) {
                        NormalMatching normalMatching = ((NormalMatching) matching);
                        checkRequestBuilders
                            .computeIfAbsent(normalMatching.subBrokerId, k -> new HashMap<>())
                            .computeIfAbsent(normalMatching.delivererKey, k -> new HashMap<>())
                            .computeIfAbsent(normalMatching.tenantId, k -> CheckRequest.newBuilder()
                                .setTenantId(k)
                                .setDelivererKey(normalMatching.delivererKey))
                            .addMatchInfo(((NormalMatching) matching).matchInfo);
                    }
                }
                case Group -> {
                    GroupMatching groupMatching = ((GroupMatching) matching);
                    if (!routeCache.isCached(groupMatching.tenantId, unescape(groupMatching.escapedTopicFilter))) {
                        for (NormalMatching normalMatching : groupMatching.receiverList) {
                            checkRequestBuilders
                                .computeIfAbsent(normalMatching.subBrokerId, k -> new HashMap<>())
                                .computeIfAbsent(normalMatching.delivererKey, k -> new HashMap<>())
                                .computeIfAbsent(normalMatching.tenantId, k -> CheckRequest.newBuilder()
                                    .setTenantId(k)
                                    .setDelivererKey(normalMatching.delivererKey))
                                .addMatchInfo(normalMatching.matchInfo);
                        }
                    }
                }
                default -> {
                    // never happen
                }
            }
        }

        List<CompletableFuture<Void>> checkFutures = new ArrayList<>();
        for (int subBrokerId : checkRequestBuilders.keySet()) {
            for (String delivererKey : checkRequestBuilders.get(subBrokerId).keySet()) {
                for (Map.Entry<String, CheckRequest.Builder> entry :
                    checkRequestBuilders.get(subBrokerId).get(delivererKey).entrySet()) {
                    checkFutures.add(subscriptionChecker.sweep(subBrokerId, entry.getValue().build()));
                }
            }
        }
        return CompletableFuture.allOf(checkFutures.toArray(CompletableFuture[]::new))
            .thenApply(v -> GCReply.newBuilder().setReqId(request.getReqId()).build());
    }

    private void load() {
        try (IKVCloseableReader reader = readerProvider.get()) {
            boundary = reader.boundary();
            routeCache.reset(boundary);
            IKVIterator itr = reader.iterator();
            for (itr.seekToFirst(); itr.isValid(); ) {
                String tenantId = parseTenantId(itr.key());
                switch (getType(itr.key())) {
                    case Normal -> tenantsState.incNormalRoutes(tenantId);
                    case Group -> tenantsState.decNormalRoutes(tenantId);
                    default -> {
                        // never happen
                    }
                }
                itr.next();
            }
        }
    }
}
