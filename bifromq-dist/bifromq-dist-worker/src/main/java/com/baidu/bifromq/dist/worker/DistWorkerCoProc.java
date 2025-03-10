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
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static com.baidu.bifromq.dist.worker.schema.KVSchemaUtil.buildMatchRoute;
import static com.baidu.bifromq.dist.worker.schema.KVSchemaUtil.tenantBeginKey;
import static com.baidu.bifromq.dist.worker.schema.KVSchemaUtil.toGroupRouteKey;
import static com.baidu.bifromq.dist.worker.schema.KVSchemaUtil.toNormalRouteKey;
import static com.baidu.bifromq.dist.worker.schema.KVSchemaUtil.toReceiverUrl;
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
import com.baidu.bifromq.dist.rpc.proto.MatchRoute;
import com.baidu.bifromq.dist.rpc.proto.RouteGroup;
import com.baidu.bifromq.dist.rpc.proto.TopicFanout;
import com.baidu.bifromq.dist.worker.cache.ISubscriptionCache;
import com.baidu.bifromq.dist.worker.schema.GroupMatching;
import com.baidu.bifromq.dist.worker.schema.KVSchemaUtil;
import com.baidu.bifromq.dist.worker.schema.Matching;
import com.baidu.bifromq.dist.worker.schema.NormalMatching;
import com.baidu.bifromq.dist.worker.schema.RouteDetail;
import com.baidu.bifromq.dist.worker.schema.SharedTopicFilter;
import com.baidu.bifromq.plugin.subbroker.CheckRequest;
import com.baidu.bifromq.type.TopicMessagePack;
import com.baidu.bifromq.util.BSUtil;
import com.google.common.collect.Maps;
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
                    return batchDist(coProcInput.getBatchDist()).thenApply(v -> ROCoProcOutput.newBuilder()
                        .setDistService(DistServiceROCoProcOutput.newBuilder().setBatchDist(v).build()).build());
                }
                case GC -> {
                    return gc(coProcInput.getGc(), reader).thenApply(v -> ROCoProcOutput.newBuilder()
                        .setDistService(DistServiceROCoProcOutput.newBuilder().setGc(v).build()).build());
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
            default -> {
                // unreachable
            }
        }
        RWCoProcOutput output = RWCoProcOutput.newBuilder().setDistService(outputBuilder.build()).build();
        return () -> {
            routeCache.refresh(Maps.transformValues(updatedMatches, Map::keySet));
            updatedMatches.forEach(
                (tenantId, topicFilters) -> topicFilters.forEach((topicFilter, isOrderedShareSub) -> {
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
        Map<GlobalTopicFilter, Map<MatchRoute, Integer>> groupMatchRecords = new HashMap<>();
        Map<String, BatchMatchReply.TenantBatch.Code[]> resultMap = new HashMap<>();
        request.getRequestsMap().forEach((tenantId, tenantMatchRequest) -> {
            BatchMatchReply.TenantBatch.Code[] codes = resultMap.computeIfAbsent(tenantId,
                k -> new BatchMatchReply.TenantBatch.Code[tenantMatchRequest.getRouteCount()]);
            Set<ByteString> addedMatches = new HashSet<>();
            for (int i = 0; i < tenantMatchRequest.getRouteCount(); i++) {
                MatchRoute route = tenantMatchRequest.getRoute(i);
                long incarnation = route.getIncarnation();
                String topicFilter = route.getTopicFilter();
                if (isNormalTopicFilter(topicFilter)) {
                    ByteString normalRouteKey = toNormalRouteKey(tenantId, topicFilter, toReceiverUrl(route));
                    Optional<Long> incarOpt = reader.get(normalRouteKey).map(BSUtil::toLong);
                    if (incarOpt.isEmpty() || incarOpt.get() < incarnation) {
                        writer.put(normalRouteKey, BSUtil.toByteString(incarnation));
                        // match record may be duplicated in the request
                        if (!addedMatches.contains(normalRouteKey)) {
                            normalRoutesAdded.computeIfAbsent(tenantId, k -> new AtomicInteger()).incrementAndGet();
                        }
                        newMatches.computeIfAbsent(tenantId, k -> new HashMap<>()).put(topicFilter, false);
                        addedMatches.add(normalRouteKey);
                    }
                    codes[i] = BatchMatchReply.TenantBatch.Code.OK;
                } else {
                    groupMatchRecords.computeIfAbsent(new GlobalTopicFilter(tenantId, topicFilter),
                        k -> new HashMap<>()).put(route, i);
                }
            }
        });
        groupMatchRecords.forEach((globalTopicFilter, newGroupMembers) -> {
            String tenantId = globalTopicFilter.tenantId;
            String origTopicFilter = globalTopicFilter.topicFilter;
            ByteString groupMatchRecordKey = toGroupRouteKey(tenantId, origTopicFilter);
            RouteGroup.Builder matchGroup = reader.get(groupMatchRecordKey)
                .map(b -> {
                    try {
                        return RouteGroup.parseFrom(b).toBuilder();
                    } catch (InvalidProtocolBufferException e) {
                        log.error("Unable to parse GroupMatchRecord", e);
                        return RouteGroup.newBuilder();
                    }
                })
                .orElseGet(() -> {
                    // new shared subscription
                    sharedRoutesAdded.computeIfAbsent(tenantId, k -> new AtomicInteger()).incrementAndGet();
                    return RouteGroup.newBuilder();
                });
            boolean updated = false;
            int maxMembers = request.getRequestsMap().get(tenantId).getOption().getMaxReceiversPerSharedSubGroup();
            for (MatchRoute route : newGroupMembers.keySet()) {
                int resultIdx = newGroupMembers.get(route);
                String receiverUrl = toReceiverUrl(route);
                if (!matchGroup.containsMembers(receiverUrl)) {
                    if (matchGroup.getMembersCount() < maxMembers) {
                        matchGroup.putMembers(receiverUrl, route.getIncarnation());
                        resultMap.get(tenantId)[resultIdx] = BatchMatchReply.TenantBatch.Code.OK;
                        updated = true;
                    } else {
                        resultMap.get(tenantId)[resultIdx] = BatchMatchReply.TenantBatch.Code.EXCEED_LIMIT;
                    }
                } else {
                    if (matchGroup.getMembersMap().get(receiverUrl) < route.getIncarnation()) {
                        matchGroup.putMembers(receiverUrl, route.getIncarnation());
                        updated = true;
                    }
                    resultMap.get(tenantId)[resultIdx] = BatchMatchReply.TenantBatch.Code.OK;
                }
            }
            if (updated) {
                writer.put(groupMatchRecordKey, matchGroup.build().toByteString());
                SharedTopicFilter sharedTopicFilter = SharedTopicFilter.from(origTopicFilter);
                newMatches.computeIfAbsent(tenantId, k -> new HashMap<>())
                    .put(sharedTopicFilter.topicFilter, isOrderedShared(origTopicFilter));
            }
        });
        resultMap.forEach((tenantId, codes) -> {
            BatchMatchReply.TenantBatch.Builder batchBuilder = BatchMatchReply.TenantBatch.newBuilder();
            for (BatchMatchReply.TenantBatch.Code code : codes) {
                batchBuilder.addCode(code);
            }
            replyBuilder.putResults(tenantId, batchBuilder.build());
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
        Map<GlobalTopicFilter, Map<MatchRoute, Integer>> delGroupMatchRecords = new HashMap<>();
        Map<String, BatchUnmatchReply.TenantBatch.Code[]> resultMap = new HashMap<>();
        request.getRequestsMap().forEach((tenantId, tenantUnmatchRequest) -> {
            BatchUnmatchReply.TenantBatch.Code[] codes =
                resultMap.computeIfAbsent(tenantId,
                    k -> new BatchUnmatchReply.TenantBatch.Code[tenantUnmatchRequest.getRouteCount()]);
            Set<ByteString> delMatches = new HashSet<>();
            for (int i = 0; i < tenantUnmatchRequest.getRouteCount(); i++) {
                MatchRoute route = tenantUnmatchRequest.getRoute(i);
                String topicFilter = route.getTopicFilter();
                if (isNormalTopicFilter(topicFilter)) {
                    ByteString normalRouteKey = toNormalRouteKey(tenantId, topicFilter, toReceiverUrl(route));
                    Optional<Long> incarOpt = reader.get(normalRouteKey).map(BSUtil::toLong);
                    if (incarOpt.isPresent() && incarOpt.get() <= route.getIncarnation()) {
                        writer.delete(normalRouteKey);
                        if (!delMatches.contains(normalRouteKey)) {
                            normalRoutesRemoved.computeIfAbsent(tenantId, k -> new AtomicInteger()).incrementAndGet();
                        }
                        removedMatches.computeIfAbsent(tenantId, k -> new HashMap<>()).put(topicFilter, false);
                        delMatches.add(normalRouteKey);
                        codes[i] = BatchUnmatchReply.TenantBatch.Code.OK;
                    } else {
                        codes[i] = BatchUnmatchReply.TenantBatch.Code.NOT_EXISTED;
                    }
                } else {
                    delGroupMatchRecords.computeIfAbsent(new GlobalTopicFilter(tenantId, topicFilter),
                        k -> new HashMap<>()).put(route, i);
                }
            }
        });
        delGroupMatchRecords.forEach((globalTopicFilter, delGroupMembers) -> {
            String tenantId = globalTopicFilter.tenantId;
            String origTopicFilter = globalTopicFilter.topicFilter;
            ByteString groupRouteKey = toGroupRouteKey(tenantId, origTopicFilter);
            Optional<ByteString> value = reader.get(groupRouteKey);
            if (value.isPresent()) {
                Matching matching = buildMatchRoute(groupRouteKey, value.get());
                assert matching instanceof GroupMatching;
                GroupMatching groupMatching = (GroupMatching) matching;
                Map<String, Long> existing = Maps.newHashMap(groupMatching.receivers);
                delGroupMembers.forEach((route, resultIdx) -> {
                    String receiverUrl = toReceiverUrl(route);
                    if (existing.containsKey(receiverUrl) && existing.get(receiverUrl) <= route.getIncarnation()) {
                        existing.remove(receiverUrl);
                        resultMap.get(tenantId)[resultIdx] = BatchUnmatchReply.TenantBatch.Code.OK;
                    } else {
                        resultMap.get(tenantId)[resultIdx] = BatchUnmatchReply.TenantBatch.Code.NOT_EXISTED;
                    }
                });
                if (existing.size() != groupMatching.receivers.size()) {
                    if (existing.isEmpty()) {
                        writer.delete(groupRouteKey);
                        sharedRoutesRemoved.computeIfAbsent(tenantId, k -> new AtomicInteger()).incrementAndGet();
                    } else {
                        writer.put(groupRouteKey,
                            RouteGroup.newBuilder().putAllMembers(existing).build().toByteString());
                    }
                    removedMatches.computeIfAbsent(tenantId, k -> new HashMap<>())
                        .put(groupMatching.topicFilter(), isOrderedShared(origTopicFilter));
                }
            } else {
                delGroupMembers.forEach((detail, resultIdx) ->
                    resultMap.get(tenantId)[resultIdx] = BatchUnmatchReply.TenantBatch.Code.NOT_EXISTED);
            }
        });
        resultMap.forEach((tenantId, codes) -> {
            BatchUnmatchReply.TenantBatch.Builder batchBuilder = BatchUnmatchReply.TenantBatch.newBuilder();
            for (BatchUnmatchReply.TenantBatch.Code code : codes) {
                batchBuilder.addCode(code);
            }
            replyBuilder.putResults(tenantId, batchBuilder.build());
        });
        return () -> {
            normalRoutesRemoved.forEach((tenantId, removed) -> tenantsState.decNormalRoutes(tenantId, removed.get()));
            sharedRoutesRemoved.forEach((tenantId, removed) -> tenantsState.decSharedRoutes(tenantId, removed.get()));
        };
    }

    private CompletableFuture<BatchDistReply> batchDist(BatchDistRequest request) {
        List<DistPack> distPackList = request.getDistPackList();
        if (distPackList.isEmpty()) {
            return CompletableFuture.completedFuture(BatchDistReply.newBuilder().setReqId(request.getReqId()).build());
        }
        List<CompletableFuture<Map<String, Map<String, Integer>>>> distFanOutFutures = new ArrayList<>();
        for (DistPack distPack : distPackList) {
            String tenantId = distPack.getTenantId();
            ByteString tenantStartKey = tenantBeginKey(tenantId);
            Boundary tenantBoundary = intersect(toBoundary(tenantStartKey, upperBound(tenantStartKey)), boundary);
            if (isNULLRange(tenantBoundary)) {
                continue;
            }
            for (TopicMessagePack topicMsgPack : distPack.getMsgPackList()) {
                String topic = topicMsgPack.getTopic();
                distFanOutFutures.add(routeCache.get(tenantId, topic).thenApply(routes -> {
                    deliverExecutorGroup.submit(tenantId, routes, topicMsgPack);
                    return singletonMap(tenantId, singletonMap(topic, routes.size()));
                }));
            }
        }
        return CompletableFuture.allOf(distFanOutFutures.toArray(CompletableFuture[]::new))
            .thenApply(v -> distFanOutFutures.stream().map(CompletableFuture::join).collect(Collectors.toList()))
            .thenApply(fanoutMapList -> {
                // tenantId -> topic -> fanOut
                Map<String, Map<String, Integer>> tenantFanOut = new HashMap<>();
                fanoutMapList.forEach(fanoutMap -> fanoutMap.forEach(
                    (tenantId, topicFanout) -> tenantFanOut.computeIfAbsent(tenantId, k -> new HashMap<>())
                        .putAll(topicFanout)));
                return BatchDistReply.newBuilder().setReqId(request.getReqId()).putAllResult(
                        Maps.transformValues(tenantFanOut, f -> TopicFanout.newBuilder().putAllFanout(f).build()))
                    .build();
            });
    }

    private CompletableFuture<GCReply> gc(GCRequest request, IKVReader reader) {
        reader.refresh();
        // subBrokerId -> delivererKey -> tenantId-> CheckRequest
        Map<Integer, Map<String, Map<String, CheckRequest.Builder>>> checkRequestBuilders = new HashMap<>();
        IKVIterator itr = reader.iterator();
        for (itr.seekToFirst(); itr.isValid(); itr.next()) {
            Matching matching = buildMatchRoute(itr.key(), itr.value());
            switch (matching.type()) {
                case Normal -> {
                    if (!routeCache.isCached(matching.tenantId, matching.originalTopicFilter())) {
                        NormalMatching normalMatching = ((NormalMatching) matching);
                        checkRequestBuilders.computeIfAbsent(normalMatching.subBrokerId(), k -> new HashMap<>())
                            .computeIfAbsent(normalMatching.delivererKey(), k -> new HashMap<>())
                            .computeIfAbsent(normalMatching.tenantId, k -> CheckRequest.newBuilder().setTenantId(k)
                                .setDelivererKey(normalMatching.delivererKey()))
                            .addMatchInfo(((NormalMatching) matching).matchInfo());
                    }
                }
                case Group -> {
                    GroupMatching groupMatching = ((GroupMatching) matching);
                    if (!routeCache.isCached(groupMatching.tenantId, unescape(groupMatching.escapedTopicFilter))) {
                        for (NormalMatching normalMatching : groupMatching.receiverList) {
                            checkRequestBuilders.computeIfAbsent(normalMatching.subBrokerId(), k -> new HashMap<>())
                                .computeIfAbsent(normalMatching.delivererKey(), k -> new HashMap<>())
                                .computeIfAbsent(normalMatching.tenantId,
                                    k -> CheckRequest.newBuilder().setTenantId(k)
                                        .setDelivererKey(normalMatching.delivererKey()))
                                .addMatchInfo(normalMatching.matchInfo());
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
                for (Map.Entry<String, CheckRequest.Builder> entry : checkRequestBuilders.get(subBrokerId)
                    .get(delivererKey).entrySet()) {
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
                RouteDetail routeDetail = KVSchemaUtil.parseRouteDetail(itr.value());
                if (routeDetail.type() == RouteDetail.RouteType.NormalReceiver) {
                    tenantsState.incNormalRoutes(routeDetail.tenantId());
                } else {
                    tenantsState.incSharedRoutes(routeDetail.tenantId());
                }
                itr.next();
            }
        }
    }

    private record GlobalTopicFilter(String tenantId, String topicFilter) {
    }
}
