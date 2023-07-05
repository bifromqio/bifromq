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

import static com.baidu.bifromq.dist.entity.EntityUtil.matchRecordKey;
import static com.baidu.bifromq.dist.entity.EntityUtil.subInfoKey;
import static com.baidu.bifromq.dist.entity.EntityUtil.toQualifiedInboxId;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DIST_MAX_UPDATES_IN_BATCH;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basescheduler.BatchCallBuilder;
import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.dist.rpc.proto.ClearRequest;
import com.baidu.bifromq.dist.rpc.proto.ClearSubInfo;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcInput;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcOutput;
import com.baidu.bifromq.dist.rpc.proto.GroupMatchRecord;
import com.baidu.bifromq.dist.rpc.proto.InboxSubInfo;
import com.baidu.bifromq.dist.rpc.proto.LeaveMatchGroup;
import com.baidu.bifromq.dist.rpc.proto.QInboxIdList;
import com.baidu.bifromq.dist.rpc.proto.SubRequest;
import com.baidu.bifromq.dist.rpc.proto.TopicFilterList;
import com.baidu.bifromq.dist.rpc.proto.UnsubRequest;
import com.baidu.bifromq.dist.rpc.proto.UpdateRequest;
import com.baidu.bifromq.type.QoS;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.jctools.maps.NonBlockingHashMap;
import org.jctools.maps.NonBlockingHashSet;

@Slf4j
public class SubCallScheduler
    extends BatchCallScheduler<SubCall, SubCallResult, KVRangeSetting> {
    private final int maxBatchUpdates;
    private final IBaseKVStoreClient kvStoreClient;

    public SubCallScheduler(IBaseKVStoreClient kvStoreClient) {
        super("dist_server_update_batcher");
        maxBatchUpdates = DIST_MAX_UPDATES_IN_BATCH.get();
        this.kvStoreClient = kvStoreClient;
    }

    @Override
    protected BatchCallBuilder<SubCall, SubCallResult> newBuilder(String name, int maxInflights,
                                                                  KVRangeSetting batchKey) {
        return new BatchSubRequestBuilder(name, maxInflights, kvStoreClient, batchKey);
    }

    @Override
    protected Optional<KVRangeSetting> find(SubCall request) {
        return kvStoreClient.findByKey(rangeKey(request));
    }

    private ByteString rangeKey(SubCall call) {
        switch (call.type()) {
            case ADD_TOPIC_FILTER: {
                SubRequest request = ((SubCall.AddTopicFilter) call).request;
                String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                    request.getDelivererKey());
                return subInfoKey(request.getClient().getTrafficId(), qInboxId);
            }
            case INSERT_MATCH_RECORD: {
                SubRequest request = ((SubCall.InsertMatchRecord) call).request;
                String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                    request.getDelivererKey());
                return matchRecordKey(request.getClient().getTrafficId(), request.getTopicFilter(), qInboxId);
            }
            case JOIN_MATCH_GROUP: {
                SubRequest request = ((SubCall.JoinMatchGroup) call).request;
                String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                    request.getDelivererKey());
                return matchRecordKey(request.getClient().getTrafficId(), request.getTopicFilter(), qInboxId);
            }
            case REMOVE_TOPIC_FILTER: {
                UnsubRequest request = ((SubCall.RemoveTopicFilter) call).request;
                String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                    request.getDelivererKey());
                return subInfoKey(request.getClient().getTrafficId(), qInboxId);
            }
            case DELETE_MATCH_RECORD: {
                UnsubRequest request = ((SubCall.DeleteMatchRecord) call).request;
                String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                    request.getDelivererKey());
                return matchRecordKey(request.getClient().getTrafficId(), request.getTopicFilter(), qInboxId);
            }
            case LEAVE_JOIN_GROUP: {
                UnsubRequest request = ((SubCall.LeaveJoinGroup) call).request;
                String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                    request.getDelivererKey());
                return matchRecordKey(request.getClient().getTrafficId(), request.getTopicFilter(), qInboxId);
            }
            case CLEAR: {
                ClearRequest request = ((SubCall.Clear) call).request;
                String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                    request.getDelivererKey());
                return subInfoKey(request.getClient().getTrafficId(), qInboxId);
            }
            default:
                throw new UnsupportedOperationException("Unsupported request type: " + call.type());
        }
    }

    private class BatchSubRequestBuilder extends BatchCallBuilder<SubCall, SubCallResult> {
        private class BatchSubRequest implements IBatchCall<SubCall, SubCallResult> {
            private final AtomicInteger callCount = new AtomicInteger();

            // key: subInfoKeyUtf8, subKey: topicFilter
            private final Map<String, Map<String, QoS>> addTopicFilter = new NonBlockingHashMap<>();

            // key: subInfoKeyUtf8, subKey: topicFilter
            private final Map<String, Map<String, CompletableFuture<SubCallResult>>> onAddTopicFilter =
                new NonBlockingHashMap<>();

            // key: subInfoKeyUtf8, subKey: topicFilter
            private final Map<String, Set<String>> remTopicFilter = new NonBlockingHashMap<>();

            // key: subInfoKeyUtf8, subKey: topicFilter
            private final Map<String, Map<String, CompletableFuture<SubCallResult>>> onRemTopicFilter =
                new NonBlockingHashMap<>();

            // key: normal matchRecordKey
            private final Map<String, QoS> insertMatchRecord = new NonBlockingHashMap<>();

            // key: normal matchRecordKey
            private final Map<String, CompletableFuture<SubCallResult>> onInsertMatchRecord =
                new NonBlockingHashMap<>();

            // key: group matchRecordKey
            private final Map<String, Map<String, QoS>> joinMatchGroup = new NonBlockingHashMap<>();

            // key: group matchRecordKey, subKey: qInboxId
            private final Map<String, Map<String, CompletableFuture<SubCallResult>>> onJoinMatchGroup =
                new NonBlockingHashMap<>();

            // elem: normal matchRecordKey
            private final Set<String> delMatchRecord = new NonBlockingHashSet<>();

            // key: normal matchRecordKey
            private final Map<String, CompletableFuture<SubCallResult>> onDelMatchRecord = new NonBlockingHashMap<>();

            // key: group matchRecordKey value: set of qualified inboxId
            private final Map<String, Set<String>> leaveMatchGroup = new NonBlockingHashMap<>();

            // key: group matchRecordKey
            private final Map<String, CompletableFuture<SubCallResult>> onLeaveMatchGroup = new NonBlockingHashMap<>();

            // key: subInfoKey
            private final Set<ByteString> clearSubInfo = new NonBlockingHashSet<>();

            // key: subInfoKey
            private final Map<ByteString, CompletableFuture<SubCallResult>> onClearSubInfo = new NonBlockingHashMap<>();

            @Override
            public boolean isEmpty() {
                return callCount.get() == 0;
            }

            @Override
            public boolean isEnough() {
                return callCount.get() > maxBatchUpdates;
            }

            @Override
            public CompletableFuture<SubCallResult> add(SubCall subCall) {
                callCount.incrementAndGet();
                switch (subCall.type()) {
                    case ADD_TOPIC_FILTER: {
                        SubRequest request = ((SubCall.AddTopicFilter) subCall).request;
                        String subInfoKeyUtf8 = subInfoKey(request.getClient().getTrafficId(),
                            toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                                request.getDelivererKey())).toStringUtf8();
                        addTopicFilter.computeIfAbsent(subInfoKeyUtf8, k -> new NonBlockingHashMap<>())
                            .putIfAbsent(request.getTopicFilter(), request.getSubQoS());
                        return onAddTopicFilter.computeIfAbsent(subInfoKeyUtf8, k -> new NonBlockingHashMap<>())
                            .computeIfAbsent(request.getTopicFilter(), k -> new CompletableFuture<>());
                    }
                    case INSERT_MATCH_RECORD: {
                        SubRequest request = ((SubCall.InsertMatchRecord) subCall).request;
                        String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                            request.getDelivererKey());
                        String matchRecordKeyUtf8 = matchRecordKey(request.getClient().getTrafficId(),
                            request.getTopicFilter(), qInboxId).toStringUtf8();
                        insertMatchRecord.put(matchRecordKeyUtf8, request.getSubQoS());
                        return onInsertMatchRecord.computeIfAbsent(matchRecordKeyUtf8, k -> new CompletableFuture<>());
                    }
                    case JOIN_MATCH_GROUP: {
                        SubRequest request = ((SubCall.JoinMatchGroup) subCall).request;
                        String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                            request.getDelivererKey());
                        String matchRecordKeyUtf8 = matchRecordKey(request.getClient().getTrafficId(),
                            request.getTopicFilter(), qInboxId).toStringUtf8();
                        joinMatchGroup.computeIfAbsent(matchRecordKeyUtf8, k -> new NonBlockingHashMap<>())
                            .put(qInboxId, request.getSubQoS());
                        return onJoinMatchGroup.computeIfAbsent(matchRecordKeyUtf8, k -> new NonBlockingHashMap<>())
                            .computeIfAbsent(qInboxId, k -> new CompletableFuture<>());
                    }
                    case REMOVE_TOPIC_FILTER: {
                        UnsubRequest request = ((SubCall.RemoveTopicFilter) subCall).request;
                        String subInfoKeyUtf8 = subInfoKey(request.getClient().getTrafficId(),
                            toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                                request.getDelivererKey())).toStringUtf8();
                        remTopicFilter.computeIfAbsent(subInfoKeyUtf8, k -> new NonBlockingHashSet<>())
                            .add(request.getTopicFilter());
                        return onRemTopicFilter.computeIfAbsent(subInfoKeyUtf8, k -> new NonBlockingHashMap<>())
                            .computeIfAbsent(request.getTopicFilter(), k -> new CompletableFuture<>());
                    }
                    case DELETE_MATCH_RECORD: {
                        UnsubRequest request = ((SubCall.DeleteMatchRecord) subCall).request;
                        String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                            request.getDelivererKey());
                        String matchRecordKeyUtf8 = matchRecordKey(request.getClient().getTrafficId(),
                            request.getTopicFilter(), qInboxId).toStringUtf8();
                        delMatchRecord.add(matchRecordKeyUtf8);
                        return onDelMatchRecord.computeIfAbsent(matchRecordKeyUtf8, k -> new CompletableFuture<>());
                    }
                    case LEAVE_JOIN_GROUP: {
                        UnsubRequest request = ((SubCall.LeaveJoinGroup) subCall).request;
                        String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                            request.getDelivererKey());
                        String matchRecordKeyUtf8 = matchRecordKey(request.getClient().getTrafficId(),
                            request.getTopicFilter(), qInboxId).toStringUtf8();
                        leaveMatchGroup.computeIfAbsent(matchRecordKeyUtf8, k -> new NonBlockingHashSet<>())
                            .add(qInboxId);
                        return onLeaveMatchGroup.computeIfAbsent(matchRecordKeyUtf8, k -> new CompletableFuture<>());
                    }
                    case CLEAR: {
                        ClearRequest request = ((SubCall.Clear) subCall).request;
                        ByteString subInfoKey = subInfoKey(request.getClient().getTrafficId(),
                            toQualifiedInboxId(request.getBroker(), request.getInboxId(), request.getDelivererKey()));
                        clearSubInfo.add(subInfoKey);
                        return onClearSubInfo.computeIfAbsent(subInfoKey, k -> new CompletableFuture<>());
                    }
                    default:
                        throw new UnsupportedOperationException("Unsupported request type: " + subCall.type());
                }
            }

            @Override
            public void reset() {
                callCount.set(0);
                addTopicFilter.clear();
                onAddTopicFilter.clear();
                remTopicFilter.clear();
                onRemTopicFilter.clear();
                insertMatchRecord.clear();
                onInsertMatchRecord.clear();
                joinMatchGroup.clear();
                onJoinMatchGroup.clear();
                delMatchRecord.clear();
                onDelMatchRecord.clear();
                leaveMatchGroup.clear();
                onLeaveMatchGroup.clear();
                clearSubInfo.clear();
                onClearSubInfo.clear();
            }

            @Override
            public CompletableFuture<Void> execute() {
                UpdateRequest.Builder reqBuilder = UpdateRequest.newBuilder().setReqId(System.nanoTime());
                if (!addTopicFilter.isEmpty()) {
                    reqBuilder.setAddTopicFilter(com.baidu.bifromq.dist.rpc.proto.AddTopicFilter.newBuilder()
                        .putAllTopicFilter(Maps.transformValues(addTopicFilter,
                            e -> InboxSubInfo.newBuilder().putAllTopicFilters(e).build()))
                        .build());
                }
                if (!remTopicFilter.isEmpty()) {
                    reqBuilder.setRemoveTopicFilter(com.baidu.bifromq.dist.rpc.proto.RemoveTopicFilter.newBuilder()
                        .putAllTopicFilter(Maps.transformValues(remTopicFilter,
                            e -> TopicFilterList.newBuilder().addAllTopicFilter(e).build()))
                        .build());
                }
                if (!insertMatchRecord.isEmpty()) {
                    reqBuilder.setInsertMatchRecord(com.baidu.bifromq.dist.rpc.proto.InsertMatchRecord.newBuilder()
                        .putAllRecord(insertMatchRecord)
                        .build());
                }
                if (!joinMatchGroup.isEmpty()) {
                    reqBuilder.setJoinMatchGroup(com.baidu.bifromq.dist.rpc.proto.JoinMatchGroup.newBuilder()
                        .putAllRecord(Maps.transformValues(joinMatchGroup,
                            e -> GroupMatchRecord.newBuilder().putAllEntry(e).build()))
                        .build());
                }
                if (!delMatchRecord.isEmpty()) {
                    reqBuilder.setDeleteMatchRecord(com.baidu.bifromq.dist.rpc.proto.DeleteMatchRecord.newBuilder()
                        .addAllMatchRecordKey(delMatchRecord)
                        .build());
                }
                if (!leaveMatchGroup.isEmpty()) {
                    reqBuilder.setLeaveMatchGroup(LeaveMatchGroup.newBuilder()
                        .putAllRecord(Maps.transformValues(leaveMatchGroup,
                            e -> QInboxIdList.newBuilder().addAllQInboxId(e).build()))
                        .build());
                }
                if (!clearSubInfo.isEmpty()) {
                    reqBuilder.setClearSubInfo(ClearSubInfo.newBuilder().addAllSubInfoKey(clearSubInfo).build());
                }
                UpdateRequest request = reqBuilder.build();
                return kvStoreClient.execute(range.leader, KVRangeRWRequest.newBuilder()
                    .setReqId(request.getReqId())
                    .setVer(range.ver)
                    .setKvRangeId(range.id)
                    .setRwCoProc(
                        DistServiceRWCoProcInput.newBuilder().setUpdateRequest(request).build().toByteString())
                    .build()).thenApply(reply -> {
                    if (reply.getCode() == ReplyCode.Ok) {
                        try {
                            return DistServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult()).getUpdateReply();
                        } catch (InvalidProtocolBufferException e) {
                            log.error("Unable to parse rw co-proc output", e);
                            throw new RuntimeException(e);
                        }
                    }
                    log.warn("Failed to exec rw co-proc[code={}]", reply.getCode());
                    throw new RuntimeException();
                }).thenAccept(reply -> {
                    for (String subInfoKeyUtf8 : onAddTopicFilter.keySet()) {
                        Map<String, CompletableFuture<SubCallResult>> addResults = onAddTopicFilter.get(subInfoKeyUtf8);
                        for (String qInboxId : addResults.keySet()) {
                            addResults.get(qInboxId)
                                .complete(new SubCallResult.AddTopicFilterResult(reply.getAddTopicFilter()
                                    .getResultMap()
                                    .get(subInfoKeyUtf8)
                                    .getResultsMap()
                                    .get(qInboxId)));
                        }
                    }
                    for (String subInfoKeyUtf8 : onRemTopicFilter.keySet()) {
                        Map<String, CompletableFuture<SubCallResult>> remResults = onRemTopicFilter.get(subInfoKeyUtf8);
                        for (String qInboxId : remResults.keySet()) {
                            remResults.get(qInboxId)
                                .complete(new SubCallResult.RemoveTopicFilterResult(reply.getRemoveTopicFilter()
                                    .getResultMap()
                                    .get(subInfoKeyUtf8)
                                    .getResultMap()
                                    .get(qInboxId)));
                        }
                    }

                    for (String matchRecordKeyUtf8 : onInsertMatchRecord.keySet()) {
                        onInsertMatchRecord.get(matchRecordKeyUtf8)
                            .complete(new SubCallResult.InsertMatchRecordResult());
                    }

                    for (String matchRecordKeyUtf8 : onDelMatchRecord.keySet()) {
                        onDelMatchRecord.get(matchRecordKeyUtf8).complete(new SubCallResult.DeleteMatchRecordResult());
                    }

                    for (String matchRecordKeyUtf8 : onJoinMatchGroup.keySet()) {
                        for (String qInboxId : onJoinMatchGroup.get(matchRecordKeyUtf8).keySet()) {
                            onJoinMatchGroup.get(matchRecordKeyUtf8)
                                .get(qInboxId)
                                .complete(new SubCallResult.JoinMatchGroupResult(reply.getJoinMatchGroup()
                                    .getResultMap()
                                    .get(matchRecordKeyUtf8)
                                    .getResultMap()
                                    .get(qInboxId)));
                        }
                    }

                    for (String matchRecordKeyUtf8 : onLeaveMatchGroup.keySet()) {
                        onLeaveMatchGroup.get(matchRecordKeyUtf8).complete(new SubCallResult.LeaveJoinGroupResult());
                    }

                    for (int i = 0; i < request.getClearSubInfo().getSubInfoKeyCount(); i++) {
                        ByteString subInfoKey = request.getClearSubInfo().getSubInfoKey(i);
                        onClearSubInfo.get(subInfoKey)
                            .complete(new SubCallResult.ClearResult(reply.getClearSubInfo().getSubInfo(i)));
                    }
                }).exceptionally(e -> {
                    for (String subInfoKeyUtf8 : onAddTopicFilter.keySet()) {
                        Map<String, CompletableFuture<SubCallResult>> addResults = onAddTopicFilter.get(subInfoKeyUtf8);
                        for (String qInboxId : addResults.keySet()) {
                            addResults.get(qInboxId).completeExceptionally(e);
                        }
                    }
                    for (String subInfoKeyUtf8 : onRemTopicFilter.keySet()) {
                        Map<String, CompletableFuture<SubCallResult>> remResults = onRemTopicFilter.get(subInfoKeyUtf8);
                        for (String qInboxId : remResults.keySet()) {
                            remResults.get(qInboxId).completeExceptionally(e);
                        }
                    }
                    for (String matchRecordKeyUtf8 : onInsertMatchRecord.keySet()) {
                        onInsertMatchRecord.get(matchRecordKeyUtf8).completeExceptionally(e);
                    }

                    for (String matchRecordKeyUtf8 : onDelMatchRecord.keySet()) {
                        onDelMatchRecord.get(matchRecordKeyUtf8).completeExceptionally(e);
                    }

                    for (String matchRecordKeyUtf8 : onJoinMatchGroup.keySet()) {
                        for (String qInboxId : onJoinMatchGroup.get(matchRecordKeyUtf8).keySet()) {
                            onJoinMatchGroup.get(matchRecordKeyUtf8).get(qInboxId).completeExceptionally(e);
                        }
                    }

                    for (String matchRecordKeyUtf8 : onLeaveMatchGroup.keySet()) {
                        onLeaveMatchGroup.get(matchRecordKeyUtf8).completeExceptionally(e);
                    }

                    for (int i = 0; i < request.getClearSubInfo().getSubInfoKeyCount(); i++) {
                        ByteString subInfoKey = request.getClearSubInfo().getSubInfoKey(i);
                        onClearSubInfo.get(subInfoKey).completeExceptionally(e);
                    }
                    return null;
                });
            }
        }

        private final IBaseKVStoreClient kvStoreClient;
        private final KVRangeSetting range;

        private BatchSubRequestBuilder(String name, int maxInflights,
                                       IBaseKVStoreClient kvStoreClient, KVRangeSetting range) {
            super(name, maxInflights);
            this.kvStoreClient = kvStoreClient;
            this.range = range;
        }

        @Override
        public BatchSubRequest newBatch() {
            return new BatchSubRequest();
        }

        @Override
        public void close() {
        }
    }
}
