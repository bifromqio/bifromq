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

package com.baidu.bifromq.inbox.store;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static com.baidu.bifromq.inbox.store.schema.KVSchemaUtil.bufferedMsgKey;
import static com.baidu.bifromq.inbox.store.schema.KVSchemaUtil.inboxInstanceStartKey;
import static com.baidu.bifromq.inbox.store.schema.KVSchemaUtil.isInboxInstanceKey;
import static com.baidu.bifromq.inbox.store.schema.KVSchemaUtil.isInboxInstanceStartKey;
import static com.baidu.bifromq.inbox.store.schema.KVSchemaUtil.parseInboxInstanceStartKeyPrefix;
import static com.baidu.bifromq.inbox.store.schema.KVSchemaUtil.parseSeq;
import static com.baidu.bifromq.inbox.store.schema.KVSchemaUtil.parseTenantId;
import static com.baidu.bifromq.inbox.store.schema.KVSchemaUtil.qos0MsgKey;
import static com.baidu.bifromq.inbox.store.schema.KVSchemaUtil.qos0QueuePrefix;
import static com.baidu.bifromq.inbox.store.schema.KVSchemaUtil.sendBufferPrefix;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;

import com.baidu.bifromq.basehlc.HLC;
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
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.inbox.storage.proto.BatchAttachReply;
import com.baidu.bifromq.inbox.storage.proto.BatchAttachRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCheckSubReply;
import com.baidu.bifromq.inbox.storage.proto.BatchCheckSubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCommitReply;
import com.baidu.bifromq.inbox.storage.proto.BatchCommitRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateReply;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchDeleteReply;
import com.baidu.bifromq.inbox.storage.proto.BatchDeleteRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchDetachReply;
import com.baidu.bifromq.inbox.storage.proto.BatchDetachRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchFetchReply;
import com.baidu.bifromq.inbox.storage.proto.BatchFetchRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchGetReply;
import com.baidu.bifromq.inbox.storage.proto.BatchGetRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchInsertReply;
import com.baidu.bifromq.inbox.storage.proto.BatchInsertRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchSubReply;
import com.baidu.bifromq.inbox.storage.proto.BatchSubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchReply;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubReply;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubRequest;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.GCReply;
import com.baidu.bifromq.inbox.storage.proto.GCRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxMessage;
import com.baidu.bifromq.inbox.storage.proto.InboxMessageList;
import com.baidu.bifromq.inbox.storage.proto.InboxMetadata;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.InboxSubMessagePack;
import com.baidu.bifromq.inbox.storage.proto.InboxVersion;
import com.baidu.bifromq.inbox.storage.proto.SubMessagePack;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterOption;
import com.baidu.bifromq.inbox.store.schema.KVSchemaUtil;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.inboxservice.Overflowed;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessage;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class InboxStoreCoProc implements IKVRangeCoProc {
    // make it configurable?
    private static final int MAX_GC_BATCH_SIZE = 10000;
    private static long initTime;
    private final KVRangeId id;
    private final ISettingProvider settingProvider;
    private final IEventCollector eventCollector;
    private final TenantsState tenantStates;
    private final Supplier<IKVCloseableReader> rangeReaderProvider;

    InboxStoreCoProc(String clusterId,
                     String storeId,
                     KVRangeId id,
                     ISettingProvider settingProvider,
                     IEventCollector eventCollector,
                     Supplier<IKVCloseableReader> rangeReaderProvider) {
        this.id = id;
        initTime = HLC.INST.getPhysical();
        this.settingProvider = settingProvider;
        this.eventCollector = eventCollector;
        this.rangeReaderProvider = rangeReaderProvider;
        this.tenantStates = new TenantsState(eventCollector, rangeReaderProvider.get(),
            "clusterId", clusterId, "storeId", storeId, "rangeId", KVRangeIdUtil.toString(id));
        log.debug("Loading tenant states: rangeId={}", KVRangeIdUtil.toString(id));
        load();
    }

    @Override
    public CompletableFuture<ROCoProcOutput> query(ROCoProcInput input, IKVReader reader) {
        try {
            InboxServiceROCoProcInput coProcInput = input.getInboxService();
            InboxServiceROCoProcOutput.Builder outputBuilder = InboxServiceROCoProcOutput.newBuilder()
                .setReqId(coProcInput.getReqId());
            switch (coProcInput.getInputCase()) {
                case BATCHGET -> outputBuilder.setBatchGet(batchGet(coProcInput.getBatchGet(), reader));
                case BATCHFETCH -> outputBuilder.setBatchFetch(batchFetch(coProcInput.getBatchFetch(), reader));
                case GC -> outputBuilder.setGc(gcScan(coProcInput.getGc(), reader));
                case BATCHCHECKSUB -> outputBuilder.setBatchCheckSub(
                    batchCheckSub(coProcInput.getBatchCheckSub(), reader));
            }
            return CompletableFuture.completedFuture(ROCoProcOutput.newBuilder()
                .setInboxService(outputBuilder.build())
                .build());
        } catch (Throwable e) {
            log.error("Query co-proc failed", e);
            return CompletableFuture.failedFuture(new IllegalStateException("Query co-proc failed", e));
        }
    }

    @Override
    public Supplier<RWCoProcOutput> mutate(RWCoProcInput input, IKVReader reader, IKVWriter writer) {
        InboxServiceRWCoProcInput coProcInput = input.getInboxService();
        InboxServiceRWCoProcOutput.Builder outputBuilder =
            InboxServiceRWCoProcOutput.newBuilder().setReqId(coProcInput.getReqId());
        AtomicReference<Runnable> afterMutate = new AtomicReference<>();
        switch (coProcInput.getTypeCase()) {
            case BATCHCREATE -> {
                BatchCreateReply.Builder replyBuilder = BatchCreateReply.newBuilder();
                afterMutate.set(batchCreate(coProcInput.getBatchCreate(), replyBuilder, reader, writer));
                outputBuilder.setBatchCreate(replyBuilder);
            }
            case BATCHATTACH -> {
                BatchAttachReply.Builder replyBuilder = BatchAttachReply.newBuilder();
                afterMutate.set(batchAttach(coProcInput.getBatchAttach(), replyBuilder, reader, writer));
                outputBuilder.setBatchAttach(replyBuilder);
            }
            case BATCHDETACH -> {
                BatchDetachReply.Builder replyBuilder = BatchDetachReply.newBuilder();
                afterMutate.set(batchDetach(coProcInput.getBatchDetach(), replyBuilder, reader, writer));
                outputBuilder.setBatchDetach(replyBuilder);
            }
            case BATCHTOUCH -> {
                BatchTouchReply.Builder replyBuilder = BatchTouchReply.newBuilder();
                afterMutate.set(batchTouch(coProcInput.getBatchTouch(), replyBuilder, reader, writer));
                outputBuilder.setBatchTouch(replyBuilder);
            }
            case BATCHDELETE -> {
                BatchDeleteReply.Builder replyBuilder = BatchDeleteReply.newBuilder();
                afterMutate.set(batchDelete(coProcInput.getBatchDelete(), replyBuilder, reader, writer));
                outputBuilder.setBatchDelete(replyBuilder.build());
            }
            case BATCHSUB -> {
                BatchSubReply.Builder replyBuilder = BatchSubReply.newBuilder();
                afterMutate.set(batchSub(coProcInput.getBatchSub(), replyBuilder, reader, writer));
                outputBuilder.setBatchSub(replyBuilder);
            }
            case BATCHUNSUB -> {
                BatchUnsubReply.Builder replyBuilder = BatchUnsubReply.newBuilder();
                afterMutate.set(batchUnsub(coProcInput.getBatchUnsub(), replyBuilder, reader, writer));
                outputBuilder.setBatchUnsub(replyBuilder);
            }
            case BATCHINSERT -> {
                BatchInsertReply.Builder replyBuilder = BatchInsertReply.newBuilder();
                afterMutate.set(batchInsert(coProcInput.getBatchInsert(), replyBuilder, reader, writer));
                outputBuilder.setBatchInsert(replyBuilder);
            }
            case BATCHCOMMIT -> {
                BatchCommitReply.Builder replyBuilder = BatchCommitReply.newBuilder();
                afterMutate.set(batchCommit(coProcInput.getBatchCommit(), replyBuilder, reader, writer));
                outputBuilder.setBatchCommit(replyBuilder);
            }
        }
        RWCoProcOutput output = RWCoProcOutput.newBuilder().setInboxService(outputBuilder.build()).build();
        return () -> {
            afterMutate.get().run();
            return output;
        };
    }

    @Override
    public void reset(Boundary boundary) {
        tenantStates.reset();
        log.debug("Reloading tenant states: rangeId={}", KVRangeIdUtil.toString(id));
        load();
    }

    @Override
    public void close() {
        tenantStates.close();
    }

    @SneakyThrows
    private BatchGetReply batchGet(BatchGetRequest request, IKVReader reader) {
        BatchGetReply.Builder replyBuilder = BatchGetReply.newBuilder();
        for (BatchGetRequest.Params params : request.getParamsList()) {
            BatchGetReply.Result.Builder resultBuilder = BatchGetReply.Result.newBuilder();
            Collection<InboxMetadata> inboxInstances = tenantStates.getAll(params.getTenantId(), params.getInboxId());
            for (InboxMetadata metadata : inboxInstances) {
                if (!hasExpired(metadata, params.getNow())) {
                    InboxVersion.Builder inboxVerBuilder = InboxVersion.newBuilder()
                        .setIncarnation(metadata.getIncarnation())
                        .setVersion(metadata.getVersion())
                        .setKeepAliveSeconds(metadata.getKeepAliveSeconds())
                        .setExpirySeconds(metadata.getExpirySeconds())
                        .setClient(metadata.getClient());
                    if (metadata.hasLwt()) {
                        inboxVerBuilder.setLwt(metadata.getLwt());
                    }
                    resultBuilder.addVersion(inboxVerBuilder.build());
                }
            }
            replyBuilder.addResult(resultBuilder.build());
        }
        return replyBuilder.build();
    }

    private BatchCheckSubReply batchCheckSub(BatchCheckSubRequest request, IKVReader reader) {
        BatchCheckSubReply.Builder replyBuilder = BatchCheckSubReply.newBuilder();
        for (BatchCheckSubRequest.Params params : request.getParamsList()) {
            Optional<InboxMetadata> metadataOpt =
                tenantStates.get(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            if (metadataOpt.isEmpty()) {
                replyBuilder.addCode(BatchCheckSubReply.Code.NO_INBOX);
                continue;
            }
            if (hasExpired(metadataOpt.get(), request.getNow())) {
                replyBuilder.addCode(BatchCheckSubReply.Code.NO_INBOX);
                continue;
            }
            InboxMetadata metadata = metadataOpt.get();
            if (metadata.containsTopicFilters(params.getTopicFilter())) {
                replyBuilder.addCode(BatchCheckSubReply.Code.OK);
            } else {
                replyBuilder.addCode(BatchCheckSubReply.Code.NO_MATCH);
            }
        }
        return replyBuilder.build();
    }

    private BatchFetchReply batchFetch(BatchFetchRequest request, IKVReader reader) {
        BatchFetchReply.Builder replyBuilder = BatchFetchReply.newBuilder();
        for (BatchFetchRequest.Params params : request.getParamsList()) {
            replyBuilder.addResult(fetch(params, reader));
        }
        return replyBuilder.build();
    }

    private Fetched fetch(BatchFetchRequest.Params params, IKVReader reader) {
        Fetched.Builder replyBuilder = Fetched.newBuilder();
        int fetchCount = params.getMaxFetch();
        try {
            Optional<InboxMetadata> inboxMetadataOpt =
                tenantStates.get(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            if (inboxMetadataOpt.isEmpty()) {
                replyBuilder.setResult(Fetched.Result.NO_INBOX);
                return replyBuilder.build();
            }
            InboxMetadata metadata = inboxMetadataOpt.get();
            ByteString inboxInstStartKey =
                inboxInstanceStartKey(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            // deal with qos0 queue
            long startFetchFromSeq = !params.hasQos0StartAfter()
                ? metadata.getQos0StartSeq()
                : Math.max(params.getQos0StartAfter() + 1, metadata.getQos0StartSeq());
            fetchFromInbox(inboxInstStartKey, Integer.MAX_VALUE, metadata.getQos0StartSeq(), startFetchFromSeq,
                metadata.getQos0NextSeq(),
                KVSchemaUtil::qos0MsgKey, Fetched.Builder::addQos0Msg, reader,
                replyBuilder);
            // deal with qos12 queue
            startFetchFromSeq = !params.hasSendBufferStartAfter()
                ? metadata.getSendBufferStartSeq()
                : Math.max(params.getSendBufferStartAfter() + 1, metadata.getSendBufferStartSeq());
            fetchFromInbox(inboxInstStartKey, fetchCount, metadata.getSendBufferStartSeq(), startFetchFromSeq,
                metadata.getSendBufferNextSeq(),
                KVSchemaUtil::bufferedMsgKey, Fetched.Builder::addSendBufferMsg, reader,
                replyBuilder);
            return replyBuilder.setResult(Fetched.Result.OK).build();
        } catch (InvalidProtocolBufferException e) {
            return replyBuilder.setResult(Fetched.Result.ERROR).build();
        }
    }

    private void fetchFromInbox(ByteString inboxInstStartKey,
                                int fetchCount,
                                long startSeq,
                                long startFetchFromSeq,
                                long nextSeq,
                                BiFunction<ByteString, Long, ByteString> keyGenerator,
                                BiConsumer<Fetched.Builder, InboxMessage> messageConsumer,
                                IKVReader reader,
                                Fetched.Builder replyBuilder) throws InvalidProtocolBufferException {
        if (startFetchFromSeq < nextSeq) {
            while (startSeq < nextSeq && fetchCount > 0) {
                ByteString startKey = keyGenerator.apply(inboxInstStartKey, startSeq);
                Optional<ByteString> msgListData = reader.get(startKey);
                // the startSeq may not reflect the latest seq of the first message when query is non-linearized
                // it may point to the message was committed.
                if (msgListData.isEmpty()) {
                    startSeq++;
                    continue;
                }
                List<InboxMessage> messageList = InboxMessageList.parseFrom(msgListData.get()).getMessageList();
                long lastSeq = messageList.get(messageList.size() - 1).getSeq();
                if (lastSeq >= startFetchFromSeq) {
                    for (InboxMessage inboxMsg : messageList) {
                        if (inboxMsg.getSeq() >= startFetchFromSeq) {
                            messageConsumer.accept(replyBuilder, inboxMsg);
                            fetchCount--;
                            if (fetchCount == 0) {
                                break;
                            }
                        }
                    }
                }
                startSeq = lastSeq + 1;
            }
        }
    }

    private Runnable batchCreate(BatchCreateRequest request,
                                 BatchCreateReply.Builder replyBuilder,
                                 IKVReader reader,
                                 IKVWriter writer) {
        Map<String, Set<InboxMetadata>> toBeCached = new HashMap<>();
        for (BatchCreateRequest.Params params : request.getParamsList()) {
            Optional<InboxMetadata> existing =
                tenantStates.get(params.getClient().getTenantId(), params.getInboxId(), params.getIncarnation());
            if (existing.isPresent()) {
                replyBuilder.addSucceed(false);
                continue;
            }
            ByteString metadataKey =
                inboxInstanceStartKey(params.getClient().getTenantId(), params.getInboxId(),
                    params.getIncarnation());
            InboxMetadata.Builder metadataBuilder = InboxMetadata.newBuilder()
                .setInboxId(params.getInboxId())
                .setIncarnation(params.getIncarnation())
                .setVersion(0)
                .setLastActiveTime(params.getNow())
                .setKeepAliveSeconds(params.getKeepAliveSeconds())
                .setExpirySeconds(params.getExpirySeconds())
                .setLimit(params.getLimit())
                .setDropOldest(params.getDropOldest())
                .setClient(params.getClient());
            if (params.hasLwt()) {
                metadataBuilder.setLwt(params.getLwt());
            }
            InboxMetadata metadata = metadataBuilder.build();
            writer.put(metadataKey, metadata.toByteString());
            toBeCached.computeIfAbsent(params.getClient().getTenantId(), k -> new HashSet<>()).add(metadata);
            replyBuilder.addSucceed(true);
        }
        return () -> toBeCached.forEach(
            (tenantId, putSet) -> putSet.forEach(inboxMetadata -> tenantStates.upsert(tenantId, inboxMetadata)));
    }

    private Runnable batchAttach(BatchAttachRequest request,
                                 BatchAttachReply.Builder replyBuilder,
                                 IKVReader reader,
                                 IKVWriter writer) {
        Map<String, Set<InboxMetadata>> toBeCached = new HashMap<>();
        for (BatchAttachRequest.Params params : request.getParamsList()) {
            Optional<InboxMetadata> metadataOpt =
                tenantStates.get(params.getClient().getTenantId(), params.getInboxId(), params.getIncarnation());
            if (metadataOpt.isEmpty()) {
                replyBuilder.addResult(
                    BatchAttachReply.Result.newBuilder().setCode(BatchAttachReply.Code.NO_INBOX).build());
                continue;
            }
            if (metadataOpt.get().getVersion() != params.getVersion()) {
                replyBuilder.addResult(
                    BatchAttachReply.Result.newBuilder().setCode(BatchAttachReply.Code.CONFLICT).build());
                continue;
            }
            InboxMetadata.Builder metadataBuilder = metadataOpt.get().toBuilder()
                .setVersion(params.getVersion() + 1)
                .setLastActiveTime(params.getNow())
                .setKeepAliveSeconds(params.getKeepAliveSeconds())
                .setExpirySeconds(params.getExpirySeconds())
                .setClient(params.getClient());
            if (params.hasLwt()) {
                metadataBuilder.setLwt(params.getLwt());
            } else {
                metadataBuilder.clearLwt();
            }
            InboxMetadata metadata = metadataBuilder.build();
            ByteString inboxInstStartKey = inboxInstanceStartKey(params.getClient().getTenantId(), params.getInboxId(),
                params.getIncarnation());
            writer.put(inboxInstStartKey, metadata.toByteString());
            toBeCached.computeIfAbsent(params.getClient().getTenantId(), k -> new HashSet<>()).add(metadata);
            replyBuilder.addResult(BatchAttachReply.Result.newBuilder()
                .setCode(BatchAttachReply.Code.OK)
                .addAllTopicFilter(metadata.getTopicFiltersMap().keySet())
                .build());
        }
        return () -> toBeCached.forEach(
            (tenantId, putSet) -> putSet.forEach(inboxMetadata -> tenantStates.upsert(tenantId, inboxMetadata)));
    }

    private Runnable batchDetach(BatchDetachRequest request,
                                 BatchDetachReply.Builder replyBuilder,
                                 IKVReader reader,
                                 IKVWriter writer) {
        Map<String, Set<InboxMetadata>> toBeCached = new HashMap<>();
        for (BatchDetachRequest.Params params : request.getParamsList()) {
            Optional<InboxMetadata> metadataOpt =
                tenantStates.get(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            if (metadataOpt.isEmpty()) {
                replyBuilder.addResult(
                    BatchDetachReply.Result.newBuilder().setCode(BatchDetachReply.Code.NO_INBOX).build());
                continue;
            }
            if (metadataOpt.get().getVersion() != params.getVersion()) {
                replyBuilder.addResult(
                    BatchDetachReply.Result.newBuilder().setCode(BatchDetachReply.Code.CONFLICT).build());
                continue;
            }
            InboxMetadata.Builder metadataBuilder = metadataOpt.get().toBuilder()
                .setVersion(params.getVersion() + 1)
                .setLastActiveTime(params.getNow())
                .setExpirySeconds(params.getExpirySeconds());
            BatchDetachReply.Result.Builder resultBuilder = BatchDetachReply.Result.newBuilder()
                .setCode(BatchDetachReply.Code.OK)
                .addAllTopicFilter(metadataBuilder.getTopicFiltersMap().keySet());
            if (params.getDiscardLWT()) {
                metadataBuilder.clearLwt();
            } else if (metadataBuilder.hasLwt()) {
                resultBuilder.setLwt(metadataBuilder.getLwt());
            }
            InboxMetadata metadata = metadataBuilder.build();
            ByteString inboxInstStartKey =
                inboxInstanceStartKey(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            writer.put(inboxInstStartKey, metadata.toByteString());
            toBeCached.computeIfAbsent(params.getTenantId(), k -> new HashSet<>()).add(metadata);
            replyBuilder.addResult(resultBuilder.build());
        }
        return () -> toBeCached.forEach(
            (tenantId, putSet) -> putSet.forEach(inboxMetadata -> tenantStates.upsert(tenantId, inboxMetadata)));
    }

    @SneakyThrows
    private Runnable batchTouch(BatchTouchRequest request,
                                BatchTouchReply.Builder replyBuilder,
                                IKVReader reader,
                                IKVWriter writer) {
        Map<String, Set<InboxMetadata>> toBeCached = new HashMap<>();
        for (BatchTouchRequest.Params params : request.getParamsList()) {
            Optional<InboxMetadata> metadataOpt =
                tenantStates.get(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            if (metadataOpt.isEmpty()) {
                replyBuilder.addCode(BatchTouchReply.Code.NO_INBOX);
                continue;
            }
            if (metadataOpt.get().getVersion() != params.getVersion()) {
                replyBuilder.addCode(BatchTouchReply.Code.CONFLICT);
                continue;
            }
            InboxMetadata metadata = metadataOpt.get().toBuilder()
                .setLastActiveTime(params.getNow())
                .build();
            ByteString inboxInstStartKey =
                inboxInstanceStartKey(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            writer.put(inboxInstStartKey, metadata.toByteString());
            toBeCached.computeIfAbsent(params.getTenantId(), k -> new HashSet<>()).add(metadata);
            replyBuilder.addCode(BatchTouchReply.Code.OK);
        }
        return () -> toBeCached.forEach(
            (tenantId, putSet) -> putSet.forEach(inboxMetadata -> tenantStates.upsert(tenantId, inboxMetadata)));
    }

    @SneakyThrows
    private Runnable batchDelete(BatchDeleteRequest request,
                                 BatchDeleteReply.Builder replyBuilder,
                                 IKVReader reader,
                                 IKVWriter writer) {
        Map<String, Set<InboxMetadata>> toBeRemoved = new HashMap<>();
        reader.refresh();
        IKVIterator itr = reader.iterator();
        for (BatchDeleteRequest.Params params : request.getParamsList()) {
            ByteString inboxInstStartKey =
                inboxInstanceStartKey(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            Optional<InboxMetadata> metadataOpt =
                tenantStates.get(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            if (metadataOpt.isEmpty()) {
                replyBuilder.addResult(BatchDeleteReply.Result
                    .newBuilder()
                    .setCode(BatchDeleteReply.Code.NO_INBOX)
                    .build());
                continue;
            }
            if (metadataOpt.get().getVersion() != params.getVersion()) {
                replyBuilder.addResult(BatchDeleteReply.Result
                    .newBuilder()
                    .setCode(BatchDeleteReply.Code.CONFLICT)
                    .build());
                continue;
            }
            InboxMetadata metadata = metadataOpt.get();
            clearInbox(inboxInstStartKey, metadata, itr, writer);
            toBeRemoved.computeIfAbsent(params.getTenantId(), k -> new HashSet<>()).add(metadata);
            replyBuilder.addResult(BatchDeleteReply.Result
                .newBuilder()
                .setCode(BatchDeleteReply.Code.OK)
                .putAllTopicFilters(metadata.getTopicFiltersMap())
                .build());
        }
        return () -> toBeRemoved.forEach((tenantId, removeSet) -> removeSet.forEach(inboxMetadata -> tenantStates
            .remove(tenantId, inboxMetadata.getInboxId(), inboxMetadata.getIncarnation())));
    }

    private Runnable batchSub(BatchSubRequest request,
                              BatchSubReply.Builder replyBuilder,
                              IKVReader reader,
                              IKVWriter writer) {
        Map<String, Set<InboxMetadata>> toBeCached = new HashMap<>();
        for (BatchSubRequest.Params params : request.getParamsList()) {
            Optional<InboxMetadata> metadataOpt =
                tenantStates.get(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            if (metadataOpt.isEmpty()) {
                replyBuilder.addCode(BatchSubReply.Code.NO_INBOX);
                continue;
            }
            if (metadataOpt.get().getVersion() != params.getVersion()) {
                replyBuilder.addCode(BatchSubReply.Code.CONFLICT);
                continue;
            }
            int maxTopicFilters = settingProvider.provide(Setting.MaxTopicFiltersPerInbox, params.getTenantId());
            InboxMetadata metadata = metadataOpt.get();
            InboxMetadata.Builder metadataBuilder = metadataOpt.get().toBuilder();
            if (metadata.getTopicFiltersCount() < maxTopicFilters) {
                TopicFilterOption option = metadataBuilder.getTopicFiltersMap().get(params.getTopicFilter());
                if (option != null && option.equals(params.getOption())) {
                    replyBuilder.addCode(BatchSubReply.Code.EXISTS);
                } else {
                    metadataBuilder.putTopicFilters(params.getTopicFilter(), params.getOption());
                    replyBuilder.addCode(BatchSubReply.Code.OK);
                }
            } else {
                replyBuilder.addCode(BatchSubReply.Code.EXCEED_LIMIT);
            }
            metadata = metadataBuilder
                .setLastActiveTime(params.getNow())
                .build();
            ByteString inboxInstStartKey =
                inboxInstanceStartKey(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            writer.put(inboxInstStartKey, metadata.toByteString());
            toBeCached.computeIfAbsent(params.getTenantId(), k -> new HashSet<>()).add(metadata);
        }
        return () -> toBeCached.forEach(
            (tenantId, putSet) -> putSet.forEach(inboxMetadata -> tenantStates.upsert(tenantId, inboxMetadata)));
    }

    private Runnable batchUnsub(BatchUnsubRequest request,
                                BatchUnsubReply.Builder replyBuilder,
                                IKVReader reader,
                                IKVWriter write) {
        Map<String, Set<InboxMetadata>> toBeCached = new HashMap<>();
        for (BatchUnsubRequest.Params params : request.getParamsList()) {
            Optional<InboxMetadata> metadataOpt =
                tenantStates.get(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            if (metadataOpt.isEmpty()) {
                replyBuilder.addResult(BatchUnsubReply.Result.newBuilder()
                    .setCode(BatchUnsubReply.Code.NO_INBOX)
                    .build());
                continue;
            }
            if (metadataOpt.get().getVersion() != params.getVersion()) {
                replyBuilder.addResult(BatchUnsubReply.Result.newBuilder()
                    .setCode(BatchUnsubReply.Code.CONFLICT)
                    .build());
                continue;
            }
            InboxMetadata metadata = metadataOpt.get();
            InboxMetadata.Builder metadataBuilder = metadata.toBuilder();
            if (metadataBuilder.containsTopicFilters(params.getTopicFilter())) {
                metadataBuilder.removeTopicFilters(params.getTopicFilter());
                replyBuilder.addResult(BatchUnsubReply.Result.newBuilder()
                    .setCode(BatchUnsubReply.Code.OK)
                    .setOption(metadata.getTopicFiltersMap().get(params.getTopicFilter()))
                    .build());
            } else {
                replyBuilder.addResult(BatchUnsubReply.Result.newBuilder()
                    .setCode(BatchUnsubReply.Code.NO_SUB)
                    .build());
            }
            metadata = metadataBuilder
                .setLastActiveTime(params.getNow())
                .build();
            ByteString inboxInstStartKey =
                inboxInstanceStartKey(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            write.put(inboxInstStartKey, metadata.toByteString());
            toBeCached.computeIfAbsent(params.getTenantId(), k -> new HashSet<>()).add(metadata);
        }
        return () -> toBeCached.forEach(
            (tenantId, putSet) -> putSet.forEach(inboxMetadata -> tenantStates.upsert(tenantId, inboxMetadata)));
    }

    private void clearInbox(ByteString inboxInstanceStartKey,
                            InboxMetadata metadata,
                            IKVIterator itr,
                            IKVWriter writer) {
        if (metadata.getQos0NextSeq() > 0) {
            // find lowest seq of qos0 message
            itr.seek(qos0QueuePrefix(inboxInstanceStartKey));
            if (itr.isValid() && itr.key().startsWith(inboxInstanceStartKey)) {
                for (long s = parseSeq(inboxInstanceStartKey, itr.key()); s < metadata.getQos0NextSeq(); s++) {
                    writer.delete(qos0MsgKey(inboxInstanceStartKey, s));
                }
            }
        }
        if (metadata.getSendBufferNextSeq() > 0) {
            itr.seek(sendBufferPrefix(inboxInstanceStartKey));
            if (itr.isValid() && itr.key().startsWith(inboxInstanceStartKey)) {
                for (long s = parseSeq(inboxInstanceStartKey, itr.key()); s < metadata.getSendBufferNextSeq(); s++) {
                    writer.delete(bufferedMsgKey(inboxInstanceStartKey, s));
                }
            }
        }
        writer.delete(inboxInstanceStartKey);
    }

    @SneakyThrows
    private GCReply gcScan(GCRequest request, IKVReader reader) {
        GCReply.Builder replyBuilder = GCReply.newBuilder().setCode(GCReply.Code.OK);
        if (request.hasTenantId()) {
            Collection<InboxMetadata> inboxInstances = tenantStates.getAll(request.getTenantId());
            for (InboxMetadata metadata : inboxInstances) {
                if (isGCable(metadata, request)) {
                    if (replyBuilder.getCandidateCount() < MAX_GC_BATCH_SIZE) {
                        replyBuilder.addCandidate(GCReply.GCCandidate.newBuilder()
                            .setInboxId(metadata.getInboxId())
                            .setIncarnation(metadata.getIncarnation())
                            .setVersion(metadata.getVersion())
                            .setExpirySeconds(metadata.getExpirySeconds())
                            .setClient(metadata.getClient())
                            .build());
                    } else {
                        break;
                    }
                }
            }
        } else {
            out:
            for (String tenantId : tenantStates.getAllTenantIds()) {
                for (InboxMetadata metadata : tenantStates.getAll(tenantId)) {
                    if (isGCable(metadata, request)) {
                        if (replyBuilder.getCandidateCount() < MAX_GC_BATCH_SIZE) {
                            replyBuilder.addCandidate(GCReply.GCCandidate.newBuilder()
                                .setInboxId(metadata.getInboxId())
                                .setIncarnation(metadata.getIncarnation())
                                .setVersion(metadata.getVersion())
                                .setExpirySeconds(metadata.getExpirySeconds())
                                .setClient(metadata.getClient())
                                .build());
                        } else {
                            break out;
                        }
                    }
                }
            }
        }
        return replyBuilder.build();
    }

    private Runnable batchInsert(BatchInsertRequest request,
                                 BatchInsertReply.Builder replyBuilder,
                                 IKVReader reader,
                                 IKVWriter writer) {
        Map<String, Set<InboxMetadata>> toBeCached = new HashMap<>();
        Map<ClientInfo, Map<QoS, Integer>> dropCountMap = new HashMap<>();
        Map<ClientInfo, Boolean> dropOldestMap = new HashMap<>();

        for (InboxSubMessagePack params : request.getInboxSubMsgPackList()) {
            Optional<InboxMetadata> metadataOpt =
                tenantStates.get(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            if (metadataOpt.isEmpty()) {
                replyBuilder.addResult(BatchInsertReply.Result.newBuilder()
                    .setCode(BatchInsertReply.Code.NO_INBOX)
                    .build());
                continue;
            }
            try {
                InboxMetadata metadata = metadataOpt.get();
                BatchInsertReply.Result.Builder resBuilder = BatchInsertReply.Result.newBuilder()
                    .setCode(BatchInsertReply.Code.OK);
                List<SubMessage> qos0MsgList = new ArrayList<>();
                List<SubMessage> bufferMsgList = new ArrayList<>();
                for (SubMessagePack messagePack : params.getMessagePackList()) {
                    TopicFilterOption tfOption = metadata.getTopicFiltersMap().get(messagePack.getTopicFilter());
                    if (tfOption == null) {
                        resBuilder.addInsertionResult(BatchInsertReply.InsertionResult.newBuilder()
                            .setTopicFilter(messagePack.getTopicFilter())
                            .setIncarnation(messagePack.getIncarnation())
                            .setRejected(true)
                            .build());
                    } else {
                        if (tfOption.getIncarnation() > messagePack.getIncarnation()) {
                            // messages from old sub incarnation
                            log.debug("Receive message from previous subscription: topicFilter={}, inc={}, prevInc={}",
                                messagePack.getTopicFilter(), tfOption.getIncarnation(), messagePack.getIncarnation());
                        }
                        for (TopicMessagePack topicMsgPack : messagePack.getMessagesList()) {
                            String topic = topicMsgPack.getTopic();
                            for (TopicMessagePack.PublisherPack publisherPack : topicMsgPack.getMessageList()) {
                                for (Message message : publisherPack.getMessageList()) {
                                    SubMessage subMessage = new SubMessage(
                                        messagePack.getTopicFilter(),
                                        tfOption,
                                        topic,
                                        publisherPack.getPublisher(),
                                        message
                                    );
                                    QoS finalQoS = QoS.forNumber(
                                        Math.min(message.getPubQoS().getNumber(), tfOption.getQos().getNumber()));
                                    assert finalQoS != null;
                                    switch (finalQoS) {
                                        case AT_MOST_ONCE -> qos0MsgList.add(subMessage);
                                        case AT_LEAST_ONCE, EXACTLY_ONCE -> bufferMsgList.add(subMessage);
                                        default -> {
                                            // never happen, do nothing
                                        }
                                    }
                                }
                            }
                        }
                        resBuilder.addInsertionResult(BatchInsertReply.InsertionResult.newBuilder()
                            .setTopicFilter(messagePack.getTopicFilter())
                            .setIncarnation(messagePack.getIncarnation())
                            .setRejected(false)
                            .build());
                    }
                }
                InboxMetadata.Builder metadataBuilder = metadata.toBuilder();
                dropOldestMap.put(metadata.getClient(), metadata.getDropOldest());
                ByteString inboxInstStartKey =
                    inboxInstanceStartKey(params.getTenantId(), params.getInboxId(), params.getIncarnation());
                Map<QoS, Integer> dropCounts = insertInbox(inboxInstStartKey, qos0MsgList, bufferMsgList,
                    metadataBuilder, reader, writer);
                metadata = metadataBuilder.build();
                replyBuilder.addResult(resBuilder.build());
                writer.put(inboxInstStartKey, metadata.toByteString());
                toBeCached.computeIfAbsent(params.getTenantId(), k -> new HashSet<>()).add(metadata);
                Map<QoS, Integer> aggregated =
                    dropCountMap.computeIfAbsent(metadata.getClient(), k -> new HashMap<>());
                dropCounts.forEach((qos, count) -> aggregated.compute(qos, (k, v) -> {
                    if (v == null) {
                        return count;
                    }
                    return v + count;
                }));
            } catch (Throwable e) {
                log.error("Failed to insert:tenantId={}, inbox={}, inc={}",
                    params.getTenantId(), params.getInboxId(), params.getIncarnation(), e);
                replyBuilder.addResult(BatchInsertReply.Result.newBuilder()
                    .setCode(BatchInsertReply.Code.ERROR)
                    .build());
            }
        }
        return () -> {
            toBeCached.forEach(
                (tenantId, putSet) -> putSet.forEach(
                    inboxMetadata -> tenantStates.upsert(tenantId, inboxMetadata)));
            dropCountMap.forEach((client, dropCounts) -> dropCounts.forEach((qos, count) -> {
                if (count > 0) {
                    eventCollector.report(getLocal(Overflowed.class)
                        .oldest(dropOldestMap.get(client))
                        .isQoS0(qos == QoS.AT_MOST_ONCE)
                        .clientInfo(client)
                        .dropCount(count));
                }
            }));
        };
    }

    private Map<QoS, Integer> insertInbox(ByteString inboxKeyPrefix,
                                          List<SubMessage> qos0MsgList,
                                          List<SubMessage> bufferedMsgList,
                                          InboxMetadata.Builder metaBuilder,
                                          IKVReader reader,
                                          IKVWriter writer) throws InvalidProtocolBufferException {
        Map<QoS, Integer> dropCounts = new HashMap<>();
        if (!qos0MsgList.isEmpty()) {
            long startSeq = metaBuilder.getQos0StartSeq();
            long nextSeq = metaBuilder.getQos0NextSeq();
            int dropCount = insertToInbox(inboxKeyPrefix, startSeq, nextSeq, metaBuilder.getLimit(),
                metaBuilder.getDropOldest(), KVSchemaUtil::qos0MsgKey,
                metaBuilder::setQos0StartSeq, metaBuilder::setQos0NextSeq, qos0MsgList, reader, writer);
            if (dropCount > 0) {
                dropCounts.put(QoS.AT_MOST_ONCE, dropCount);
            }
        }
        if (!bufferedMsgList.isEmpty()) {
            long startSeq = metaBuilder.getSendBufferStartSeq();
            long nextSeq = metaBuilder.getSendBufferNextSeq();
            int dropCount = insertToInbox(inboxKeyPrefix, startSeq, nextSeq, metaBuilder.getLimit(),
                false, KVSchemaUtil::bufferedMsgKey,
                metaBuilder::setSendBufferStartSeq, metaBuilder::setSendBufferNextSeq, bufferedMsgList, reader,
                writer);
            if (dropCount > 0) {
                dropCounts.put(QoS.AT_LEAST_ONCE, dropCount);
            }
        }
        return dropCounts;
    }

    private int insertToInbox(ByteString inboxKeyPrefix,
                              long startSeq,
                              long nextSeq,
                              int limit,
                              boolean dropOldest,
                              BiFunction<ByteString, Long, ByteString> keyGenerator,
                              Function<Long, InboxMetadata.Builder> startSeqSetter,
                              Function<Long, InboxMetadata.Builder> nextSeqSetter,
                              List<SubMessage> messages,
                              IKVReader reader,
                              IKVWriter writer) throws InvalidProtocolBufferException {
        int newMsgCount = messages.size();
        int currCount = (int) (nextSeq - startSeq);
        int dropCount = currCount + newMsgCount - limit;
        if (dropOldest) {
            if (dropCount > 0) {
                if (dropCount >= currCount) {
                    // drop all
                    writer.clear(Boundary.newBuilder()
                        .setStartKey(keyGenerator.apply(inboxKeyPrefix, startSeq))
                        .setEndKey(keyGenerator.apply(inboxKeyPrefix, nextSeq))
                        .build());
                    // and trim if needed
                    if (dropCount > currCount) {
                        messages = messages.subList(dropCount - currCount, newMsgCount);
                    }
                    writer.insert(keyGenerator.apply(inboxKeyPrefix, startSeq + dropCount),
                        buildInboxMessageList(startSeq + dropCount, messages).toByteString());
                } else {
                    reader.refresh();
                    IKVIterator itr = reader.iterator();
                    // drop partially
                    itr.seekForPrev(keyGenerator.apply(inboxKeyPrefix, startSeq + dropCount));
                    long beginSeq = parseSeq(inboxKeyPrefix, itr.key());
                    List<InboxMessage> msgList = InboxMessageList.parseFrom(itr.value()).getMessageList();
                    InboxMessageList.Builder msgListBuilder = InboxMessageList.newBuilder();
                    List<InboxMessage> subMsgList =
                        msgList.subList((int) (startSeq + dropCount - beginSeq), msgList.size());
                    if (!subMsgList.isEmpty()) {
                        msgListBuilder
                            .addAllMessage(subMsgList)
                            .addAllMessage(buildInboxMessageList(subMsgList.get(subMsgList.size() - 1).getSeq() + 1,
                                messages).getMessageList());
                    } else {
                        msgListBuilder.addAllMessage(
                            buildInboxMessageList(startSeq + dropCount, messages).getMessageList());
                    }
                    writer.clear(Boundary.newBuilder()
                        .setStartKey(keyGenerator.apply(inboxKeyPrefix, startSeq))
                        .setEndKey(keyGenerator.apply(inboxKeyPrefix, startSeq + dropCount))
                        .build());
                    if (beginSeq == startSeq + dropCount) {
                        // override existing key
                        writer.put(keyGenerator.apply(inboxKeyPrefix, startSeq + dropCount),
                            msgListBuilder.build().toByteString());
                    } else {
                        // insert new key
                        writer.insert(keyGenerator.apply(inboxKeyPrefix, startSeq + dropCount),
                            msgListBuilder.build().toByteString());
                    }
                }
                startSeq += dropCount;
            } else {
                writer.insert(keyGenerator.apply(inboxKeyPrefix, nextSeq),
                    buildInboxMessageList(nextSeq, messages).toByteString());
            }
            nextSeq += newMsgCount;
            startSeqSetter.apply(startSeq);
            nextSeqSetter.apply(nextSeq);
        } else {
            if (dropCount < newMsgCount) {
                List<SubMessage> subMessages = dropCount > 0 ? messages.subList(0, newMsgCount - dropCount) : messages;
                writer.insert(keyGenerator.apply(inboxKeyPrefix, nextSeq),
                    buildInboxMessageList(nextSeq, subMessages).toByteString());
                nextSeq += subMessages.size();
            }
            // else drop all new messages;
        }
        startSeqSetter.apply(startSeq);
        nextSeqSetter.apply(nextSeq);
        return Math.max(dropCount, 0);
    }

    private InboxMessageList buildInboxMessageList(long beginSeq, List<SubMessage> subMessages) {
        InboxMessageList.Builder listBuilder = InboxMessageList.newBuilder();
        for (SubMessage subMessage : subMessages) {
            listBuilder.addMessage(InboxMessage.newBuilder()
                .setSeq(beginSeq)
                .setTopicFilter(subMessage.topicFilter)
                .setOption(subMessage.option)
                .setMsg(TopicMessage.newBuilder()
                    .setTopic(subMessage.topic)
                    .setPublisher(subMessage.publisher)
                    .setMessage(subMessage.message)
                    .build())
                .build());
            beginSeq++;
        }
        return listBuilder.build();
    }

    private Runnable batchCommit(BatchCommitRequest request,
                                 BatchCommitReply.Builder replyBuilder,
                                 IKVReader reader,
                                 IKVWriter writer) {
        Map<String, Set<InboxMetadata>> toBeCached = new HashMap<>();
        for (BatchCommitRequest.Params params : request.getParamsList()) {
            Optional<InboxMetadata> metadataOpt =
                tenantStates.get(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            if (metadataOpt.isEmpty()) {
                replyBuilder.addCode(BatchCommitReply.Code.NO_INBOX);
                continue;
            }
            if (metadataOpt.get().getVersion() != params.getVersion()) {
                replyBuilder.addCode(BatchCommitReply.Code.CONFLICT);
                continue;
            }
            try {
                ByteString inboxInstStartKey =
                    inboxInstanceStartKey(params.getTenantId(), params.getInboxId(), params.getIncarnation());
                InboxMetadata metadata = metadataOpt.get();
                InboxMetadata.Builder metaBuilder = metadata.toBuilder();
                commitInbox(inboxInstStartKey, params, metaBuilder, reader, writer);
                metadata = metaBuilder
                    .setLastActiveTime(params.getNow())
                    .build();
                writer.put(inboxInstStartKey, metadata.toByteString());
                replyBuilder.addCode(BatchCommitReply.Code.OK);
                toBeCached.computeIfAbsent(params.getTenantId(), k -> new HashSet<>()).add(metadata);
            } catch (Throwable e) {
                log.error("Failed to commit:tenantId={}, inbox={}, inc={}",
                    params.getTenantId(), params.getInboxId(), params.getIncarnation(), e);
                replyBuilder.addCode(BatchCommitReply.Code.ERROR);

            }
        }
        return () -> toBeCached.forEach(
            (tenantId, putSet) -> putSet.forEach(inboxMetadata -> tenantStates.upsert(tenantId, inboxMetadata)));
    }

    private void commitInbox(ByteString scopedInboxId,
                             BatchCommitRequest.Params params,
                             InboxMetadata.Builder metaBuilder,
                             IKVReader reader,
                             IKVWriter writer) throws InvalidProtocolBufferException {
        if (params.hasQos0UpToSeq()) {
            long startSeq = metaBuilder.getQos0StartSeq();
            long nextSeq = metaBuilder.getQos0NextSeq();
            long commitSeq = params.getQos0UpToSeq();
            commitToInbox(scopedInboxId, startSeq, nextSeq, commitSeq, KVSchemaUtil::qos0MsgKey,
                metaBuilder::setQos0StartSeq, reader, writer);
        }
        if (params.hasSendBufferUpToSeq()) {
            long startSeq = metaBuilder.getSendBufferStartSeq();
            long nextSeq = metaBuilder.getSendBufferNextSeq();
            long commitSeq = params.getSendBufferUpToSeq();
            commitToInbox(scopedInboxId, startSeq, nextSeq, commitSeq, KVSchemaUtil::bufferedMsgKey,
                metaBuilder::setSendBufferStartSeq, reader, writer);
        }
    }

    private void commitToInbox(ByteString scopedInboxId,
                               long startSeq,
                               long nextSeq,
                               long commitSeq,
                               BiFunction<ByteString, Long, ByteString> keyGenerator,
                               Function<Long, InboxMetadata.Builder> metadataSetter,
                               IKVReader reader,
                               IKVWriter writer) throws InvalidProtocolBufferException {
        if (startSeq <= commitSeq && commitSeq < nextSeq) {
            while (startSeq <= commitSeq) {
                ByteString msgKey = keyGenerator.apply(scopedInboxId, startSeq);
                Optional<ByteString> msgListData = reader.get(msgKey);
                if (msgListData.isEmpty()) {
                    break;
                }
                List<InboxMessage> msgList = InboxMessageList.parseFrom(msgListData.get()).getMessageList();
                long lastSeq = msgList.get(msgList.size() - 1).getSeq();
                if (lastSeq <= commitSeq) {
                    writer.delete(msgKey);
                    startSeq = lastSeq + 1;
                } else {
                    writer.delete(msgKey);
                    msgList = msgList.subList((int) (commitSeq - startSeq + 1), msgList.size());
                    writer.insert(keyGenerator.apply(scopedInboxId, commitSeq + 1),
                        InboxMessageList.newBuilder().addAllMessage(msgList).build().toByteString());
                    startSeq = commitSeq + 1;
                    break;
                }
            }
            metadataSetter.apply(startSeq);
        }
    }

    private void load() {
        try (IKVCloseableReader reader = rangeReaderProvider.get()) {
            IKVIterator itr = reader.iterator();
            int probe = 0;
            for (itr.seekToFirst(); itr.isValid(); ) {
                if (isInboxInstanceStartKey(itr.key())) {
                    probe = 0;
                    try {
                        tenantStates.upsert(parseTenantId(itr.key()), InboxMetadata.parseFrom(itr.value()));
                    } catch (InvalidProtocolBufferException e) {
                        log.error("Unexpected error", e);
                    } finally {
                        itr.next();
                        probe++;
                    }
                } else {
                    if (probe < 20) {
                        itr.next();
                        probe++;
                    } else {
                        if (isInboxInstanceKey(itr.key())) {
                            itr.seek(upperBound(parseInboxInstanceStartKeyPrefix(itr.key())));
                        } else {
                            itr.next();
                            probe++;
                        }
                    }
                }
            }
            log.debug("Tenant states loaded: rangeId={}", KVRangeIdUtil.toString(id));
        }
    }

    private boolean hasExpired(InboxMetadata metadata, long nowTS) {
        return hasExpired(metadata, metadata.getExpirySeconds(), nowTS);
    }

    private boolean hasExpired(InboxMetadata metadata, int expirySeconds, long nowTS) {
        Duration lastActiveTime = Duration.ofMillis(metadata.getLastActiveTime());
        if (Duration.ofMillis(initTime).compareTo(lastActiveTime) > 0) {
            // if lastActiveTime is before boot time, it may be expired
            // detach operation will refresh lastActiveTime
            return true;
        }
        Duration now = Duration.ofMillis(nowTS);
        // now > 1.5 * keepAlive + expirySeconds since last active time
        Duration expireAt = lastActiveTime
            .plus(Duration.ofMillis((long) (Duration.ofSeconds(metadata.getKeepAliveSeconds()).toMillis() * 1.5)))
            .plus(Duration.ofSeconds(expirySeconds));
        return now.compareTo(expireAt) > 0;
    }

    private boolean isGCable(InboxMetadata metadata, GCRequest request) {
        if (request.hasTenantId() && !request.getTenantId().equals(metadata.getClient().getTenantId())) {
            return false;
        }
        if (request.hasExpirySeconds()) {
            return hasExpired(metadata, request.getExpirySeconds(), request.getNow());
        }
        return hasExpired(metadata, request.getNow());
    }

    private record SubMessage(String topicFilter,
                              TopicFilterOption option,
                              String topic,
                              ClientInfo publisher,
                              Message message) {
    }
}
