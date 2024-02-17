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

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.intersect;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static com.baidu.bifromq.inbox.util.KeyUtil.bufferMsgKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.inboxKeyPrefix;
import static com.baidu.bifromq.inbox.util.KeyUtil.inboxPrefix;
import static com.baidu.bifromq.inbox.util.KeyUtil.isBufferMessageKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.isMetadataKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.isQoS0MessageKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseInboxKeyPrefix;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseIncarnation;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseSeq;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseTenantId;
import static com.baidu.bifromq.inbox.util.KeyUtil.qos0InboxMsgKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.qos0InboxPrefix;
import static com.baidu.bifromq.inbox.util.KeyUtil.sendBufferPrefix;
import static com.baidu.bifromq.inbox.util.KeyUtil.tenantPrefix;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProc;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.api.IKVWriter;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ROCoProcOutput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.BatchAttachReply;
import com.baidu.bifromq.inbox.storage.proto.BatchAttachRequest;
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
import com.baidu.bifromq.inbox.storage.proto.CollectMetricsReply;
import com.baidu.bifromq.inbox.storage.proto.CollectMetricsRequest;
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
import com.baidu.bifromq.inbox.util.KeyUtil;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.inboxservice.Overflowed;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessage;
import com.baidu.bifromq.type.TopicMessagePack;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class InboxStoreCoProc implements IKVRangeCoProc {
    private static long initTime;
    private final ISettingProvider settingProvider;
    private final IEventCollector eventCollector;
    private final Cache<ByteString, Optional<InboxMetadata>> inboxMetadataCache;

    InboxStoreCoProc(ISettingProvider settingProvider,
                     IEventCollector eventCollector,
                     Duration purgeDelay) {
        initTime = HLC.INST.getPhysical();
        this.settingProvider = settingProvider;
        this.eventCollector = eventCollector;
        inboxMetadataCache = Caffeine.newBuilder()
            .expireAfterAccess(purgeDelay)
            .build();
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
                case COLLECTMETRICS -> outputBuilder.setCollectedMetrics(
                    collect(coProcInput.getCollectMetrics(), reader));
                case GC -> outputBuilder.setGc(gcScan(coProcInput.getGc(), reader));
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
    public void close() {
    }

    @SneakyThrows
    private BatchGetReply batchGet(BatchGetRequest request, IKVReader reader) {
        BatchGetReply.Builder replyBuilder = BatchGetReply.newBuilder();
        IKVIterator kvItr = reader.iterator();
        for (BatchGetRequest.Params params : request.getParamsList()) {
            BatchGetReply.Result.Builder resultBuilder = BatchGetReply.Result.newBuilder();
            ByteString inboxPrefix = inboxPrefix(params.getTenantId(), params.getInboxId());
            kvItr.seek(inboxPrefix);
            if (kvItr.isValid() && isMetadataKey(kvItr.key()) && kvItr.key().startsWith(inboxPrefix)) {
                InboxMetadata metadata = InboxMetadata.parseFrom(kvItr.value());
                if (!hasExpired(metadata, params.getNow())) {
                    InboxVersion.Builder inboxVerBuilder = InboxVersion.newBuilder()
                        .setIncarnation(parseIncarnation(kvItr.key()))
                        .setVersion(metadata.getVersion())
                        .setKeepAliveSeconds(metadata.getKeepAliveSeconds())
                        .setExpirySeconds(metadata.getExpirySeconds())
                        .setClient(metadata.getClient());
                    if (metadata.hasLwt()) {
                        inboxVerBuilder.setLwt(metadata.getLwt());
                    }
                    resultBuilder.addVersion(inboxVerBuilder.build());
                }
                kvItr.seek(upperBound(kvItr.key()));
            }
            replyBuilder.addResult(resultBuilder.build());
        }
        return replyBuilder.build();
    }

    private BatchFetchReply batchFetch(BatchFetchRequest request, IKVReader reader) {
        BatchFetchReply.Builder replyBuilder = BatchFetchReply.newBuilder();
        IKVIterator itr = reader.iterator();
        for (BatchFetchRequest.Params params : request.getParamsList()) {
            replyBuilder.addResult(fetch(params, itr, reader));

        }
        return replyBuilder.build();
    }

    private Fetched fetch(BatchFetchRequest.Params params, IKVIterator itr, IKVReader reader) {
        Fetched.Builder replyBuilder = Fetched.newBuilder();
        int fetchCount = params.getMaxFetch();
        try {
            ByteString metadataKey = inboxKeyPrefix(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            Optional<InboxMetadata> inboxMetadataOpt = getInboxMetadata(metadataKey, reader);
            if (inboxMetadataOpt.isEmpty()) {
                replyBuilder.setResult(Fetched.Result.NO_INBOX);
                return replyBuilder.build();
            }
            InboxMetadata metadata = inboxMetadataOpt.get();
            // deal with qos0 queue
            long startFetchFromSeq = !params.hasQos0StartAfter()
                ? metadata.getQos0StartSeq()
                : Math.max(params.getQos0StartAfter() + 1, metadata.getQos0StartSeq());
            fetchFromInbox(metadataKey, Integer.MAX_VALUE, startFetchFromSeq, metadata.getQos0NextSeq(),
                KeyUtil::isQoS0MessageKey, KeyUtil::qos0InboxMsgKey, Fetched.Builder::addQos0Msg, itr, replyBuilder);
            // deal with qos12 queue
            startFetchFromSeq = !params.hasSendBufferStartAfter()
                ? metadata.getSendBufferStartSeq()
                : Math.max(params.getSendBufferStartAfter() + 1, metadata.getSendBufferStartSeq());
            fetchFromInbox(metadataKey, fetchCount, startFetchFromSeq, metadata.getSendBufferNextSeq(),
                KeyUtil::isBufferMessageKey, KeyUtil::bufferMsgKey, Fetched.Builder::addSendBufferMsg, itr,
                replyBuilder);
            return replyBuilder.setResult(Fetched.Result.OK).build();
        } catch (InvalidProtocolBufferException e) {
            return replyBuilder.setResult(Fetched.Result.ERROR).build();
        }
    }

    private int fetchFromInbox(ByteString inboxKeyPrefix,
                               int fetchCount,
                               long startFetchFromSeq,
                               long nextSeq,
                               BiFunction<ByteString, ByteString, Boolean> keyChecker,
                               BiFunction<ByteString, Long, ByteString> keyGenerator,
                               BiConsumer<Fetched.Builder, InboxMessage> messageConsumer,
                               IKVIterator itr,
                               Fetched.Builder replyBuilder) throws InvalidProtocolBufferException {
        if (startFetchFromSeq < nextSeq) {
            itr.seekForPrev(keyGenerator.apply(inboxKeyPrefix, startFetchFromSeq));
            if (itr.isValid() && keyChecker.apply(itr.key(), inboxKeyPrefix)) {
                long beginSeq = parseSeq(inboxKeyPrefix, itr.key());
                InboxMessageList messageList = InboxMessageList.parseFrom(itr.value());
                for (int i = (int) (startFetchFromSeq - beginSeq); i < messageList.getMessageCount(); i++) {
                    if (fetchCount > 0) {
                        assert messageList.getMessage(i).getSeq() == beginSeq + i;
                        messageConsumer.accept(replyBuilder, messageList.getMessage(i));
                        fetchCount--;
                    } else {
                        break;
                    }
                }
            }
            itr.next();
            outer:
            while (fetchCount > 0 && itr.isValid() && keyChecker.apply(itr.key(), inboxKeyPrefix)) {
                long startSeq = parseSeq(inboxKeyPrefix, itr.key());
                InboxMessageList messageList = InboxMessageList.parseFrom(itr.value());
                for (int i = 0; i < messageList.getMessageCount(); i++) {
                    if (fetchCount > 0) {
                        assert messageList.getMessage(i).getSeq() == startSeq + i;
                        messageConsumer.accept(replyBuilder, messageList.getMessage(i));
                        fetchCount--;
                    } else {
                        break outer;
                    }
                }
                itr.next();
            }
        }
        return fetchCount;
    }

    private Runnable batchCreate(BatchCreateRequest request,
                                 BatchCreateReply.Builder replyBuilder,
                                 IKVReader reader,
                                 IKVWriter writer) {
        Map<ByteString, InboxMetadata> toBeCached = new HashMap<>();
        for (BatchCreateRequest.Params params : request.getParamsList()) {
            ByteString metadataKey =
                inboxKeyPrefix(params.getClient().getTenantId(), params.getInboxId(), params.getIncarnation());
            if (reader.exist(metadataKey)) {
                replyBuilder.addSucceed(false);
                continue;
            }
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
            toBeCached.put(metadataKey, metadata);
            replyBuilder.addSucceed(true);
        }
        return () -> toBeCached.forEach(
            (inboxMetadataKey, inboxMetadata) -> inboxMetadataCache.put(inboxMetadataKey, Optional.of(inboxMetadata)));
    }

    private Runnable batchAttach(BatchAttachRequest request,
                                 BatchAttachReply.Builder replyBuilder,
                                 IKVReader reader,
                                 IKVWriter writer) {
        Map<ByteString, InboxMetadata> toBeCached = new HashMap<>();
        for (BatchAttachRequest.Params params : request.getParamsList()) {
            ByteString metadataKey =
                inboxKeyPrefix(params.getClient().getTenantId(), params.getInboxId(), params.getIncarnation());
            Optional<InboxMetadata> metadataOpt = getInboxMetadata(metadataKey, reader);
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

            writer.put(metadataKey, metadata.toByteString());
            toBeCached.put(metadataKey, metadata);
            replyBuilder.addResult(BatchAttachReply.Result.newBuilder()
                .setCode(BatchAttachReply.Code.OK)
                .addAllTopicFilter(metadata.getTopicFiltersMap().keySet())
                .build());
        }
        return () -> toBeCached.forEach(
            (inboxMetadataKey, inboxMetadata) -> inboxMetadataCache.put(inboxMetadataKey, Optional.of(inboxMetadata)));
    }

    private Runnable batchDetach(BatchDetachRequest request,
                                 BatchDetachReply.Builder replyBuilder,
                                 IKVReader reader,
                                 IKVWriter writer) {
        Map<ByteString, InboxMetadata> toBeCached = new HashMap<>();
        for (BatchDetachRequest.Params params : request.getParamsList()) {
            ByteString metadataKey =
                inboxKeyPrefix(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            Optional<InboxMetadata> metadataOpt = getInboxMetadata(metadataKey, reader);
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
            writer.put(metadataKey, metadata.toByteString());
            toBeCached.put(metadataKey, metadata);
            replyBuilder.addResult(resultBuilder.build());
        }
        return () -> toBeCached.forEach(
            (inboxMetadataKey, inboxMetadata) -> inboxMetadataCache.put(inboxMetadataKey, Optional.of(inboxMetadata)));

    }

    @SneakyThrows
    private Runnable batchTouch(BatchTouchRequest request,
                                BatchTouchReply.Builder replyBuilder,
                                IKVReader reader,
                                IKVWriter writer) {
        Map<ByteString, InboxMetadata> toBeCached = new HashMap<>();
        for (BatchTouchRequest.Params params : request.getParamsList()) {
            ByteString metadataKey = inboxKeyPrefix(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            Optional<InboxMetadata> metadataOpt = getInboxMetadata(metadataKey, reader);
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
            writer.put(metadataKey, metadata.toByteString());
            toBeCached.put(metadataKey, metadata);
            replyBuilder.addCode(BatchTouchReply.Code.OK);
        }
        return () -> toBeCached.forEach(
            (inboxMetadataKey, inboxMetadata) -> inboxMetadataCache.put(inboxMetadataKey, Optional.of(inboxMetadata)));
    }

    @SneakyThrows
    private Runnable batchDelete(BatchDeleteRequest request,
                                 BatchDeleteReply.Builder replyBuilder,
                                 IKVReader reader,
                                 IKVWriter writer) {
        Set<ByteString> toBeRemoved = new HashSet<>();
        for (BatchDeleteRequest.Params params : request.getParamsList()) {
            ByteString metadataKey = inboxKeyPrefix(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            Optional<InboxMetadata> metadataOpt = getInboxMetadata(metadataKey, reader);
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
            InboxMetadata metadata = metadataOpt.get().toBuilder()
                .setVersion(metadataOpt.get().getVersion() + 1)
                .clearLwt()
                .build();
            writer.put(metadataKey, metadata.toByteString());
            clearInbox(metadataKey, metadata, reader.iterator(), writer);
            toBeRemoved.add(metadataKey);
            replyBuilder.addResult(BatchDeleteReply.Result
                .newBuilder()
                .setCode(BatchDeleteReply.Code.OK)
                .addAllTopicFilters(metadata.getTopicFiltersMap().keySet())
                .build());
        }
        return () -> toBeRemoved.forEach(inboxMetadataCache::invalidate);
    }

    private Runnable batchSub(BatchSubRequest request,
                              BatchSubReply.Builder replyBuilder,
                              IKVReader reader,
                              IKVWriter writer) {
        Map<ByteString, InboxMetadata> toBeCached = new HashMap<>();
        for (BatchSubRequest.Params params : request.getParamsList()) {
            ByteString metadataKey = inboxKeyPrefix(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            Optional<InboxMetadata> metadataOpt = getInboxMetadata(metadataKey, reader);
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
            writer.put(metadataKey, metadata.toByteString());
            toBeCached.put(metadataKey, metadata);
        }
        return () -> toBeCached.forEach((scopedInboxId, inboxMetadata) ->
            inboxMetadataCache.put(scopedInboxId, Optional.of(inboxMetadata)));
    }

    private Runnable batchUnsub(BatchUnsubRequest request,
                                BatchUnsubReply.Builder replyBuilder,
                                IKVReader reader,
                                IKVWriter write) {
        Map<ByteString, InboxMetadata> toBeCached = new HashMap<>();
        for (BatchUnsubRequest.Params params : request.getParamsList()) {
            ByteString metadataKey = inboxKeyPrefix(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            Optional<InboxMetadata> metadataOpt = getInboxMetadata(metadataKey, reader);
            if (metadataOpt.isEmpty()) {
                replyBuilder.addCode(BatchUnsubReply.Code.NO_INBOX);
                continue;
            }
            if (metadataOpt.get().getVersion() != params.getVersion()) {
                replyBuilder.addCode(BatchUnsubReply.Code.CONFLICT);
                continue;
            }
            InboxMetadata metadata = metadataOpt.get();
            InboxMetadata.Builder metadataBuilder = metadata.toBuilder();
            if (metadataBuilder.containsTopicFilters(params.getTopicFilter())) {
                metadataBuilder.removeTopicFilters(params.getTopicFilter());
                replyBuilder.addCode(BatchUnsubReply.Code.OK);
            } else {
                replyBuilder.addCode(BatchUnsubReply.Code.NO_SUB);
            }
            metadata = metadataBuilder
                .setLastActiveTime(request.getNow())
                .build();
            write.put(metadataKey, metadata.toByteString());
            toBeCached.put(metadataKey, metadata);
        }

        return () -> toBeCached.forEach((scopedInboxId, inboxMetadata) ->
            inboxMetadataCache.put(scopedInboxId, Optional.of(inboxMetadata)));
    }

    private void clearInbox(ByteString inboxKeyPrefix, InboxMetadata metadata, IKVIterator itr,
                            IKVWriter writer) {
        if (metadata.getQos0NextSeq() > 0) {
            // find lowest seq of qos0 message
            itr.seek(qos0InboxPrefix(inboxKeyPrefix));
            if (itr.isValid() && isQoS0MessageKey(itr.key(), inboxKeyPrefix)) {
                for (long s = parseSeq(inboxKeyPrefix, itr.key()); s < metadata.getQos0NextSeq(); s++) {
                    writer.delete(qos0InboxMsgKey(inboxKeyPrefix, s));
                }
            }
        }
        if (metadata.getSendBufferNextSeq() > 0) {
            itr.seek(sendBufferPrefix(inboxKeyPrefix));
            if (itr.isValid() && isBufferMessageKey(itr.key(), inboxKeyPrefix)) {
                for (long s = parseSeq(inboxKeyPrefix, itr.key()); s < metadata.getSendBufferNextSeq(); s++) {
                    writer.delete(bufferMsgKey(inboxKeyPrefix, s));
                }
            }
        }
        writer.delete(inboxKeyPrefix);
    }

    @SneakyThrows
    private GCReply gcScan(GCRequest request, IKVReader reader) {
        long start = System.nanoTime();
        long yieldThreshold = TimeUnit.NANOSECONDS.convert(100, TimeUnit.MILLISECONDS);
        GCReply.Builder replyBuilder = GCReply.newBuilder().setCode(GCReply.Code.OK);
        IKVIterator itr = reader.iterator();
        if (request.hasCursor()) {
            itr.seek(request.getCursor());
        } else if (request.hasTenantId()) {
            itr.seek(KeyUtil.tenantPrefix(request.getTenantId()));
        } else {
            itr.seekToFirst();
        }
        while (itr.isValid()) {
            if (System.nanoTime() - start > yieldThreshold) {
                if (!request.hasTenantId() || itr.key().startsWith(KeyUtil.tenantPrefix(request.getTenantId()))) {
                    replyBuilder.setCursor(itr.key());
                }
                break;
            }
            if (isMetadataKey(itr.key())) {
                InboxMetadata metadata = InboxMetadata.parseFrom(itr.value());
                if (isGCable(metadata, request)) {
                    if (replyBuilder.getCandidateCount() < request.getLimit()) {
                        replyBuilder.addCandidate(GCReply.GCCandidate.newBuilder()
                            .setInboxId(metadata.getInboxId())
                            .setIncarnation(metadata.getIncarnation())
                            .setVersion(metadata.getVersion())
                            .setExpirySeconds(metadata.getExpirySeconds())
                            .setClient(metadata.getClient())
                            .build());
                    } else {
                        replyBuilder.setCursor(itr.key());
                        break;
                    }
                }
            }
            ByteString inboxKeyPrefix = parseInboxKeyPrefix(itr.key());
            itr.seek(upperBound(inboxKeyPrefix));
        }
        return replyBuilder.build();
    }

    private CollectMetricsReply collect(CollectMetricsRequest request, IKVReader reader) {
        CollectMetricsReply.Builder builder = CollectMetricsReply.newBuilder().setReqId(request.getReqId());
        try {
            IKVIterator itr = reader.iterator();
            for (itr.seekToFirst(); itr.isValid(); ) {
                String tenantId = parseTenantId(itr.key());
                ByteString startKey = tenantPrefix(tenantId);
                ByteString endKey = upperBound(tenantPrefix(tenantId));
                builder.putUsedSpaces(tenantId, reader.size(intersect(reader.boundary(), Boundary.newBuilder()
                    .setStartKey(startKey)
                    .setEndKey(endKey)
                    .build())));
                itr.seek(endKey);
            }
        } catch (Exception e) {
            // never happens
        }
        return builder.build();
    }

    private record SubMessage(String topicFilter,
                              TopicFilterOption option,
                              String topic,
                              ClientInfo publisher,
                              Message message) {
    }

    private Runnable batchInsert(BatchInsertRequest request,
                                 BatchInsertReply.Builder replyBuilder,
                                 IKVReader reader,
                                 IKVWriter writer) {
        IKVIterator itr = reader.iterator();
        Map<ByteString, InboxMetadata> toBeCached = new HashMap<>();
        Map<ClientInfo, Map<QoS, Integer>> dropCountMap = new HashMap<>();
        Map<ClientInfo, Boolean> dropOldestMap = new HashMap<>();

        for (InboxSubMessagePack params : request.getInboxSubMsgPackList()) {
            ByteString metadataKey = inboxKeyPrefix(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            Optional<InboxMetadata> metadataOpt = getInboxMetadata(metadataKey, reader);
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
                Map<String, Boolean> reject = new HashMap<>();
                for (SubMessagePack messagePack : params.getMessagePackList()) {
                    TopicFilterOption tfOption = metadata.getTopicFiltersMap().get(messagePack.getTopicFilter());
                    if (tfOption == null) {
                        reject.put(messagePack.getTopicFilter(), true);
                    } else {
                        reject.put(messagePack.getTopicFilter(), false);
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
                                    }
                                }
                            }
                        }
                    }
                }
                resBuilder.addAllInsertionResult(reject.entrySet().stream()
                    .map(e -> BatchInsertReply.InsertionResult.newBuilder()
                        .setTopicFilter(e.getKey())
                        .setRejected(e.getValue())
                        .build()).toList());
                InboxMetadata.Builder metadataBuilder = metadata.toBuilder();
                dropOldestMap.put(metadata.getClient(), metadata.getDropOldest());
                Map<QoS, Integer> dropCounts = insertInbox(metadataKey, qos0MsgList, bufferMsgList,
                    metadataBuilder, itr, reader, writer);
                metadata = metadataBuilder.build();
                replyBuilder.addResult(resBuilder.build());
                writer.put(metadataKey, metadata.toByteString());
                toBeCached.put(metadataKey, metadata);
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
                (scopedInboxId, inboxMetadata) -> inboxMetadataCache.put(scopedInboxId, Optional.of(inboxMetadata)));
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
                                          IKVIterator itr,
                                          IKVReader reader,
                                          IKVWriter writer) throws InvalidProtocolBufferException {
        Map<QoS, Integer> dropCounts = new HashMap<>();
        if (!qos0MsgList.isEmpty()) {
            long startSeq = metaBuilder.getQos0StartSeq();
            long nextSeq = metaBuilder.getQos0NextSeq();
            int dropCount = insertToInbox(inboxKeyPrefix, startSeq, nextSeq, metaBuilder.getLimit(),
                metaBuilder.getDropOldest(), KeyUtil::qos0InboxMsgKey,
                metaBuilder::setQos0StartSeq, metaBuilder::setQos0NextSeq, qos0MsgList, itr, writer);
            if (dropCount > 0) {
                dropCounts.put(QoS.AT_MOST_ONCE, dropCount);
            }
        }
        if (!bufferedMsgList.isEmpty()) {
            long startSeq = metaBuilder.getSendBufferStartSeq();
            long nextSeq = metaBuilder.getSendBufferNextSeq();
            int dropCount = insertToInbox(inboxKeyPrefix, startSeq, nextSeq, metaBuilder.getLimit(),
                false, KeyUtil::bufferMsgKey,
                metaBuilder::setSendBufferStartSeq, metaBuilder::setSendBufferNextSeq, bufferedMsgList, itr, writer);
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
                              IKVIterator itr,
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
        IKVIterator itr = reader.iterator();
        Map<ByteString, InboxMetadata> toUpdate = new HashMap<>();
        for (BatchCommitRequest.Params params : request.getParamsList()) {
            ByteString metadataKey = inboxKeyPrefix(params.getTenantId(), params.getInboxId(), params.getIncarnation());
            Optional<InboxMetadata> metadataOpt = getInboxMetadata(metadataKey, reader);
            if (metadataOpt.isEmpty()) {
                replyBuilder.addCode(BatchCommitReply.Code.NO_INBOX);
                continue;
            }
            if (metadataOpt.get().getVersion() != params.getVersion()) {
                replyBuilder.addCode(BatchCommitReply.Code.CONFLICT);
                continue;
            }
            try {
                InboxMetadata metadata = metadataOpt.get();
                InboxMetadata.Builder metaBuilder = metadata.toBuilder();
                commitInbox(metadataKey, params, metaBuilder, itr, writer);
                metadata = metaBuilder
                    .setLastActiveTime(params.getNow())
                    .build();
                writer.put(metadataKey, metadata.toByteString());
                replyBuilder.addCode(BatchCommitReply.Code.OK);
                toUpdate.put(metadataKey, metadata);
            } catch (Throwable e) {
                log.error("Failed to commit:tenantId={}, inbox={}, inc={}",
                    params.getTenantId(), params.getInboxId(), params.getIncarnation(), e);
                replyBuilder.addCode(BatchCommitReply.Code.ERROR);

            }
        }
        return () -> toUpdate.forEach((scopedInboxId, inboxMetadata) ->
            inboxMetadataCache.put(scopedInboxId, Optional.of(inboxMetadata)));
    }

    private void commitInbox(ByteString scopedInboxId,
                             BatchCommitRequest.Params params,
                             InboxMetadata.Builder metaBuilder,
                             IKVIterator itr,
                             IKVWriter writer) throws InvalidProtocolBufferException {
        if (params.hasQos0UpToSeq()) {
            long startSeq = metaBuilder.getQos0StartSeq();
            long nextSeq = metaBuilder.getQos0NextSeq();
            long commitSeq = params.getQos0UpToSeq();
            commitToInbox(scopedInboxId, startSeq, nextSeq, commitSeq, KeyUtil::qos0InboxMsgKey,
                metaBuilder::setQos0StartSeq, itr, writer);
        }
        if (params.hasSendBufferUpToSeq()) {
            long startSeq = metaBuilder.getSendBufferStartSeq();
            long nextSeq = metaBuilder.getSendBufferNextSeq();
            long commitSeq = params.getSendBufferUpToSeq();
            commitToInbox(scopedInboxId, startSeq, nextSeq, commitSeq, KeyUtil::bufferMsgKey,
                metaBuilder::setSendBufferStartSeq, itr, writer);
        }
    }

    private void commitToInbox(ByteString scopedInboxId,
                               long startSeq,
                               long nextSeq,
                               long commitSeq,
                               BiFunction<ByteString, Long, ByteString> keyGenerator,
                               Function<Long, InboxMetadata.Builder> metadataSetter,
                               IKVIterator itr,
                               IKVWriter writer) throws InvalidProtocolBufferException {
        if (startSeq <= commitSeq && commitSeq < nextSeq) {
            itr.seekForPrev(keyGenerator.apply(scopedInboxId, commitSeq));
            long beginSeq = parseSeq(scopedInboxId, itr.key());
            List<InboxMessage> msgList = InboxMessageList.parseFrom(itr.value()).getMessageList();
            int startIdx = (int) (commitSeq - beginSeq + 1);
            if (startIdx < msgList.size()) {
                msgList = msgList.subList(startIdx, msgList.size());
                writer.insert(keyGenerator.apply(scopedInboxId, commitSeq + 1),
                    InboxMessageList.newBuilder().addAllMessage(msgList).build().toByteString());
            }
            writer.clear(Boundary.newBuilder()
                .setStartKey(keyGenerator.apply(scopedInboxId, startSeq))
                .setEndKey(keyGenerator.apply(scopedInboxId, commitSeq + 1))
                .build());
            startSeq = commitSeq + 1;
            metadataSetter.apply(startSeq);
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

    private Optional<InboxMetadata> getInboxMetadata(ByteString metadataKey, IKVReader reader) {
        return inboxMetadataCache.get(metadataKey, k -> {
            Optional<ByteString> value = reader.get(metadataKey);
            if (value.isPresent()) {
                try {
                    InboxMetadata metadata = InboxMetadata.parseFrom(value.get());
                    return Optional.of(metadata);
                } catch (InvalidProtocolBufferException e) {
                    return Optional.empty();
                }
            } else {
                return Optional.empty();
            }
        });
    }
}
