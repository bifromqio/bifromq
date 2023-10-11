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

package com.baidu.bifromq.inbox.store;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.intersect;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static com.baidu.bifromq.inbox.util.KeyUtil.buildMsgKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.isInboxMetadataKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.isQoS0MessageKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.isQoS1MessageKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.isQoS2MessageIndexKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseQoS2Index;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseScopedInboxId;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseScopedInboxIdFromScopedTopicFilter;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseSeq;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseTenantId;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseTopicFilterFromScopedTopicFilter;
import static com.baidu.bifromq.inbox.util.KeyUtil.qos0InboxMsgKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.qos0InboxPrefix;
import static com.baidu.bifromq.inbox.util.KeyUtil.qos1InboxMsgKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.qos1InboxPrefix;
import static com.baidu.bifromq.inbox.util.KeyUtil.qos2InboxIndex;
import static com.baidu.bifromq.inbox.util.KeyUtil.qos2InboxMsgKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.qos2InboxPrefix;
import static com.baidu.bifromq.inbox.util.KeyUtil.scopedTopicFilter;
import static com.baidu.bifromq.inbox.util.KeyUtil.tenantPrefix;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;

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
import com.baidu.bifromq.inbox.storage.proto.BatchCheckReply;
import com.baidu.bifromq.inbox.storage.proto.BatchCheckRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCommitReply;
import com.baidu.bifromq.inbox.storage.proto.BatchCommitRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateReply;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchFetchReply;
import com.baidu.bifromq.inbox.storage.proto.BatchFetchRequest;
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
import com.baidu.bifromq.inbox.storage.proto.CommitParams;
import com.baidu.bifromq.inbox.storage.proto.CreateParams;
import com.baidu.bifromq.inbox.storage.proto.FetchParams;
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
import com.baidu.bifromq.inbox.storage.proto.InsertResult;
import com.baidu.bifromq.inbox.storage.proto.MessagePack;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterList;
import com.baidu.bifromq.inbox.util.KeyUtil;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.inboxservice.Overflowed;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessage;
import com.baidu.bifromq.type.TopicMessagePack;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
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
    private final ISettingProvider settingProvider;
    private final IEventCollector eventCollector;
    private final Clock clock;
    private final Duration purgeDelay;
    private final Cache<ByteString, Optional<InboxMetadata>> inboxMetadataCache;

    InboxStoreCoProc(KVRangeId id,
                     Supplier<IKVReader> rangeReaderProvider,
                     ISettingProvider settingProvider,
                     IEventCollector eventCollector,
                     Clock clock,
                     Duration purgeDelay) {
        this.settingProvider = settingProvider;
        this.eventCollector = eventCollector;
        this.clock = clock;
        this.purgeDelay = purgeDelay;
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
                case BATCHCHECK -> outputBuilder.setBatchCheck(batchCheck(coProcInput.getBatchCheck(), reader));
                case BATCHFETCH -> outputBuilder.setBatchFetch(batchFetch(coProcInput.getBatchFetch(), reader));
                case COLLECTMETRICS ->
                    outputBuilder.setCollectedMetrics(collect(coProcInput.getCollectMetrics(), reader));
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

    @SneakyThrows
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
            case BATCHTOUCH -> {
                BatchTouchReply.Builder replyBuilder = BatchTouchReply.newBuilder();
                afterMutate.set(batchTouch(coProcInput.getBatchTouch(), replyBuilder, reader, writer));
                outputBuilder.setBatchTouch(replyBuilder);
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
    private BatchCheckReply batchCheck(BatchCheckRequest request, IKVReader reader) {
        BatchCheckReply.Builder replyBuilder = BatchCheckReply.newBuilder();
        for (ByteString scopedInboxId : request.getScopedInboxIdList()) {
            Optional<InboxMetadata> inboxMetadataOpt = getInboxMetadata(scopedInboxId, reader);
            if (inboxMetadataOpt.isPresent()) {
                replyBuilder.putExists(scopedInboxId.toStringUtf8(), !hasExpired(inboxMetadataOpt.get()));
            } else {
                replyBuilder.putExists(scopedInboxId.toStringUtf8(), false);
            }
        }
        return replyBuilder.build();
    }

    private BatchFetchReply batchFetch(BatchFetchRequest request, IKVReader reader) {
        BatchFetchReply.Builder replyBuilder = BatchFetchReply.newBuilder();
        IKVIterator itr = reader.iterator();
        for (String scopedInboxIdUtf8 : request.getInboxFetchMap().keySet()) {
            ByteString scopedInboxId = ByteString.copyFromUtf8(scopedInboxIdUtf8);
            FetchParams inboxFetch = request.getInboxFetchMap().get(scopedInboxIdUtf8);
            replyBuilder.putResult(scopedInboxIdUtf8, fetch(scopedInboxId, inboxFetch, itr, reader));
        }
        return replyBuilder.build();
    }

    private Fetched fetch(ByteString scopedInboxId, FetchParams request, IKVIterator itr, IKVReader reader) {
        Fetched.Builder replyBuilder = Fetched.newBuilder();
        int fetchCount = request.getMaxFetch();
        try {
            Optional<InboxMetadata> inboxMetadataOpt = getInboxMetadata(scopedInboxId, reader);
            if (inboxMetadataOpt.isEmpty()) {
                replyBuilder.setResult(Fetched.Result.NO_INBOX);
                return replyBuilder.build();
            }
            InboxMetadata metadata = inboxMetadataOpt.get();
            if (hasExpired(metadata)) {
                replyBuilder.setResult(Fetched.Result.NO_INBOX);
                return replyBuilder.build();
            }
            // deal with qos0 queue
            long startFetchFromSeq = !request.hasQos0StartAfter()
                ? metadata.getQos0StartSeq()
                : Math.max(request.getQos0StartAfter() + 1, metadata.getQos0StartSeq());
            fetchCount = fetchFromInbox(scopedInboxId, fetchCount, startFetchFromSeq, metadata.getQos0NextSeq(),
                KeyUtil::isQoS0MessageKey, KeyUtil::qos0InboxMsgKey, Fetched.Builder::addQos0Seq,
                Fetched.Builder::addQos0Msg, itr, replyBuilder);
            // deal with qos1 queue
            startFetchFromSeq = !request.hasQos1StartAfter()
                ? metadata.getQos1StartSeq()
                : Math.max(request.getQos1StartAfter() + 1, metadata.getQos1StartSeq());
            fetchCount = fetchFromInbox(scopedInboxId, fetchCount, startFetchFromSeq, metadata.getQos1NextSeq(),
                KeyUtil::isQoS1MessageKey, KeyUtil::qos1InboxMsgKey, Fetched.Builder::addQos1Seq,
                Fetched.Builder::addQos1Msg, itr, replyBuilder);
            // deal with qos2 queue
            startFetchFromSeq = !request.hasQos2StartAfter()
                ? metadata.getQos2StartSeq()
                : Math.max(request.getQos2StartAfter() + 1, metadata.getQos2StartSeq());
            if (startFetchFromSeq < metadata.getQos2NextSeq()) {
                itr.seek(qos2InboxIndex(scopedInboxId, startFetchFromSeq));
                while (fetchCount > 0 && itr.isValid() && isQoS2MessageIndexKey(itr.key(), scopedInboxId)) {
                    replyBuilder.addQos2Seq(parseQoS2Index(scopedInboxId, itr.key()));
                    Optional<ByteString> msgBytes = reader.get(qos2InboxMsgKey(scopedInboxId, itr.value()));
                    assert msgBytes.isPresent();
                    replyBuilder.addQos2Msg(InboxMessage.parseFrom(msgBytes.get()));
                    fetchCount--;
                    itr.next();
                }
            }
            return replyBuilder.setResult(Fetched.Result.OK).build();
        } catch (InvalidProtocolBufferException e) {
            return replyBuilder.setResult(Fetched.Result.ERROR).build();
        }
    }

    private int fetchFromInbox(ByteString scopedInboxId,
                               int fetchCount,
                               long startFetchFromSeq,
                               long nextSeq,
                               BiFunction<ByteString, ByteString, Boolean> keyChecker,
                               BiFunction<ByteString, Long, ByteString> keyGenerator,
                               BiConsumer<Fetched.Builder, Long> seqConsumer,
                               BiConsumer<Fetched.Builder, InboxMessage> messageConsumer,
                               IKVIterator itr,
                               Fetched.Builder replyBuilder) throws InvalidProtocolBufferException {
        if (startFetchFromSeq < nextSeq) {
            itr.seekForPrev(keyGenerator.apply(scopedInboxId, startFetchFromSeq));
            if (itr.isValid() && keyChecker.apply(itr.key(), scopedInboxId)) {
                long beginSeq = parseSeq(scopedInboxId, itr.key());
                InboxMessageList messageList = InboxMessageList.parseFrom(itr.value());
                for (int i = (int) (startFetchFromSeq - beginSeq); i < messageList.getMessageCount(); i++) {
                    if (fetchCount > 0) {
                        seqConsumer.accept(replyBuilder, beginSeq + i);
                        messageConsumer.accept(replyBuilder, messageList.getMessage(i));
                        fetchCount--;
                    } else {
                        break;
                    }
                }
            }
            itr.next();
            outer:
            while (fetchCount > 0 && itr.isValid() && keyChecker.apply(itr.key(), scopedInboxId)) {
                long startSeq = parseSeq(scopedInboxId, itr.key());
                InboxMessageList messageList = InboxMessageList.parseFrom(itr.value());
                for (int i = 0; i < messageList.getMessageCount(); i++) {
                    if (fetchCount > 0) {
                        seqConsumer.accept(replyBuilder, startSeq + i);
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
                                 IKVWriter writeClient) {
        Map<ByteString, InboxMetadata> toBeCached = new HashMap<>();
        for (String scopedInboxIdUtf8 : request.getInboxesMap().keySet()) {
            ByteString scopedInboxId = ByteString.copyFromUtf8(scopedInboxIdUtf8);
            CreateParams inboxParams = request.getInboxesMap().get(scopedInboxIdUtf8);
            Optional<InboxMetadata> existing = getInboxMetadata(scopedInboxId, reader);
            if (existing.isPresent()) {
                InboxMetadata metadata = existing.get();
                if (hasExpired(metadata)) {
                    replyBuilder.putSubs(scopedInboxIdUtf8, TopicFilterList.newBuilder()
                        .addAllTopicFilters(metadata.getTopicFiltersMap().keySet())
                        .build());
                    // clear all message belong to previous inbox
                    clearInbox(scopedInboxId, metadata, reader.iterator(), writeClient);
                    metadata = InboxMetadata.newBuilder()
                        .setExpireSeconds(inboxParams.getExpireSeconds())
                        .setLimit(inboxParams.getLimit())
                        .setLastFetchTime(clock.millis())
                        .setDropOldest(inboxParams.getDropOldest())
                        .setQos0NextSeq(0)
                        .setQos1NextSeq(0)
                        .setQos2NextSeq(0)
                        .setClient(inboxParams.getClient())
                        .build();
                    writeClient.put(scopedInboxId, metadata.toByteString());
                    toBeCached.put(scopedInboxId, metadata);
                }
            } else {
                InboxMetadata metadata = InboxMetadata.newBuilder()
                    .setExpireSeconds(inboxParams.getExpireSeconds())
                    .setLimit(inboxParams.getLimit())
                    .setLastFetchTime(clock.millis())
                    .setDropOldest(inboxParams.getDropOldest())
                    .setQos0NextSeq(0)
                    .setQos1NextSeq(0)
                    .setQos2NextSeq(0)
                    .setClient(inboxParams.getClient())
                    .build();
                writeClient.put(scopedInboxId, metadata.toByteString());
                toBeCached.put(scopedInboxId, metadata);
            }
        }
        return () -> toBeCached.forEach(
            (scopedInboxId, inboxMetadata) -> inboxMetadataCache.put(scopedInboxId, Optional.of(inboxMetadata)));
    }

    private Runnable batchSub(BatchSubRequest request,
                              BatchSubReply.Builder replyBuilder,
                              IKVReader reader,
                              IKVWriter writeClient) {
        replyBuilder.setReqId(request.getReqId());
        Map<ByteString, InboxMetadata> toUpdate = new HashMap<>();
        Map<ByteString, Map<String, QoS>> subMap = new HashMap<>();
        request.getTopicFiltersMap().forEach((scopedTopicFilterUtf8, subQoS) -> {
            ByteString scopedTopicFilter = ByteString.copyFromUtf8(scopedTopicFilterUtf8);
            ByteString scopedInboxId =
                parseScopedInboxIdFromScopedTopicFilter(ByteString.copyFromUtf8(scopedTopicFilterUtf8));
            String topicFilter = parseTopicFilterFromScopedTopicFilter(scopedTopicFilter);
            subMap.computeIfAbsent(scopedInboxId, s -> new HashMap<>())
                .put(topicFilter, subQoS);
        });
        subMap.forEach((scopedInboxId, topicFilters) -> {
            Optional<InboxMetadata> existing = getInboxMetadata(scopedInboxId, reader);
            if (existing.isEmpty()) {
                topicFilters.forEach((topicFilter, subQoS) -> replyBuilder.putResults(
                    scopedTopicFilter(scopedInboxId, topicFilter).toStringUtf8(), BatchSubReply.Result.NO_INBOX));
                return;
            }
            InboxMetadata metadata = existing.get();
            if (hasExpired(metadata)) {
                topicFilters.forEach((topicFilter, subQoS) ->
                    replyBuilder.putResults(scopedTopicFilter(scopedInboxId, topicFilter).toStringUtf8(),
                        BatchSubReply.Result.NO_INBOX));
            } else {
                InboxMetadata.Builder metadataBuilder = metadata.toBuilder();
                int maxTopicFilters = settingProvider
                    .provide(Setting.MaxTopicFiltersPerInbox, parseTenantId(scopedInboxId));
                topicFilters.forEach((topicFilter, subQoS) -> {
                    if (metadataBuilder.getTopicFiltersCount() < maxTopicFilters) {
                        metadataBuilder.putTopicFilters(topicFilter, subQoS);
                        replyBuilder.putResults(scopedTopicFilter(scopedInboxId, topicFilter).toStringUtf8(),
                            BatchSubReply.Result.OK);
                    } else {
                        replyBuilder.putResults(scopedTopicFilter(scopedInboxId, topicFilter).toStringUtf8(),
                            BatchSubReply.Result.EXCEED_LIMIT);
                    }
                });
                metadata = metadataBuilder.setLastFetchTime(clock.millis()).build();
                writeClient.put(scopedInboxId, metadata.toByteString());
                toUpdate.put(scopedInboxId, metadata);
            }
        });
        return () -> toUpdate.forEach((scopedInboxId, inboxMetadata) ->
            inboxMetadataCache.put(scopedInboxId, Optional.of(inboxMetadata)));
    }

    private Runnable batchUnsub(BatchUnsubRequest request,
                                BatchUnsubReply.Builder replyBuilder,
                                IKVReader reader,
                                IKVWriter writeClient) {
        replyBuilder.setReqId(request.getReqId());
        Map<ByteString, InboxMetadata> toUpdate = new HashMap<>();
        request.getTopicFiltersList().forEach(scopedTopicFilter -> {
            ByteString scopedInboxId = parseScopedInboxIdFromScopedTopicFilter(scopedTopicFilter);
            String topicFilter = parseTopicFilterFromScopedTopicFilter(scopedTopicFilter);
            Optional<InboxMetadata> existing = getInboxMetadata(scopedInboxId, reader);
            try {
                if (existing.isEmpty()) {
                    replyBuilder.putResults(scopedTopicFilter.toStringUtf8(), BatchUnsubReply.Result.NO_INBOX);
                    return;
                }
                InboxMetadata metadata = existing.get();
                if (hasExpired(metadata)) {
                    replyBuilder.putResults(scopedTopicFilter.toStringUtf8(), BatchUnsubReply.Result.NO_INBOX);
                } else {
                    InboxMetadata.Builder metadataBuilder = metadata.toBuilder();
                    if (metadataBuilder.containsTopicFilters(topicFilter)) {
                        metadataBuilder.removeTopicFilters(topicFilter);
                        replyBuilder.putResults(scopedTopicFilter.toStringUtf8(), BatchUnsubReply.Result.OK);
                    } else {
                        replyBuilder.putResults(scopedTopicFilter.toStringUtf8(), BatchUnsubReply.Result.NO_SUB);
                    }
                    metadata = metadataBuilder.setLastFetchTime(clock.millis()).build();
                    writeClient.put(scopedInboxId, metadata.toByteString());
                    toUpdate.put(scopedInboxId, metadata);
                }
            } catch (Throwable e) {
                replyBuilder.putResults(scopedTopicFilter.toStringUtf8(), BatchUnsubReply.Result.ERROR);
            }
        });
        return () -> toUpdate.forEach((scopedInboxId, inboxMetadata) ->
            inboxMetadataCache.put(scopedInboxId, Optional.of(inboxMetadata)));
    }

    private void clearInbox(ByteString scopedInboxId, InboxMetadata metadata, IKVIterator itr, IKVWriter writeClient) {
        if (metadata.getQos0NextSeq() > 0) {
            // find lowest seq of qos0 message
            itr.seek(qos0InboxPrefix(scopedInboxId));
            if (itr.isValid() && isQoS0MessageKey(itr.key(), scopedInboxId)) {
                for (long s = parseSeq(scopedInboxId, itr.key()); s < metadata.getQos0NextSeq(); s++) {
                    writeClient.delete(qos0InboxMsgKey(scopedInboxId, s));
                }
            }
        }
        if (metadata.getQos1NextSeq() > 0) {
            itr.seek(qos1InboxPrefix(scopedInboxId));
            if (itr.isValid() && isQoS1MessageKey(itr.key(), scopedInboxId)) {
                for (long s = parseSeq(scopedInboxId, itr.key()); s < metadata.getQos1NextSeq(); s++) {
                    writeClient.delete(qos1InboxMsgKey(scopedInboxId, s));
                }
            }
        }
        if (metadata.getQos2NextSeq() > 0) {
            itr.seek(qos2InboxPrefix(scopedInboxId));
            if (itr.isValid() && isQoS2MessageIndexKey(itr.key(), scopedInboxId)) {
                for (long seq = parseQoS2Index(scopedInboxId, itr.key()); seq < metadata.getQos2NextSeq(); seq++) {
                    writeClient.delete(qos2InboxIndex(scopedInboxId, seq));
                    writeClient.delete(qos2InboxMsgKey(scopedInboxId, itr.value()));
                }
            }
        }
        writeClient.delete(scopedInboxId);
    }

    @SneakyThrows
    private GCReply gcScan(GCRequest request, IKVReader reader) {
        long start = System.nanoTime();
        long yieldThreshold = TimeUnit.NANOSECONDS.convert(100, TimeUnit.MILLISECONDS);
        GCReply.Builder replyBuilder = GCReply.newBuilder().setReqId(request.getReqId());
        IKVIterator itr = reader.iterator();
        if (request.hasScopedInboxId()) {
            itr.seek(request.getScopedInboxId());
        } else {
            itr.seekToFirst();
        }
        while (itr.isValid()) {
            if (System.nanoTime() - start > yieldThreshold) {
                replyBuilder.setNextScopedInboxId(itr.key());
                break;
            }
            ByteString scopedInboxId = parseScopedInboxId(itr.key());
            if (isInboxMetadataKey(itr.key())) {
                InboxMetadata metadata = InboxMetadata.parseFrom(itr.value());
                if (isGCable(metadata, request)) {
                    if (replyBuilder.getScopedInboxIdCount() < request.getLimit()) {
                        replyBuilder.addScopedInboxId(scopedInboxId);
                    } else {
                        replyBuilder.setNextScopedInboxId(itr.key());
                        break;
                    }
                }
            }
            itr.seek(upperBound(scopedInboxId));
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

    private Runnable batchInsert(BatchInsertRequest request,
                                 BatchInsertReply.Builder replyBuilder,
                                 IKVReader reader,
                                 IKVWriter writer) {
        IKVIterator itr = reader.iterator();
        Map<ByteString, InboxMetadata> toUpdate = new HashMap<>();
        Map<SubInfo, InsertResult.Result> results = new LinkedHashMap<>();
        Map<ByteString, List<MessagePack>> subMsgPacksByInbox = new HashMap<>();
        for (MessagePack subMsgPack : request.getSubMsgPackList()) {
            SubInfo subInfo = subMsgPack.getSubInfo();
            ByteString scopedInboxId = KeyUtil.scopedInboxId(subInfo.getTenantId(), subInfo.getInboxId());
            results.put(subInfo, null);
            subMsgPacksByInbox.computeIfAbsent(scopedInboxId, k -> new LinkedList<>()).add(subMsgPack);
        }
        Map<ClientInfo, Map<QoS, Integer>> dropCountMap = new HashMap<>();
        Map<ClientInfo, Boolean> dropOldestMap = new HashMap<>();
        for (ByteString scopedInboxId : subMsgPacksByInbox.keySet()) {
            List<MessagePack> subMsgPacks = subMsgPacksByInbox.get(scopedInboxId);
            Optional<InboxMetadata> existing = getInboxMetadata(scopedInboxId, reader);
            if (existing.isEmpty()) {
                subMsgPacks.forEach(pack -> results.put(pack.getSubInfo(), InsertResult.Result.NO_INBOX));
                continue;
            }
            try {
                InboxMetadata metadata = existing.get();
                if (hasExpired(metadata)) {
                    subMsgPacks.forEach(pack -> results.put(pack.getSubInfo(), InsertResult.Result.NO_INBOX));
                    continue;
                }
                Iterator<MessagePack> packsItr = subMsgPacks.iterator();
                while (packsItr.hasNext()) {
                    MessagePack msgPack = packsItr.next();
                    // topic filter not found
                    if (!metadata.containsTopicFilters(msgPack.getSubInfo().getTopicFilter())) {
                        results.put(msgPack.getSubInfo(), InsertResult.Result.NO_INBOX);
                        packsItr.remove();
                    }
                }
                dropOldestMap.put(metadata.getClient(), metadata.getDropOldest());
                InboxMetadata.Builder metaBuilder = metadata.toBuilder();
                if (!subMsgPacks.isEmpty()) {
                    Map<QoS, Integer> dropCounts =
                        insertInbox(scopedInboxId, subMsgPacks, metaBuilder, itr, reader, writer);
                    metadata = metaBuilder.build();
                    writer.put(scopedInboxId, metadata.toByteString());
                    subMsgPacks.forEach(pack -> results.put(pack.getSubInfo(), InsertResult.Result.OK));
                    toUpdate.put(scopedInboxId, metadata);

                    Map<QoS, Integer> aggregated =
                        dropCountMap.computeIfAbsent(metadata.getClient(), k -> new HashMap<>());

                    dropCounts.forEach((qos, count) -> aggregated.compute(qos, (k, v) -> {
                        if (v == null) {
                            return count;
                        }
                        return v + count;
                    }));
                }
            } catch (Throwable e) {
                log.error("Error inserting messages to inbox {}", scopedInboxId, e);
                subMsgPacks.forEach(pack -> results.put(pack.getSubInfo(), InsertResult.Result.ERROR));
            }
        }

        for (Map.Entry<SubInfo, InsertResult.Result> entry : results.entrySet()) {
            replyBuilder.addResults(InsertResult.newBuilder()
                .setSubInfo(entry.getKey())
                .setResult(entry.getValue())
                .build());
        }
        return () -> {
            toUpdate.forEach((scopedInboxId, inboxMetadata) ->
                inboxMetadataCache.put(scopedInboxId, Optional.of(inboxMetadata)));
            dropCountMap.forEach((client, dropCounts) -> dropCounts.forEach((qos, count) -> {
                if (count > 0) {
                    eventCollector.report(getLocal(Overflowed.class)
                        .oldest(dropOldestMap.get(client))
                        .qos(qos)
                        .clientInfo(client)
                        .dropCount(count));
                }
            }));
        };
    }

    private Map<QoS, Integer> insertInbox(ByteString scopedInboxId,
                                          List<MessagePack> msgPacks,
                                          InboxMetadata.Builder metaBuilder,
                                          IKVIterator itr,
                                          IKVReader reader,
                                          IKVWriter writer) throws InvalidProtocolBufferException {
        List<InboxMessage> qos0MsgList = new ArrayList<>();
        List<InboxMessage> qos1MsgList = new ArrayList<>();
        List<InboxMessage> qos2MsgList = new ArrayList<>();
        for (MessagePack inboxMsgPack : msgPacks) {
            SubInfo subInfo = inboxMsgPack.getSubInfo();
            for (TopicMessagePack topicMsgPack : inboxMsgPack.getMessagesList()) {
                String topic = topicMsgPack.getTopic();
                for (TopicMessagePack.PublisherPack publisherPack : topicMsgPack.getMessageList()) {
                    for (Message message : publisherPack.getMessageList()) {
                        InboxMessage inboxMsg = InboxMessage.newBuilder()
                            .setTopicFilter(subInfo.getTopicFilter())
                            .setMsg(TopicMessage.newBuilder()
                                .setTopic(topic)
                                .setMessage(message)
                                .setPublisher(publisherPack.getPublisher())
                                .build())
                            .build();
                        QoS finalQoS =
                            QoS.forNumber(Math.min(message.getPubQoS().getNumber(), subInfo.getSubQoS().getNumber()));
                        assert finalQoS != null;
                        switch (finalQoS) {
                            case AT_MOST_ONCE -> qos0MsgList.add(inboxMsg);
                            case AT_LEAST_ONCE -> qos1MsgList.add(inboxMsg);
                            case EXACTLY_ONCE -> qos2MsgList.add(inboxMsg);
                        }
                    }
                }
            }
        }
        Map<QoS, Integer> dropCounts = new HashMap<>();
        if (!qos0MsgList.isEmpty()) {
            long startSeq = metaBuilder.getQos0StartSeq();
            long nextSeq = metaBuilder.getQos0NextSeq();
            int dropCount = insertToInbox(scopedInboxId, startSeq, nextSeq, metaBuilder.getLimit(),
                metaBuilder.getDropOldest(), KeyUtil::qos0InboxMsgKey,
                metaBuilder::setQos0StartSeq, metaBuilder::setQos0NextSeq, qos0MsgList, itr, writer);
            if (dropCount > 0) {
                dropCounts.put(QoS.AT_MOST_ONCE, dropCount);
            }
        }
        if (!qos1MsgList.isEmpty()) {
            long startSeq = metaBuilder.getQos1StartSeq();
            long nextSeq = metaBuilder.getQos1NextSeq();
            int dropCount = insertToInbox(scopedInboxId, startSeq, nextSeq, metaBuilder.getLimit(),
                metaBuilder.getDropOldest(), KeyUtil::qos1InboxMsgKey,
                metaBuilder::setQos1StartSeq, metaBuilder::setQos1NextSeq, qos1MsgList, itr, writer);
            if (dropCount > 0) {
                dropCounts.put(QoS.AT_LEAST_ONCE, dropCount);
            }
        }
        if (!qos2MsgList.isEmpty()) {
            int dropCount = insertQoS2Inbox(scopedInboxId, metaBuilder, qos2MsgList, itr, reader, writer);
            if (dropCount > 0) {
                dropCounts.put(QoS.EXACTLY_ONCE, dropCount);
            }
        }
        return dropCounts;
    }

    private int insertToInbox(ByteString scopedInboxId,
                              long startSeq,
                              long nextSeq,
                              int limit,
                              boolean dropOldest,
                              BiFunction<ByteString, Long, ByteString> keyGenerator,
                              Function<Long, InboxMetadata.Builder> startSeqSetter,
                              Function<Long, InboxMetadata.Builder> nextSeqSetter,
                              List<InboxMessage> messages,
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
                        .setStartKey(keyGenerator.apply(scopedInboxId, startSeq))
                        .setEndKey(keyGenerator.apply(scopedInboxId, nextSeq))
                        .build());
                    // and trim if needed
                    if (dropCount > currCount) {
                        messages = messages.subList(dropCount - currCount, newMsgCount);
                    }
                    writer.insert(keyGenerator.apply(scopedInboxId, startSeq + dropCount),
                        InboxMessageList.newBuilder().addAllMessage(messages).build().toByteString());
                } else {
                    // drop partially
                    itr.seekForPrev(keyGenerator.apply(scopedInboxId, startSeq + dropCount));
                    long beginSeq = parseSeq(scopedInboxId, itr.key());
                    List<InboxMessage> msgList = InboxMessageList.parseFrom(itr.value()).getMessageList();
                    InboxMessageList.Builder msgListBuilder = InboxMessageList.newBuilder();
                    msgListBuilder
                        .addAllMessage(msgList.subList((int) (startSeq + dropCount - beginSeq), msgList.size()))
                        .addAllMessage(messages);
                    writer.clear(Boundary.newBuilder()
                        .setStartKey(keyGenerator.apply(scopedInboxId, startSeq))
                        .setEndKey(keyGenerator.apply(scopedInboxId, startSeq + dropCount))
                        .build());
                    if (beginSeq == startSeq + dropCount) {
                        // override existing key
                        writer.put(keyGenerator.apply(scopedInboxId, startSeq + dropCount),
                            msgListBuilder.build().toByteString());
                    } else {
                        // insert new key
                        writer.insert(keyGenerator.apply(scopedInboxId, startSeq + dropCount),
                            msgListBuilder.build().toByteString());
                    }
                }
                startSeq += dropCount;
                nextSeq += newMsgCount;
            } else {
                writer.insert(keyGenerator.apply(scopedInboxId, nextSeq), InboxMessageList.newBuilder()
                    .addAllMessage(messages).build().toByteString());
                nextSeq += newMsgCount;
            }
            startSeqSetter.apply(startSeq);
            nextSeqSetter.apply(nextSeq);
        } else {
            if (dropCount < newMsgCount) {
                InboxMessageList messageList = InboxMessageList.newBuilder()
                    .addAllMessage(dropCount > 0 ? messages.subList(0, newMsgCount - dropCount) : messages)
                    .build();
                writer.insert(keyGenerator.apply(scopedInboxId, nextSeq), messageList.toByteString());
                nextSeq += messageList.getMessageCount();
            }
            // else drop all new messages;
        }
        startSeqSetter.apply(startSeq);
        nextSeqSetter.apply(nextSeq);
        return Math.max(dropCount, 0);
    }

    private int insertQoS2Inbox(ByteString scopedInboxId,
                                InboxMetadata.Builder metaBuilder,
                                List<InboxMessage> messages,
                                IKVIterator itr,
                                IKVReader reader,
                                IKVWriter writeClient) {
        // filter out those already existed
        List<InboxMessage> uniqueInboxMsgList = new ArrayList<>(messages.size());
        Set<ByteString> msgKeySet = new HashSet<>();
        for (InboxMessage msg : messages) {
            ByteString msgKey = buildMsgKey(msg);
            if (!reader.exist(qos2InboxMsgKey(scopedInboxId, msgKey)) && !msgKeySet.contains(msgKey)) {
                uniqueInboxMsgList.add(msg);
                msgKeySet.add(msgKey);
            }
        }
        messages = uniqueInboxMsgList;
        itr.seek(qos2InboxIndex(scopedInboxId, 0));
        long nextSeq = metaBuilder.getQos2NextSeq();
        long oldestSeq =
            itr.isValid() && isQoS2MessageIndexKey(itr.key(), scopedInboxId)
                ? parseQoS2Index(scopedInboxId, itr.key())
                : nextSeq;
        assert oldestSeq <= nextSeq;
        int messageCount = messages.size();
        int limit = metaBuilder.getLimit();
        int current = (int) (nextSeq - oldestSeq);
        int total = current + messageCount;
        int dropCount = total - limit;
        if (total > limit) {
            if (metaBuilder.getDropOldest()) {
                //             limit
                // [...........................]
                // [-------------------][++++++++++++]
                //  nextSeq - oldestSeq  messageCount   > limit
                if (total - limit < limit) {
                    int delCount = dropCount;
                    while (itr.isValid() && delCount > 0) {
                        writeClient.delete(itr.key());
                        writeClient.delete(qos2InboxMsgKey(scopedInboxId, itr.value()));
                        itr.next();
                        delCount--;
                    }
                    for (InboxMessage message : messages) {
                        ByteString msgKey = buildMsgKey(message);
                        writeClient.insert(qos2InboxIndex(scopedInboxId, nextSeq), msgKey);
                        writeClient.insert(qos2InboxMsgKey(scopedInboxId, msgKey), message.toByteString());
                        nextSeq++;
                    }
                } else {
                    //             limit
                    // [...........................]
                    // [-------------------][++++++++++++++++++++++++++++++++++++++++++]
                    //  nextSeq - oldestSeq  messageCount   > limit
                    int delCount = (int) (nextSeq - oldestSeq);
                    while (itr.isValid() && delCount > 0) {
                        writeClient.delete(itr.key());
                        writeClient.delete(qos2InboxMsgKey(scopedInboxId, itr.value()));
                        itr.next();
                        delCount--;
                    }
                    for (int i = messageCount - limit; i < messageCount; i++) {
                        InboxMessage message = messages.get(i);
                        ByteString msgKey = buildMsgKey(message);
                        writeClient.insert(qos2InboxIndex(scopedInboxId, nextSeq), msgKey);
                        writeClient.insert(qos2InboxMsgKey(scopedInboxId, msgKey), message.toByteString());
                        nextSeq++;
                    }
                }
            } else {
                //             limit
                // [...........................]
                // [-------------------][++++++++++++]
                //  nextSeq - oldestSeq  messageCount   > limit
                for (int i = 0; i < limit - current && i < messageCount; i++) {
                    InboxMessage message = messages.get(i);
                    ByteString msgKey = buildMsgKey(message);
                    writeClient.insert(qos2InboxIndex(scopedInboxId, nextSeq), msgKey);
                    writeClient.insert(qos2InboxMsgKey(scopedInboxId, msgKey), message.toByteString());
                    nextSeq++;
                    if (nextSeq - oldestSeq >= limit) {
                        break;
                    }
                }
            }
        } else {
            for (InboxMessage message : messages) {
                ByteString msgKey = buildMsgKey(message);
                writeClient.insert(qos2InboxIndex(scopedInboxId, nextSeq), msgKey);
                writeClient.insert(qos2InboxMsgKey(scopedInboxId, msgKey), message.toByteString());
                nextSeq++;
            }
        }
        metaBuilder.setQos2NextSeq(nextSeq);
        return Math.max(dropCount, 0);
    }

    @SneakyThrows
    private Runnable batchTouch(BatchTouchRequest request,
                                BatchTouchReply.Builder replyBuilder,
                                IKVReader reader,
                                IKVWriter writer) {
        Map<ByteString, InboxMetadata> toRemove = new HashMap<>();
        Map<ByteString, InboxMetadata> toUpdate = new HashMap<>();
        for (String scopedInboxIdUtf8 : request.getScopedInboxIdMap().keySet()) {
            ByteString scopedInboxId = ByteString.copyFromUtf8(scopedInboxIdUtf8);
            Optional<InboxMetadata> existing = getInboxMetadata(scopedInboxId, reader);
            if (existing.isPresent()) {
                InboxMetadata metadata = existing.get();
                if (hasExpired(metadata) || !request.getScopedInboxIdMap().get(scopedInboxIdUtf8)) {
                    clearInbox(scopedInboxId, metadata, reader.iterator(), writer);
                    replyBuilder.putSubs(scopedInboxIdUtf8, TopicFilterList.newBuilder()
                        .addAllTopicFilters(metadata.getTopicFiltersMap().keySet())
                        .build());
                    toRemove.put(scopedInboxId, metadata);
                } else {
                    metadata = metadata.toBuilder().setLastFetchTime(clock.millis()).build();
                    writer.put(scopedInboxId, metadata.toByteString());
                    toUpdate.put(scopedInboxId, metadata);
                }
            }
        }
        return () -> {
            toRemove.forEach((scopedInboxId, inboxMetadata) ->
                inboxMetadataCache.asMap().remove(scopedInboxId, Optional.of(inboxMetadata)));
            toUpdate.forEach((scopedInboxId, inboxMetadata) ->
                inboxMetadataCache.put(scopedInboxId, Optional.of(inboxMetadata)));
        };
    }

    private Runnable batchCommit(BatchCommitRequest request,
                                 BatchCommitReply.Builder replyBuilder,
                                 IKVReader reader,
                                 IKVWriter writer) {
        IKVIterator itr = reader.iterator();
        Map<ByteString, InboxMetadata> toUpdate = new HashMap<>();
        for (String scopedInboxIdUtf8 : request.getInboxCommitMap().keySet()) {
            ByteString scopedInboxId = ByteString.copyFromUtf8(scopedInboxIdUtf8);
            CommitParams commitParams = request.getInboxCommitMap().get(scopedInboxIdUtf8);
            assert commitParams.hasQos0UpToSeq() || commitParams.hasQos1UpToSeq() || commitParams.hasQos2UpToSeq();
            Optional<InboxMetadata> existing = getInboxMetadata(scopedInboxId, reader);
            if (existing.isEmpty()) {
                replyBuilder.putResult(scopedInboxIdUtf8, false);
                continue;
            }
            try {
                InboxMetadata metadata = existing.get();
                if (hasExpired(metadata)) {
                    replyBuilder.putResult(scopedInboxIdUtf8, false);
                    continue;
                }
                InboxMetadata.Builder metaBuilder = metadata.toBuilder();
                commitInbox(scopedInboxId, commitParams, metaBuilder, itr, writer);
                metadata = metaBuilder.build();
                writer.put(scopedInboxId, metadata.toByteString());
                replyBuilder.putResult(scopedInboxIdUtf8, true);
                toUpdate.put(scopedInboxId, metadata);
            } catch (Throwable e) {
                log.error("Failed to commit inbox", e);
                replyBuilder.putResult(scopedInboxIdUtf8, false);
            }
        }
        return () -> toUpdate.forEach((scopedInboxId, inboxMetadata) ->
            inboxMetadataCache.put(scopedInboxId, Optional.of(inboxMetadata)));
    }

    private void commitInbox(ByteString scopedInboxId,
                             CommitParams commitParams,
                             InboxMetadata.Builder metaBuilder,
                             IKVIterator itr,
                             IKVWriter writer) throws InvalidProtocolBufferException {
        if (commitParams.hasQos0UpToSeq()) {
            long startSeq = metaBuilder.getQos0StartSeq();
            long nextSeq = metaBuilder.getQos0NextSeq();
            long commitSeq = commitParams.getQos0UpToSeq();
            commitToInbox(scopedInboxId, startSeq, nextSeq, commitSeq, KeyUtil::qos0InboxMsgKey,
                metaBuilder::setQos0StartSeq, itr, writer);
        }
        if (commitParams.hasQos1UpToSeq()) {
            long startSeq = metaBuilder.getQos1StartSeq();
            long nextSeq = metaBuilder.getQos1NextSeq();
            long commitSeq = commitParams.getQos1UpToSeq();
            commitToInbox(scopedInboxId, startSeq, nextSeq, commitSeq, KeyUtil::qos1InboxMsgKey,
                metaBuilder::setQos1StartSeq, itr, writer);
        }
        if (commitParams.hasQos2UpToSeq()) {
            itr.seek(qos2InboxPrefix(scopedInboxId));
            while (itr.isValid() && isQoS2MessageIndexKey(itr.key(), scopedInboxId)
                && parseQoS2Index(scopedInboxId, itr.key()) <= commitParams.getQos2UpToSeq()) {
                writer.delete(itr.key());
                writer.delete(qos2InboxMsgKey(scopedInboxId, itr.value()));
                itr.next();
            }
            metaBuilder.setQos2StartSeq(commitParams.getQos2UpToSeq() + 1);
        }
        metaBuilder.setLastFetchTime(clock.millis());
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

    private boolean hasExpired(InboxMetadata metadata) {
        Duration now = Duration.ofMillis(clock.millis());
        Duration expireAt =
            Duration.ofMillis(metadata.getLastFetchTime()).plus(Duration.ofSeconds(metadata.getExpireSeconds()));
        return now.compareTo(expireAt) > 0;
    }

    private boolean isGCable(InboxMetadata metadata, GCRequest request) {
        if (request.hasTenantId() && !request.getTenantId().equals(metadata.getClient().getTenantId())) {
            return false;
        }
        Duration now = Duration.ofMillis(clock.millis());
        Duration expireAt;
        if (request.hasExpirySeconds() && request.getExpirySeconds() >= 0) {
            expireAt = Duration.ofMillis(metadata.getLastFetchTime())
                .plus(Duration.ofSeconds(request.getExpirySeconds()));
        } else {
            expireAt = Duration.ofMillis(metadata.getLastFetchTime())
                .plus(Duration.ofSeconds(metadata.getExpireSeconds()))
                .plus(purgeDelay);
        }
        return now.compareTo(expireAt) > 0;
    }

    private Optional<InboxMetadata> getInboxMetadata(ByteString scopedInboxId, IKVReader reader) {
        return inboxMetadataCache.get(scopedInboxId, k -> {
            Optional<ByteString> value = reader.get(scopedInboxId);
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
