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

import static com.baidu.bifromq.basekv.localengine.RangeUtil.upperBound;
import static com.baidu.bifromq.basekv.utils.KeyRangeUtil.intersect;
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

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.Range;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProc;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.api.IKVWriter;
import com.baidu.bifromq.basekv.store.range.ILoadTracker;
import com.baidu.bifromq.inbox.storage.proto.BatchCheckReply;
import com.baidu.bifromq.inbox.storage.proto.BatchCheckRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCommitReply;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateReply;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchFetchReply;
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
import com.baidu.bifromq.inbox.storage.proto.InboxComSertReply;
import com.baidu.bifromq.inbox.storage.proto.InboxComSertRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCommitRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchFetchRequest;
import com.baidu.bifromq.inbox.storage.proto.InsertResult;
import com.baidu.bifromq.inbox.storage.proto.InboxMessage;
import com.baidu.bifromq.inbox.storage.proto.InboxMessageList;
import com.baidu.bifromq.inbox.storage.proto.InboxMetadata;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.MessagePack;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterList;
import com.baidu.bifromq.inbox.util.KeyUtil;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.inboxservice.Overflowed;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessage;
import com.baidu.bifromq.type.TopicMessagePack;
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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class InboxStoreCoProc implements IKVRangeCoProc {
    private final ISettingProvider settingProvider;
    private final IEventCollector eventCollector;
    private final Clock clock;
    private final Duration purgeDelay;

    InboxStoreCoProc(KVRangeId id,
                     Supplier<IKVRangeReader> rangeReaderProvider,
                     ISettingProvider settingProvider,
                     IEventCollector eventCollector,
                     Clock clock,
                     Duration purgeDelay,
                     ILoadTracker loadTracker) {
        this.settingProvider = settingProvider;
        this.eventCollector = eventCollector;
        this.clock = clock;
        this.purgeDelay = purgeDelay;
    }

    @Override
    public CompletableFuture<ByteString> query(ByteString input, IKVReader reader) {
        try {
            InboxServiceROCoProcInput coProcInput = InboxServiceROCoProcInput.parseFrom(input);
            InboxServiceROCoProcOutput.Builder outputBuilder = InboxServiceROCoProcOutput.newBuilder()
                .setReqId(coProcInput.getReqId());
            switch (coProcInput.getInputCase()) {
                case BATCHCHECK -> outputBuilder.setBatchCheck(batchCheck(coProcInput.getBatchCheck(), reader));
                case BATCHFETCH -> outputBuilder.setBatchFetch(batchFetch(coProcInput.getBatchFetch(), reader));
                case COLLECTMETRICS ->
                    outputBuilder.setCollectedMetrics(collect(coProcInput.getCollectMetrics(), reader));
                case GC -> outputBuilder.setGc(gcScan(coProcInput.getGc(), reader));
            }
            return CompletableFuture.completedFuture(outputBuilder.build().toByteString());
        } catch (Throwable e) {
            log.error("Query co-proc failed", e);
            return CompletableFuture.failedFuture(new IllegalStateException("Query co-proc failed", e));
        }
    }

    @SneakyThrows
    @Override
    public Supplier<ByteString> mutate(ByteString input, IKVReader reader, IKVWriter writer) {
        InboxServiceRWCoProcInput coProcInput = InboxServiceRWCoProcInput.parseFrom(input);
        InboxServiceRWCoProcOutput.Builder replyBuilder =
            InboxServiceRWCoProcOutput.newBuilder().setReqId(coProcInput.getReqId());
        switch (coProcInput.getTypeCase()) {
            case BATCHCREATE -> replyBuilder.setBatchCreate(batchCreate(coProcInput.getBatchCreate(), reader, writer));
            case BATCHTOUCH -> replyBuilder.setBatchTouch(batchTouch(coProcInput.getBatchTouch(), reader, writer));
            case BATCHSUB -> replyBuilder
                .setBatchSub(batchSub(coProcInput.getBatchSub(), reader, writer));
            case BATCHUNSUB -> replyBuilder
                .setBatchUnsub(batchUnsub(coProcInput.getBatchUnsub(), reader, writer));
            case BATCHINSERT -> replyBuilder.setBatchInsert(batchInsert(coProcInput.getBatchInsert(), reader, writer));
            case BATCHCOMMIT -> replyBuilder.setBatchCommit(batchCommit(coProcInput.getBatchCommit(), reader, writer));
            case INSERTANDCOMMIT ->
                replyBuilder.setInsertAndCommit(batchInsertAndCommit(coProcInput.getInsertAndCommit(), reader, writer));
        }
        ByteString output = replyBuilder.build().toByteString();
        return () -> output;
    }

    @Override
    public void close() {
    }

    @SneakyThrows
    private BatchCheckReply batchCheck(BatchCheckRequest request, IKVReader reader) {
        BatchCheckReply.Builder replyBuilder = BatchCheckReply.newBuilder();
        for (ByteString scopedInboxId : request.getScopedInboxIdList()) {
            Optional<ByteString> value = reader.get(scopedInboxId);
            if (value.isPresent()) {
                replyBuilder.putExists(scopedInboxId.toStringUtf8(), !hasExpired(InboxMetadata.parseFrom(value.get())));
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
            replyBuilder.putResult(scopedInboxIdUtf8, batchFetch(scopedInboxId, inboxFetch, itr, reader));
        }
        return replyBuilder.build();
    }

    private Fetched batchFetch(ByteString scopedInboxId, FetchParams request, IKVIterator itr, IKVReader reader) {
        Fetched.Builder replyBuilder = Fetched.newBuilder();
        int fetchCount = request.getMaxFetch();
        try {
            itr.seek(scopedInboxId);
            if (!itr.isValid() || !itr.key().equals(scopedInboxId)) {
                replyBuilder.setResult(Fetched.Result.NO_INBOX);
                return replyBuilder.build();
            }
            InboxMetadata metadata = InboxMetadata.parseFrom(itr.value());
            if (hasExpired(metadata)) {
                replyBuilder.setResult(Fetched.Result.NO_INBOX);
                return replyBuilder.build();
            }
            // deal with qos0 queue
            long startFetchFromSeq = !request.hasQos0StartAfter()
                ? metadata.getQos0LastFetchBeforeSeq()
                : Math.max(request.getQos0StartAfter() + 1, metadata.getQos0LastFetchBeforeSeq());
            fetchCount = fetchFromInbox(scopedInboxId, fetchCount, startFetchFromSeq, metadata.getQos0NextSeq(),
                KeyUtil::isQoS0MessageKey, KeyUtil::qos0InboxMsgKey, Fetched.Builder::addQos0Seq,
                Fetched.Builder::addQos0Msg, itr, replyBuilder);
            // deal with qos1 queue
            startFetchFromSeq =
                !request.hasQos1StartAfter()
                    ? metadata.getQos1LastCommitBeforeSeq()
                    : Math.max(request.getQos1StartAfter() + 1, metadata.getQos1LastCommitBeforeSeq());
            fetchCount = fetchFromInbox(scopedInboxId, fetchCount, startFetchFromSeq, metadata.getQos1NextSeq(),
                KeyUtil::isQoS1MessageKey, KeyUtil::qos1InboxMsgKey, Fetched.Builder::addQos1Seq,
                Fetched.Builder::addQos1Msg, itr, replyBuilder);
            // deal with qos2 queue
            startFetchFromSeq = !request.hasQos2StartAfter()
                ? metadata.getQos2LastCommitBeforeSeq()
                : Math.max(request.getQos2StartAfter() + 1, metadata.getQos2LastCommitBeforeSeq());
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

    @SneakyThrows
    private BatchCreateReply batchCreate(BatchCreateRequest request, IKVReader reader, IKVWriter writeClient) {
        BatchCreateReply.Builder replyBuilder = BatchCreateReply.newBuilder();
        for (String scopedInboxIdUtf8 : request.getInboxesMap().keySet()) {
            ByteString scopedInboxId = ByteString.copyFromUtf8(scopedInboxIdUtf8);
            CreateParams inboxParams = request.getInboxesMap().get(scopedInboxIdUtf8);
            Optional<ByteString> value = reader.get(scopedInboxId);
            if (value.isPresent()) {
                InboxMetadata metadata = InboxMetadata.parseFrom(value.get());
                if (hasExpired(metadata)) {
                    // clear all message belong to previous inbox
                    clearInbox(scopedInboxId, metadata, reader.iterator(), writeClient);
                    writeClient.put(
                        scopedInboxId,
                        InboxMetadata.newBuilder()
                            .setExpireSeconds(inboxParams.getExpireSeconds())
                            .setLimit(inboxParams.getLimit())
                            .setLastFetchTime(clock.millis())
                            .setDropOldest(inboxParams.getDropOldest())
                            .setQos0NextSeq(0)
                            .setQos1NextSeq(0)
                            .setQos2NextSeq(0)
                            .setClient(inboxParams.getClient())
                            .build()
                            .toByteString());
                    replyBuilder.putSubs(scopedInboxIdUtf8, TopicFilterList.newBuilder()
                        .addAllTopicFilters(metadata.getTopicFiltersMap().keySet())
                        .build());
                }
            } else {
                writeClient.put(
                    scopedInboxId,
                    InboxMetadata.newBuilder()
                        .setExpireSeconds(inboxParams.getExpireSeconds())
                        .setLimit(inboxParams.getLimit())
                        .setLastFetchTime(clock.millis())
                        .setDropOldest(inboxParams.getDropOldest())
                        .setQos0NextSeq(0)
                        .setQos1NextSeq(0)
                        .setQos2NextSeq(0)
                        .setClient(inboxParams.getClient())
                        .build()
                        .toByteString());
            }
        }
        return replyBuilder.build();
    }

    private BatchSubReply batchSub(BatchSubRequest request, IKVReader reader, IKVWriter writeClient) {
        BatchSubReply.Builder replyFilter = BatchSubReply.newBuilder().setReqId(request.getReqId());
        Map<ByteString, Map<String, QoS>> subMap = new HashMap<>();
        request.getTopicFiltersMap()
            .forEach((scopedTopicFilterUtf8, subQoS) -> {
                ByteString scopedTopicFilter = ByteString.copyFromUtf8(scopedTopicFilterUtf8);
                ByteString scopedInboxId =
                    parseScopedInboxIdFromScopedTopicFilter(ByteString.copyFromUtf8(scopedTopicFilterUtf8));
                String topicFilter = parseTopicFilterFromScopedTopicFilter(scopedTopicFilter);
                subMap.computeIfAbsent(scopedInboxId, s -> new HashMap<>())
                    .put(topicFilter, subQoS);
            });

        subMap.forEach((scopedInboxId, topicFilters) -> {
            Optional<ByteString> value = reader.get(scopedInboxId);
            try {
                if (value.isEmpty()) {
                    topicFilters.forEach((topicFilter, subQoS) ->
                        replyFilter.putResults(scopedTopicFilter(scopedInboxId, topicFilter).toStringUtf8(),
                            BatchSubReply.Result.NO_INBOX));
                    return;
                }
                InboxMetadata metadata = InboxMetadata.parseFrom(value.get());
                if (hasExpired(metadata)) {
                    topicFilters.forEach((topicFilter, subQoS) ->
                        replyFilter.putResults(scopedTopicFilter(scopedInboxId, topicFilter).toStringUtf8(),
                            BatchSubReply.Result.NO_INBOX));
                } else {
                    InboxMetadata.Builder metadataBuilder = metadata.toBuilder();
                    int maxTopicFilters = settingProvider
                        .provide(Setting.MaxTopicFiltersPerInbox, parseTenantId(scopedInboxId));

                    topicFilters.forEach((topicFilter, subQoS) -> {
                        if (metadataBuilder.getTopicFiltersCount() < maxTopicFilters) {
                            metadataBuilder.putTopicFilters(topicFilter, subQoS);
                            replyFilter.putResults(scopedTopicFilter(scopedInboxId, topicFilter).toStringUtf8(),
                                BatchSubReply.Result.OK);
                        } else {
                            replyFilter.putResults(scopedTopicFilter(scopedInboxId, topicFilter).toStringUtf8(),
                                BatchSubReply.Result.EXCEED_LIMIT);
                        }
                    });
                    metadata = metadataBuilder.setLastFetchTime(clock.millis()).build();
                    writeClient.put(scopedInboxId, metadata.toByteString());
                }
            } catch (Throwable e) {
                topicFilters.forEach((topicFilter, subQoS) -> replyFilter
                    .putResults(scopedTopicFilter(scopedInboxId, topicFilter).toStringUtf8(),
                        BatchSubReply.Result.ERROR));
            }
        });
        return replyFilter.build();
    }

    private BatchUnsubReply batchUnsub(BatchUnsubRequest request, IKVReader reader,
                                       IKVWriter writeClient) {
        BatchUnsubReply.Builder replyFilter = BatchUnsubReply.newBuilder().setReqId(request.getReqId());
        request.getTopicFiltersList().forEach(scopedTopicFilter -> {
            ByteString scopedInboxId = parseScopedInboxIdFromScopedTopicFilter(scopedTopicFilter);
            String topicFilter = parseTopicFilterFromScopedTopicFilter(scopedTopicFilter);
            Optional<ByteString> value = reader.get(scopedInboxId);
            try {
                if (value.isEmpty()) {
                    replyFilter.putResults(scopedTopicFilter.toStringUtf8(), BatchUnsubReply.Result.NO_INBOX);
                    return;
                }
                InboxMetadata metadata = InboxMetadata.parseFrom(value.get());
                if (hasExpired(metadata)) {
                    replyFilter.putResults(scopedTopicFilter.toStringUtf8(), BatchUnsubReply.Result.NO_INBOX);
                } else {
                    InboxMetadata.Builder metadataBuilder = metadata.toBuilder();
                    if (metadataBuilder.containsTopicFilters(topicFilter)) {
                        metadataBuilder.removeTopicFilters(topicFilter);
                        replyFilter.putResults(scopedTopicFilter.toStringUtf8(), BatchUnsubReply.Result.OK);
                    } else {
                        replyFilter.putResults(scopedTopicFilter.toStringUtf8(), BatchUnsubReply.Result.NO_SUB);
                    }
                    metadata = metadataBuilder.setLastFetchTime(clock.millis()).build();
                    writeClient.put(scopedInboxId, metadata.toByteString());
                }
            } catch (Throwable e) {
                replyFilter.putResults(scopedTopicFilter.toStringUtf8(), BatchUnsubReply.Result.ERROR);
            }
        });
        return replyFilter.build();
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
        try (IKVIterator itr = reader.iterator()) {
            if (request.hasScopedInboxId()) {
                itr.seek(request.getScopedInboxId());
            } else {
                itr.seekToFirst();
            }
            while (itr.isValid() && System.nanoTime() - start < yieldThreshold) {
                ByteString scopedInboxId = parseScopedInboxId(itr.key());
                if (request.hasScopedInboxId() &&
                    request.getScopedInboxId().equals(scopedInboxId)) {
                    // skip the cursor
                    continue;
                }
                if (isInboxMetadataKey(itr.key())) {
                    InboxMetadata metadata = InboxMetadata.parseFrom(itr.value());
                    if (isGCable(metadata)) {
                        if (replyBuilder.getScopedInboxIdCount() < request.getLimit()) {
                            replyBuilder.addScopedInboxId(scopedInboxId);
                        } else {
                            break;
                        }
                    }
                }
                itr.seek(upperBound(scopedInboxId));
            }
        }
        return replyBuilder.build();
    }

    private CollectMetricsReply collect(CollectMetricsRequest request, IKVReader reader) {
        CollectMetricsReply.Builder builder = CollectMetricsReply.newBuilder().setReqId(request.getReqId());
        try (IKVIterator itr = reader.iterator()) {
            for (itr.seekToFirst(); itr.isValid(); ) {
                String tenantId = parseTenantId(itr.key());
                ByteString startKey = tenantPrefix(tenantId);
                ByteString endKey = upperBound(tenantPrefix(tenantId));
                builder.putUsedSpaces(tenantId, reader.size(intersect(reader.range(), Range.newBuilder()
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

    private InboxComSertReply batchInsertAndCommit(InboxComSertRequest request, IKVReader reader, IKVWriter writer) {
        IKVIterator itr = reader.iterator();
        Map<SubInfo, InsertResult.Result> results = new LinkedHashMap<>();
        Map<ByteString, List<MessagePack>> subMsgPacksByInbox = new HashMap<>();
        List<MessagePack> subMsgPackList = request.getInsertList();
        Map<ByteString, CommitParams> commitInboxs = new HashMap<>();
        request.getCommitMap().forEach((k, v) -> commitInboxs.put(ByteString.copyFromUtf8(k), v));


        InboxComSertReply.Builder replyBuilder = InboxComSertReply.newBuilder();
        for (MessagePack subMsgPack : subMsgPackList) {
            SubInfo subInfo = subMsgPack.getSubInfo();
            ByteString scopedInboxId = KeyUtil.scopedInboxId(subInfo.getTenantId(), subInfo.getInboxId());
            results.put(subInfo, null);
            subMsgPacksByInbox.computeIfAbsent(scopedInboxId, k -> new LinkedList<>()).add(subMsgPack);
        }
        for (ByteString scopedInboxId : subMsgPacksByInbox.keySet()) {
            List<MessagePack> subMsgPacks = subMsgPacksByInbox.get(scopedInboxId);
            CommitParams inboxCommit = commitInboxs.remove(scopedInboxId);
            Optional<ByteString> metadataBytes = reader.get(scopedInboxId);
            if (metadataBytes.isEmpty()) {
                subMsgPacks.forEach(pack -> results.put(pack.getSubInfo(), InsertResult.Result.NO_INBOX));
                if (inboxCommit != null) {
                    replyBuilder.putCommitResults(scopedInboxId.toStringUtf8(), false);
                }
                continue;
            }
            try {
                InboxMetadata metadata = InboxMetadata.parseFrom(metadataBytes.get());
                if (hasExpired(metadata)) {
                    subMsgPacks.forEach(pack -> results.put(pack.getSubInfo(), InsertResult.Result.NO_INBOX));
                    if (inboxCommit != null) {
                        replyBuilder.putCommitResults(scopedInboxId.toStringUtf8(), false);
                    }
                    continue;
                }
                metadata = insertInbox(scopedInboxId, subMsgPacks, metadata, itr, reader, writer);
                if (inboxCommit != null) {
                    // handle insert and commit inbox
                    metadata = commitInbox(scopedInboxId, inboxCommit, metadata, itr, writer);
                    replyBuilder.putCommitResults(scopedInboxId.toStringUtf8(), true);
                }
                writer.put(scopedInboxId, metadata.toByteString());
                subMsgPacks.forEach(pack -> results.put(pack.getSubInfo(), InsertResult.Result.OK));
            } catch (Throwable e) {
                log.error("Error inserting messages to inbox {}", scopedInboxId, e);
                subMsgPacks.forEach(pack -> results.put(pack.getSubInfo(), InsertResult.Result.NO_INBOX));
            }
        }
        for (Map.Entry<SubInfo, InsertResult.Result> entry : results.entrySet()) {
            replyBuilder.addInsertResults(InsertResult.newBuilder()
                .setSubInfo(entry.getKey())
                .setResult(entry.getValue())
                .build());
        }
        // handle commit only inbox
        for (ByteString scopedInboxId : commitInboxs.keySet()) {
            CommitParams inboxCommit = commitInboxs.get(scopedInboxId);
            assert inboxCommit.hasQos0UpToSeq() || inboxCommit.hasQos1UpToSeq() || inboxCommit.hasQos2UpToSeq();
            Optional<ByteString> metadataBytes = reader.get(scopedInboxId);
            if (metadataBytes.isEmpty()) {
                replyBuilder.putCommitResults(scopedInboxId.toStringUtf8(), false);
                continue;
            }
            try {
                InboxMetadata metadata = InboxMetadata.parseFrom(metadataBytes.get());
                if (hasExpired(metadata)) {
                    replyBuilder.putCommitResults(scopedInboxId.toStringUtf8(), false);
                    continue;
                }
                metadata = commitInbox(scopedInboxId, inboxCommit, metadata, itr, writer);
                writer.put(scopedInboxId, metadata.toByteString());
                replyBuilder.putCommitResults(scopedInboxId.toStringUtf8(), true);
            } catch (InvalidProtocolBufferException e) {
                replyBuilder.putCommitResults(scopedInboxId.toStringUtf8(), false);
            }
        }
        return replyBuilder.build();
    }

    private BatchInsertReply batchInsert(BatchInsertRequest request, IKVReader reader, IKVWriter writer) {
        IKVIterator itr = reader.iterator();
        Map<SubInfo, InsertResult.Result> results = new LinkedHashMap<>();
        Map<ByteString, List<MessagePack>> subMsgPacksByInbox = new HashMap<>();
        for (MessagePack subMsgPack : request.getSubMsgPackList()) {
            SubInfo subInfo = subMsgPack.getSubInfo();
            ByteString scopedInboxId = KeyUtil.scopedInboxId(subInfo.getTenantId(), subInfo.getInboxId());
            results.put(subInfo, null);
            subMsgPacksByInbox.computeIfAbsent(scopedInboxId, k -> new LinkedList<>()).add(subMsgPack);
        }
        for (ByteString scopedInboxId : subMsgPacksByInbox.keySet()) {
            List<MessagePack> subMsgPacks = subMsgPacksByInbox.get(scopedInboxId);
            Optional<ByteString> metadataBytes = reader.get(scopedInboxId);
            if (metadataBytes.isEmpty()) {
                subMsgPacks.forEach(pack -> results.put(pack.getSubInfo(), InsertResult.Result.NO_INBOX));
                continue;
            }
            try {
                InboxMetadata metadata = InboxMetadata.parseFrom(metadataBytes.get());
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
                if (!subMsgPacks.isEmpty()) {
                    metadata = insertInbox(scopedInboxId, subMsgPacks, metadata, itr, reader, writer);
                    writer.put(scopedInboxId, metadata.toByteString());
                    subMsgPacks.forEach(pack -> results.put(pack.getSubInfo(), InsertResult.Result.OK));
                }
            } catch (Throwable e) {
                log.error("Error inserting messages to inbox {}", scopedInboxId, e);
                subMsgPacks.forEach(pack -> results.put(pack.getSubInfo(), InsertResult.Result.ERROR));
            }
        }

        BatchInsertReply.Builder replyBuilder = BatchInsertReply.newBuilder();
        for (Map.Entry<SubInfo, InsertResult.Result> entry : results.entrySet()) {
            replyBuilder.addResults(InsertResult.newBuilder()
                .setSubInfo(entry.getKey())
                .setResult(entry.getValue())
                .build());
        }
        return replyBuilder.build();
    }

    private InboxMetadata insertInbox(ByteString scopedInboxId,
                                      List<MessagePack> msgPacks,
                                      InboxMetadata metadata,
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
        if (!qos0MsgList.isEmpty()) {
            long nextSeq = insertToInbox(scopedInboxId, QoS.AT_MOST_ONCE, metadata.getQos0NextSeq(),
                KeyUtil::isQoS0MessageKey, KeyUtil::qos0InboxMsgKey, metadata, qos0MsgList, itr, writer);
            metadata = metadata.toBuilder().setQos0NextSeq(nextSeq).build();
        }
        if (!qos1MsgList.isEmpty()) {
            long nextSeq = insertToInbox(scopedInboxId, QoS.AT_LEAST_ONCE, metadata.getQos1NextSeq(),
                KeyUtil::isQoS1MessageKey, KeyUtil::qos1InboxMsgKey, metadata, qos1MsgList, itr, writer);
            metadata = metadata.toBuilder().setQos1NextSeq(nextSeq).build();
        }
        if (!qos2MsgList.isEmpty()) {
            metadata = insertQoS2Inbox(scopedInboxId, metadata, qos2MsgList, itr, reader, writer);
        }
        return metadata;
    }

    private long insertToInbox(ByteString scopedInboxId,
                               QoS qos,
                               long nextSeq,
                               BiFunction<ByteString, ByteString, Boolean> keyChecker,
                               BiFunction<ByteString, Long, ByteString> keyGenerator,
                               InboxMetadata metadata,
                               List<InboxMessage> messages,
                               IKVIterator itr,
                               IKVWriter writeClient) throws InvalidProtocolBufferException {
        itr.seek(keyGenerator.apply(scopedInboxId, 0L));
        long oldestSeq = itr.isValid() && keyChecker.apply(itr.key(), scopedInboxId) ?
            parseSeq(scopedInboxId, itr.key()) : nextSeq;
        int current = (int) (nextSeq - oldestSeq);
        int dropCount = current + messages.size() - metadata.getLimit();
        int actualDropped = 0;
        if (metadata.getDropOldest()) {
            if (messages.size() >= metadata.getLimit()) {
                if (current > 0) {
                    // drop all messages in the queue
                    writeClient.deleteRange(
                        Range.newBuilder()
                            .setStartKey(keyGenerator.apply(scopedInboxId, oldestSeq))
                            .setEndKey(keyGenerator.apply(scopedInboxId, nextSeq))
                            .build());
                    actualDropped += current;
                }
                // TODO: put a limit on value size?
                InboxMessageList messageList =
                    InboxMessageList.newBuilder()
                        .addAllMessage(
                            messages.size() > metadata.getLimit()
                                ? messages.subList(messages.size() - metadata.getLimit(), messages.size())
                                : messages)
                        .build();
                actualDropped += messages.size() - metadata.getLimit();
                writeClient.insert(keyGenerator.apply(scopedInboxId, nextSeq), messageList.toByteString());
                nextSeq += metadata.getLimit();
            } else {
                if (dropCount > 0) {
                    long delBeforeSeq = dropCount + oldestSeq;
                    itr.seekForPrev(keyGenerator.apply(scopedInboxId, delBeforeSeq));
                    ByteString delKeyEntryKey = itr.key();
                    long deleteEntryKeySeq = parseSeq(scopedInboxId, delKeyEntryKey);
                    if (deleteEntryKeySeq < delBeforeSeq) {
                        // entry value need to be partially deleted
                        InboxMessageList inboxMessageList = InboxMessageList.parseFrom(itr.value());
                        int listLength = inboxMessageList.getMessageCount();
                        InboxMessageList trimList = InboxMessageList.newBuilder()
                            .addAllMessage(inboxMessageList.getMessageList()
                                .subList((int) (delBeforeSeq - deleteEntryKeySeq), listLength))
                            .build();
                        writeClient.delete(delKeyEntryKey);
                        writeClient.insert(keyGenerator.apply(scopedInboxId, delBeforeSeq), trimList.toByteString());
                    }
                    if (deleteEntryKeySeq > oldestSeq) {
                        // fully delete entries before the delKeyEntryKey
                        itr.seekForPrev(keyGenerator.apply(scopedInboxId, deleteEntryKeySeq - 1));
                        writeClient.deleteRange(Range.newBuilder()
                            .setStartKey(keyGenerator.apply(scopedInboxId, oldestSeq))
                            .setEndKey(itr.key())
                            .build());
                    }
                    actualDropped += dropCount;
                }
                // TODO: put a limit on value size?
                writeClient.insert(
                    keyGenerator.apply(scopedInboxId, nextSeq),
                    InboxMessageList.newBuilder().addAllMessage(messages).build().toByteString());
                nextSeq += messages.size();
            }
        } else {
            if (dropCount < messages.size()) {
                // TODO: put a limit on value size?
                InboxMessageList messageList =
                    InboxMessageList.newBuilder()
                        .addAllMessage(dropCount > 0 ? messages.subList(0, messages.size() - dropCount) : messages)
                        .build();
                writeClient.insert(keyGenerator.apply(scopedInboxId, nextSeq), messageList.toByteString());
                if (dropCount > 0) {
                    actualDropped += dropCount;
                    nextSeq += messages.size() - dropCount;
                } else {
                    nextSeq += messages.size();
                }
            } else {
                actualDropped += dropCount;
            }
        }
        if (actualDropped > 0) {
            eventCollector.report(getLocal(Overflowed.class)
                .oldest(metadata.getDropOldest())
                .qos(qos)
                .clientInfo(metadata.getClient())
                .dropCount(actualDropped));
        }
        return nextSeq;
    }

    private InboxMetadata insertQoS2Inbox(ByteString scopedInboxId,
                                          InboxMetadata metadata,
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
        long nextSeq = metadata.getQos2NextSeq();
        long oldestSeq =
            itr.isValid() && isQoS2MessageIndexKey(itr.key(), scopedInboxId)
                ? parseQoS2Index(scopedInboxId, itr.key())
                : nextSeq;
        assert oldestSeq <= nextSeq;
        int messageCount = messages.size();
        int limit = metadata.getLimit();
        int current = (int) (nextSeq - oldestSeq);
        int total = current + messageCount;
        int dropCount = total - limit;
        if (total > limit) {
            if (metadata.getDropOldest()) {
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
        if (dropCount > 0) {
            eventCollector.report(getLocal(Overflowed.class)
                .oldest(metadata.getDropOldest())
                .qos(QoS.EXACTLY_ONCE)
                .clientInfo(metadata.getClient())
                .dropCount(dropCount));
        }
        return metadata.toBuilder().setQos2NextSeq(nextSeq).build();
    }

    @SneakyThrows
    private BatchTouchReply batchTouch(BatchTouchRequest request, IKVReader reader, IKVWriter writer) {
        BatchTouchReply.Builder replyBuilder = BatchTouchReply.newBuilder();
        for (String scopedInboxIdUtf8 : request.getScopedInboxIdMap().keySet()) {
            ByteString scopedInboxId = ByteString.copyFromUtf8(scopedInboxIdUtf8);
            Optional<ByteString> metadataBytes = reader.get(scopedInboxId);
            if (metadataBytes.isPresent()) {
                InboxMetadata metadata = InboxMetadata.parseFrom(metadataBytes.get());
                if (hasExpired(metadata) || !request.getScopedInboxIdMap().get(scopedInboxIdUtf8)) {
                    clearInbox(scopedInboxId, metadata, reader.iterator(), writer);
                    replyBuilder.putSubs(scopedInboxIdUtf8, TopicFilterList.newBuilder()
                        .addAllTopicFilters(metadata.getTopicFiltersMap().keySet())
                        .build());
                } else {
                    metadata = metadata.toBuilder().setLastFetchTime(clock.millis()).build();
                    writer.put(scopedInboxId, metadata.toByteString());
                }
            }
        }
        return replyBuilder.build();
    }

    private BatchCommitReply batchCommit(BatchCommitRequest request, IKVReader reader, IKVWriter writer) {
        BatchCommitReply.Builder replyBuilder = BatchCommitReply.newBuilder();
        IKVIterator itr = reader.iterator();
        for (String scopedInboxIdUtf8 : request.getInboxCommitMap().keySet()) {
            ByteString scopedInboxId = ByteString.copyFromUtf8(scopedInboxIdUtf8);
            CommitParams inboxCommit = request.getInboxCommitMap().get(scopedInboxIdUtf8);
            assert inboxCommit.hasQos0UpToSeq() || inboxCommit.hasQos1UpToSeq() || inboxCommit.hasQos2UpToSeq();
            Optional<ByteString> metadataBytes = reader.get(scopedInboxId);
            if (metadataBytes.isEmpty()) {
                replyBuilder.putResult(scopedInboxIdUtf8, false);
                continue;
            }
            try {
                InboxMetadata metadata = InboxMetadata.parseFrom(metadataBytes.get());
                if (hasExpired(metadata)) {
                    replyBuilder.putResult(scopedInboxIdUtf8, false);
                    continue;
                }
                metadata = commitInbox(scopedInboxId, inboxCommit, metadata, itr, writer);
                writer.put(scopedInboxId, metadata.toByteString());
                replyBuilder.putResult(scopedInboxIdUtf8, true);
            } catch (InvalidProtocolBufferException e) {
                replyBuilder.putResult(scopedInboxIdUtf8, false);
            }
        }
        return replyBuilder.build();
    }

    private InboxMetadata commitInbox(ByteString scopedInboxId,
                                      CommitParams inboxCommit,
                                      InboxMetadata metadata,
                                      IKVIterator itr,
                                      IKVWriter writer) throws InvalidProtocolBufferException {
        if (inboxCommit.hasQos0UpToSeq()) {
            metadata = commitToInbox(scopedInboxId, inboxCommit.getQos0UpToSeq(),
                KeyUtil::isQoS0MessageKey, KeyUtil::qos0InboxMsgKey,
                (inboxMetadata, seq) -> inboxMetadata.toBuilder().setQos0LastFetchBeforeSeq(seq).build(),
                metadata, itr, writer);
        }
        if (inboxCommit.hasQos1UpToSeq()) {
            metadata = commitToInbox(scopedInboxId, inboxCommit.getQos1UpToSeq(),
                KeyUtil::isQoS1MessageKey, KeyUtil::qos1InboxMsgKey,
                (inboxMetadata, seq) -> inboxMetadata.toBuilder().setQos1LastCommitBeforeSeq(seq).build(),
                metadata, itr, writer);
        }
        if (inboxCommit.hasQos2UpToSeq()) {
            itr.seek(qos2InboxPrefix(scopedInboxId));
            while (itr.isValid() && isQoS2MessageIndexKey(itr.key(), scopedInboxId)
                && parseQoS2Index(scopedInboxId, itr.key()) <= inboxCommit.getQos2UpToSeq()) {
                writer.delete(itr.key());
                writer.delete(qos2InboxMsgKey(scopedInboxId, itr.value()));
                itr.next();
            }
            metadata = metadata.toBuilder().setQos2LastCommitBeforeSeq(inboxCommit.getQos2UpToSeq() + 1).build();
        }

        metadata = metadata.toBuilder().setLastFetchTime(clock.millis()).build();
        return metadata;
    }

    private InboxMetadata commitToInbox(ByteString scopedInboxId,
                                        long upToSeq,
                                        BiFunction<ByteString, ByteString, Boolean> keyChecker,
                                        BiFunction<ByteString, Long, ByteString> keyGenerator,
                                        BiFunction<InboxMetadata, Long, InboxMetadata> metaDataMapper,
                                        InboxMetadata metadata,
                                        IKVIterator itr,
                                        IKVWriter writer) throws InvalidProtocolBufferException {
        long oldestSeq = 0;
        itr.seek(keyGenerator.apply(scopedInboxId, 0L));
        if (itr.isValid() && keyChecker.apply(itr.key(), scopedInboxId)) {
            oldestSeq = parseSeq(scopedInboxId, itr.key());
        }
        itr.seekForPrev(keyGenerator.apply(scopedInboxId, upToSeq));
        if (itr.isValid() && keyChecker.apply(itr.key(), scopedInboxId)) {
            long currentEntrySeq = parseSeq(scopedInboxId, itr.key());
            if (currentEntrySeq <= upToSeq) {
                InboxMessageList inboxMessageList = InboxMessageList.parseFrom(itr.value());
                long deletedRange = upToSeq - currentEntrySeq + 1;
                int messageCount = inboxMessageList.getMessageCount();
                writer.delete(itr.key());
                if (deletedRange < messageCount) {
                    writer.put(keyGenerator.apply(scopedInboxId, upToSeq + 1),
                        InboxMessageList.newBuilder()
                            .addAllMessage(inboxMessageList.getMessageList()
                                .subList((int) deletedRange, messageCount))
                            .build().toByteString());
                }
                if (currentEntrySeq > oldestSeq) {
                    itr.seekForPrev(keyGenerator.apply(scopedInboxId, currentEntrySeq - 1));
                    writer.deleteRange(Range.newBuilder()
                        .setStartKey(keyGenerator.apply(scopedInboxId, oldestSeq))
                        .setEndKey(itr.key())
                        .build());
                }
                oldestSeq = upToSeq + 1;
            }
        }
        return metaDataMapper.apply(metadata, oldestSeq);
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
                long startSeq = parseSeq(scopedInboxId, itr.key());
                InboxMessageList messageList = InboxMessageList.parseFrom(itr.value());
                for (int i = (int) (startFetchFromSeq - startSeq); i < messageList.getMessageCount(); i++) {
                    if (fetchCount > 0) {
                        seqConsumer.accept(replyBuilder, startSeq + i);
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

    private boolean hasExpired(InboxMetadata metadata) {
        Duration now = Duration.ofMillis(clock.millis());
        Duration expireAt =
            Duration.ofMillis(metadata.getLastFetchTime()).plus(Duration.ofSeconds(metadata.getExpireSeconds()));
        return now.compareTo(expireAt) > 0;
    }

    private boolean isGCable(InboxMetadata metadata) {
        Duration now = Duration.ofMillis(clock.millis());
        Duration expireAt = Duration.ofMillis(metadata.getLastFetchTime())
            .plus(Duration.ofSeconds(metadata.getExpireSeconds()))
            .plus(purgeDelay);
        return now.compareTo(expireAt) > 0;
    }
}
