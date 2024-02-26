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

package com.baidu.bifromq.retain.store;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.compare;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static com.baidu.bifromq.metrics.TenantMetric.MqttRetainNumGauge;
import static com.baidu.bifromq.metrics.TenantMetric.MqttRetainSpaceGauge;
import static com.baidu.bifromq.retain.utils.KeyUtil.parseTenantId;
import static com.baidu.bifromq.retain.utils.KeyUtil.parseTenantNS;
import static com.baidu.bifromq.retain.utils.KeyUtil.tenantNS;
import static com.baidu.bifromq.retain.utils.TopicUtil.isWildcardTopicFilter;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

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
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.metrics.ITenantMeter;
import com.baidu.bifromq.retain.rpc.proto.BatchMatchReply;
import com.baidu.bifromq.retain.rpc.proto.BatchMatchRequest;
import com.baidu.bifromq.retain.rpc.proto.BatchRetainReply;
import com.baidu.bifromq.retain.rpc.proto.BatchRetainRequest;
import com.baidu.bifromq.retain.rpc.proto.GCReply;
import com.baidu.bifromq.retain.rpc.proto.GCRequest;
import com.baidu.bifromq.retain.rpc.proto.MatchError;
import com.baidu.bifromq.retain.rpc.proto.MatchResult;
import com.baidu.bifromq.retain.rpc.proto.MatchResultPack;
import com.baidu.bifromq.retain.rpc.proto.Matched;
import com.baidu.bifromq.retain.rpc.proto.RetainMessage;
import com.baidu.bifromq.retain.rpc.proto.RetainResult;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceROCoProcInput;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceROCoProcOutput;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceRWCoProcInput;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceRWCoProcOutput;
import com.baidu.bifromq.retain.rpc.proto.RetainSetMetadata;
import com.baidu.bifromq.retain.utils.KeyUtil;
import com.baidu.bifromq.retain.utils.TopicUtil;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.TopicMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class RetainStoreCoProc implements IKVRangeCoProc {
    private final Supplier<IKVReader> rangeReaderProvider;
    private final Map<String, RetainSetMetadata> metadataMap = new ConcurrentHashMap<>();
    private final String[] tags;

    RetainStoreCoProc(String clusterId,
                      String storeId,
                      KVRangeId id,
                      Supplier<IKVReader> rangeReaderProvider) {
        this.tags = new String[] {"clusterId", clusterId, "storeId", storeId, "rangeId", KVRangeIdUtil.toString(id)};
        this.rangeReaderProvider = rangeReaderProvider;
        loadMetadata();
    }

    @Override
    public CompletableFuture<ROCoProcOutput> query(ROCoProcInput input, IKVReader reader) {
        RetainServiceROCoProcInput coProcInput = input.getRetainService();
        return switch (coProcInput.getTypeCase()) {
            case BATCHMATCH -> batchMatch(coProcInput.getBatchMatch(), reader)
                .thenApply(v -> ROCoProcOutput.newBuilder()
                    .setRetainService(RetainServiceROCoProcOutput.newBuilder()
                        .setBatchMatch(v).build()).build());
            default -> {
                log.error("Unknown co proc type {}", coProcInput.getTypeCase());
                yield CompletableFuture.failedFuture(
                    new IllegalStateException("Unknown co proc type " + coProcInput.getTypeCase()));
            }
        };
    }

    @SneakyThrows
    @Override
    public Supplier<RWCoProcOutput> mutate(RWCoProcInput input, IKVReader reader, IKVWriter writer) {
        RetainServiceRWCoProcInput coProcInput = input.getRetainService();
        RetainServiceRWCoProcOutput.Builder outputBuilder = RetainServiceRWCoProcOutput.newBuilder();
        AtomicReference<Runnable> afterMutate = new AtomicReference<>();
        switch (coProcInput.getTypeCase()) {
            case BATCHRETAIN -> {
                BatchRetainReply.Builder replyBuilder = BatchRetainReply.newBuilder();
                afterMutate.set(batchRetain(coProcInput.getBatchRetain(), replyBuilder, reader, writer));
                outputBuilder.setBatchRetain(replyBuilder);
            }
            case GC -> {
                GCReply.Builder replyBuilder = GCReply.newBuilder();
                afterMutate.set(gc(coProcInput.getGc(), replyBuilder, reader, writer));
                outputBuilder.setGc(replyBuilder);
            }
        }
        RWCoProcOutput output = RWCoProcOutput.newBuilder().setRetainService(outputBuilder.build()).build();
        return () -> {
            afterMutate.get().run();
            return output;
        };
    }

    @Override
    public void reset(Boundary boundary) {
        clearMetrics();
        loadMetadata();
    }

    @Override
    public void close() {
        metadataMap.keySet().forEach(tenantId -> {
            ITenantMeter.stopGauging(tenantId, MqttRetainNumGauge, tags);
            ITenantMeter.stopGauging(tenantId, MqttRetainSpaceGauge, tags);
        });
    }

    private CompletableFuture<BatchMatchReply> batchMatch(BatchMatchRequest request, IKVReader reader) {
        BatchMatchReply.Builder replyBuilder = BatchMatchReply.newBuilder().setReqId(request.getReqId());
        request.getMatchParamsMap().forEach((tenantId, matchParams) -> {
            MatchResultPack.Builder resultPackBuilder = MatchResultPack.newBuilder();
            matchParams.getTopicFiltersMap().forEach((topicFilter, limit) -> {
                MatchResult.Builder resultBuilder = MatchResult.newBuilder();
                try {
                    resultBuilder.setOk(Matched.newBuilder()
                        .addAllMessages(match(tenantNS(tenantId), topicFilter, limit, matchParams.getNow(), reader)));
                } catch (Throwable e) {
                    resultBuilder.setError(MatchError.getDefaultInstance());
                }
                resultPackBuilder.putResults(topicFilter, resultBuilder.build());
            });
            replyBuilder.putResultPack(tenantId, resultPackBuilder.build());
        });
        return CompletableFuture.completedFuture(replyBuilder.build());
    }

    private List<TopicMessage> match(ByteString tenantNS,
                                     String topicFilter,
                                     int limit,
                                     long now,
                                     IKVReader reader) throws Exception {
        if (limit == 0) {
            // TODO: report event: nothing to match
            return emptyList();
        }
        Boundary range = Boundary.newBuilder()
            .setStartKey(tenantNS)
            .setEndKey(upperBound(tenantNS))
            .build();
        reader.refresh();
        IKVIterator itr = reader.iterator();
        itr.seek(range.getStartKey());
        if (!itr.isValid()) {
            return emptyList();
        }
        if (!isWildcardTopicFilter(topicFilter)) {
            Optional<ByteString> val = reader.get(KeyUtil.retainKey(tenantNS, topicFilter));
            if (val.isPresent()) {
                TopicMessage message = TopicMessage.parseFrom(val.get());
                if (expireAt(message.getMessage()) > now) {
                    return singletonList(message);
                }
            }
            return emptyList();
        }
        // deal with wildcard topic filter
        List<String> matchLevels = TopicUtil.parse(topicFilter, false);
        List<TopicMessage> messages = new LinkedList<>();
        itr.seek(KeyUtil.retainKeyPrefix(tenantNS, matchLevels));
        out:
        while (itr.isValid() && compare(itr.key(), range.getEndKey()) < 0 && messages.size() < limit) {
            List<String> topicLevels = KeyUtil.parseTopic(itr.key());
            switch (RetainMatcher.match(topicLevels, matchLevels)) {
                case MATCHED_AND_CONTINUE -> {
                    TopicMessage message = TopicMessage.parseFrom(itr.value());
                    if (expireAt(message.getMessage()) > now) {
                        messages.add(message);
                    }
                    itr.next();
                }
                case MATCHED_AND_STOP -> {
                    TopicMessage message = TopicMessage.parseFrom(itr.value());
                    if (expireAt(message.getMessage()) > now) {
                        messages.add(message);
                    }
                    break out;
                }
                case MISMATCH_AND_CONTINUE -> itr.next();
                case MISMATCH_AND_STOP -> {
                    break out;
                }
            }
//            // TODO: optimize single level wildcard match to stop early
//            if (TopicUtil.match(topicLevels, matchLevels)) {
//                TopicMessage message = TopicMessage.parseFrom(itr.value());
//                if (expireAt(message.getMessage()) > now) {
//                    messages.add(message);
//                }
//            }
//            itr.next();
        }
        return messages;
    }

    private Runnable batchRetain(BatchRetainRequest request,
                                 BatchRetainReply.Builder replyBuilder,
                                 IKVReader reader,
                                 IKVWriter writer) {
        replyBuilder.setReqId(request.getReqId());
        Map<String, RetainSetMetadata> toBeCached = new HashMap<>();
        request.getParamsMap().forEach((tenantId, retainParam) -> {
            RetainSetMetadata.Builder metadataBuilder = getMetadata(tenantId)
                .map(RetainSetMetadata::toBuilder)
                .orElse(RetainSetMetadata.newBuilder().setEstExpire(Long.MAX_VALUE));
            Map<String, RetainResult.Code> results =
                retain(tenantId, retainParam.getTopicMessagesMap(), metadataBuilder, reader, writer);
            replyBuilder.putResults(tenantId, RetainResult.newBuilder()
                .putAllResults(results)
                .build());
            if (metadataBuilder.getCount() > 0) {
                writer.put(tenantNS(tenantId), metadataBuilder.build().toByteString());
            } else {
                writer.delete(tenantNS(tenantId));
            }
            toBeCached.put(tenantId, metadataBuilder.build());
        });
        return () -> {
            toBeCached.forEach(this::cacheMetadata);
        };
    }

    private Map<String, RetainResult.Code> retain(String tenantId,
                                                  Map<String, RetainMessage> retainMessages,
                                                  RetainSetMetadata.Builder metadataBuilder,
                                                  IKVReader reader,
                                                  IKVWriter writer) {
        Map<String, RetainResult.Code> results = new HashMap<>();
        for (Map.Entry<String, RetainMessage> entry : retainMessages.entrySet()) {
            String topic = entry.getKey();
            RetainMessage retainMessage = entry.getValue();
            RetainResult.Code result = doRetain(tenantId, topic, retainMessage, metadataBuilder, reader, writer);
            results.put(topic, result);
        }
        return results;
    }

    private RetainResult.Code doRetain(String tenantId,
                                       String topic,
                                       RetainMessage retainMessage,
                                       RetainSetMetadata.Builder metadataBuilder,
                                       IKVReader reader,
                                       IKVWriter writer) {
        try {
            TopicMessage topicMessage = TopicMessage.newBuilder()
                .setTopic(topic)
                .setMessage(retainMessage.getMessage())
                .setPublisher(retainMessage.getPublisher())
                .build();
            ByteString tenantNS = KeyUtil.tenantNS(tenantId);
            ByteString retainKey = KeyUtil.retainKey(tenantNS, topicMessage.getTopic());
            Optional<ByteString> val = reader.get(retainKey);
            if (topicMessage.getMessage().getPayload().isEmpty()) {
                // delete existing retained
                if (val.isPresent()) {
                    TopicMessage existing = TopicMessage.parseFrom(val.get());
                    writer.delete(retainKey);
                    metadataBuilder
                        .setCount(metadataBuilder.getCount() - 1)
                        .setUsedSpace(metadataBuilder.getUsedSpace() - sizeOf(existing));
                }
                return RetainResult.Code.CLEARED;
            }
            if (val.isEmpty()) {
                // retain new message
                writer.put(retainKey, topicMessage.toByteString());
                metadataBuilder
                    .setEstExpire(Math.min(expireAt(topicMessage.getMessage()), metadataBuilder.getEstExpire()))
                    .setUsedSpace(metadataBuilder.getUsedSpace() + sizeOf(topicMessage))
                    .setCount(metadataBuilder.getCount() + 1).build();
            } else {
                // replace existing
                TopicMessage existing = TopicMessage.parseFrom(val.get());
                writer.put(retainKey, topicMessage.toByteString());
                metadataBuilder
                    .setEstExpire(Math.min(expireAt(topicMessage.getMessage()), metadataBuilder.getEstExpire()))
                    .setUsedSpace(metadataBuilder.getUsedSpace() + sizeOf(topicMessage) - sizeOf(existing));
            }
            return RetainResult.Code.RETAINED;
        } catch (Throwable e) {
            log.error("Retain failed", e);
            return RetainResult.Code.ERROR;
        }
    }

    private void doGC(long now,
                      String tenantId,
                      RetainSetMetadata.Builder metadataBuilder,
                      IKVReader reader,
                      IKVWriter writer) {
        doGC(now, null, tenantId, metadataBuilder, reader, writer);
    }

    @SneakyThrows
    private void doGC(long now,
                      Integer expirySeconds,
                      String tenantId,
                      RetainSetMetadata.Builder metadataBuilder,
                      IKVReader reader,
                      IKVWriter writer) {
        Boundary range = Boundary.newBuilder()
            .setStartKey(tenantNS(tenantId))
            .setEndKey(upperBound(tenantNS(tenantId))).build();
        reader.refresh();
        IKVIterator itr = reader.iterator();
        itr.seek(range.getStartKey());
        itr.next();
        int expires = 0;
        int freedSpace = 0;
        long earliestExp = Long.MAX_VALUE;
        for (; itr.isValid() && compare(itr.key(), range.getEndKey()) < 0; itr.next()) {
            TopicMessage retainedMsg = TopicMessage.parseFrom(itr.value());
            long expireTime = expireAt(retainedMsg.getMessage().getTimestamp(),
                expirySeconds != null ? expirySeconds : retainedMsg.getMessage().getExpiryInterval());
            if (expireTime <= now) {
                writer.delete(itr.key());
                freedSpace += sizeOf(retainedMsg);
                expires++;
            } else {
                earliestExp = Math.min(expireTime, earliestExp);
            }
        }
        metadataBuilder
            .setCount(metadataBuilder.getCount() - expires)
            .setUsedSpace(metadataBuilder.getUsedSpace() - freedSpace)
            .setEstExpire(earliestExp == Long.MAX_VALUE ? now : earliestExp);
    }

    private Runnable gc(GCRequest request, GCReply.Builder replyBuilder, IKVReader reader, IKVWriter writer) {
        replyBuilder.setReqId(request.getReqId());
        long now = request.getNow();
        Map<String, RetainSetMetadata> toBeCached = new HashMap<>();
        if (request.hasTenantId()) {
            String tenantId = request.getTenantId();
            Optional<RetainSetMetadata> metadata = getMetadata(tenantId);
            if (metadata.isPresent()) {
                RetainSetMetadata.Builder metadataBuilder = metadata.get().toBuilder();
                if (request.hasExpirySeconds()) {
                    doGC(now, request.getExpirySeconds(), tenantId, metadataBuilder, reader, writer);
                } else {
                    doGC(now, tenantId, metadataBuilder, reader, writer);
                }
                if (!metadataBuilder.build().equals(metadata.get())) {
                    if (metadataBuilder.getCount() > 0) {
                        writer.put(tenantNS(tenantId), metadataBuilder.build().toByteString());
                    } else {
                        writer.delete(tenantNS(tenantId));
                    }
                    toBeCached.put(tenantId, metadataBuilder.build());
                }
            }
        } else {
            for (Map.Entry<String, RetainSetMetadata> entry : metadataMap.entrySet()) {
                String tenantId = entry.getKey();
                RetainSetMetadata metadata = entry.getValue();
                RetainSetMetadata.Builder metadataBuilder = metadata.toBuilder();
                if (request.hasExpirySeconds()) {
                    doGC(now, request.getExpirySeconds(), tenantId, metadataBuilder, reader, writer);
                } else {
                    doGC(now, tenantId, metadataBuilder, reader, writer);
                }
                if (!metadataBuilder.build().equals(metadata)) {
                    if (metadataBuilder.getCount() > 0) {
                        writer.put(tenantNS(tenantId), metadataBuilder.build().toByteString());
                    } else {
                        writer.delete(tenantNS(tenantId));
                    }
                    toBeCached.put(tenantId, metadataBuilder.build());
                }
            }
        }
        return () -> {
            toBeCached.forEach(this::cacheMetadata);
        };
    }

    private void loadMetadata() {
        IKVReader reader = rangeReaderProvider.get();
        IKVIterator itr = reader.iterator();
        for (itr.seekToFirst(); itr.isValid(); ) {
            ByteString tenantNS = parseTenantNS(itr.key());
            String tenantId = parseTenantId(tenantNS);
            try {
                RetainSetMetadata metadata = RetainSetMetadata.parseFrom(itr.value());
                cacheMetadata(tenantId, metadata);
            } catch (InvalidProtocolBufferException e) {
                log.error("Unable to parse RetainSetMetadata", e);
            } finally {
                itr.seek(upperBound(tenantNS));
            }
        }
    }

    private long expireAt(Message message) {
        return expireAt(message.getTimestamp(), message.getExpiryInterval());
    }

    private long expireAt(long timestamp, int expirySeconds) {
        return Duration.ofMillis(timestamp).plusSeconds(expirySeconds).toMillis();
    }

    private int sizeOf(TopicMessage retained) {
        return retained.getTopic().length() + retained.getMessage().getPayload().size();
    }

    private Optional<RetainSetMetadata> getMetadata(String tenantId) {
        return Optional.ofNullable(metadataMap.get(tenantId));
    }

    private void clearMetrics() {
        metadataMap.keySet().forEach(tenantId -> {
            ITenantMeter.stopGauging(tenantId, MqttRetainNumGauge, tags);
            ITenantMeter.stopGauging(tenantId, MqttRetainSpaceGauge, tags);
        });
    }

    private void cacheMetadata(String tenantId, RetainSetMetadata metadata) {
        metadataMap.compute(tenantId, (k, v) -> {
            if (v == null) {
                if (metadata.getCount() > 0) {
                    ITenantMeter.gauging(tenantId, MqttRetainNumGauge,
                        () -> metadataMap.getOrDefault(k, RetainSetMetadata.getDefaultInstance()).getCount(), tags);

                    ITenantMeter.gauging(tenantId, MqttRetainSpaceGauge,
                        () -> metadataMap.getOrDefault(k, RetainSetMetadata.getDefaultInstance()).getUsedSpace(), tags);
                    return metadata;
                }
                return null;
            } else {
                if (metadata.getCount() == 0) {
                    ITenantMeter.stopGauging(tenantId, MqttRetainNumGauge, tags);
                    ITenantMeter.stopGauging(tenantId, MqttRetainSpaceGauge, tags);
                    return null;
                }
                return metadata;
            }
        });
    }
}
