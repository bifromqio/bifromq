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

package com.baidu.bifromq.retain.store;

import static com.baidu.bifromq.basekv.localengine.RangeUtil.upperBound;
import static com.baidu.bifromq.basekv.utils.KeyRangeUtil.compare;
import static com.baidu.bifromq.basekv.utils.KeyRangeUtil.intersect;
import static com.baidu.bifromq.plugin.settingprovider.Setting.RetainedTopicLimit;
import static com.baidu.bifromq.retain.utils.KeyUtil.parseTenantId;
import static com.baidu.bifromq.retain.utils.KeyUtil.tenantNS;
import static com.baidu.bifromq.retain.utils.TopicUtil.isWildcardTopicFilter;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.Range;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProc;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.api.IKVWriter;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.retain.rpc.proto.BatchMatchReply;
import com.baidu.bifromq.retain.rpc.proto.BatchMatchRequest;
import com.baidu.bifromq.retain.rpc.proto.BatchRetainReply;
import com.baidu.bifromq.retain.rpc.proto.BatchRetainRequest;
import com.baidu.bifromq.retain.rpc.proto.CollectMetricsReply;
import com.baidu.bifromq.retain.rpc.proto.CollectMetricsRequest;
import com.baidu.bifromq.retain.rpc.proto.GCReply;
import com.baidu.bifromq.retain.rpc.proto.GCRequest;
import com.baidu.bifromq.retain.rpc.proto.MatchError;
import com.baidu.bifromq.retain.rpc.proto.MatchResult;
import com.baidu.bifromq.retain.rpc.proto.MatchResultPack;
import com.baidu.bifromq.retain.rpc.proto.Matched;
import com.baidu.bifromq.retain.rpc.proto.RetainResult;
import com.baidu.bifromq.retain.rpc.proto.RetainResultPack;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceROCoProcInput;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceROCoProcOutput;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceRWCoProcInput;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceRWCoProcOutput;
import com.baidu.bifromq.retain.rpc.proto.RetainSetMetadata;
import com.baidu.bifromq.retain.utils.KeyUtil;
import com.baidu.bifromq.retain.utils.TopicUtil;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.TopicMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Clock;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class RetainStoreCoProc implements IKVRangeCoProc {
    private final Supplier<IKVRangeReader> rangeReaderProvider;
    private final ISettingProvider settingProvider;
    private final Clock clock;

    RetainStoreCoProc(KVRangeId id,
                      Supplier<IKVRangeReader> rangeReaderProvider,
                      ISettingProvider settingProvider,
                      Clock clock) {
        this.rangeReaderProvider = rangeReaderProvider;
        this.settingProvider = settingProvider;
        this.clock = clock;
    }

    @Override
    public CompletableFuture<ByteString> query(ByteString input, IKVReader reader) {
        try {
            RetainServiceROCoProcInput coProcInput = RetainServiceROCoProcInput.parseFrom(input);
            switch (coProcInput.getTypeCase()) {
                case BATCHMATCH:
                    return batchMatch(coProcInput.getBatchMatch(), reader)
                        .thenApply(v -> RetainServiceROCoProcOutput.newBuilder()
                            .setBatchMatch(v).build().toByteString());
                case COLLECTMETRICS:
                    return CompletableFuture.completedFuture(RetainServiceROCoProcOutput.newBuilder()
                        .setCollectMetrics(collectMetrics(coProcInput.getCollectMetrics(), reader)).build()
                        .toByteString());
                default:
                    log.error("Unknown co proc type {}", coProcInput.getTypeCase());
                    return CompletableFuture.failedFuture(
                        new IllegalStateException("Unknown co proc type " + coProcInput.getTypeCase()));

            }
        } catch (InvalidProtocolBufferException e) {
            log.error("Unable to parse ro co-proc", e);
            return CompletableFuture.failedFuture(new IllegalStateException("Unable to parse ro co-proc", e));
        }
    }

    @SneakyThrows
    @Override
    public Supplier<ByteString> mutate(ByteString input, IKVReader reader, IKVWriter writer) {
        RetainServiceRWCoProcInput coProcInput = RetainServiceRWCoProcInput.parseFrom(input);
        final ByteString output = switch (coProcInput.getTypeCase()) {
            case BATCHRETAIN -> RetainServiceRWCoProcOutput.newBuilder()
                .setBatchRetain(batchRetain(coProcInput.getBatchRetain(), reader, writer)).build()
                .toByteString();
            case GC -> RetainServiceRWCoProcOutput.newBuilder()
                .setGc(gc(coProcInput.getGc(), reader, writer)).build().toByteString();
            default -> {
                log.error("Unknown co proc type {}", coProcInput.getTypeCase());
                throw new IllegalStateException("Unknown co proc type " + coProcInput.getTypeCase());
            }
        };
        return () -> output;
    }

    @Override
    public void close() {

    }

    private CompletableFuture<BatchMatchReply> batchMatch(BatchMatchRequest request, IKVReader reader) {
        BatchMatchReply.Builder replyBuilder = BatchMatchReply.newBuilder().setReqId(request.getReqId());
        request.getMatchParamsMap().forEach((tenantId, matchParams) -> {
            MatchResultPack.Builder resultPackBuilder = MatchResultPack.newBuilder();
            matchParams.getTopicFiltersMap().forEach((topicFilter, limit) -> {
                MatchResult.Builder resultBuilder = MatchResult.newBuilder();
                try {
                    resultBuilder.setOk(Matched.newBuilder()
                        .addAllMessages(match(tenantNS(tenantId), topicFilter, limit, reader)));
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
                                     IKVReader reader) throws Exception {
        if (limit == 0) {
            // TODO: report event: nothing to match
            return emptyList();
        }
        Range range = Range.newBuilder()
            .setStartKey(tenantNS)
            .setEndKey(upperBound(tenantNS))
            .build();
        try (IKVIterator itr = reader.iterator()) {
            itr.seek(range.getStartKey());
            if (!itr.isValid()) {
                return emptyList();
            }
            if (!isWildcardTopicFilter(topicFilter)) {
                Optional<ByteString> val = reader.get(KeyUtil.retainKey(tenantNS, topicFilter));
                if (val.isPresent()) {
                    TopicMessage message = TopicMessage.parseFrom(val.get());
                    if (message.getMessage().getExpireTimestamp() > clock.millis()) {
                        return singletonList(message);
                    }
                }
                return emptyList();
            }
            // deal with wildcard topic filter
            List<String> matchLevels = TopicUtil.parse(topicFilter, false);
            List<TopicMessage> messages = new LinkedList<>();
            itr.seek(KeyUtil.retainKeyPrefix(tenantNS, matchLevels));
            while (itr.isValid() && compare(itr.key(), range.getEndKey()) < 0 && messages.size() < limit) {
                List<String> topicLevels = KeyUtil.parseTopic(itr.key());
                if (TopicUtil.match(topicLevels, matchLevels)) {
                    TopicMessage message = TopicMessage.parseFrom(itr.value());
                    if (message.getMessage().getExpireTimestamp() > clock.millis()) {
                        messages.add(message);
                    }
                }
                itr.next();
            }
            return messages;
        }
    }

    private BatchRetainReply batchRetain(BatchRetainRequest request, IKVReader reader, IKVWriter writer) {
        BatchRetainReply.Builder replyBuilder =
            BatchRetainReply.newBuilder().setReqId(request.getReqId());
        request.getRetainMessagePackMap().forEach((tenantId, retainMsgPack) -> {
            int maxRetainTopics = settingProvider.provide(RetainedTopicLimit, tenantId);
            RetainResultPack.Builder resultBuilder = RetainResultPack.newBuilder();
            retainMsgPack.getTopicMessagesMap().forEach((topic, retainMsg) -> {
                resultBuilder.putResults(topic, retain(tenantId, topic, retainMsg.getMessage(),
                    retainMsg.getPublisher(), maxRetainTopics, reader, writer));
            });
            replyBuilder.putResults(tenantId, resultBuilder.build());
        });
        return replyBuilder.build();
    }

    private RetainResult retain(String tenantId,
                                String topic,
                                Message message,
                                ClientInfo publisher,
                                int maxRetainTopics,
                                IKVReader reader,
                                IKVWriter writer) {
        try {
            ByteString tenantNS = KeyUtil.tenantNS(tenantId);
            Optional<ByteString> metaBytes = reader.get(tenantNS);
            TopicMessage topicMessage = TopicMessage.newBuilder()
                .setTopic(topic)
                .setMessage(message)
                .setPublisher(publisher)
                .build();
            ByteString retainKey = KeyUtil.retainKey(tenantNS, topicMessage.getTopic());
            long now = clock.millis();
            if (topicMessage.getMessage().getExpireTimestamp() <= now) {
                // already expired
                return RetainResult.ERROR;
            }
            if (metaBytes.isEmpty()) {
                if (maxRetainTopics > 0 && !topicMessage.getMessage().getPayload().isEmpty()) {
                    // this is the first message to be retained
                    RetainSetMetadata metadata = RetainSetMetadata.newBuilder()
                        .setCount(1)
                        .setEstExpire(topicMessage.getMessage().getExpireTimestamp())
                        .build();
                    writer.put(tenantNS, metadata.toByteString());
                    writer.put(retainKey, topicMessage.toByteString());
                    return RetainResult.RETAINED;
                } else {
                    if (topicMessage.getMessage().getPayload().isEmpty()) {
                        return RetainResult.CLEARED;
                    } else {
                        return RetainResult.ERROR;
                    }
                }
            } else {
                RetainSetMetadata metadata = RetainSetMetadata.parseFrom(metaBytes.get());
                Optional<ByteString> val = reader.get(retainKey);
                if (topicMessage.getMessage().getPayload().isEmpty()) {
                    // delete existing retained
                    if (val.isPresent()) {
                        TopicMessage existing = TopicMessage.parseFrom(val.get());
                        if (existing.getMessage().getExpireTimestamp() <= now) {
                            // the existing has already expired
                            metadata = gc(now, tenantNS, metadata, reader, writer);
                            if (metadata.getCount() > 0) {
                                writer.put(tenantNS, metadata.toByteString());
                            }
                        } else {
                            writer.delete(retainKey);
                            metadata = metadata.toBuilder().setCount(metadata.getCount() - 1).build();
                            if (metadata.getCount() > 0) {
                                writer.put(tenantNS, metadata.toByteString());
                            } else {
                                // last retained message has been removed, no metadata needed
                                writer.delete(tenantNS);
                            }
                        }

                    }
                    return RetainResult.CLEARED;
                }
                if (val.isEmpty()) {
                    // retain new message
                    if (metadata.getCount() >= maxRetainTopics) {
                        if (metadata.getEstExpire() <= now) {
                            // try to make some room via gc
                            metadata = gc(now, tenantNS, metadata, reader, writer);
                            if (metadata.getCount() < maxRetainTopics) {
                                metadata = metadata.toBuilder()
                                    .setEstExpire(Math.min(topicMessage.getMessage().getExpireTimestamp(),
                                        metadata.getEstExpire()))
                                    .setCount(metadata.getCount() + 1).build();
                                writer.put(retainKey, topicMessage.toByteString());
                                writer.put(tenantNS, metadata.toByteString());
                                return RetainResult.RETAINED;
                            } else {
                                // still no enough room
                                writer.put(tenantNS, metadata.toByteString());
                                return RetainResult.ERROR;
                            }
                        } else {
                            // no enough room
                            // TODO: report event: exceed limit
                            return RetainResult.ERROR;
                        }
                    } else {
                        metadata = metadata.toBuilder()
                            .setEstExpire(Math.min(topicMessage.getMessage().getExpireTimestamp(),
                                metadata.getEstExpire()))
                            .setCount(metadata.getCount() + 1).build();
                        writer.put(retainKey, topicMessage.toByteString());
                        writer.put(tenantNS, metadata.toByteString());
                        return RetainResult.RETAINED;
                    }
                } else {
                    // replace existing
                    TopicMessage existing = TopicMessage.parseFrom(val.get());
                    if (existing.getMessage().getExpireTimestamp() <= now &&
                        metadata.getCount() >= maxRetainTopics) {
                        metadata = gc(now, tenantNS, metadata, reader, writer);
                        if (metadata.getCount() < maxRetainTopics) {
                            metadata = metadata.toBuilder()
                                .setEstExpire(Math.min(topicMessage.getMessage().getExpireTimestamp(),
                                    metadata.getEstExpire()))
                                .setCount(metadata.getCount() + 1)
                                .build();
                            writer.put(retainKey, topicMessage.toByteString());
                            writer.put(tenantNS, metadata.toByteString());
                            return RetainResult.RETAINED;
                        } else {
                            if (metadata.getCount() > 0) {
                                writer.put(tenantNS, metadata.toByteString());
                            }
                            // no enough room
                            // TODO: report event: exceed limit
                            return RetainResult.ERROR;
                        }
                    }
                    if (metadata.getCount() <= maxRetainTopics) {
                        metadata = metadata.toBuilder()
                            .setEstExpire(Math.min(topicMessage.getMessage().getExpireTimestamp(),
                                metadata.getEstExpire()))
                            .build();
                        writer.put(retainKey, topicMessage.toByteString());
                        writer.put(tenantNS, metadata.toByteString());
                        return RetainResult.RETAINED;
                    } else {
                        // no enough room
                        // TODO: report event: exceed limit
                        return RetainResult.ERROR;
                    }
                }
            }
        } catch (Throwable e) {
            log.error("Retain failed", e);
            return RetainResult.ERROR;
        }
    }

    @SneakyThrows
    private RetainSetMetadata gc(long now, ByteString tenantNS, RetainSetMetadata metadata,
                                 IKVReader reader,
                                 IKVWriter writer) {
        Range range = Range.newBuilder().setStartKey(tenantNS).setEndKey(upperBound(tenantNS)).build();
        try (IKVIterator itr = reader.iterator()) {
            itr.seek(range.getStartKey());
            itr.next();
            int expires = 0;
            long earliestExp = Long.MAX_VALUE;
            for (; itr.isValid() && compare(itr.key(), range.getEndKey()) < 0; itr.next()) {
                long expireTime = TopicMessage.parseFrom(itr.value()).getMessage().getExpireTimestamp();
                if (expireTime <= now) {
                    writer.delete(itr.key());
                    expires++;
                } else {
                    earliestExp = Math.min(expireTime, earliestExp);
                }
            }
            metadata = metadata.toBuilder()
                .setCount(metadata.getCount() - expires)
                .setEstExpire(earliestExp == Long.MAX_VALUE ? now : earliestExp)
                .build();
            if (metadata.getCount() == 0) {
                writer.delete(tenantNS);
            }
        }
        return metadata;
    }

    private GCReply gc(GCRequest request, IKVReader reader, IKVWriter writer) {
        long now = clock.millis();
        try (IKVIterator itr = reader.iterator()) {
            itr.seekToFirst();
            while (itr.isValid()) {
                ByteString tenantNS = KeyUtil.parseTenantNS(itr.key());
                if (KeyUtil.isTenantNS(itr.key())) {
                    RetainSetMetadata metadata = RetainSetMetadata.parseFrom(itr.value());
                    RetainSetMetadata updated = gc(now, tenantNS, metadata, reader, writer);
                    if (!updated.equals(metadata) && updated.getCount() > 0) {
                        writer.put(tenantNS, updated.toByteString());
                    }
                }
                itr.seek(upperBound(tenantNS));
            }
            return GCReply.newBuilder().setReqId(request.getReqId()).build();
        } catch (Throwable e) {
            log.error("Unable to parse metadata");
            return GCReply.newBuilder().setReqId(request.getReqId()).build();
        }
    }

    private CollectMetricsReply collectMetrics(CollectMetricsRequest request, IKVReader reader) {
        CollectMetricsReply.Builder builder = CollectMetricsReply.newBuilder().setReqId(request.getReqId());
        try (IKVIterator itr = reader.iterator()) {
            for (itr.seekToFirst(); itr.isValid(); ) {
                ByteString startKey = itr.key();
                ByteString endKey = upperBound(itr.key());
                builder.putUsedSpaces(parseTenantId(startKey),
                    reader.size(intersect(reader.range(), Range.newBuilder()
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
}
