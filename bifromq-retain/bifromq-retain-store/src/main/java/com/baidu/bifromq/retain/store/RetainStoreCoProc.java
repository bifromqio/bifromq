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

import static com.baidu.bifromq.basekv.utils.KeyRangeUtil.compare;
import static com.baidu.bifromq.retain.utils.TopicUtil.isWildcardTopicFilter;

import com.baidu.bifromq.basekv.localengine.RangeUtil;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.Range;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProc;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.api.IKVWriter;
import com.baidu.bifromq.retain.rpc.proto.GCReply;
import com.baidu.bifromq.retain.rpc.proto.GCRequest;
import com.baidu.bifromq.retain.rpc.proto.MatchCoProcReply;
import com.baidu.bifromq.retain.rpc.proto.MatchCoProcRequest;
import com.baidu.bifromq.retain.rpc.proto.RetainCoProcReply;
import com.baidu.bifromq.retain.rpc.proto.RetainCoProcRequest;
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
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class RetainStoreCoProc implements IKVRangeCoProc {
    private final Supplier<IKVRangeReader> rangeReaderProvider;
    private final Clock clock;

    RetainStoreCoProc(KVRangeId id, Supplier<IKVRangeReader> rangeReaderProvider, Clock clock) {
        this.rangeReaderProvider = rangeReaderProvider;
        this.clock = clock;
    }

    @Override
    public CompletableFuture<ByteString> query(ByteString input, IKVReader reader) {
        try {
            RetainServiceROCoProcInput coProcInput = RetainServiceROCoProcInput.parseFrom(input);
            switch (coProcInput.getTypeCase()) {
                case MATCHREQUEST:
                    return match(coProcInput.getMatchRequest(), reader)
                        .thenApply(v -> RetainServiceROCoProcOutput.newBuilder()
                            .setMatchReply(v).build().toByteString());
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
        final ByteString output;
        switch (coProcInput.getTypeCase()) {
            case RETAINREQUEST:
                output = RetainServiceRWCoProcOutput.newBuilder()
                    .setRetainReply(retain(coProcInput.getRetainRequest(), reader, writer)).build()
                    .toByteString();
                break;
            case GCREQUEST:
                output = RetainServiceRWCoProcOutput.newBuilder()
                    .setGcReply(gc(coProcInput.getGcRequest(), reader, writer)).build().toByteString();
                break;
            default:
                log.error("Unknown co proc type {}", coProcInput.getTypeCase());
                throw new IllegalStateException("Unknown co proc type " + coProcInput.getTypeCase());
        }
        return () -> output;
    }

    @Override
    public void close() {

    }

    @SneakyThrows
    private CompletableFuture<MatchCoProcReply> match(MatchCoProcRequest request, IKVReader reader) {
        log.trace("Handling match request:\n{}", request);
        if (request.getLimit() == 0) {
            // TODO: report event: nothing to match
            return CompletableFuture.completedFuture(MatchCoProcReply.newBuilder()
                .setReqId(request.getReqId())
                .setResult(MatchCoProcReply.Result.OK)
                .build());
        }
        ByteString tenantNS = request.getTenantNS();
        Range range = Range.newBuilder()
            .setStartKey(tenantNS)
            .setEndKey(RangeUtil.upperBound(tenantNS))
            .build();
        IKVIterator itr = reader.iterator();
        itr.seek(range.getStartKey());
        if (!itr.isValid()) {
            return CompletableFuture.completedFuture(MatchCoProcReply.newBuilder()
                .setReqId(request.getReqId())
                .setResult(MatchCoProcReply.Result.OK)
                .build());
        }
        if (!isWildcardTopicFilter(request.getTopicFilter())) {
            Optional<ByteString> val = reader.get(KeyUtil.retainKey(tenantNS, request.getTopicFilter()));
            if (val.isPresent()) {
                TopicMessage message = TopicMessage.parseFrom(val.get());
                if (message.getMessage().getExpireTimestamp() > clock.millis()) {
                    return CompletableFuture.completedFuture(MatchCoProcReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(MatchCoProcReply.Result.OK)
                        .addMessages(message)
                        .build());
                }
            }
            return CompletableFuture.completedFuture(MatchCoProcReply.newBuilder()
                .setReqId(request.getReqId())
                .setResult(MatchCoProcReply.Result.OK)
                .build());
        }
        // deal with wildcard topic filter
        List<String> matchLevels = TopicUtil.parse(request.getTopicFilter(), false);
        MatchCoProcReply.Builder replyBuilder = MatchCoProcReply.newBuilder()
            .setReqId(request.getReqId()).setResult(MatchCoProcReply.Result.OK);
        itr.seek(KeyUtil.retainKeyPrefix(tenantNS, matchLevels));
        while (itr.isValid() &&
            compare(itr.key(), range.getEndKey()) < 0 &&
            replyBuilder.getMessagesCount() < request.getLimit()) {
            List<String> topicLevels = KeyUtil.parseTopic(itr.key());
            if (TopicUtil.match(topicLevels, matchLevels)) {
                TopicMessage message = TopicMessage.parseFrom(itr.value());
                if (message.getMessage().getExpireTimestamp() > clock.millis()) {
                    replyBuilder.addMessages(message);
                }
            }
            itr.next();
        }
        return CompletableFuture.completedFuture(replyBuilder.build());
    }

    private RetainCoProcReply retain(RetainCoProcRequest request, IKVReader reader, IKVWriter writer) {
        try {
            ByteString tenantNS = KeyUtil.tenantNS(request.getTenantId());
            Optional<ByteString> metaBytes = reader.get(tenantNS);
            TopicMessage topicMessage = TopicMessage.newBuilder()
                .setTopic(request.getTopic())
                .setMessage(Message.newBuilder()
                    .setMessageId(request.getReqId())
                    .setPubQoS(request.getQos())
                    .setTimestamp(request.getTimestamp())
                    .setExpireTimestamp(request.getExpireTimestamp())
                    .setPayload(request.getMessage())
                    .build())
                .setSender(request.getSender())
                .build();
            ByteString retainKey = KeyUtil.retainKey(tenantNS, topicMessage.getTopic());
            long now = clock.millis();
            if (topicMessage.getMessage().getExpireTimestamp() <= now) {
                // already expired
                return RetainCoProcReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(RetainCoProcReply.Result.ERROR)
                    .build();
            }
            if (!metaBytes.isPresent()) {
                if (request.getMaxRetainedTopics() > 0 && !topicMessage.getMessage().getPayload().isEmpty()) {
                    // this is the first message to be retained
                    RetainSetMetadata metadata = RetainSetMetadata.newBuilder()
                        .setCount(1)
                        .setEstExpire(topicMessage.getMessage().getExpireTimestamp())
                        .build();
                    writer.put(tenantNS, metadata.toByteString());
                    writer.put(retainKey, topicMessage.toByteString());
                    return RetainCoProcReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(RetainCoProcReply.Result.RETAINED)
                        .build();
                } else {
                    if (topicMessage.getMessage().getPayload().isEmpty()) {
                        return RetainCoProcReply.newBuilder()
                            .setReqId(request.getReqId())
                            .setResult(RetainCoProcReply.Result.CLEARED)
                            .build();
                    } else {
                        return RetainCoProcReply.newBuilder()
                            .setReqId(request.getReqId())
                            .setResult(RetainCoProcReply.Result.ERROR)
                            .build();
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
                    return RetainCoProcReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(RetainCoProcReply.Result.CLEARED)
                        .build();
                }
                if (!val.isPresent()) {
                    // retain new message
                    if (metadata.getCount() >= request.getMaxRetainedTopics()) {
                        if (metadata.getEstExpire() <= now) {
                            // try to make some room via gc
                            metadata = gc(now, tenantNS, metadata, reader, writer);
                            if (metadata.getCount() < request.getMaxRetainedTopics()) {
                                metadata = metadata.toBuilder()
                                    .setEstExpire(Math.min(topicMessage.getMessage().getExpireTimestamp(),
                                        metadata.getEstExpire()))
                                    .setCount(metadata.getCount() + 1).build();
                                writer.put(retainKey, topicMessage.toByteString());
                                writer.put(tenantNS, metadata.toByteString());
                                return RetainCoProcReply.newBuilder()
                                    .setReqId(request.getReqId())
                                    .setResult(RetainCoProcReply.Result.RETAINED)
                                    .build();
                            } else {
                                // still no enough room
                                writer.put(tenantNS, metadata.toByteString());
                                return RetainCoProcReply.newBuilder()
                                    .setReqId(request.getReqId())
                                    .setResult(RetainCoProcReply.Result.ERROR)
                                    .build();
                            }
                        } else {
                            // no enough room
                            // TODO: report event: exceed limit
                            return RetainCoProcReply.newBuilder()
                                .setReqId(request.getReqId())
                                .setResult(RetainCoProcReply.Result.ERROR)
                                .build();
                        }
                    } else {
                        metadata = metadata.toBuilder()
                            .setEstExpire(Math.min(topicMessage.getMessage().getExpireTimestamp(),
                                metadata.getEstExpire()))
                            .setCount(metadata.getCount() + 1).build();
                        writer.put(retainKey, topicMessage.toByteString());
                        writer.put(tenantNS, metadata.toByteString());
                        return RetainCoProcReply.newBuilder()
                            .setReqId(request.getReqId())
                            .setResult(RetainCoProcReply.Result.RETAINED)
                            .build();
                    }
                } else {
                    // replace existing
                    TopicMessage existing = TopicMessage.parseFrom(val.get());
                    if (existing.getMessage().getExpireTimestamp() <= now &&
                        metadata.getCount() >= request.getMaxRetainedTopics()) {
                        metadata = gc(now, tenantNS, metadata, reader, writer);
                        if (metadata.getCount() < request.getMaxRetainedTopics()) {
                            metadata = metadata.toBuilder()
                                .setEstExpire(Math.min(topicMessage.getMessage().getExpireTimestamp(),
                                    metadata.getEstExpire()))
                                .setCount(metadata.getCount() + 1)
                                .build();
                            writer.put(retainKey, topicMessage.toByteString());
                            writer.put(tenantNS, metadata.toByteString());
                            return RetainCoProcReply.newBuilder()
                                .setReqId(request.getReqId())
                                .setResult(RetainCoProcReply.Result.RETAINED)
                                .build();
                        } else {
                            if (metadata.getCount() > 0) {
                                writer.put(tenantNS, metadata.toByteString());
                            }
                            // no enough room
                            // TODO: report event: exceed limit
                            return RetainCoProcReply.newBuilder()
                                .setReqId(request.getReqId())
                                .setResult(RetainCoProcReply.Result.ERROR)
                                .build();
                        }
                    }
                    if (metadata.getCount() <= request.getMaxRetainedTopics()) {
                        metadata = metadata.toBuilder()
                            .setEstExpire(Math.min(topicMessage.getMessage().getExpireTimestamp(),
                                metadata.getEstExpire()))
                            .build();
                        writer.put(retainKey, topicMessage.toByteString());
                        writer.put(tenantNS, metadata.toByteString());
                        return RetainCoProcReply.newBuilder()
                            .setReqId(request.getReqId())
                            .setResult(RetainCoProcReply.Result.RETAINED)
                            .build();
                    } else {
                        // no enough room
                        // TODO: report event: exceed limit
                        return RetainCoProcReply.newBuilder()
                            .setReqId(request.getReqId())
                            .setResult(RetainCoProcReply.Result.ERROR)
                            .build();
                    }
                }
            }
        } catch (Throwable e) {
            return RetainCoProcReply.newBuilder()
                .setReqId(request.getReqId())
                .setResult(RetainCoProcReply.Result.ERROR)
                .build();
        }
    }

    @SneakyThrows
    private RetainSetMetadata gc(long now, ByteString tenantNS, RetainSetMetadata metadata,
                                 IKVReader reader,
                                 IKVWriter writer) {
        Range range = Range.newBuilder().setStartKey(tenantNS).setEndKey(RangeUtil.upperBound(tenantNS)).build();
        IKVIterator itr = reader.iterator();
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
        return metadata;
    }

    private GCReply gc(GCRequest request, IKVReader reader, IKVWriter writer) {
        long now = clock.millis();
        IKVIterator itr = reader.iterator();
        try {
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
                itr.seek(RangeUtil.upperBound(tenantNS));
            }
            return GCReply.newBuilder().setReqId(request.getReqId()).build();
        } catch (InvalidProtocolBufferException e) {
            log.error("Unable to parse metadata");
            return GCReply.newBuilder().setReqId(request.getReqId()).build();
        }
    }
}
