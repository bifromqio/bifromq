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

import static com.baidu.bifromq.retain.store.schema.KVSchemaUtil.parseTenantId;
import static com.baidu.bifromq.retain.store.schema.KVSchemaUtil.retainMessageKey;
import static com.baidu.bifromq.util.TopicConst.MULTI_WILDCARD;
import static java.util.Collections.emptyList;

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
import com.baidu.bifromq.retain.store.index.RetainTopicIndex;
import com.baidu.bifromq.retain.store.index.RetainedMsgInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.TopicMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class RetainStoreCoProc implements IKVRangeCoProc {
    private final Supplier<IKVCloseableReader> rangeReaderProvider;
    private final TenantsState tenantsState;
    private final String[] tags;
    private RetainTopicIndex index;

    RetainStoreCoProc(String clusterId,
                      String storeId,
                      KVRangeId id,
                      Supplier<IKVCloseableReader> rangeReaderProvider) {
        this.tags = new String[] {"clusterId", clusterId, "storeId", storeId, "rangeId", KVRangeIdUtil.toString(id)};
        this.rangeReaderProvider = rangeReaderProvider;
        this.tenantsState = new TenantsState(rangeReaderProvider.get(), tags);
        load();
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
                afterMutate.set(batchRetain(coProcInput.getBatchRetain(), replyBuilder, writer));
                outputBuilder.setBatchRetain(replyBuilder);
            }
            case GC -> {
                GCReply.Builder replyBuilder = GCReply.newBuilder();
                afterMutate.set(gc(coProcInput.getGc(), replyBuilder, writer));
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
        load();
    }

    @Override
    public void close() {
        index = null;
        tenantsState.destroy();
    }

    private CompletableFuture<BatchMatchReply> batchMatch(BatchMatchRequest request, IKVReader reader) {
        BatchMatchReply.Builder replyBuilder = BatchMatchReply.newBuilder().setReqId(request.getReqId());
        for (String tenantId : request.getMatchParamsMap().keySet()) {
            MatchResultPack.Builder resultPackBuilder = MatchResultPack.newBuilder();
            for (String topicFilter : request.getMatchParamsMap().get(tenantId).getTopicFiltersMap().keySet()) {
                MatchResult.Builder resultBuilder = MatchResult.newBuilder();
                try {
                    resultBuilder.setOk(Matched.newBuilder()
                        .addAllMessages(match(tenantId, topicFilter,
                            request.getMatchParamsMap().get(tenantId).getTopicFiltersMap().get(topicFilter),
                            request.getMatchParamsMap().get(tenantId).getNow(), reader)));
                } catch (Throwable e) {
                    resultBuilder.setError(MatchError.getDefaultInstance());
                }
                resultPackBuilder.putResults(topicFilter, resultBuilder.build());
            }
            replyBuilder.putResultPack(tenantId, resultPackBuilder.build());
        }
        return CompletableFuture.completedFuture(replyBuilder.build());
    }

    private List<TopicMessage> match(String tenantId,
                                     String topicFilter,
                                     int limit,
                                     long now,
                                     IKVReader reader) throws Exception {
        if (limit == 0) {
            return emptyList();
        }
        Set<RetainedMsgInfo> matchedMsgInfos = index.match(tenantId, topicFilter);
        List<TopicMessage> messages = new LinkedList<>();
        for (RetainedMsgInfo msgInfo : matchedMsgInfos) {
            if (messages.size() >= limit) {
                break;
            }
            Optional<ByteString> val = reader.get(retainMessageKey(msgInfo.tenantId, msgInfo.topic));
            if (val.isPresent()) {
                TopicMessage message = TopicMessage.parseFrom(val.get());
                if (expireAt(message.getMessage()) > now) {
                    messages.add(message);
                }
            }
        }
        return messages;
    }


    private Runnable batchRetain(BatchRetainRequest request,
                                 BatchRetainReply.Builder replyBuilder,
                                 IKVWriter writer) {
        replyBuilder.setReqId(request.getReqId());
        Map<String, Map<String, Message>> addTopics = new HashMap<>();
        Map<String, Map<String, Message>> updateTopics = new HashMap<>();
        Map<String, Set<String>> removeTopics = new HashMap<>();
        for (String tenantId : request.getParamsMap().keySet()) {
            Map<String, RetainResult.Code> results = new HashMap<>();
            for (Map.Entry<String, RetainMessage> entry :
                request.getParamsMap().get(tenantId).getTopicMessagesMap().entrySet()) {
                String topic = entry.getKey();
                RetainMessage retainMessage = entry.getValue();
                try {
                    TopicMessage topicMessage = TopicMessage.newBuilder()
                        .setTopic(topic)
                        .setMessage(retainMessage.getMessage())
                        .setPublisher(retainMessage.getPublisher())
                        .build();
                    ByteString retainKey = retainMessageKey(tenantId, topicMessage.getTopic());
                    Set<RetainedMsgInfo> retainedMsgInfos = index.match(tenantId, topic);
                    if (topicMessage.getMessage().getPayload().isEmpty()) {
                        // delete existing retained
                        if (!retainedMsgInfos.isEmpty()) {
                            writer.delete(retainKey);
                            removeTopics.computeIfAbsent(tenantId, k -> new HashSet<>()).add(topic);
                        }
                        results.put(topic, RetainResult.Code.CLEARED);
                        continue;
                    }
                    if (retainedMsgInfos.isEmpty()) {
                        // retain new message
                        writer.put(retainKey, topicMessage.toByteString());
                        addTopics.computeIfAbsent(tenantId, k -> new HashMap<>())
                            .put(topic, topicMessage.getMessage());
                    } else {
                        // replace existing
                        writer.put(retainKey, topicMessage.toByteString());
                        updateTopics.computeIfAbsent(tenantId, k -> new HashMap<>())
                            .put(topic, topicMessage.getMessage());
                    }
                    results.put(topic, RetainResult.Code.RETAINED);
                } catch (Throwable e) {
                    log.error("Retain failed", e);
                    results.put(topic, RetainResult.Code.ERROR);
                }
            }
            replyBuilder.putResults(tenantId, RetainResult.newBuilder()
                .putAllResults(results)
                .build());
        }
        return () -> {
            addTopics.forEach((tenantId, topics) -> {
                topics.forEach(
                    (topic, msg) -> index.add(tenantId, topic, msg.getTimestamp(), msg.getExpiryInterval()));
                tenantsState.increaseTopicCount(tenantId, topics.size());
            });
            updateTopics.forEach((tenantId, topics) -> {
                topics.forEach((topic, msg) -> index.remove(tenantId, topic));
                topics.forEach((topic, msg) -> index.add(tenantId, topic, msg.getTimestamp(), msg.getExpiryInterval()));
            });
            removeTopics.forEach((tenantId, topics) -> {
                topics.forEach(topic -> index.remove(tenantId, topic));
                tenantsState.increaseTopicCount(tenantId, -topics.size());
            });
        };
    }

    private Runnable gc(GCRequest request, GCReply.Builder replyBuilder, IKVWriter writer) {
        replyBuilder.setReqId(request.getReqId());
        long now = request.getNow();
        Map<String, Set<String>> removedTopics = new HashMap<>();
        Set<RetainedMsgInfo> retainedMsgInfos = request.hasTenantId()
            ? index.match(request.getTenantId(), MULTI_WILDCARD) : index.findAll();
        for (RetainedMsgInfo msgInfo : retainedMsgInfos) {
            long expireTime = expireAt(msgInfo.timestamp,
                (request.hasExpirySeconds() ? request.getExpirySeconds() : msgInfo.expirySeconds));
            if (expireTime <= now) {
                writer.delete(retainMessageKey(msgInfo.tenantId, msgInfo.topic));
                removedTopics.computeIfAbsent(msgInfo.tenantId, k -> new HashSet<>()).add(msgInfo.topic);
            }
        }
        return () -> {
            removedTopics.forEach((tenantId, topics) -> topics.forEach(topic -> index.remove(tenantId, topic)));
            removedTopics.forEach((tenantId, topics) -> tenantsState.increaseTopicCount(tenantId, -topics.size()));
        };
    }

    private void load() {
        index = new RetainTopicIndex();
        tenantsState.destroy();

        try (IKVCloseableReader reader = rangeReaderProvider.get()) {
            IKVIterator itr = reader.iterator();
            for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                try {
                    String tenantId = parseTenantId(itr.key());
                    TopicMessage topicMessage = TopicMessage.parseFrom(itr.value());
                    index.add(tenantId, topicMessage.getTopic(), topicMessage.getMessage().getTimestamp(),
                        topicMessage.getMessage().getExpiryInterval());
                    tenantsState.increaseTopicCount(tenantId, 1);
                } catch (InvalidProtocolBufferException e) {
                    log.error("Failed to parse retained message", e);
                }
            }
        }
    }

    private long expireAt(Message message) {
        return expireAt(message.getTimestamp(), message.getExpiryInterval());
    }

    private long expireAt(long timestamp, int expirySeconds) {
        return Duration.ofMillis(timestamp).plusSeconds(expirySeconds).toMillis();
    }
}
