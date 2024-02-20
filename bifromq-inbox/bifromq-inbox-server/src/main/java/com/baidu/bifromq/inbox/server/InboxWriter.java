/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.inbox.server;

import static com.baidu.bifromq.plugin.subbroker.TypeUtil.toResult;

import com.baidu.bifromq.inbox.records.ScopedInbox;
import com.baidu.bifromq.inbox.rpc.proto.SendReply;
import com.baidu.bifromq.inbox.rpc.proto.SendRequest;
import com.baidu.bifromq.inbox.server.scheduler.IInboxInsertScheduler;
import com.baidu.bifromq.inbox.storage.proto.BatchInsertReply;
import com.baidu.bifromq.inbox.storage.proto.InboxSubMessagePack;
import com.baidu.bifromq.inbox.storage.proto.SubMessagePack;
import com.baidu.bifromq.plugin.subbroker.DeliveryPack;
import com.baidu.bifromq.plugin.subbroker.DeliveryReply;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.type.MatchInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxWriter implements InboxWriterPipeline.ISendRequestHandler {
    private final IInboxInsertScheduler insertScheduler;

    public InboxWriter(IInboxInsertScheduler insertScheduler) {
        this.insertScheduler = insertScheduler;
    }

    @Override
    public CompletableFuture<SendReply> handle(SendRequest request) {
        // scopedInbox -> topicFilter -> messagePack
        Map<ScopedInbox, Map<String, List<TopicMessagePack>>> msgsByInbox = new HashMap<>();
        // group messages by inboxId
        for (String tenantId : request.getRequest().getPackageMap().keySet()) {
            for (DeliveryPack pack : request.getRequest().getPackageMap().get(tenantId).getPackList()) {
                for (MatchInfo matchInfo : pack.getMatchInfoList()) {
                    msgsByInbox.computeIfAbsent(ScopedInbox.from(tenantId, matchInfo), k -> new HashMap<>())
                        .computeIfAbsent(matchInfo.getTopicFilter(), k -> new LinkedList<>())
                        .add(pack.getMessagePack());
                }
            }
        }
        List<CompletableFuture<BatchInsertReply.Result>> replyFutures = msgsByInbox.entrySet()
            .stream()
            .map(entry -> insertScheduler.schedule(InboxSubMessagePack.newBuilder()
                    .setTenantId(entry.getKey().tenantId())
                    .setInboxId(entry.getKey().inboxId())
                    .setIncarnation(entry.getKey().incarnation())
                    .addAllMessagePack(entry.getValue().entrySet().stream()
                        .map(e -> SubMessagePack.newBuilder()
                            .setTopicFilter(e.getKey())
                            .addAllMessages(e.getValue())
                            .build()).toList())
                    .build())
                .exceptionally(
                    e -> {
                        log.debug("Failed to insert", e);
                        return BatchInsertReply.Result.newBuilder()
                            .setCode(BatchInsertReply.Code.ERROR)
                            .build();
                    }))
            .toList();
        return CompletableFuture.allOf(replyFutures.toArray(new CompletableFuture[0]))
            .thenApply(v -> replyFutures.stream().map(CompletableFuture::join).collect(Collectors.toList()))
            .thenApply(results -> {
                assert results.size() == msgsByInbox.size();
                int i = 0;
                SendReply.Builder replyBuilder = SendReply.newBuilder().setReqId(request.getReqId());
                Map<String, Map<MatchInfo, DeliveryResult.Code>> tenantMatchResultMap = new HashMap<>();
                for (ScopedInbox scopedInbox : msgsByInbox.keySet()) {
                    BatchInsertReply.Result result = results.get(i++);
                    Map<MatchInfo, DeliveryResult.Code> matchResultMap =
                        tenantMatchResultMap.computeIfAbsent(scopedInbox.tenantId(), k -> new HashMap<>());
                    switch (result.getCode()) {
                        case OK -> {
                            for (BatchInsertReply.InsertionResult insertionResult : result.getInsertionResultList()) {
                                matchResultMap.putIfAbsent(scopedInbox.convertTo(insertionResult.getTopicFilter()),
                                    insertionResult.getRejected() ? DeliveryResult.Code.NO_SUB :
                                        DeliveryResult.Code.OK);
                            }
                        }
                        case NO_INBOX -> {
                            for (String topicFilter : msgsByInbox.get(scopedInbox)
                                .keySet()) {
                                matchResultMap.putIfAbsent(scopedInbox.convertTo(topicFilter),
                                    DeliveryResult.Code.NO_RECEIVER);
                            }
                        }
                        case ERROR -> {
                            for (String topicFilter : msgsByInbox.get(scopedInbox)
                                .keySet()) {
                                matchResultMap.putIfAbsent(scopedInbox.convertTo(topicFilter),
                                    DeliveryResult.Code.ERROR);
                            }
                        }
                    }
                }
                return replyBuilder
                    .setReqId(request.getReqId())
                    .setReply(DeliveryReply.newBuilder()
                        .putAllResult(toResult(tenantMatchResultMap))
                        .build())
                    .build();
            });
    }
}
