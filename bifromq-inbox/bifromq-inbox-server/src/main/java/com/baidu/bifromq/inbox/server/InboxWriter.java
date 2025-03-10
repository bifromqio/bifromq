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

import com.baidu.bifromq.inbox.record.TenantInboxInstance;
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
class InboxWriter implements InboxWriterPipeline.ISendRequestHandler {
    private final IInboxInsertScheduler insertScheduler;

    InboxWriter(IInboxInsertScheduler insertScheduler) {
        this.insertScheduler = insertScheduler;
    }

    @Override
    public CompletableFuture<SendReply> handle(SendRequest request) {
        // scopedInbox -> matchInfo -> messagePack
        Map<TenantInboxInstance, Map<MatchInfo, List<TopicMessagePack>>> msgPackByInbox = new HashMap<>();
        // group messages by inboxId
        for (String tenantId : request.getRequest().getPackageMap().keySet()) {
            for (DeliveryPack pack : request.getRequest().getPackageMap().get(tenantId).getPackList()) {
                for (MatchInfo matchInfo : pack.getMatchInfoList()) {
                    TenantInboxInstance tenantInboxInstance = TenantInboxInstance.from(tenantId, matchInfo);
                    msgPackByInbox.computeIfAbsent(tenantInboxInstance, k -> new HashMap<>())
                        .computeIfAbsent(matchInfo, k -> new LinkedList<>()).add(pack.getMessagePack());
                }
            }
        }
        List<CompletableFuture<BatchInsertReply.Result>> replyFutures = msgPackByInbox.entrySet().stream().map(
            entry -> insertScheduler.schedule(InboxSubMessagePack.newBuilder().setTenantId(entry.getKey().tenantId())
                .setInboxId(entry.getKey().instance().inboxId()).setIncarnation(entry.getKey().instance().incarnation())
                .addAllMessagePack(entry.getValue().entrySet().stream().map(
                    e -> SubMessagePack.newBuilder().setTopicFilter(e.getKey().getTopicFilter())
                        .setIncarnation(e.getKey().getIncarnation()).addAllMessages(e.getValue()).build()).toList())
                .build()).exceptionally(e -> {
                log.debug("Failed to insert", e);
                return BatchInsertReply.Result.newBuilder().setCode(BatchInsertReply.Code.ERROR).build();
            })).toList();
        return CompletableFuture.allOf(replyFutures.toArray(new CompletableFuture[0]))
            .thenApply(v -> replyFutures.stream().map(CompletableFuture::join).collect(Collectors.toList()))
            .thenApply(results -> {
                SendReply.Builder replyBuilder = SendReply.newBuilder().setReqId(request.getReqId());
                Map<String, Map<MatchInfo, DeliveryResult.Code>> tenantMatchResultMap = new HashMap<>();
                int i = 0;
                for (TenantInboxInstance tenantInboxInstance : msgPackByInbox.keySet()) {
                    String receiverId = tenantInboxInstance.receiverId();
                    BatchInsertReply.Result result = results.get(i++);
                    Map<MatchInfo, DeliveryResult.Code> matchResultMap =
                        tenantMatchResultMap.computeIfAbsent(tenantInboxInstance.tenantId(), k -> new HashMap<>());
                    switch (result.getCode()) {
                        case OK -> {
                            assert result.getInsertionResultCount() == msgPackByInbox.get(tenantInboxInstance).size();
                            result.getInsertionResultList().forEach(insertionResult -> {
                                DeliveryResult.Code code =
                                    insertionResult.getRejected() ? DeliveryResult.Code.NO_SUB : DeliveryResult.Code.OK;
                                matchResultMap.putIfAbsent(MatchInfo.newBuilder().setReceiverId(receiverId)
                                    .setTopicFilter(insertionResult.getTopicFilter())
                                    .setIncarnation(insertionResult.getIncarnation()).build(), code);
                            });
                        }
                        case NO_INBOX -> {
                            for (MatchInfo matchInfo : msgPackByInbox.get(tenantInboxInstance).keySet()) {
                                matchResultMap.putIfAbsent(matchInfo, DeliveryResult.Code.NO_RECEIVER);
                            }
                        }
                        case ERROR -> {
                            for (MatchInfo matchInfo : msgPackByInbox.get(tenantInboxInstance).keySet()) {
                                matchResultMap.putIfAbsent(matchInfo, DeliveryResult.Code.ERROR);
                            }
                        }
                        default -> {
                            log.error("Unexpected result code: {}", result.getCode());
                            for (MatchInfo matchInfo : msgPackByInbox.get(tenantInboxInstance).keySet()) {
                                matchResultMap.putIfAbsent(matchInfo, DeliveryResult.Code.ERROR);
                            }
                        }
                    }
                }
                return replyBuilder.setReqId(request.getReqId())
                    .setReply(DeliveryReply.newBuilder().putAllResult(toResult(tenantMatchResultMap)).build()).build();
            });
    }
}
