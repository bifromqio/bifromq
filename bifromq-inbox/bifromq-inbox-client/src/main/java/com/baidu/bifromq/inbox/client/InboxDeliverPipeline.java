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

package com.baidu.bifromq.inbox.client;

import static java.util.Collections.emptyMap;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.inbox.rpc.proto.InboxMessagePack;
import com.baidu.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import com.baidu.bifromq.inbox.rpc.proto.SendReply;
import com.baidu.bifromq.inbox.rpc.proto.SendRequest;
import com.baidu.bifromq.inbox.rpc.proto.SendResult;
import com.baidu.bifromq.plugin.subbroker.DeliveryPack;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.baidu.bifromq.type.SubInfo;
import com.google.common.collect.Iterables;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

class InboxDeliverPipeline implements IDeliverer {
    private final IRPCClient.IRequestPipeline<SendRequest, SendReply> ppln;

    InboxDeliverPipeline(String delivererKey, IRPCClient rpcClient) {
        ppln = rpcClient.createRequestPipeline("", null, delivererKey,
            emptyMap(), InboxServiceGrpc.getReceiveMethod());
    }

    @Override
    public CompletableFuture<Map<SubInfo, DeliveryResult>> deliver(Iterable<DeliveryPack> packs) {
        long reqId = System.nanoTime();
        return ppln.invoke(SendRequest.newBuilder()
                .setReqId(reqId)
                .addAllInboxMsgPack(Iterables.transform(packs,
                    e -> InboxMessagePack.newBuilder()
                        .setMessages(e.messagePack)
                        .addAllSubInfo(e.inboxes)
                        .build()))
                .build())
            .thenApply(sendReply -> sendReply.getResultList().stream()
                .collect(Collectors.toMap(SendResult::getSubInfo, e ->
                    switch (e.getResult()) {
                        case OK -> DeliveryResult.OK;
                        case NO_INBOX -> DeliveryResult.NO_INBOX;
                        default -> DeliveryResult.FAILED;
                    })));
    }

    @Override
    public void close() {
        ppln.close();
    }
}
