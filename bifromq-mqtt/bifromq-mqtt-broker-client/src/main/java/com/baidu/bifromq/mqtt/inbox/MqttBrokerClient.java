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

package com.baidu.bifromq.mqtt.inbox;

import static java.util.Collections.emptyMap;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.DeliveryPack;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.HasInboxRequest;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.OnlineInboxBrokerGrpc;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteReply;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteRequest;
import com.baidu.bifromq.mqtt.inbox.util.InboxGroupKeyUtil;
import com.baidu.bifromq.plugin.inboxbroker.HasResult;
import com.baidu.bifromq.plugin.inboxbroker.IInboxWriter;
import com.baidu.bifromq.plugin.inboxbroker.InboxPack;
import com.baidu.bifromq.plugin.inboxbroker.WriteResult;
import com.baidu.bifromq.type.SubInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class MqttBrokerClient implements IMqttBrokerClient {
    private final AtomicBoolean hasStopped = new AtomicBoolean();
    private final IRPCClient rpcClient;

    MqttBrokerClient(@NonNull IRPCClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    public IInboxWriter openInboxWriter(String inboxGroupKey) {
        Preconditions.checkState(!hasStopped.get());
        return new InboxPipeline(inboxGroupKey);
    }

    @Override
    public CompletableFuture<HasResult> hasInbox(long reqId, String trafficId, String inboxId, String inboxGroupKey) {
        Preconditions.checkState(!hasStopped.get());
        return rpcClient.invoke(trafficId, InboxGroupKeyUtil.parseServerId(inboxGroupKey),
                HasInboxRequest.newBuilder().setReqId(reqId).setInboxId(inboxId).build(),
                OnlineInboxBrokerGrpc.getHasInboxMethod())
            .thenApply(hasInboxReply -> {
                switch (hasInboxReply.getResult()) {
                    case YES:
                        return HasResult.YES;
                    case NO:
                        return HasResult.NO;
                    case ERROR:
                    default:
                        return HasResult.error(new RuntimeException("Inbox service internal error"));
                }
            })
            .exceptionally(HasResult::error);
    }


    @Override
    public void close() {
        if (hasStopped.compareAndSet(false, true)) {
            log.info("Closing MQTT broker client");
            log.debug("Stopping rpc client");
            rpcClient.stop();
            log.info("MQTT broker client closed");
        }
    }

    private class InboxPipeline implements IInboxWriter {
        private final IRPCClient.IRequestPipeline<WriteRequest, WriteReply> ppln;

        InboxPipeline(String inboxGroupKey) {
            ppln = rpcClient.createRequestPipeline("", InboxGroupKeyUtil.parseServerId(inboxGroupKey), "",
                emptyMap(), OnlineInboxBrokerGrpc.getWriteMethod());
        }

        @Override
        public CompletableFuture<Map<SubInfo, WriteResult>> write(Iterable<InboxPack> messagePacks) {
            Preconditions.checkState(!hasStopped.get());
            long reqId = System.nanoTime();
            return ppln.invoke(WriteRequest.newBuilder()
                    .setReqId(reqId)
                    .addAllDeliveryPack(Iterables.transform(messagePacks, e -> DeliveryPack.newBuilder()
                        .setMessagePack(e.messagePack)
                        .addAllSubscriber(e.inboxes)
                        .build()))
                    .build())
                .thenApply(writeReply -> writeReply.getResultList().stream()
                    .collect(Collectors.toMap(e -> e.getSubInfo(), e -> {
                        switch (e.getResult()) {
                            case OK:
                                return WriteResult.OK;
                            case NO_INBOX:
                                return WriteResult.NO_INBOX;
                            case ERROR:
                            default:
                                return WriteResult.error(new RuntimeException("inbox server internal error"));
                        }
                    })))
                .exceptionally(e -> {
                    WriteResult error = WriteResult.error(e);
                    Map<SubInfo, WriteResult> resultMap = new HashMap<>();
                    for (InboxPack inboxWrite : messagePacks) {
                        for (SubInfo subInfo : inboxWrite.inboxes) {
                            resultMap.put(subInfo, error);
                        }
                    }
                    return resultMap;
                });
        }

        @Override
        public void close() {
            ppln.close();
        }
    }
}
