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
import com.baidu.bifromq.mqtt.inbox.rpc.proto.HasInboxReply;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.HasInboxRequest;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.OnlineInboxBrokerGrpc;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteReply;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteRequest;
import com.baidu.bifromq.mqtt.inbox.util.DeliveryGroupKeyUtil;
import com.baidu.bifromq.plugin.subbroker.DeliveryPack;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.baidu.bifromq.type.SubInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import io.reactivex.rxjava3.core.Observable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class MqttBrokerClient implements IMqttBrokerClient {
    private final AtomicBoolean hasStopped = new AtomicBoolean();
    private final IRPCClient rpcClient;

    MqttBrokerClient(MqttBrokerClientBuilder builder) {
        this.rpcClient = IRPCClient.newBuilder()
            .bluePrint(RPCBluePrint.INSTANCE)
            .executor(builder.executor)
            .eventLoopGroup(builder.eventLoopGroup)
            .sslContext(builder.sslContext)
            .crdtService(builder.crdtService)
            .build();
    }

    public IDeliverer open(String delivererKey) {
        Preconditions.checkState(!hasStopped.get());
        return new DeliveryPipeline(delivererKey);
    }

    @Override
    public CompletableFuture<Boolean> hasInbox(long reqId, String tenantId, String inboxId, String delivererKey) {
        Preconditions.checkState(!hasStopped.get());
        return rpcClient.invoke(tenantId, DeliveryGroupKeyUtil.parseServerId(delivererKey),
                HasInboxRequest.newBuilder().setReqId(reqId).setInboxId(inboxId).build(),
                OnlineInboxBrokerGrpc.getHasInboxMethod())
            .thenApply(HasInboxReply::getResult);
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

    @Override
    public Observable<IRPCClient.ConnState> connState() {
        return rpcClient.connState();
    }

    private class DeliveryPipeline implements IDeliverer {
        private final IRPCClient.IRequestPipeline<WriteRequest, WriteReply> ppln;

        DeliveryPipeline(String deliveryGroupKey) {
            ppln = rpcClient.createRequestPipeline("", DeliveryGroupKeyUtil.parseServerId(deliveryGroupKey), "",
                emptyMap(), OnlineInboxBrokerGrpc.getWriteMethod());
        }

        @Override
        public CompletableFuture<Map<SubInfo, DeliveryResult>> deliver(Iterable<DeliveryPack> packs) {
            Preconditions.checkState(!hasStopped.get());
            long reqId = System.nanoTime();
            return ppln.invoke(WriteRequest.newBuilder()
                    .setReqId(reqId)
                    .addAllDeliveryPack(
                        Iterables.transform(packs, e -> com.baidu.bifromq.mqtt.inbox.rpc.proto.DeliveryPack.newBuilder()
                            .setMessagePack(e.messagePack)
                            .addAllSubscriber(e.inboxes)
                            .build()))
                    .build())
                .thenApply(writeReply -> writeReply.getResultList().stream()
                    .collect(Collectors.toMap(e -> e.getSubInfo(), e -> {
                        switch (e.getResult()) {
                            case NO_INBOX:
                                return DeliveryResult.NO_INBOX;
                            case OK:
                            default:
                                return DeliveryResult.OK;
                        }
                    })));
        }

        @Override
        public void close() {
            ppln.close();
        }
    }
}
