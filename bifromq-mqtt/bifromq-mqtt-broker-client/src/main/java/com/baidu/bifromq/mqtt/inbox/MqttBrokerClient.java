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

package com.baidu.bifromq.mqtt.inbox;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.OnlineInboxBrokerGrpc;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.SubReply;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.UnsubReply;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.UnsubRequest;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WritePack;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteReply;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteRequest;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteResult;
import com.baidu.bifromq.mqtt.inbox.util.DeliveryGroupKeyUtil;
import com.baidu.bifromq.plugin.subbroker.DeliveryPack;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import io.reactivex.rxjava3.core.Observable;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

@Slf4j
final class MqttBrokerClient implements IMqttBrokerClient {
    private final AtomicBoolean hasStopped = new AtomicBoolean();
    private final IRPCClient rpcClient;
    private final Set<String> mqttBrokers;

    MqttBrokerClient(MqttBrokerClientBuilder builder) {
        this.rpcClient = IRPCClient.newBuilder()
            .bluePrint(RPCBluePrint.INSTANCE)
            .executor(builder.executor)
            .eventLoopGroup(builder.eventLoopGroup)
            .sslContext(builder.sslContext)
            .crdtService(builder.crdtService)
            .build();
        this.mqttBrokers = ConcurrentHashMap.newKeySet();
        this.rpcClient.serverList().subscribe(servers -> {
            mqttBrokers.clear();
            mqttBrokers.addAll(servers.keySet());
        });
    }

    public IDeliverer open(String delivererKey) {
        Preconditions.checkState(!hasStopped.get());
        return new DeliveryPipeline(delivererKey);
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

    @Override
    public CompletableFuture<MqttSubResult> sub(long reqId, String tenantId, String inboxId,
                                                String topicFilter, QoS qos) {
        List<CompletableFuture<SubReply>> futures = new ArrayList<>();
        Iterator<String> itr = mqttBrokers.iterator();
        while (itr.hasNext()) {
            futures.add(rpcClient.invoke(tenantId, itr.next(), SubRequest.newBuilder()
                            .setReqId(reqId)
                            .setTenantId(tenantId)
                            .setInboxId(inboxId)
                            .setTopicFilter(topicFilter)
                            .setSubQoS(qos)
                            .build(),
                    OnlineInboxBrokerGrpc.getSubMethod()));
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(v -> {
                    boolean allOfSub = futures.stream()
                            .map(CompletableFuture::join)
                            .map(SubReply::getResult)
                            .anyMatch(Boolean::booleanValue);
                    if (allOfSub) {
                        return MqttSubResult.OK;
                    }else {
                        return MqttSubResult.ERROR;
                    }
                });
    }

    @Override
    public CompletableFuture<MqttUnsubResult> unsub(long reqId, String tenantId, String inboxId, String topicFilter) {
        List<CompletableFuture<UnsubReply>> futures = new ArrayList<>();
        Iterator<String> itr = mqttBrokers.iterator();
        while (itr.hasNext()) {
            futures.add(rpcClient.invoke(tenantId, itr.next(), UnsubRequest.newBuilder()
                            .setReqId(reqId)
                            .setTenantId(tenantId)
                            .setInboxId(inboxId)
                            .setTopicFilter(topicFilter)
                            .build(),
                    OnlineInboxBrokerGrpc.getUnsubMethod()));
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(v -> {
                    Optional<UnsubReply.Result> allOfUnsub = futures.stream()
                            .map(CompletableFuture::join)
                            .map(UnsubReply::getResult)
                            .max(Enum::compareTo);
                    if (allOfUnsub.isPresent() && allOfUnsub.get() == UnsubReply.Result.OK) {
                        return MqttUnsubResult.OK;
                    }else {
                        return MqttUnsubResult.ERROR;
                    }
                });
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
                    .addAllPack(Iterables.transform(packs, e -> WritePack.newBuilder()
                        .setMessagePack(e.messagePack)
                        .addAllSubscriber(e.inboxes)
                        .build()))
                    .build())
                .thenApply(writeReply -> writeReply.getResultList().stream()
                    .collect(Collectors.toMap(WriteResult::getSubInfo, entry ->
                        switch (entry.getResult()) {
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
}
