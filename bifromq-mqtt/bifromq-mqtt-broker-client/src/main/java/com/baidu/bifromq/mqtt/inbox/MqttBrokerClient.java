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

import static com.baidu.bifromq.mqtt.inbox.rpc.proto.SubReply.Result.ERROR;
import static com.baidu.bifromq.mqtt.inbox.rpc.proto.SubReply.Result.OK;
import static java.util.Collections.emptyMap;

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
import io.reactivex.rxjava3.disposables.Disposable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class MqttBrokerClient implements IMqttBrokerClient {
    private final AtomicBoolean hasStopped = new AtomicBoolean();
    private final IRPCClient rpcClient;
    private final Set<String> mqttBrokers;
    private final Disposable disposable;

    MqttBrokerClient(MqttBrokerClientBuilder builder) {
        this.rpcClient = IRPCClient.newBuilder()
            .bluePrint(RPCBluePrint.INSTANCE)
            .executor(builder.executor)
            .eventLoopGroup(builder.eventLoopGroup)
            .sslContext(builder.sslContext)
            .crdtService(builder.crdtService)
            .build();
        this.mqttBrokers = ConcurrentHashMap.newKeySet();
        disposable = this.rpcClient.serverList()
            .subscribe(servers -> {
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
    public CompletableFuture<SubReply> sub(long reqId, String tenantId, String inboxId,
                                           String topicFilter, QoS qos) {
        // TODO: lookup session dict to find the target broker instead of broadcasting
        List<CompletableFuture<SubReply>> futures = mqttBrokers.stream()
            .map(broker -> rpcClient.invoke(tenantId, broker, SubRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setTopicFilter(topicFilter)
                .setSubQoS(qos)
                .build(), OnlineInboxBrokerGrpc.getSubMethod())).toList();
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
            .thenApply(v -> {
                boolean allOfSub = futures.stream()
                    .map(CompletableFuture::join)
                    .map(SubReply::getResult)
                    .anyMatch(r -> r == OK);
                if (allOfSub) {
                    return SubReply.newBuilder().setReqId(reqId).setResult(OK).build();
                } else {
                    return SubReply.newBuilder().setReqId(reqId).setResult(ERROR).build();
                }
            });
    }

    @Override
    public CompletableFuture<UnsubReply> unsub(long reqId, String tenantId, String inboxId, String topicFilter) {
        // TODO: lookup session dict registry to find the target broker instead of broadcasting
        List<CompletableFuture<UnsubReply>> futures = mqttBrokers.stream()
            .map(broker -> rpcClient.invoke(tenantId, broker, UnsubRequest.newBuilder()
                    .setReqId(reqId)
                    .setTenantId(tenantId)
                    .setInboxId(inboxId)
                    .setTopicFilter(topicFilter)
                    .build(),
                OnlineInboxBrokerGrpc.getUnsubMethod())).toList();
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
            .thenApply(v -> {
                boolean success = futures.stream()
                    .map(CompletableFuture::join)
                    .map(UnsubReply::getResult)
                    .anyMatch(r -> r == UnsubReply.Result.OK || r == UnsubReply.Result.NO_SUB);
                return UnsubReply.newBuilder()
                    .setResult(success ? UnsubReply.Result.OK : UnsubReply.Result.ERROR)
                    .build();
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
            disposable.dispose();
            ppln.close();
        }
    }
}
