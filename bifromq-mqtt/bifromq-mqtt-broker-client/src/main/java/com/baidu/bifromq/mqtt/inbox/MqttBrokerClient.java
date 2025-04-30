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

import static com.baidu.bifromq.base.util.CompletableFutureUtil.unwrap;
import static com.baidu.bifromq.mqtt.inbox.rpc.proto.SubReply.Result.ERROR;
import static com.baidu.bifromq.mqtt.inbox.util.DelivererKeyUtil.parseServerId;
import static com.baidu.bifromq.mqtt.inbox.util.DelivererKeyUtil.parseTenantId;
import static java.util.Collections.emptyMap;

import com.baidu.bifromq.baserpc.client.IRPCClient;
import com.baidu.bifromq.baserpc.client.exception.ServerNotFoundException;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.OnlineInboxBrokerGrpc;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.SubReply;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.UnsubReply;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.UnsubRequest;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteReply;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteRequest;
import com.baidu.bifromq.plugin.subbroker.CheckReply;
import com.baidu.bifromq.plugin.subbroker.CheckRequest;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.baidu.bifromq.type.MatchInfo;
import com.baidu.bifromq.type.QoS;
import com.google.common.base.Preconditions;
import io.reactivex.rxjava3.core.Observable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class MqttBrokerClient implements IMqttBrokerClient {
    private final AtomicBoolean hasStopped = new AtomicBoolean();
    private final IRPCClient rpcClient;

    MqttBrokerClient(IRPCClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    @Override
    public CompletableFuture<CheckReply> check(CheckRequest request) {
        return rpcClient.invoke(request.getTenantId(), parseServerId(request.getDelivererKey()), request,
                OnlineInboxBrokerGrpc.getCheckSubscriptionsMethod())
            .exceptionally(unwrap(e -> {
                log.debug("Failed to check subscription", e);
                CheckReply.Builder replyBuilder = CheckReply.newBuilder();
                CheckReply.Code code =
                    e instanceof ServerNotFoundException ? CheckReply.Code.NO_RECEIVER : CheckReply.Code.ERROR;
                for (MatchInfo matchInfo : request.getMatchInfoList()) {
                    replyBuilder.addCode(code);
                }
                return replyBuilder.build();
            }));
    }

    public IDeliverer open(String delivererKey) {
        Preconditions.checkState(!hasStopped.get());
        IRPCClient.IRequestPipeline<WriteRequest, WriteReply> ppln =
            rpcClient.createRequestPipeline(parseTenantId(delivererKey), parseServerId(delivererKey), null, emptyMap(),
                OnlineInboxBrokerGrpc.getWriteMethod());
        return new DeliveryPipeline(ppln);
    }

    @Override
    public void close() {
        if (hasStopped.compareAndSet(false, true)) {
            log.debug("Closing MQTT broker client");
            log.debug("Stopping rpc client");
            rpcClient.stop();
            log.debug("MQTT broker client closed");
        }
    }

    @Override
    public Observable<IRPCClient.ConnState> connState() {
        return rpcClient.connState();
    }

    @Override
    public CompletableFuture<SubReply> sub(long reqId, String tenantId, String sessionId, String topicFilter, QoS qos,
                                           String brokerServerId) {
        return rpcClient.invoke(tenantId, brokerServerId,
                SubRequest.newBuilder().setReqId(reqId).setTenantId(tenantId).setSessionId(sessionId)
                    .setTopicFilter(topicFilter).setSubQoS(qos).build(), OnlineInboxBrokerGrpc.getSubMethod())
            .exceptionally(e -> {
                log.debug("Failed to sub", e);
                return SubReply.newBuilder().setReqId(reqId).setResult(ERROR).build();
            });
    }

    @Override
    public CompletableFuture<UnsubReply> unsub(long reqId, String tenantId, String sessionId, String topicFilter,
                                               String brokerServerId) {
        return rpcClient.invoke(tenantId, brokerServerId,
            UnsubRequest.newBuilder().setReqId(reqId).setTenantId(tenantId).setSessionId(sessionId)
                .setTopicFilter(topicFilter).build(), OnlineInboxBrokerGrpc.getUnsubMethod()).exceptionally(e -> {
            log.debug("Failed to unsub", e);
            return UnsubReply.newBuilder().setResult(UnsubReply.Result.ERROR).build();
        });
    }
}
