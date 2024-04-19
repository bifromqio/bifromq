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

package com.baidu.bifromq.sessiondict.server;

import static com.baidu.bifromq.baserpc.UnaryResponse.response;
import static com.baidu.bifromq.metrics.ITenantMeter.gauging;
import static com.baidu.bifromq.metrics.ITenantMeter.stopGauging;
import static com.baidu.bifromq.metrics.TenantMetric.MqttConnectionGauge;
import static com.baidu.bifromq.metrics.TenantMetric.MqttLivePersistentSessionGauge;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CHANNEL_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_BROKER_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_SESSION_TYPE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_SESSION_TYPE_P_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_SESSION_TYPE_T_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;

import com.baidu.bifromq.mqtt.inbox.IMqttBrokerClient;
import com.baidu.bifromq.sessiondict.rpc.proto.GetReply;
import com.baidu.bifromq.sessiondict.rpc.proto.GetRequest;
import com.baidu.bifromq.sessiondict.rpc.proto.KillReply;
import com.baidu.bifromq.sessiondict.rpc.proto.KillRequest;
import com.baidu.bifromq.sessiondict.rpc.proto.Quit;
import com.baidu.bifromq.sessiondict.rpc.proto.Session;
import com.baidu.bifromq.sessiondict.rpc.proto.SessionDictServiceGrpc;
import com.baidu.bifromq.sessiondict.rpc.proto.SubReply;
import com.baidu.bifromq.sessiondict.rpc.proto.SubRequest;
import com.baidu.bifromq.sessiondict.rpc.proto.UnsubReply;
import com.baidu.bifromq.sessiondict.rpc.proto.UnsubRequest;
import com.baidu.bifromq.type.ClientInfo;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class SessionDictService extends SessionDictServiceGrpc.SessionDictServiceImplBase {
    private record SessionCollection(AtomicInteger persistentSessions,
                                     Map<ISessionRegister.ClientKey, ISessionRegister> clients) {
        static final SessionCollection EMPTY = new SessionCollection(new AtomicInteger(), Collections.emptyMap());
    }

    private final Map<String, SessionCollection> tenantSessions;
    private final IMqttBrokerClient mqttBrokerClient;

    SessionDictService(IMqttBrokerClient mqttBrokerClient) {
        this.mqttBrokerClient = mqttBrokerClient;
        tenantSessions = new ConcurrentHashMap<>();
    }

    @Override
    public StreamObserver<Session> dict(StreamObserver<Quit> responseObserver) {
        return new SessionRegister(((sessionOwner, reg, register) -> {
            String tenantId = sessionOwner.getTenantId();
            ISessionRegister.ClientKey clientKey =
                new ISessionRegister.ClientKey(sessionOwner.getMetadataOrDefault(MQTT_USER_ID_KEY, ""),
                    sessionOwner.getMetadataOrDefault(MQTT_CLIENT_ID_KEY, ""));
            if (reg) {
                ISessionRegister prevRegister = tenantSessions.computeIfAbsent(tenantId, k -> {
                    SessionCollection sessionCollection =
                        new SessionCollection(new AtomicInteger(), new ConcurrentHashMap<>());
                    gauging(tenantId, MqttConnectionGauge, sessionCollection.clients::size);
                    gauging(tenantId, MqttLivePersistentSessionGauge, sessionCollection.persistentSessions::get);
                    return sessionCollection;
                }).clients.put(clientKey, register);
                if (sessionOwner.getMetadataOrDefault(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_T_VALUE)
                    .equals(MQTT_CLIENT_SESSION_TYPE_P_VALUE)) {
                    tenantSessions.get(tenantId).persistentSessions.incrementAndGet();
                }
                if (prevRegister != null) {
                    // kick the session registered via previous register
                    prevRegister.kick(tenantId, clientKey, sessionOwner);
                }
            } else {
                tenantSessions.computeIfPresent(tenantId, (k, v) -> {
                    v.clients.remove(clientKey, register);
                    if (sessionOwner.getMetadataOrDefault(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_T_VALUE)
                        .equals(MQTT_CLIENT_SESSION_TYPE_P_VALUE)) {
                        tenantSessions.get(tenantId).persistentSessions.decrementAndGet();
                    }
                    if (v.clients.isEmpty()) {
                        v = null;
                        stopGauging(tenantId, MqttConnectionGauge);
                        stopGauging(tenantId, MqttLivePersistentSessionGauge);
                    }
                    return v;
                });
            }
        }), responseObserver);
    }

    @Override
    public void kill(KillRequest request, StreamObserver<KillReply> responseObserver) {
        response(ignore -> {
            String tenantId = request.getTenantId();
            ISessionRegister.ClientKey clientKey =
                new ISessionRegister.ClientKey(request.getUserId(), request.getClientId());
            AtomicReference<ISessionRegister> found = new AtomicReference<>();
            tenantSessions.computeIfPresent(tenantId, (k, v) -> {
                ISessionRegister sessionRegister = v.clients.remove(clientKey);
                if (sessionRegister != null) {
                    found.set(sessionRegister);
                    if (sessionRegister.owner(tenantId, clientKey)
                        .getMetadataOrDefault(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_T_VALUE)
                        .equals(MQTT_CLIENT_SESSION_TYPE_P_VALUE)) {
                        v.persistentSessions.decrementAndGet();
                    }
                    if (v.clients.isEmpty()) {
                        v = null;
                        stopGauging(tenantId, MqttConnectionGauge);
                    }
                }
                return v;
            });

            if (found.get() != null) {
                try {
                    found.get().kick(tenantId, clientKey, request.getKiller());
                } catch (Throwable e) {
                    log.debug("Failed to kick", e);
                    return CompletableFuture.completedFuture(KillReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(KillReply.Result.ERROR)
                        .build());
                }
            }
            return CompletableFuture.completedFuture(KillReply.newBuilder()
                .setReqId(request.getReqId())
                .setResult(KillReply.Result.OK)
                .build());
        }, responseObserver);
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetReply> responseObserver) {
        response(tenantId -> {
            ISessionRegister.ClientKey clientKey =
                new ISessionRegister.ClientKey(request.getUserId(), request.getClientId());
            ISessionRegister register = tenantSessions.getOrDefault(tenantId, SessionCollection.EMPTY)
                .clients.get(clientKey);
            if (register == null) {
                return CompletableFuture.completedFuture(GetReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(GetReply.Result.NOT_FOUND)
                    .build());
            }
            ClientInfo owner = register.owner(tenantId, clientKey);
            if (owner == null) {
                return CompletableFuture.completedFuture(GetReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(GetReply.Result.NOT_FOUND)
                    .build());
            }
            return CompletableFuture.completedFuture(GetReply.newBuilder()
                .setReqId(request.getReqId())
                .setResult(GetReply.Result.OK)
                .setOwner(owner)
                .build());
        }, responseObserver);
    }

    @Override
    public void sub(SubRequest request, StreamObserver<SubReply> responseObserver) {
        response(tenantId -> {
            ISessionRegister.ClientKey clientKey =
                new ISessionRegister.ClientKey(request.getUserId(), request.getClientId());
            ISessionRegister register = tenantSessions.getOrDefault(tenantId, SessionCollection.EMPTY)
                .clients.get(clientKey);
            if (register == null) {
                return CompletableFuture.completedFuture(SubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(SubReply.Result.NO_SESSION)
                    .build());
            }
            ClientInfo owner = register.owner(tenantId, clientKey);
            if (owner == null) {
                return CompletableFuture.completedFuture(SubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(SubReply.Result.NO_SESSION)
                    .build());
            } else {
                return mqttBrokerClient.sub(request.getReqId(),
                        request.getTenantId(),
                        owner.getMetadataOrDefault(MQTT_CHANNEL_ID_KEY, ""),
                        request.getTopicFilter(),
                        request.getQos(),
                        owner.getMetadataOrDefault(MQTT_CLIENT_BROKER_KEY, ""))
                    .thenApply(r -> SubReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(SubReply.Result.forNumber(r.getResult().getNumber()))
                        .build());
            }
        }, responseObserver);
    }

    @Override
    public void unsub(UnsubRequest request, StreamObserver<UnsubReply> responseObserver) {
        response(tenantId -> {
            ISessionRegister.ClientKey clientKey =
                new ISessionRegister.ClientKey(request.getUserId(), request.getClientId());
            ISessionRegister register = tenantSessions.getOrDefault(tenantId, SessionCollection.EMPTY)
                .clients.get(clientKey);
            if (register == null) {
                return CompletableFuture.completedFuture(UnsubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(UnsubReply.Result.NO_SESSION)
                    .build());
            }
            ClientInfo owner = register.owner(tenantId, clientKey);
            if (owner == null) {
                return CompletableFuture.completedFuture(UnsubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(UnsubReply.Result.NO_SESSION)
                    .build());
            } else {
                return mqttBrokerClient.unsub(request.getReqId(),
                        request.getTenantId(),
                        owner.getMetadataOrDefault(MQTT_CHANNEL_ID_KEY, ""),
                        request.getTopicFilter(),
                        owner.getMetadataOrDefault(MQTT_CLIENT_BROKER_KEY, ""))
                    .thenApply(r -> UnsubReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(UnsubReply.Result.forNumber(r.getResult().getNumber()))
                        .build());
            }
        }, responseObserver);
    }

    void close() {
        tenantSessions.forEach((tenantId, collection) -> collection.clients.values().forEach(ISessionRegister::stop));
    }
}
