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
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CHANNEL_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_BROKER_KEY;

import com.baidu.bifromq.mqtt.inbox.IMqttBrokerClient;
import com.baidu.bifromq.sessiondict.rpc.proto.GetReply;
import com.baidu.bifromq.sessiondict.rpc.proto.GetRequest;
import com.baidu.bifromq.sessiondict.rpc.proto.KillAllReply;
import com.baidu.bifromq.sessiondict.rpc.proto.KillAllRequest;
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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class SessionDictService extends SessionDictServiceGrpc.SessionDictServiceImplBase {
    private final SessionStore sessionStore;
    private final IMqttBrokerClient mqttBrokerClient;

    SessionDictService(IMqttBrokerClient mqttBrokerClient) {
        this.mqttBrokerClient = mqttBrokerClient;
        this.sessionStore = new SessionStore();
    }

    @Override
    public StreamObserver<Session> dict(StreamObserver<Quit> responseObserver) {
        return new SessionRegister(((sessionOwner, reg, register) -> {
            if (reg) {
                sessionStore.add(sessionOwner, register);
            } else {
                sessionStore.remove(sessionOwner, register);
            }
        }), responseObserver);
    }

    @Override
    public void kill(KillRequest request, StreamObserver<KillReply> responseObserver) {
        response(ignore -> {
            String tenantId = request.getTenantId();
            ISessionRegister.ClientKey clientKey =
                new ISessionRegister.ClientKey(request.getUserId(), request.getClientId());
            try {
                kick(tenantId, clientKey, request.getKiller());
                return CompletableFuture.completedFuture(KillReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(KillReply.Result.OK)
                    .build());
            } catch (Throwable e) {
                log.debug("Failed to kick", e);
                return CompletableFuture.completedFuture(KillReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(KillReply.Result.ERROR)
                    .build());
            }
        }, responseObserver);
    }

    @Override
    public void killAll(KillAllRequest request, StreamObserver<KillAllReply> responseObserver) {
        response(ignore -> {
            String tenantId = request.getTenantId();
            Set<ISessionRegister.ClientKey> clientKeys;
            if (request.hasUserId()) {
                clientKeys = sessionStore.findClients(tenantId, request.getUserId());
            } else {
                clientKeys = sessionStore.findClients(tenantId);
            }
            try {
                // TODO: support disconnect a constant rate
                clientKeys.forEach(clientKey -> kick(tenantId, clientKey, request.getKiller()));
                return CompletableFuture.completedFuture(KillAllReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(KillAllReply.Result.OK)
                    .build());
            } catch (Throwable e) {
                log.debug("Failed to kick", e);
                return CompletableFuture.completedFuture(KillAllReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(KillAllReply.Result.ERROR)
                    .build());
            }
        }, responseObserver);
    }

    private void kick(String tenantId, ISessionRegister.ClientKey clientKey, ClientInfo killer) {
        ISessionRegister sessionRegister = sessionStore.get(tenantId, clientKey);
        if (sessionRegister != null) {
            sessionRegister.kick(tenantId, clientKey, killer);
        }
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetReply> responseObserver) {
        response(tenantId -> {
            ISessionRegister.ClientKey clientKey =
                new ISessionRegister.ClientKey(request.getUserId(), request.getClientId());
            ISessionRegister register = sessionStore.get(tenantId, clientKey);
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
            ISessionRegister register = sessionStore.get(tenantId, clientKey);
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
            ISessionRegister register = sessionStore.get(tenantId, clientKey);
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
        sessionStore.stop();
    }
}
