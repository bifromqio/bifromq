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
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;

import com.baidu.bifromq.sessiondict.rpc.proto.KillReply;
import com.baidu.bifromq.sessiondict.rpc.proto.KillRequest;
import com.baidu.bifromq.sessiondict.rpc.proto.Quit;
import com.baidu.bifromq.sessiondict.rpc.proto.Session;
import com.baidu.bifromq.sessiondict.rpc.proto.SessionDictServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class SessionDictService extends SessionDictServiceGrpc.SessionDictServiceImplBase {
    private final Map<String, Map<ISessionRegister.ClientKey, ISessionRegister>> tenantSessions;

    SessionDictService() {
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
                    Map<ISessionRegister.ClientKey, ISessionRegister> clients = new ConcurrentHashMap<>();
                    gauging(tenantId, MqttConnectionGauge, clients::size);
                    return clients;
                }).put(clientKey, register);
                if (prevRegister != null) {
                    // kick the session registered via previous register
                    prevRegister.kick(tenantId, clientKey, sessionOwner);
                }
            } else {
                tenantSessions.computeIfPresent(tenantId, (k, v) -> {
                    v.remove(clientKey, register);
                    if (v.isEmpty()) {
                        v = null;
                        stopGauging(tenantId, MqttConnectionGauge);
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
                found.set(v.remove(clientKey));
                if (v.isEmpty()) {
                    v = null;
                    stopGauging(tenantId, MqttConnectionGauge);
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

    void close() {
        tenantSessions.forEach((tenantId, clientKeys) -> clientKeys.values().forEach(ISessionRegister::stop));
    }
}
