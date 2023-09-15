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

package com.baidu.bifromq.sessiondict.server;

import static com.baidu.bifromq.baserpc.UnaryResponse.response;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;

import com.baidu.bifromq.sessiondict.rpc.proto.KillReply;
import com.baidu.bifromq.sessiondict.rpc.proto.KillRequest;
import com.baidu.bifromq.sessiondict.rpc.proto.Quit;
import com.baidu.bifromq.sessiondict.rpc.proto.Session;
import com.baidu.bifromq.sessiondict.rpc.proto.SessionDictServiceGrpc;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class SessionDictService extends SessionDictServiceGrpc.SessionDictServiceImplBase {
    private final Cache<ISessionRegister.SessionKey, ISessionRegister> sessions;

    SessionDictService() {
        sessions = Caffeine.newBuilder()
            .weakValues()
            .build();
    }

    @Override
    public StreamObserver<Session> dict(StreamObserver<Quit> responseObserver) {
        return new SessionRegister(((sessionOwner, reg, register) -> {
            ISessionRegister.SessionKey sessionKey = new ISessionRegister.SessionKey(sessionOwner.getTenantId(),
                new ISessionRegister.ClientKey(sessionOwner.getMetadataOrDefault(MQTT_USER_ID_KEY, ""),
                    sessionOwner.getMetadataOrDefault(MQTT_CLIENT_ID_KEY, "")));
            if (reg) {
                ISessionRegister prevRegister = sessions.asMap().put(sessionKey, register);
                if (prevRegister != null) {
                    // kick the session registered via previous register
                    prevRegister.kick(sessionKey, sessionOwner);
                }
            } else {
                sessions.asMap().remove(sessionKey, register);
            }
        }), responseObserver);
    }

    @Override
    public void kill(KillRequest request, StreamObserver<KillReply> responseObserver) {
        response(tenantId -> {
            ISessionRegister.SessionKey sessionKey = new ISessionRegister.SessionKey(request.getTenantId(),
                new ISessionRegister.ClientKey(request.getUserId(), request.getClientId()));
            ISessionRegister reg = sessions.asMap().remove(sessionKey);
            if (reg != null) {
                try {
                    reg.kick(sessionKey, request.getKiller());
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
        sessions.asMap().forEach((sessionKey, register) -> register.stop());
    }
}
