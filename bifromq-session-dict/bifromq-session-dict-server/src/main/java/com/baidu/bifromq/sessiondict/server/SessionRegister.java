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

import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;

import com.baidu.bifromq.baserpc.AckStream;
import com.baidu.bifromq.sessiondict.rpc.proto.Quit;
import com.baidu.bifromq.sessiondict.rpc.proto.Session;
import com.baidu.bifromq.type.ClientInfo;
import io.grpc.stub.StreamObserver;
import io.reactivex.rxjava3.disposables.Disposable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class SessionRegister extends AckStream<Session, Quit> implements ISessionRegister {
    // keep the session registered via this stream
    private final Map<String, Map<ClientKey, ClientInfo>> registeredSession = new ConcurrentHashMap<>();
    private final IRegistrationListener regListener;
    private final Disposable disposable;

    SessionRegister(IRegistrationListener listener, StreamObserver<Quit> responseObserver) {
        super(responseObserver);
        this.regListener = listener;
        disposable = ack()
            .doFinally(() -> registeredSession.forEach((tenantId, clientKeys) ->
                clientKeys.values().forEach(clientInfo -> regListener.on(clientInfo, false, this))))
            .subscribe(session -> {
                ClientInfo owner = session.getOwner();
                String tenantId = owner.getTenantId();
                ClientKey clientKey = new ClientKey(owner.getMetadataOrDefault(MQTT_USER_ID_KEY, ""),
                    owner.getMetadataOrDefault(MQTT_CLIENT_ID_KEY, ""));
                if (session.getKeep()) {
                    try {
                        KickResult kickResult = doKick(tenantId, clientKey, owner);
                        if (kickResult == KickResult.IGNORED_SELF_KICK) {
                            return;
                        }
                        registeredSession.compute(tenantId, (t, m) -> {
                            if (m == null) {
                                m = new HashMap<>();
                            }
                            m.put(clientKey, session.getOwner());
                            return m;
                        });
                        listener.on(owner, true, this);
                    } catch (Throwable e) {
                        log.debug("Failed to kick", e);
                    }
                } else {
                    AtomicBoolean found = new AtomicBoolean();
                    registeredSession.compute(owner.getTenantId(), (t, m) -> {
                        if (m == null) {
                            return null;
                        } else {
                            found.set(m.remove(clientKey, owner));
                            if (m.isEmpty()) {
                                m = null;
                            }
                            return m;
                        }
                    });
                    if (found.get()) {
                        listener.on(owner, false, this);
                    }
                }
            });
    }

    @Override
    public boolean kick(String tenantId, ClientKey clientKey, ClientInfo kicker) {
        return doKick(tenantId, clientKey, kicker) == KickResult.KICKED;
    }

    private enum KickResult {
        KICKED, NOT_FOUND, IGNORED_SELF_KICK
    }

    private KickResult doKick(String tenantId, ClientKey clientKey, ClientInfo kicker) {
        AtomicReference<KickResult> result = new AtomicReference<>(KickResult.NOT_FOUND);
        AtomicReference<ClientInfo> toKick = new AtomicReference<>();
        registeredSession.computeIfPresent(tenantId, (k, v) -> {
            if (v.containsKey(clientKey)) {
                if (!v.get(clientKey).equals(kicker)) {
                    toKick.set(v.remove(clientKey));
                    result.set(KickResult.KICKED);
                } else {
                    result.set(KickResult.IGNORED_SELF_KICK);
                }
            } else {
                result.set(KickResult.NOT_FOUND);
            }
            if (v.isEmpty()) {
                v = null;
            }
            return v;
        });
        if (toKick.get() != null) {
            send(Quit.newBuilder()
                .setReqId(System.nanoTime())
                .setOwner(toKick.get())
                .setKiller(kicker)
                .build());
            regListener.on(toKick.get(), false, this);
        }
        return result.get();
    }

    @Override
    public ClientInfo owner(String tenantId, ClientKey clientKey) {
        return registeredSession.getOrDefault(tenantId, Collections.emptyMap()).get(clientKey);
    }

    @Override
    public void stop() {
        disposable.dispose();
        close();
    }
}
