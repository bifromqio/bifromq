/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

import static com.baidu.bifromq.metrics.ITenantMeter.gauging;
import static com.baidu.bifromq.metrics.ITenantMeter.stopGauging;
import static com.baidu.bifromq.metrics.TenantMetric.MqttConnectionGauge;
import static com.baidu.bifromq.metrics.TenantMetric.MqttLivePersistentSessionGauge;
import static com.baidu.bifromq.sessiondict.server.ISessionRegister.ClientKey.ClientKeyComparator;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_SESSION_TYPE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_SESSION_TYPE_P_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_SESSION_TYPE_T_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;

import com.baidu.bifromq.type.ClientInfo;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SessionStore {
    private record SessionCollection(AtomicInteger persistentSessions,
                                     NavigableMap<ISessionRegister.ClientKey, ISessionRegister> clients) {
        static final SessionCollection EMPTY =
            new SessionCollection(new AtomicInteger(), new ConcurrentSkipListMap<>(ClientKeyComparator));
    }

    private final Map<String, SessionCollection> tenantSessions = new ConcurrentHashMap<>();

    public void add(ClientInfo sessionOwner, ISessionRegister register) {
        String tenantId = sessionOwner.getTenantId();
        ISessionRegister.ClientKey clientKey =
            new ISessionRegister.ClientKey(sessionOwner.getMetadataOrDefault(MQTT_USER_ID_KEY, ""),
                sessionOwner.getMetadataOrDefault(MQTT_CLIENT_ID_KEY, ""));
        ISessionRegister prevRegister = tenantSessions.computeIfAbsent(tenantId, k -> {
            SessionCollection sessionCollection =
                new SessionCollection(new AtomicInteger(), new ConcurrentSkipListMap<>(ClientKeyComparator));
            gauging(tenantId, MqttConnectionGauge, sessionCollection.clients::size);
            gauging(tenantId, MqttLivePersistentSessionGauge, sessionCollection.persistentSessions::get);
            return sessionCollection;
        }).clients.put(clientKey, register);
        if (prevRegister == null
            && sessionOwner.getMetadataOrDefault(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_T_VALUE)
            .equals(MQTT_CLIENT_SESSION_TYPE_P_VALUE)) {
            tenantSessions.get(tenantId).persistentSessions.incrementAndGet();
        }
        if (prevRegister != null) {
            // kick the session registered via previous register
            prevRegister.kick(tenantId, clientKey, sessionOwner);
        }
    }

    public void remove(ClientInfo sessionOwner, ISessionRegister register) {
        String tenantId = sessionOwner.getTenantId();
        ISessionRegister.ClientKey clientKey =
            new ISessionRegister.ClientKey(sessionOwner.getMetadataOrDefault(MQTT_USER_ID_KEY, ""),
                sessionOwner.getMetadataOrDefault(MQTT_CLIENT_ID_KEY, ""));
        tenantSessions.computeIfPresent(tenantId, (k, v) -> {
            boolean removed = v.clients.remove(clientKey, register);
            if (removed && sessionOwner.getMetadataOrDefault(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_T_VALUE)
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

    public ISessionRegister get(String tenantId, ISessionRegister.ClientKey clientKey) {
        return tenantSessions.getOrDefault(tenantId, SessionCollection.EMPTY).clients.get(clientKey);
    }

    public Set<ISessionRegister.ClientKey> findClients(String tenantId) {
        return tenantSessions.getOrDefault(tenantId, SessionCollection.EMPTY).clients.keySet();
    }

    public Set<ISessionRegister.ClientKey> findClients(String tenantId, String userId) {
        ISessionRegister.ClientKey fromKey =
            new ISessionRegister.ClientKey(userId, "");
        ISessionRegister.ClientKey toKey = new ISessionRegister.ClientKey(userId + "\0", "");
        return tenantSessions.getOrDefault(tenantId, SessionCollection.EMPTY).clients
            .subMap(fromKey, true, toKey, false).keySet();
    }

    public void stop() {
        tenantSessions.forEach((tenantId, collection) -> collection.clients.values().forEach(ISessionRegister::stop));
        tenantSessions.clear();
    }
}
