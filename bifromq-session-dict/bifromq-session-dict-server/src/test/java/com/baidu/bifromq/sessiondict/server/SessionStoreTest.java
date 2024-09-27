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

import static com.baidu.bifromq.metrics.TenantMetric.MqttConnectionGauge;
import static com.baidu.bifromq.metrics.TenantMetric.MqttLivePersistentSessionGauge;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_SESSION_TYPE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_SESSION_TYPE_P_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_SESSION_TYPE_T_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.metrics.ITenantMeter;
import com.baidu.bifromq.metrics.TenantMetric;
import com.baidu.bifromq.sessiondict.rpc.proto.ServerRedirection;
import com.baidu.bifromq.type.ClientInfo;
import com.google.common.collect.Sets;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import lombok.SneakyThrows;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SessionStoreTest {
    private AutoCloseable closeable;
    private SimpleMeterRegistry meterRegistry;

    @Mock
    private ISessionRegister register1;
    @Mock
    private ISessionRegister register2;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        meterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(meterRegistry);
    }

    @AfterMethod
    @SneakyThrows
    public void tearDown() {
        closeable.close();
        Metrics.globalRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
        Metrics.globalRegistry.remove(meterRegistry);
    }

    @Test
    public void testAdd() {
        SessionStore sessionStore = new SessionStore();
        ClientInfo sessionOwner = ClientInfo.newBuilder()
            .setTenantId("tenantId")
            .putMetadata(MQTT_USER_ID_KEY, "userId")
            .putMetadata(MQTT_CLIENT_ID_KEY, "clientId")
            .build();
        sessionStore.add(sessionOwner, register1);

        ISessionRegister.ClientKey clientKey = new ISessionRegister.ClientKey("userId", "clientId");
        assertSame(sessionStore.get(sessionOwner.getTenantId(), clientKey), register1);
    }

    @Test
    public void testAddDuplicatePersistentSession() {
        SessionStore sessionStore = new SessionStore();
        ClientInfo sessionOwner = ClientInfo.newBuilder()
            .setTenantId("tenant1")
            .putMetadata(MQTT_USER_ID_KEY, "userId")
            .putMetadata(MQTT_CLIENT_ID_KEY, "clientId")
            .putMetadata(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_P_VALUE)
            .build();

        sessionStore.add(sessionOwner, register1);
        assertGaugeValue("tenant1", MqttLivePersistentSessionGauge, 1.0);

        sessionStore.add(sessionOwner, register2);
        assertGaugeValue("tenant1", MqttLivePersistentSessionGauge,
            1.0); // Should not increment because it's a duplicate key
    }

    @Test
    public void testAddAndKickPrevious() {
        SessionStore sessionStore = new SessionStore();
        ClientInfo sessionOwner = ClientInfo.newBuilder()
            .setTenantId("tenantId")
            .putMetadata(MQTT_USER_ID_KEY, "userId")
            .putMetadata(MQTT_CLIENT_ID_KEY, "clientId")
            .build();
        sessionStore.add(sessionOwner, register1);
        sessionStore.add(sessionOwner, register2);

        ISessionRegister.ClientKey clientKey = new ISessionRegister.ClientKey("userId", "clientId");
        assertSame(sessionStore.get(sessionOwner.getTenantId(), clientKey), register2);

        verify(register1).kick(eq(sessionOwner.getTenantId()), eq(clientKey), eq(sessionOwner),
            eq(ServerRedirection.newBuilder().setType(ServerRedirection.Type.NO_MOVE).build()));
    }

    @Test
    public void testTenantMetric() {
        SessionStore sessionStore = new SessionStore();
        ClientInfo sessionOwner1 = ClientInfo.newBuilder()
            .setTenantId("tenantId")
            .putMetadata(MQTT_USER_ID_KEY, "userId")
            .putMetadata(MQTT_CLIENT_ID_KEY, "clientId")
            .putMetadata(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_T_VALUE)
            .build();

        ClientInfo sessionOwner2 = ClientInfo.newBuilder()
            .setTenantId("tenantId")
            .putMetadata(MQTT_USER_ID_KEY, "userId")
            .putMetadata(MQTT_CLIENT_ID_KEY, "clientId1")
            .putMetadata(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_P_VALUE)
            .build();
        assertNoGauge("tenantId", MqttConnectionGauge);
        assertNoGauge("tenantId", MqttLivePersistentSessionGauge);
        sessionStore.add(sessionOwner1, register1);
        sessionStore.add(sessionOwner2, register2);
        assertHasGauge("tenantId", MqttConnectionGauge);
        assertGaugeValue("tenantId", MqttConnectionGauge, 2.0);
        assertHasGauge("tenantId", MqttLivePersistentSessionGauge);
        assertGaugeValue("tenantId", MqttLivePersistentSessionGauge, 1.0);
        sessionStore.remove(sessionOwner1, register1);
        assertGaugeValue("tenantId", MqttConnectionGauge, 1.0);
        assertGaugeValue("tenantId", MqttLivePersistentSessionGauge, 1.0);
        sessionStore.remove(sessionOwner2, register2);
        assertNoGauge("tenantId", MqttConnectionGauge);
        assertNoGauge("tenantId", MqttLivePersistentSessionGauge);
    }

    @Test
    public void testRemove() {
        SessionStore sessionStore = new SessionStore();
        ClientInfo sessionOwner = ClientInfo.newBuilder()
            .setTenantId("tenantId")
            .putMetadata(MQTT_USER_ID_KEY, "userId")
            .putMetadata(MQTT_CLIENT_ID_KEY, "clientId")
            .build();
        sessionStore.add(sessionOwner, register1);

        ISessionRegister.ClientKey clientKey = new ISessionRegister.ClientKey("userId", "clientId");

        // remove using different register
        sessionStore.remove(sessionOwner, register2);
        assertSame(sessionStore.get(sessionOwner.getTenantId(), clientKey), register1);

        sessionStore.remove(sessionOwner, register1);
        assertNull(sessionStore.get(sessionOwner.getTenantId(), clientKey));
    }

    @Test
    public void testRemoveNonexistentPersistentSession() {
        SessionStore sessionStore = new SessionStore();
        ClientInfo sessionOwner = ClientInfo.newBuilder()
            .setTenantId("tenant2")
            .putMetadata(MQTT_USER_ID_KEY, "userId")
            .putMetadata(MQTT_CLIENT_ID_KEY, "clientId")
            .putMetadata(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_P_VALUE)
            .build();

        // Add a persistent session
        sessionStore.add(sessionOwner, register1);
        assertGaugeValue("tenant2", MqttLivePersistentSessionGauge, 1.0);

        // Attempt to remove a non-existent session
        ClientInfo nonExistentSessionOwner = ClientInfo.newBuilder()
            .setTenantId("tenant2")
            .putMetadata(MQTT_USER_ID_KEY, "userId")
            .putMetadata(MQTT_CLIENT_ID_KEY, "nonExistentClientId")
            .putMetadata(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_P_VALUE)
            .build();
        sessionStore.remove(nonExistentSessionOwner, register2);
        assertGaugeValue("tenant2", MqttLivePersistentSessionGauge, 1.0); // Should remain unchanged
    }

    @Test
    public void testFindTenantClients() {
        SessionStore sessionStore = new SessionStore();
        assertTrue(sessionStore.findClients("tenantId").isEmpty());
        ClientInfo sessionOwner1 = ClientInfo.newBuilder()
            .setTenantId("tenantId")
            .putMetadata(MQTT_USER_ID_KEY, "userId1")
            .putMetadata(MQTT_CLIENT_ID_KEY, "clientId")
            .build();

        ClientInfo sessionOwner2 = ClientInfo.newBuilder()
            .setTenantId("tenantId1")
            .putMetadata(MQTT_USER_ID_KEY, "userId1")
            .putMetadata(MQTT_CLIENT_ID_KEY, "clientId1")
            .build();

        ClientInfo sessionOwner3 = ClientInfo.newBuilder()
            .setTenantId("tenantId1")
            .putMetadata(MQTT_USER_ID_KEY, "userId1")
            .putMetadata(MQTT_CLIENT_ID_KEY, "clientId2")
            .build();

        sessionStore.add(sessionOwner1, register1);
        sessionStore.add(sessionOwner2, register2);
        sessionStore.add(sessionOwner3, register2);
        assertEquals(sessionStore.findClients("tenantId"),
            Sets.newHashSet(new ISessionRegister.ClientKey("userId1", "clientId")));
        assertEquals(sessionStore.findClients("tenantId1").size(), 2);
    }

    @Test
    public void findAllTenantUserClients() {
        SessionStore sessionStore = new SessionStore();
        assertTrue(sessionStore.findClients("tenantId").isEmpty());
        ClientInfo sessionOwner1 = ClientInfo.newBuilder()
            .setTenantId("tenantId")
            .putMetadata(MQTT_USER_ID_KEY, "userId1")
            .putMetadata(MQTT_CLIENT_ID_KEY, "clientId")
            .build();

        ClientInfo sessionOwner2 = ClientInfo.newBuilder()
            .setTenantId("tenantId")
            .putMetadata(MQTT_USER_ID_KEY, "userId1")
            .putMetadata(MQTT_CLIENT_ID_KEY, "clientId1")
            .build();

        ClientInfo sessionOwner3 = ClientInfo.newBuilder()
            .setTenantId("tenantId")
            .putMetadata(MQTT_USER_ID_KEY, "userId2")
            .putMetadata(MQTT_CLIENT_ID_KEY, "clientId2")
            .build();

        sessionStore.add(sessionOwner1, register1);
        sessionStore.add(sessionOwner2, register2);
        sessionStore.add(sessionOwner3, register2);
        assertEquals(sessionStore.findClients("tenantId", "userId1").size(), 2);
    }

    private void assertNoGauge(String tenantId, TenantMetric tenantMetric) {
        Optional<Meter> gauge = getGauge(tenantId, tenantMetric);
        assertTrue(gauge.isEmpty());
    }

    private void assertHasGauge(String tenantId, TenantMetric tenantMetric) {
        Optional<Meter> gauge = getGauge(tenantId, tenantMetric);
        assertFalse(gauge.isEmpty());
    }

    private void assertGaugeValue(String tenantId, TenantMetric tenantMetric, double value) {
        Optional<Meter> meter = getGauge(tenantId, tenantMetric);
        assertTrue(meter.isPresent());
        assertEquals(((Gauge) meter.get()).value(), value);
    }

    private Optional<Meter> getGauge(String tenantId, TenantMetric tenantMetric) {
        return meterRegistry.getMeters().stream()
            .filter(m -> m.getId().getName().equals(tenantMetric.metricName)
                && tenantId.equals(m.getId().getTag(ITenantMeter.TAG_TENANT_ID))).findFirst();
    }
}
