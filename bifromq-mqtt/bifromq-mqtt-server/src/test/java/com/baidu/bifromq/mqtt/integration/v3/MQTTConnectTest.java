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

package com.baidu.bifromq.mqtt.integration.v3;

import static org.eclipse.paho.client.mqttv3.MqttException.REASON_CODE_BROKER_UNAVAILABLE;
import static org.eclipse.paho.client.mqttv3.MqttException.REASON_CODE_FAILED_AUTHENTICATION;
import static org.eclipse.paho.client.mqttv3.MqttException.REASON_CODE_NOT_AUTHORIZED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.mqtt.TestUtils;
import com.baidu.bifromq.mqtt.integration.MQTTTest;
import com.baidu.bifromq.mqtt.integration.v3.client.MqttTestClient;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import com.baidu.bifromq.plugin.authprovider.type.Ok;
import com.baidu.bifromq.plugin.authprovider.type.Reject;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.AuthError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.NotAuthorizedClient;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.UnauthenticatedClient;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.InvalidTopic;
import java.io.EOFException;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.testng.annotations.Test;

@Slf4j
public class MQTTConnectTest extends MQTTTest {
    private MqttTestClient mqttClient;

    @Override
    protected void doSetup(Method method) {
        mqttClient = new MqttTestClient(BROKER_URI, "mqtt_client_test");
        log.info("Mqtt client created");
    }


    @Override
    protected void doTearDown(Method method) {
        mqttClient.close();
    }

    @Test(groups = "integration")
    public void connectWithCleanSessionTrue() {
        when(authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder()
                    .setTenantId(tenantId)
                    .setUserId("testUser")
                    .build()).build()));

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setUserName("abcdef/testClient");
        mqttClient.connect(connOpts);
        assertTrue(mqttClient.isConnected());
        mqttClient.disconnect();
        assertFalse(mqttClient.isConnected());
    }

    @Test(groups = "integration")
    public void connectWithCleanSessionFalse() {
        when(authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder()
                    .setTenantId(tenantId)
                    .setUserId("testUser")
                    .build()).build()));

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(false);
        connOpts.setUserName("abcdef/testClient");
        mqttClient.connect(connOpts);
        assertTrue(mqttClient.isConnected());
        mqttClient.disconnect();
        assertFalse(mqttClient.isConnected());
    }

    @Test(groups = "integration")
    public void testBadWillTopic() {
        when(authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder()
                    .setTenantId(tenantId)
                    .setUserId("testUser")
                    .build()).build()));

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setWill("$share/badwilltopic", new byte[] {}, 0, false);
        connOpts.setMqttVersion(4);

        MqttException e = TestUtils.expectThrow(() -> mqttClient.connect(connOpts));
        assertTrue(e.getCause() instanceof EOFException);

        connOpts.setWill("$oshare/badwilltopic", new byte[] {}, 0, false);
        e = TestUtils.expectThrow(() -> mqttClient.connect(connOpts));
        assertTrue(e.getCause() instanceof EOFException);

        verify(eventCollector, times(2))
            .report(argThat(event -> event instanceof InvalidTopic));
    }

    @Test(groups = "integration")
    public void testUnauthenticated() {
        when(authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setReject(Reject.newBuilder()
                    .setCode(Reject.Code.BadPass)
                    .build())
                .build()));

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setMqttVersion(4);
        connOpts.setCleanSession(true);
        connOpts.setUserName("abcdef/testClient");
        MqttException e = TestUtils.expectThrow(() -> mqttClient.connect(connOpts));
        assertEquals(e.getReasonCode(), REASON_CODE_FAILED_AUTHENTICATION);

        verify(eventCollector).report(argThat(event -> event instanceof UnauthenticatedClient));
    }

    @Test(groups = "integration")
    public void testBanned() {
        when(authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setReject(Reject.newBuilder()
                    .setCode(Reject.Code.NotAuthorized)
                    .build())
                .build()));

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setMqttVersion(4);
        connOpts.setCleanSession(true);
        connOpts.setUserName("abcdef/testClient");
        MqttException e = TestUtils.expectThrow(() -> mqttClient.connect(connOpts));
        assertEquals(e.getReasonCode(), REASON_CODE_NOT_AUTHORIZED);

        verify(eventCollector).report(argThat(event -> event instanceof NotAuthorizedClient));
    }

    @Test(groups = "integration")
    public void testAuthError() {
        when(authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setReject(Reject.newBuilder()
                    .setCode(Reject.Code.Error)
                    .setReason("mocked auth error")
                    .build())
                .build()));

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setMqttVersion(4);
        connOpts.setCleanSession(true);
        connOpts.setUserName("abcdef/testClient");
        MqttException e = TestUtils.expectThrow(() -> mqttClient.connect(connOpts));
        assertEquals(e.getReasonCode(), REASON_CODE_BROKER_UNAVAILABLE);

        verify(eventCollector).report(argThat(event -> event instanceof AuthError));
    }
}
