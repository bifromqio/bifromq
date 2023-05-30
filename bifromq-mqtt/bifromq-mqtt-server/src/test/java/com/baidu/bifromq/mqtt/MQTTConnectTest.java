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

package com.baidu.bifromq.mqtt;

import static org.eclipse.paho.client.mqttv3.MqttException.REASON_CODE_BROKER_UNAVAILABLE;
import static org.eclipse.paho.client.mqttv3.MqttException.REASON_CODE_FAILED_AUTHENTICATION;
import static org.eclipse.paho.client.mqttv3.MqttException.REASON_CODE_NOT_AUTHORIZED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.mqtt.client.MqttTestClient;
import com.baidu.bifromq.plugin.authprovider.AuthData;
import com.baidu.bifromq.plugin.authprovider.AuthResult;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.AuthError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.AuthFailedClient;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.BannedClient;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.InvalidWillTopic;
import java.io.EOFException;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
@Ignore
public class MQTTConnectTest extends MQTTTest {
    private MqttTestClient mqttClient;

    @Before
    public void setup() {
        super.setup();
        mqttClient = new MqttTestClient(brokerURI, "mqtt_client_test");
        log.info("Mqtt client created");
    }

    @After
    public void teardown() {
        mqttClient.close();
        super.teardown();
    }

    @Test
    public void connectWithCleanSessionTrue() {
        when(authProvider.auth(any(AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(AuthResult.pass()
                .trafficId("testTraffic")
                .userId("testUser")
                .build()));

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setUserName("abcdef/testClient");
        mqttClient.connect(connOpts);
        assertTrue(mqttClient.isConnected());
        mqttClient.disconnect();
        assertFalse(mqttClient.isConnected());
        // TODO: verify event report
    }

    @Test
    public void connectWithCleanSessionFalse() {
        when(authProvider.auth(any(AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(AuthResult.pass()
                .trafficId("testTraffic")
                .userId("testUser")
                .build()));

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(false);
        connOpts.setUserName("abcdef/testClient");
        mqttClient.connect(connOpts);
        assertTrue(mqttClient.isConnected());
        mqttClient.disconnect();
        assertFalse(mqttClient.isConnected());
        // TODO: verify event report
    }

    @Test
    public void testBadWillTopic() {
        when(authProvider.auth(any(AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(AuthResult.pass()
                .trafficId("testTraffic")
                .userId("testUser")
                .build()));

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setWill("$share/badwilltopic", new byte[] {}, 0, false);
        connOpts.setMqttVersion(4);

        MqttException e = TestUtils.expectThrow(() -> mqttClient.connect(connOpts));
        assertTrue(e.getCause() instanceof EOFException);

        connOpts.setWill("$oshare/badwilltopic", new byte[] {}, 0, false);
        e = TestUtils.expectThrow(() -> mqttClient.connect(connOpts));
        assertTrue(e.getCause() instanceof EOFException);

        verify(eventCollector, times(2))
            .report(argThat(event -> event instanceof InvalidWillTopic));
    }

    @Test
    public void testUnauthenticated() {
        when(authProvider.auth(any(AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(AuthResult.NO_PASS));

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setMqttVersion(4);
        connOpts.setCleanSession(true);
        connOpts.setUserName("abcdef/testClient");
        MqttException e = TestUtils.expectThrow(() -> mqttClient.connect(connOpts));
        assertEquals(REASON_CODE_FAILED_AUTHENTICATION, e.getReasonCode());

        verify(eventCollector).report(argThat(event -> event instanceof AuthFailedClient));
    }

    @Test
    public void testBanned() {
        when(authProvider.auth(any(AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(AuthResult.BANNED));

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setMqttVersion(4);
        connOpts.setCleanSession(true);
        connOpts.setUserName("abcdef/testClient");
        MqttException e = TestUtils.expectThrow(() -> mqttClient.connect(connOpts));
        assertEquals(REASON_CODE_NOT_AUTHORIZED, e.getReasonCode());

        verify(eventCollector).report(argThat(event -> event instanceof BannedClient));
    }

    @Test
    public void testAuthError() {
        when(authProvider.auth(any(AuthData.class)))
            .thenReturn(CompletableFuture
                .completedFuture(AuthResult.error(new RuntimeException("mocked auth error"))));

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setMqttVersion(4);
        connOpts.setCleanSession(true);
        connOpts.setUserName("abcdef/testClient");
        MqttException e = TestUtils.expectThrow(() -> mqttClient.connect(connOpts));
        assertEquals(REASON_CODE_BROKER_UNAVAILABLE, e.getReasonCode());

        verify(eventCollector).report(argThat(event -> event instanceof AuthError));
    }
}
