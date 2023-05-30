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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.mqtt.client.MqttMsg;
import com.baidu.bifromq.mqtt.client.MqttTestClient;
import com.baidu.bifromq.plugin.authprovider.ActionInfo;
import com.baidu.bifromq.plugin.authprovider.AuthData;
import com.baidu.bifromq.plugin.authprovider.AuthResult;
import com.baidu.bifromq.plugin.authprovider.CheckResult;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.type.ClientInfo;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
@Ignore
public class MQTTLastWillTest extends MQTTTest {

    @Test
    public void lastWillQoS1() {
        String trafficId = "ashdsha";
        String deviceKey = "testDevice";
        String userName = trafficId + "/" + deviceKey;
        String willTopic = "willTopic";
        ByteString willPayload = ByteString.copyFromUtf8("bye");
        when(authProvider.auth(any(AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(AuthResult.pass()
                .trafficId(trafficId)
                .userId(deviceKey)
                .build()));
        when(authProvider.check(any(ClientInfo.class), any(ActionInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(CheckResult.ALLOW));

        doAnswer(invocationOnMock -> {
            Event event = invocationOnMock.getArgument(0);
            log.info("event: {}", event);
            return null;
        }).when(eventCollector).report(any(Event.class));

        MqttConnectOptions lwtPubConnOpts = new MqttConnectOptions();
        lwtPubConnOpts.setCleanSession(true);
        lwtPubConnOpts.setWill(willTopic, willPayload.toByteArray(), 1, false);
        lwtPubConnOpts.setUserName(userName);
        MqttTestClient lwtPubClient = new MqttTestClient(brokerURI, "lwtPubclient");
        lwtPubClient.connect(lwtPubConnOpts);

        MqttConnectOptions lwtSubConnOpts = new MqttConnectOptions();
        lwtSubConnOpts.setCleanSession(true);
        lwtSubConnOpts.setUserName(userName);

        MqttTestClient lwtSubClient = new MqttTestClient(brokerURI, "lwtSubClient");
        lwtSubClient.connect(lwtSubConnOpts);
        Observable<MqttMsg> topicSub = lwtSubClient.subscribe(willTopic, 1);

        log.info("Kill client");
        sessionDictClient.kill(System.nanoTime(), trafficId, deviceKey,
            "lwtPubclient", ClientInfo.getDefaultInstance()).join();

        MqttMsg msg = topicSub.blockingFirst();
        assertEquals(willTopic, msg.topic);
        assertEquals(1, msg.qos);
        assertEquals(willPayload, msg.payload);
        assertFalse(msg.isRetain);
    }

    @Test
    public void lastWillQoS1Retained() {
        String trafficId = "ashdsha";
        String deviceKey = "testDevice";
        String userName = trafficId + "/" + deviceKey;
        String willTopic = "willTopic";
        ByteString willPayload = ByteString.copyFromUtf8("bye");
        when(authProvider.auth(any(AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(AuthResult.pass()
                .trafficId(trafficId)
                .userId(deviceKey)
                .build()));
        when(authProvider.check(any(ClientInfo.class), any(ActionInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(CheckResult.ALLOW));

        doAnswer(invocationOnMock -> {
            Event event = invocationOnMock.getArgument(0);
            log.info("event: {}", event.type());
            return null;
        }).when(eventCollector).report(any(Event.class));

        MqttConnectOptions lwtPubConnOpts = new MqttConnectOptions();
        lwtPubConnOpts.setCleanSession(true);
        lwtPubConnOpts.setWill(willTopic, willPayload.toByteArray(), 1, true);
        lwtPubConnOpts.setUserName(userName);
        MqttTestClient lwtPubClient = new MqttTestClient(brokerURI, "lwtPubclient");
        lwtPubClient.connect(lwtPubConnOpts);
        sessionDictClient.kill(System.nanoTime(), trafficId, deviceKey,
            "lwtPubclient", ClientInfo.getDefaultInstance()).join();

        MqttConnectOptions lwtSubConnOpts = new MqttConnectOptions();
        lwtSubConnOpts.setCleanSession(true);
        lwtSubConnOpts.setUserName(userName);

        MqttTestClient lwtSubClient = new MqttTestClient(brokerURI, "lwtSubClient");
        lwtSubClient.connect(lwtSubConnOpts);
        Observable<MqttMsg> topicSub = lwtSubClient.subscribe(willTopic, 1);

        MqttMsg msg = topicSub.blockingFirst();
        assertEquals(willTopic, msg.topic);
        assertEquals(1, msg.qos);
        assertEquals(willPayload, msg.payload);
        assertTrue(msg.isRetain);
    }

    @Test
    public void lastWillQoS2() {
        String trafficId = "ashdsha";
        String deviceKey = "testDevice";
        String userName = trafficId + "/" + deviceKey;
        String willTopic = "willTopic";
        ByteString willPayload = ByteString.copyFromUtf8("bye");
        when(authProvider.auth(any(AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(AuthResult.pass()
                .trafficId(trafficId)
                .userId(deviceKey)
                .build()));
        when(authProvider.check(any(ClientInfo.class), any(ActionInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(CheckResult.ALLOW));

        doAnswer(invocationOnMock -> {
            Event event = invocationOnMock.getArgument(0);
            log.info("event: {}", event.type());
            return null;
        }).when(eventCollector).report(any(Event.class));

        MqttConnectOptions lwtPubConnOpts = new MqttConnectOptions();
        lwtPubConnOpts.setCleanSession(true);
        lwtPubConnOpts.setWill(willTopic, willPayload.toByteArray(), 2, false);
        lwtPubConnOpts.setUserName(userName);
        MqttTestClient lwtPubClient = new MqttTestClient(brokerURI, "lwtPubclient");
        lwtPubClient.connect(lwtPubConnOpts);

        MqttConnectOptions lwtSubConnOpts = new MqttConnectOptions();
        lwtSubConnOpts.setCleanSession(true);
        lwtSubConnOpts.setUserName(userName);

        MqttTestClient lwtSubClient = new MqttTestClient(brokerURI, "lwtSubClient");
        lwtSubClient.connect(lwtSubConnOpts);
        Observable<MqttMsg> topicSub = lwtSubClient.subscribe(willTopic, 2);

        sessionDictClient.kill(System.nanoTime(), trafficId, deviceKey,
            "lwtPubclient", ClientInfo.getDefaultInstance()).join();

        MqttMsg msg = topicSub.blockingFirst();
        assertEquals(willTopic, msg.topic);
        assertEquals(2, msg.qos);
        assertEquals(willPayload, msg.payload);
        assertFalse(msg.isRetain);
    }

    @Test
    public void lastWillQoS2Retained() {
        String trafficId = "ashdsha";
        String deviceKey = "testDevice";
        String userName = trafficId + "/" + deviceKey;
        String willTopic = "willTopic";
        ByteString willPayload = ByteString.copyFromUtf8("bye");
        when(authProvider.auth(any(AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(AuthResult.pass()
                .trafficId(trafficId)
                .userId(deviceKey)
                .build()));
        when(authProvider.check(any(ClientInfo.class), any(ActionInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(CheckResult.ALLOW));

        doAnswer(invocationOnMock -> {
            Event event = invocationOnMock.getArgument(0);
            log.info("event: {}", event.type());
            return null;
        }).when(eventCollector).report(any(Event.class));

        MqttConnectOptions lwtPubConnOpts = new MqttConnectOptions();
        lwtPubConnOpts.setCleanSession(true);
        lwtPubConnOpts.setWill(willTopic, willPayload.toByteArray(), 2, true);
        lwtPubConnOpts.setUserName(userName);
        MqttTestClient lwtPubClient = new MqttTestClient(brokerURI, "lwtPubclient");
        lwtPubClient.connect(lwtPubConnOpts);
        sessionDictClient.kill(System.nanoTime(), trafficId, deviceKey,
            "lwtPubclient", ClientInfo.getDefaultInstance()).join();

        MqttConnectOptions lwtSubConnOpts = new MqttConnectOptions();
        lwtSubConnOpts.setCleanSession(true);
        lwtSubConnOpts.setUserName(userName);

        MqttTestClient lwtSubClient = new MqttTestClient(brokerURI, "lwtSubClient");
        lwtSubClient.connect(lwtSubConnOpts);
        Observable<MqttMsg> topicSub = lwtSubClient.subscribe(willTopic, 2);

        MqttMsg msg = topicSub.blockingFirst();
        assertEquals(willTopic, msg.topic);
        assertEquals(2, msg.qos);
        assertEquals(willPayload, msg.payload);
        assertTrue(msg.isRetain);
    }
}
