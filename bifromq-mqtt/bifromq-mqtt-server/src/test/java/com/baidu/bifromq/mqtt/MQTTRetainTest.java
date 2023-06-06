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

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.mqtt.client.MqttMsg;
import com.baidu.bifromq.mqtt.client.MqttTestClient;
import com.baidu.bifromq.plugin.authprovider.ActionInfo;
import com.baidu.bifromq.plugin.authprovider.AuthData;
import com.baidu.bifromq.plugin.authprovider.AuthResult;
import com.baidu.bifromq.plugin.authprovider.CheckResult;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.type.ClientInfo;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.observers.TestObserver;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class MQTTRetainTest extends MQTTTest {
    @Test
    public void retainAndSubscribe() {
//        retainAndSubscribe(0, 0);
//        retainAndSubscribe(0, 1);
//        retainAndSubscribe(0, 2);

//        retainAndSubscribe(1, 0);
        retainAndSubscribe(1, 1);
        retainAndSubscribe(1, 2);

//        retainAndSubscribe(2, 0);
        retainAndSubscribe(2, 1);
        retainAndSubscribe(2, 2);
    }

    public void retainAndSubscribe(int pubQoS, int subQoS) {
        String trafficId = "ashdsha";
        String deviceKey = "testDevice";
        String clientId = "testClient1";
        String topic = "retainTopic" + pubQoS + subQoS;
        ByteString payload = ByteString.copyFromUtf8("hello");
        when(authProvider.auth(any(AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(AuthResult.pass()
                .trafficId(trafficId)
                .userId(deviceKey)
                .build()));
        when(authProvider.check(any(ClientInfo.class), any(ActionInfo.class)))
            .thenAnswer((Answer<CompletableFuture<CheckResult>>) invocation ->
                CompletableFuture.completedFuture(CheckResult.ALLOW));

        doAnswer(invocationOnMock -> {
            Event event = invocationOnMock.getArgument(0);
            log.info("event: {}", event.type());
            return null;
        }).when(eventCollector).report(any(Event.class));

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setUserName(trafficId + "/" + deviceKey);

        MqttTestClient client = new MqttTestClient(brokerURI, clientId);
        client.connect(connOpts);
        client.publish(topic, pubQoS, payload, true);

        Observable<MqttMsg> topicSub = client.subscribe(topic, subQoS);

        MqttMsg msg = topicSub.blockingFirst();
        assertEquals(topic, msg.topic);
        assertEquals(Math.min(pubQoS, subQoS), msg.qos);
        assertFalse(msg.isDup);
        assertTrue(msg.isRetain);
        assertEquals(payload, msg.payload);

        // unsub and sub again
        client.unsubscribe(topic);
        topicSub = client.subscribe("#", subQoS);
        msg = topicSub.blockingFirst();
        assertEquals(topic, msg.topic);
        assertEquals(Math.min(pubQoS, subQoS), msg.qos);
        assertFalse(msg.isDup);
        assertTrue(msg.isRetain);
        assertEquals(payload, msg.payload);

        client.disconnect();
        client.close();
    }

    @Test
    public void subMultipleTimes() {
        // test for [MQTT-3.8.4-3]
        String trafficId = "ashdsha";
        String deviceKey = "testDevice";
        String clientId = "testClient1";
        String topic = "retainTopic";
        ByteString payload = ByteString.copyFromUtf8("hello");
        when(authProvider.auth(any(AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(AuthResult.pass()
                .trafficId(trafficId)
                .userId(deviceKey)
                .build()));
        when(authProvider.check(any(ClientInfo.class), any(ActionInfo.class)))
            .thenAnswer((Answer<CompletableFuture<CheckResult>>) invocation ->
                CompletableFuture.completedFuture(CheckResult.ALLOW));

        doAnswer(invocationOnMock -> {
            Event event = invocationOnMock.getArgument(0);
            log.info("event: {}", event.type());
            return null;
        }).when(eventCollector).report(any(Event.class));

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setUserName(trafficId + "/" + deviceKey);

        MqttTestClient client = new MqttTestClient(brokerURI, clientId);
        client.connect(connOpts);
        client.publish(topic, 1, payload, true);

        Observable<MqttMsg> topicSub = client.subscribe(topic, 1);

        MqttMsg msg = topicSub.blockingFirst();
        assertEquals(topic, msg.topic);
        assertEquals(1, msg.qos);
        assertFalse(msg.isDup);
        assertTrue(msg.isRetain);
        assertEquals(payload, msg.payload);

        // sub again without unsub
        topicSub = client.subscribe(topic, 1);
        msg = topicSub.blockingFirst();
        assertEquals(topic, msg.topic);
        assertEquals(1, msg.qos);
        assertFalse(msg.isDup);
        assertTrue(msg.isRetain);
        assertEquals(payload, msg.payload);

        client.disconnect();
        client.close();
    }

    @Test
    public void clearRetained() {
        clearRetained(0, 0);
        clearRetained(0, 1);
        clearRetained(0, 2);

        clearRetained(1, 0);
        clearRetained(1, 1);
        clearRetained(1, 2);

        clearRetained(2, 0);
        clearRetained(2, 1);
        clearRetained(2, 2);
    }

    @SneakyThrows
    public void clearRetained(int pubRetainQoS, int pubClearQoS) {
        String trafficId = "ashdsha";
        String deviceKey = "testDevice";
        String clientId = "testClient1";
        String topic = "retainTopic" + pubRetainQoS + pubClearQoS;
        ByteString payload = ByteString.copyFromUtf8("hello");
        when(authProvider.auth(any(AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(AuthResult.pass()
                .trafficId(trafficId)
                .userId(deviceKey)
                .build()));
        when(authProvider.check(any(ClientInfo.class), any(ActionInfo.class)))
            .thenAnswer((Answer<CompletableFuture<CheckResult>>) invocation ->
                CompletableFuture.completedFuture(CheckResult.ALLOW));

        lenient().doAnswer(invocationOnMock -> {
            Event event = invocationOnMock.getArgument(0);
            log.info("event: {}", event);
            return null;
        }).when(eventCollector).report(any(Event.class));

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setUserName(trafficId + "/" + deviceKey);

        MqttTestClient client = new MqttTestClient(brokerURI, clientId);
        client.connect(connOpts);
        Observable<MqttMsg> topicSub = client.subscribe(topic, 1);
        client.publish(topic, pubRetainQoS, payload, true);
        assertEquals(payload, topicSub.blockingFirst().payload);
        log.info("Pub to clear retain");
        client.publish(topic, pubClearQoS, ByteString.EMPTY, true);
        log.info("Unsubscribe from topic");
        client.unsubscribe(topic);

        log.info("subscribe until no retain message received");
        await().until(() -> {
            Observable<MqttMsg> topicSub1 = client.subscribe(topic, 1);
            TestObserver<MqttMsg> testObserver = TestObserver.create();
            topicSub1.subscribe(testObserver);

            log.info("Publish topic");
            client.publish(topic, pubRetainQoS, payload, false);

            testObserver.awaitCount(1);
            boolean isRetain = false;
            for (MqttMsg msg : testObserver.values()) {
                if (msg.isRetain) {
                    isRetain = true;
                    break;
                }
            }
            client.unsubscribe(topic);
            return !isRetain;
        });

        client.disconnect();
        client.close();
    }

    @Test
    public void retainMatchLimit() {
        String trafficId = "ashdsha";
        String deviceKey = "testDevice";
        String clientId = "testClient1";
        ByteString payload = ByteString.copyFromUtf8("hello");
        when(authProvider.auth(any(AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(AuthResult.pass()
                .trafficId(trafficId)
                .userId(deviceKey)
                .build()));
        when(authProvider.check(any(ClientInfo.class), any(ActionInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(CheckResult.ALLOW));
        when(settingProvider.provide(any(), any(ClientInfo.class)))
            .thenAnswer(invocationOnMock -> {
                Setting setting = invocationOnMock.getArgument(0);
                if (setting == Setting.RetainMessageMatchLimit) {
                    return 2;
                }
                return setting.current(invocationOnMock.getArgument(1));
            });

        doAnswer(invocationOnMock -> {
            Event event = invocationOnMock.getArgument(0);
            log.info("event: {}", event);
            return null;
        }).when(eventCollector).report(any(Event.class));

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setUserName(trafficId + "/" + deviceKey);

        MqttTestClient client = new MqttTestClient(brokerURI, clientId);
        client.connect(connOpts);
        client.publish("topic1", 0, payload, true);
        client.publish("topic2", 1, payload, true);
        client.publish("topic3", 2, payload, true);

        Observable<MqttMsg> topicSub = client.subscribe("#", 1);
        TestObserver<MqttMsg> testObserver = TestObserver.create();
        topicSub.subscribe(testObserver);
        await().until(() -> testObserver.values().size() == 2);
    }
}
