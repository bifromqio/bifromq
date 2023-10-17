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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.mqtt.client.MqttMsg;
import com.baidu.bifromq.mqtt.client.MqttTestClient;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import com.baidu.bifromq.plugin.authprovider.type.MQTTAction;
import com.baidu.bifromq.plugin.authprovider.type.Ok;
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
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Slf4j
public class MQTTRetainTest {
    private final MQTTTest mqttTest = MQTTTest.getInstance();
    private final String tenantId = "testRetainTraffic";
    private final String deviceKey = "testDevice";

    @AfterClass(alwaysRun = true)
    public void resetMocks() {
        reset(mqttTest.authProvider, mqttTest.eventCollector);
        clearInvocations(mqttTest.eventCollector);
    }

    @Test(groups = "integration")
    public void retainAndSubscribe() {
        when(mqttTest.authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder()
                    .setTenantId(tenantId)
                    .setUserId(deviceKey)
                    .build())
                .build()));
        when(mqttTest.authProvider.check(any(ClientInfo.class), any(MQTTAction.class)))
            .thenAnswer((Answer<CompletableFuture<Boolean>>) invocation ->
                CompletableFuture.completedFuture(true));

        doAnswer(invocationOnMock -> {
            Event event = invocationOnMock.getArgument(0);
            log.info("event: {}", event.type());
            return null;
        }).when(mqttTest.eventCollector).report(any(Event.class));

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
        String clientId = "testClient1";
        String topic = "retainTopic" + pubQoS + subQoS;
        ByteString payload = ByteString.copyFromUtf8("hello");

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setUserName(tenantId + "/" + deviceKey);

        MqttTestClient client = new MqttTestClient(MQTTTest.brokerURI, clientId);
        client.connect(connOpts);
        client.publish(topic, pubQoS, payload, true);

        Observable<MqttMsg> topicSub = client.subscribe(topic, subQoS);

        MqttMsg msg = topicSub.blockingFirst();
        assertEquals(msg.topic, topic);
        assertEquals(msg.qos, Math.min(pubQoS, subQoS));
        assertFalse(msg.isDup);
        assertTrue(msg.isRetain);
        assertEquals(msg.payload, payload);

        // unsub and sub again
        client.unsubscribe(topic);
        topicSub = client.subscribe("#", subQoS);
        msg = topicSub.blockingFirst();
        assertEquals(msg.topic, topic);
        assertEquals(msg.qos, Math.min(pubQoS, subQoS));
        assertFalse(msg.isDup);
        assertTrue(msg.isRetain);
        assertEquals(msg.payload, payload);

        // clear retain message
        client.publish(topic, pubQoS, ByteString.EMPTY, true);
        client.disconnect();
        client.close();
    }

    @Test(groups = "integration")
    public void subMultipleTimes() {
        // test for [MQTT-3.8.4-3]
        String clientId = "testClient1";
        String topic = "retainTopic";
        ByteString payload = ByteString.copyFromUtf8("hello");
        when(mqttTest.authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder()
                    .setTenantId(tenantId)
                    .setUserId(deviceKey)
                    .build())
                .build()));
        when(mqttTest.authProvider.check(any(ClientInfo.class), any(MQTTAction.class)))
            .thenAnswer((Answer<CompletableFuture<Boolean>>) invocation ->
                CompletableFuture.completedFuture(true));

        doAnswer(invocationOnMock -> {
            Event event = invocationOnMock.getArgument(0);
            log.info("event: {}", event.type());
            return null;
        }).when(mqttTest.eventCollector).report(any(Event.class));

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setUserName(tenantId + "/" + deviceKey);

        MqttTestClient client = new MqttTestClient(MQTTTest.brokerURI, clientId);
        client.connect(connOpts);
        client.publish(topic, 1, payload, true);

        Observable<MqttMsg> topicSub = client.subscribe(topic, 1);

        MqttMsg msg = topicSub.blockingFirst();
        assertEquals(msg.topic, topic);
        assertEquals(msg.qos, 1);
        assertFalse(msg.isDup);
        assertTrue(msg.isRetain);
        assertEquals(msg.payload, payload);

        // sub again without unsub
        topicSub = client.subscribe(topic, 1);
        msg = topicSub.blockingFirst();
        assertEquals(msg.topic, topic);
        assertEquals(msg.qos, 1);
        assertFalse(msg.isDup);
        assertTrue(msg.isRetain);
        assertEquals(msg.payload, payload);

        // clear retain message
        client.publish(topic, 1, ByteString.EMPTY, true);
        client.disconnect();
        client.close();
    }

    @Test(groups = "integration")
    public void clearRetained() {
        when(mqttTest.authProvider.auth(any(MQTT3AuthData.class)))
                .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                        .setOk(Ok.newBuilder()
                                .setTenantId(tenantId)
                                .setUserId(deviceKey)
                                .build())
                        .build()));
        when(mqttTest.authProvider.check(any(ClientInfo.class), any(MQTTAction.class)))
                .thenAnswer((Answer<CompletableFuture<Boolean>>) invocation ->
                        CompletableFuture.completedFuture(true));

        lenient().doAnswer(invocationOnMock -> {
            Event event = invocationOnMock.getArgument(0);
            log.info("event: {}", event);
            return null;
        }).when(mqttTest.eventCollector).report(any(Event.class));

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
        String clientId = "testClient1";
        String topic = "retainTopic" + pubRetainQoS + pubClearQoS;
        ByteString payload = ByteString.copyFromUtf8("hello");

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setUserName(tenantId + "/" + deviceKey);

        MqttTestClient client = new MqttTestClient(MQTTTest.brokerURI, clientId);
        client.connect(connOpts);
        Observable<MqttMsg> topicSub = client.subscribe(topic, 1);
        client.publish(topic, pubRetainQoS, payload, true);
        assertEquals(topicSub.blockingFirst().payload, payload);
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
            testObserver.dispose();
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

    @Test(groups = "integration")
    public void retainMatchLimit() {
        String clientId = "testClient1";
        ByteString payload = ByteString.copyFromUtf8("hello");
        when(mqttTest.authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder()
                    .setTenantId(tenantId)
                    .setUserId(deviceKey)
                    .build())
                .build()));
        when(mqttTest.authProvider.check(any(ClientInfo.class), any(MQTTAction.class)))
            .thenReturn(CompletableFuture.completedFuture(true));
        when(mqttTest.settingProvider.provide(Setting.RetainMessageMatchLimit, tenantId)).thenReturn(2);


        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setUserName(tenantId + "/" + deviceKey);

        MqttTestClient client = new MqttTestClient(MQTTTest.brokerURI, clientId);
        client.connect(connOpts);
        client.publish("topic1", 0, payload, true);
        client.publish("topic2", 1, payload, true);
        client.publish("topic3", 2, payload, true);

        Observable<MqttMsg> topicSub = client.subscribe("#", 1);
        TestObserver<MqttMsg> testObserver = TestObserver.create();
        topicSub.subscribe(testObserver);
        await().until(() -> testObserver.values().size() == 2);

        // clear retain message
        client.publish("topic1", 0, ByteString.EMPTY, true);
        client.publish("topic2", 1, ByteString.EMPTY, true);
        client.publish("topic3", 2, ByteString.EMPTY, true);
    }
}
