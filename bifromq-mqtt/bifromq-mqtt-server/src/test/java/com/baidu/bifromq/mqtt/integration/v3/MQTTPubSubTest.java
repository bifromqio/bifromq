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

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.baidu.bifromq.mqtt.integration.MQTTTest;
import com.baidu.bifromq.mqtt.integration.v3.client.MqttMsg;
import com.baidu.bifromq.mqtt.integration.v3.client.MqttResponse;
import com.baidu.bifromq.mqtt.integration.v3.client.MqttTestAsyncClient;
import com.baidu.bifromq.mqtt.integration.v3.client.MqttTestClient;
import com.baidu.bifromq.plugin.authprovider.type.CheckResult;
import com.baidu.bifromq.plugin.authprovider.type.Granted;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import com.baidu.bifromq.plugin.authprovider.type.Ok;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.observers.TestObserver;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttWireMessage;
import org.testng.annotations.Test;

@Slf4j
public class MQTTPubSubTest extends MQTTTest {
    private final String deviceKey = "testDevice";

    protected void doSetup(Method method) {
        when(authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder()
                    .setTenantId(tenantId)
                    .setUserId(deviceKey)
                    .build())
                .build()));
        when(authProvider.checkPermission(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(CheckResult.newBuilder()
                .setGranted(Granted.getDefaultInstance())
                .build()));
    }

    @Test(groups = "integration")
    public void multiTopicPubSubCleanSessionFalseQoS1Basic() {
        String[] topics = new String[] {"/QoS1/1", "/QoS1/2", "/QoS1/3", "/QoS1/4"};
        String[] topicFilters = new String[] {"#"};
        MqttMessage[] mqttMessages = new MqttMessage[topics.length];
        for (int index = 0; index < topics.length; index++) {
            MqttMessage message = new MqttMessage();
            message.setQos(1);
            message.setPayload(("hello").getBytes());
            mqttMessages[index] = message;
        }
        pubSubMulti(topics, topicFilters, mqttMessages, false);
    }

    @Test(groups = "integration")
    public void multiTopicPubSubCleanSessionFalseQoS2Basic() {
        String[] topics = new String[] {"/QoS2/1", "/QoS2/2", "/QoS2/3", "/QoS2/4"};
        String[] topicFilters = new String[] {"#"};
        MqttMessage[] mqttMessages = new MqttMessage[topics.length];
        for (int index = 0; index < topics.length; index++) {
            MqttMessage message = new MqttMessage();
            message.setQos(2);
            message.setPayload(("hello").getBytes());
            mqttMessages[index] = message;
        }
        pubSubMulti(topics, topicFilters, mqttMessages, false);
    }

    @Test(groups = "integration")
    public void multiTopicPubSubCleanSessionTrueQoS1Basic() {
        String[] topics = new String[] {"/QoS1/5", "/QoS1/6", "/QoS1/7", "/QoS1/8"};
        String[] topicFilters = new String[] {"#"};
        MqttMessage[] mqttMessages = new MqttMessage[topics.length];
        for (int index = 0; index < topics.length; index++) {
            MqttMessage message = new MqttMessage();
            message.setQos(1);
            message.setPayload(("hello").getBytes());
            mqttMessages[index] = message;
        }
        pubSubMulti(topics, topicFilters, mqttMessages, true);
    }

    @Test(groups = "integration")
    public void multiTopicPubSubCleanSessionTrueQoS2Basic() {
        String[] topics = new String[] {"/QoS2/5", "/QoS2/6", "/QoS2/7", "/QoS2/8"};
        String[] topicFilters = new String[] {"#"};
        MqttMessage[] mqttMessages = new MqttMessage[topics.length];
        for (int index = 0; index < topics.length; index++) {
            MqttMessage message = new MqttMessage();
            message.setQos(2);
            message.setPayload(("hello").getBytes());
            mqttMessages[index] = message;
        }
        pubSubMulti(topics, topicFilters, mqttMessages, true);
    }

    @Test(groups = "integration")
    public void multiTopicPubSubCleanSessionTrueMixQoSBasic() {
        String[] topics = new String[] {"/MixQoS/1", "/MixQoS/2", "/MixQoS/3", "/MixQoS/4"};
        String[] topicFilters = new String[] {"#"};
        MqttMessage[] mqttMessages = new MqttMessage[topics.length];
        for (int index = 0; index < topics.length; index++) {
            MqttMessage message = new MqttMessage();
            message.setQos(1 + (index) % 2);
            message.setPayload(("hello").getBytes());
            mqttMessages[index] = message;
        }
        pubSubMulti(topics, topicFilters, mqttMessages, true);
    }

    @Test(groups = "integration")
    public void singleTopicPubSubCleanSessionTrueMixQoSBasic() {
        String[] topics = new String[] {"/MixQoS/5", "/MixQoS/6", "/MixQoS/7", "/MixQoS/8"};
        String[] topicFilters = new String[] {"#"};
        MqttMessage[] mqttMessages = new MqttMessage[topics.length];
        for (int index = 0; index < topics.length; index++) {
            MqttMessage message = new MqttMessage();
            message.setQos(1 + (index) % 2);
            message.setPayload(("hello-" + index).getBytes());
            mqttMessages[index] = message;
        }
        pubSubMulti(topics, topicFilters, mqttMessages, true);
    }

    @Test(groups = "integration")
    public void pubQoS0AndDisconnectQuickly() throws InterruptedException {

        String topic = "greeting";
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setUserName(tenantId + "/" + deviceKey);

        MqttTestAsyncClient pubClient = new MqttTestAsyncClient(BROKER_URI, MqttClient.generateClientId());
        pubClient.connect(connOpts);

        MqttTestClient subClient = new MqttTestClient(BROKER_URI, MqttClient.generateClientId());
        subClient.connect(connOpts);
        Observable<MqttMsg> topicSub = subClient.subscribe(topic, 1);

        // publish qos0 and quick disconnect
        CompletableFuture<CheckResult> checkFuture = new CompletableFuture<>();
        when(authProvider.checkPermission(any(), any())).thenReturn(checkFuture);
        pubClient.publish(topic, 0, ByteString.copyFromUtf8("hello"), false);
        pubClient.disconnect().join();
        pubClient.close();
        Thread.sleep(100); // delay a little bit
        checkFuture.complete(CheckResult.newBuilder()
            .setGranted(Granted.getDefaultInstance())
            .build());

        TestObserver<MqttMsg> msgObserver = topicSub.test();
        subClient.unsubscribe(topic);
        subClient.disconnect();
        subClient.close();
        msgObserver.awaitDone(100, TimeUnit.MILLISECONDS);
        msgObserver.assertNoValues();
    }

    @Test(groups = "integration")
    public void pubSubCleanSessionTrue() {
        pubSub("/topic/0/0", 0, "/topic/0/0", 0);
        pubSub("/topic/0/1", 0, "/topic/0/1", 1);
        pubSub("/topic/0/2", 0, "/topic/0/2", 2);
        pubSub("/topic/1/0", 1, "/topic/1/0", 0);
        pubSub("/topic/1/1", 1, "/topic/1/1", 1);
        pubSub("/topic/1/2", 1, "/topic/1/2", 2);
        pubSub("/topic/2/0", 2, "/topic/2/0", 0);
        pubSub("/topic/2/1", 2, "/topic/2/1", 1);
        pubSub("/topic/2/2", 2, "/topic/2/2", 2);
        pubSub("/topic1/0/0", 0, "#", 0);
        pubSub("/topic1/0/1", 0, "#", 1);
        pubSub("/topic1/0/2", 0, "#", 2);
        pubSub("/topic1/1/0", 1, "#", 0);
        pubSub("/topic1/1/1", 1, "#", 1);
        pubSub("/topic1/1/2", 1, "#", 2);
        pubSub("/topic1/2/0", 2, "#", 0);
        pubSub("/topic1/2/1", 2, "#", 1);
        pubSub("/topic1/2/2", 2, "#", 2);
    }

    @Test(groups = "integration")
    public void pubSubCleanSessionFalse() {
        pubSub("/topic/0/0", 0, "/topic/0/0", 0, false);
        pubSub("/topic/0/1", 0, "/topic/0/1", 1, false);
        pubSub("/topic/0/2", 0, "/topic/0/2", 2, false);
        pubSub("/topic/1/0", 1, "/topic/1/0", 0, false);
        pubSub("/topic/1/1", 1, "/topic/1/1", 1, false);
//        pubSub("/topic/1/2", 1, "/topic/1/2", 2, false);
        pubSub("/topic/2/0", 2, "/topic/2/0", 0, false);
        pubSub("/topic/2/1", 2, "/topic/2/1", 1, false);
        pubSub("/topic/2/2", 2, "/topic/2/2", 2, false);
        pubSub("/topic1/0/0", 0, "#", 0, false);
        pubSub("/topic1/0/1", 0, "#", 1, false);
        pubSub("/topic1/0/2", 0, "#", 2, false);
        pubSub("/topic1/1/0", 1, "#", 0, false);
        pubSub("/topic1/1/1", 1, "#", 1, false);
        pubSub("/topic1/1/2", 1, "#", 2, false);
        pubSub("/topic1/2/0", 2, "#", 0, false);
        pubSub("/topic1/2/1", 2, "#", 1, false);
        pubSub("/topic1/2/2", 2, "#", 2, false);
    }

    @Test(groups = "integration")
    public void receiveOfflineMessage() {
        receiveOfflineMessage(1, 1);
        receiveOfflineMessage(2, 1);
        receiveOfflineMessage(1, 2);
        receiveOfflineMessage(2, 2);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_0_0_true() {
        pubSubInOrder(0, 0, true);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_0_1_true() {
        pubSubInOrder(0, 1, true);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_0_2_true() {
        pubSubInOrder(0, 2, true);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_1_0_true() {
        pubSubInOrder(1, 0, true);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_1_1_true() {
        pubSubInOrder(1, 1, true);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_1_2_true() {
        pubSubInOrder(1, 2, true);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_2_0_true() {
        pubSubInOrder(2, 0, true);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_2_1_true() {
        pubSubInOrder(2, 1, true);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_2_2_true() {
        pubSubInOrder(2, 2, true);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_0_0_false() {
        pubSubInOrder(0, 0, false);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_0_1_false() {
        pubSubInOrder(0, 1, false);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_0_2_false() {
        pubSubInOrder(0, 2, false);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_1_0_false() {
        pubSubInOrder(1, 0, false);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_1_1_false() {
        pubSubInOrder(1, 1, false);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_1_2_false() {
        pubSubInOrder(1, 2, false);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_2_0_false() {
        pubSubInOrder(2, 0, false);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_2_1_false() {
        pubSubInOrder(2, 1, false);
    }

    @Test(groups = "integration")
    public void pubSubInOrder_2_2_false() {
        pubSubInOrder(2, 2, false);
    }


    @SneakyThrows
    private void pubSubInOrder(int pubQoS, int subQoS, boolean cleanSession) {
        String topic = "a/b";
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(cleanSession);
        connOpts.setUserName(tenantId + "/" + deviceKey);

        MqttTestClient pubClient = new MqttTestClient(BROKER_URI, MqttClient.generateClientId());
        pubClient.connect(connOpts);

        MqttTestClient subClient = new MqttTestClient(BROKER_URI, MqttClient.generateClientId());
        subClient.connect(connOpts);
        Observable<MqttMsg> topicSub = subClient.subscribe(topic, subQoS);

        List<ByteString> pubMsgList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            ByteString msg = ByteString.copyFromUtf8(Integer.toUnsignedString(i));
            pubMsgList.add(msg);
            pubClient.publish(topic, pubQoS, msg, false);
        }
        TestObserver<MqttMsg> observer = new TestObserver<>();
        topicSub.subscribe(observer);
        await().until(() -> {
            List<ByteString> mqttMsgs = observer.values().stream().map(msg -> msg.payload).toList();
            return mqttMsgs.equals(pubMsgList);
        });
        pubClient.disconnect();
        pubClient.close();
        subClient.unsubscribe(topic);
        subClient.disconnect();
        subClient.close();
    }

    private void receiveOfflineMessage(int pubQoS, int subQoS) {
        String topic = "topic/" + pubQoS + "/" + subQoS;
        MqttConnectOptions subClientOpts = new MqttConnectOptions();
        subClientOpts.setCleanSession(false);
        subClientOpts.setUserName(tenantId + "/subClient");

        log.info("Connect sub client and disconnect");
        // make a offline subscription
        MqttTestClient subClient = new MqttTestClient(BROKER_URI, MqttClient.generateClientId());
        subClient.connect(subClientOpts);
        subClient.subscribe(topic, subQoS);
        subClient.disconnect();

        log.info("Connect pub client and pub some message");
        MqttConnectOptions pubClientOpts = new MqttConnectOptions();
        pubClientOpts.setCleanSession(true);
        pubClientOpts.setUserName(tenantId + "/pubClient");
        MqttTestClient pubClient = new MqttTestClient(BROKER_URI, MqttClient.generateClientId());
        pubClient.connect(pubClientOpts);
        pubClient.publish(topic, pubQoS, ByteString.copyFromUtf8("hello"), false);

        log.info("Reconnect sub client");
        CountDownLatch latch = new CountDownLatch(1);
        Disposable disposable = subClient.messageArrived().subscribe(msg -> {
            log.debug("offline message arrived: pubQos = {}, subQos = {}", pubQoS, subQoS);
            latch.countDown();
            assertEquals(msg.topic, topic);
            assertEquals(msg.qos, Math.min(pubQoS, subQoS));
            assertFalse(msg.isDup);
            assertFalse(msg.isRetain);
            assertEquals(msg.payload, ByteString.copyFromUtf8("hello"));
        });
        subClient.connect(subClientOpts);
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            fail();
        }
        disposable.dispose();

        pubClient.disconnect();
        pubClient.close();
        subClient.unsubscribe(topic);
        subClient.disconnect();
        subClient.close();
    }

    private void pubSub(String topic, int pubQoS, String topicFilter, int subQoS) {
        pubSub(topic, pubQoS, topicFilter, subQoS, true);
    }

    private void pubSub(String topic, int pubQoS, String topicFilter, int subQoS, boolean cleanSession) {

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(cleanSession);
        connOpts.setUserName(tenantId + "/" + deviceKey);

        MqttTestClient client = new MqttTestClient(BROKER_URI, MqttClient.generateClientId());
        client.connect(connOpts);
        Observable<MqttMsg> topicSub = client.subscribe(topicFilter, subQoS);
        client.publish(topic, pubQoS, ByteString.copyFromUtf8("hello"), false);
        MqttMsg msg = topicSub.timeout(10, TimeUnit.SECONDS).blockingFirst();
        assertEquals(msg.topic, topic);
        assertEquals(msg.qos, Math.min(pubQoS, subQoS));
        assertFalse(msg.isDup);
        assertFalse(msg.isRetain);
        assertEquals(msg.payload, ByteString.copyFromUtf8("hello"));
        client.unsubscribe(topicFilter);
        client.disconnect();
        client.close();
    }

    private void pubSubMulti(String[] topics, String[] topicFilters, MqttMessage[] mqttMessages, boolean cleanSession) {
        List<MqttResponse> responseList = new ArrayList<>();
        List<MqttMsg> msgList = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(mqttMessages.length * 2);
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(cleanSession);
        connOpts.setUserName(tenantId + "/" + deviceKey);

        MqttTestClient client = new MqttTestClient(BROKER_URI, MqttClient.generateClientId());
        client.connect(connOpts);
        client.messageArrived().subscribe(mqttMsg -> {
            msgList.add(mqttMsg);
            latch.countDown();
        });
        client.deliveryComplete().subscribe(mqttResponse -> {
            responseList.add(mqttResponse);
            latch.countDown();
        });
        for (int index = 0; index < topicFilters.length; index++) {
            try {
                client.subscribe(topicFilters[index], mqttMessages[index].getQos());
            } catch (Exception exception) {
                log.error("subscribe timeout exception: {}", index);
            }
        }
        for (int index = 0; index < topics.length; index++) {
            try {
                client.publish(topics[index], mqttMessages[index].getQos(),
                    ByteString.copyFrom(mqttMessages[index].getPayload()), false);
            } catch (Exception timeoutException) {
                log.error("publish timeout exception, index: {}, messageId:{}, isDup: {}",
                    index, mqttMessages[index].getId(), mqttMessages[index].isDuplicate());
            }
        }
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
            assertEquals(responseList.size(), msgList.size());
            assertTrue(checkMsgIdConsecutive(msgList, responseList));
            for (int index = 0; index < responseList.size(); index++) {
                MqttResponse mqttResponse = responseList.get(index);
                if (mqttMessages[index].getQos() == 1) {
                    assertEquals(MqttWireMessage.MESSAGE_TYPE_PUBACK, mqttResponse.type);
                } else if (mqttMessages[index].getQos() == 2) {
                    assertEquals(MqttWireMessage.MESSAGE_TYPE_PUBCOMP, mqttResponse.type);
                } else {
                    fail();
                }
                MqttMsg mqttMsg = msgList.get(index);
                assertFalse(mqttMsg.isDup);
                assertFalse(mqttMsg.isRetain);
                if (topics.length == 1) {
                    assertEquals(mqttMsg.payload.toStringUtf8(), "hello-" + index);
                } else {
                    assertTrue(mqttMsg.payload.toStringUtf8().contains("hello"));
                }
            }
        } catch (Exception exception) {
            fail();
        } finally {
            for (String topicFilter : topicFilters) {
                log.info("unsub {}", topicFilter);
                client.unsubscribe(topicFilter);
            }
            log.info("finished");
            client.disconnect();
            client.close();
        }
    }

    private boolean checkMsgIdConsecutive(List<MqttMsg> msgList,
                                          List<MqttResponse> responseList) {
        for (int index = 0; index < msgList.size() - 1; index++) {
            MqttMsg mqttMessage = msgList.get(index);
            MqttResponse mqttResponse = responseList.get(index);
            if (mqttMessage.id + 1 != msgList.get(index + 1).id ||
                mqttResponse.messageId + 1 != responseList.get(index + 1).messageId) {
                return false;
            }
        }
        return true;
    }
}
