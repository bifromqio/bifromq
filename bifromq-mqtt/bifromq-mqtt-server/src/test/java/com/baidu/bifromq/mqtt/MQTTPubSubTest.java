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
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.mqtt.client.MqttMsg;
import com.baidu.bifromq.mqtt.client.MqttResponse;
import com.baidu.bifromq.mqtt.client.MqttTestAsyncClient;
import com.baidu.bifromq.mqtt.client.MqttTestClient;
import com.baidu.bifromq.plugin.authprovider.ActionInfo;
import com.baidu.bifromq.plugin.authprovider.AuthData;
import com.baidu.bifromq.plugin.authprovider.AuthResult;
import com.baidu.bifromq.plugin.authprovider.CheckResult;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.type.ClientInfo;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttWireMessage;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class MQTTPubSubTest extends MQTTTest {
    private String trafficId = "ashdsha";
    private String deviceKey = "testDevice";

    @Before
    public void setup() {
        System.setProperty("distservice_topic_match_expiry_seconds", "1");
        super.setup();
        when(authProvider.auth(any(AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(AuthResult.pass()
                .trafficId(trafficId)
                .userId(deviceKey)
                .build()));
        when(authProvider.check(any(ClientInfo.class), any(ActionInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(CheckResult.ALLOW));

        doAnswer(invocationOnMock -> {
            Event event = invocationOnMock.getArgument(0);
            log.debug("event: {}", event);
            return null;
        }).when(eventCollector).report(any(Event.class));
    }

    @Test
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

    @Test
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

    @Test
    public void multiTopicPubSubCleanSessionTrueQoS1Basic() {
        String[] topics = new String[] {"/QoS1/1", "/QoS1/2", "/QoS1/3", "/QoS1/4"};
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

    @Test
    public void multiTopicPubSubCleanSessionTrueQoS2Basic() {
        String[] topics = new String[] {"/QoS2/1", "/QoS2/2", "/QoS2/3", "/QoS2/4"};
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

    @Test
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

    @Test
    public void singleTopicPubSubCleanSessionTrueMixQoSBasic() {
        String[] topics = new String[] {"/MixQoS/1", "/MixQoS/1", "/MixQoS/1", "/MixQoS/1"};
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

    @Test
    public void pubQoS0AndDisconnectQuickly() throws InterruptedException {

        String topic = "greeting";
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setUserName(trafficId + "/" + deviceKey);

        MqttTestAsyncClient pubClient = new MqttTestAsyncClient(brokerURI, MqttClient.generateClientId());
        pubClient.connect(connOpts);

        MqttTestClient subClient = new MqttTestClient(brokerURI, MqttClient.generateClientId());
        subClient.connect(connOpts);
        Observable<MqttMsg> topicSub = subClient.subscribe(topic, 1);

        // publish qos0 and quick disconnect
        CompletableFuture<CheckResult> checkFuture = new CompletableFuture<>();
        when(authProvider.check(any(ClientInfo.class), any(ActionInfo.class))).thenReturn(checkFuture);
        pubClient.publish(topic, 0, ByteString.copyFromUtf8("hello"), false);
        pubClient.disconnect().join();
        pubClient.close();
        Thread.sleep(100); // delay a little bit
        checkFuture.complete(CheckResult.ALLOW);


        MqttMsg msg = topicSub.blockingFirst();
        assertEquals(topic, msg.topic);
        assertEquals(0, msg.qos);
        assertFalse(msg.isDup);
        assertFalse(msg.isRetain);
        assertEquals(ByteString.copyFromUtf8("hello"), msg.payload);
        subClient.unsubscribe(topic);
        subClient.disconnect();
        subClient.close();
    }

    @Test
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

    @Test
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

    @Test
    public void receiveOfflineMessage() {
        receiveOfflineMessage(1, 1);
        receiveOfflineMessage(2, 1);
        receiveOfflineMessage(1, 2);
        receiveOfflineMessage(2, 2);
    }

    private void receiveOfflineMessage(int pubQoS, int subQoS) {
        String topic = "topic/" + pubQoS + "/" + subQoS;
        MqttConnectOptions subClientOpts = new MqttConnectOptions();
        subClientOpts.setCleanSession(false);
        subClientOpts.setUserName(trafficId + "/subClient");

        // make a offline subscription
        MqttTestClient subClient = new MqttTestClient(brokerURI, MqttClient.generateClientId());
        subClient.connect(subClientOpts);
        subClient.subscribe(topic, subQoS);
        subClient.disconnect();

        MqttConnectOptions pubClientOpts = new MqttConnectOptions();
        pubClientOpts.setCleanSession(true);
        pubClientOpts.setUserName(trafficId + "/pubClient");
        MqttTestClient pubClient = new MqttTestClient(brokerURI, MqttClient.generateClientId());
        pubClient.connect(pubClientOpts);
        pubClient.publish(topic, pubQoS, ByteString.copyFromUtf8("hello"), false);

        subClient.connect(subClientOpts);
        MqttMsg msg = subClient.messageArrived().blockingFirst();
        assertEquals(topic, msg.topic);
        assertEquals(Math.min(pubQoS, subQoS), msg.qos);
        assertFalse(msg.isDup);
        assertFalse(msg.isRetain);
        assertEquals(ByteString.copyFromUtf8("hello"), msg.payload);

        pubClient.disconnect();
        pubClient.close();
        subClient.disconnect();
        subClient.close();
    }

    private void pubSub(String topic, int pubQoS, String topicFilter, int subQoS) {
        pubSub(topic, pubQoS, topicFilter, subQoS, true);
    }

    private void pubSub(String topic, int pubQoS, String topicFilter, int subQoS, boolean cleanSession) {

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(cleanSession);
        connOpts.setUserName(trafficId + "/" + deviceKey);

        MqttTestClient client = new MqttTestClient(brokerURI, MqttClient.generateClientId());
        client.connect(connOpts);
        Observable<MqttMsg> topicSub = client.subscribe(topicFilter, subQoS);
        client.publish(topic, pubQoS, ByteString.copyFromUtf8("hello"), false);
        MqttMsg msg = topicSub.blockingFirst();
        assertEquals(topic, msg.topic);
        assertEquals(Math.min(pubQoS, subQoS), msg.qos);
        assertFalse(msg.isDup);
        assertFalse(msg.isRetain);
        assertEquals(ByteString.copyFromUtf8("hello"), msg.payload);
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
        connOpts.setUserName(trafficId + "/" + deviceKey);

        MqttTestClient client = new MqttTestClient(brokerURI, MqttClient.generateClientId());
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
            latch.await();
            assertEquals(msgList.size(), responseList.size());
            assertTrue(checkMsgIdConsecutive(msgList, responseList));
            for (int index = 0; index < responseList.size(); index++) {
                MqttResponse mqttResponse = responseList.get(index);
                if (mqttMessages[index].getQos() == 1) {
                    assertEquals(mqttResponse.type, MqttWireMessage.MESSAGE_TYPE_PUBACK);
                } else if (mqttMessages[index].getQos() == 2) {
                    assertEquals(mqttResponse.type, MqttWireMessage.MESSAGE_TYPE_PUBCOMP);
                } else {
                    fail();
                }
                MqttMsg mqttMsg = msgList.get(index);
                assertFalse(mqttMsg.isDup);
                assertFalse(mqttMsg.isRetain);
                if (topics.length == 1) {
                    assertTrue(mqttMsg.payload.toStringUtf8().equals("hello-" + index));
                } else {
                    assertTrue(mqttMsg.payload.toStringUtf8().contains("hello"));
                }
            }
        } catch (Exception exception) {
            fail();
        } finally {
            for (int index = 0; index < topicFilters.length; index++) {
                client.unsubscribe(topicFilters[index]);
            }
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
