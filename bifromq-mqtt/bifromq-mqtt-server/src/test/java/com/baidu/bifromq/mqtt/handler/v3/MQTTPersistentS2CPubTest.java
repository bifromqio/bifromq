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

package com.baidu.bifromq.mqtt.handler.v3;


import static com.baidu.bifromq.plugin.eventcollector.EventType.CLIENT_CONNECTED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.INBOX_TRANSIENT_ERROR;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS0_DROPPED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS0_PUSHED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS1_CONFIRMED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS1_DROPPED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS1_PUSHED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS2_CONFIRMED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS2_DROPPED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS2_PUSHED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS2_RECEIVED;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CHANNEL_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ADDRESS_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_3_1_1_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_TYPE_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;
import static com.baidu.bifromq.type.QoS.AT_LEAST_ONCE;
import static com.baidu.bifromq.type.QoS.EXACTLY_ONCE;
import static io.netty.handler.codec.mqtt.MqttMessageType.PUBREL;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.Fetched.Builder;
import com.baidu.bifromq.inbox.storage.proto.Fetched.Result;
import com.baidu.bifromq.inbox.storage.proto.InboxMessage;
import com.baidu.bifromq.mqtt.handler.BaseMQTTTest;
import com.baidu.bifromq.mqtt.utils.MQTTMessageUtils;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessage;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class MQTTPersistentS2CPubTest extends BaseMQTTTest {


    @BeforeMethod
    public void setup() {
        super.setup();
        connectAndVerify(false);
    }

    @AfterMethod
    public void clean() throws Exception {
        super.clean();
    }

    @Test
    public void qoS0Pub() {
        mockAuthCheck(true);
        inboxFetchConsumer.accept(fetch(5, 128, QoS.AT_MOST_ONCE), null);
        channel.runPendingTasks();
        for (int i = 0; i < 5; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_MOST_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), "testTopic");
        }
        verifyEvent(6, CLIENT_CONNECTED, QOS0_PUSHED, QOS0_PUSHED, QOS0_PUSHED, QOS0_PUSHED, QOS0_PUSHED);
        assertEquals(fetchHints.size(), 1);
    }

    @Test
    public void qoS0PubAuthFailed() {
        // not by pass
        mockAuthCheck(false);
        mockDistUnSub(true);
        inboxFetchConsumer.accept(fetch(5, 128, QoS.AT_MOST_ONCE), null);
        channel.runPendingTasks();
        for (int i = 0; i < 5; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertNull(message);
        }
        verifyEvent(6, CLIENT_CONNECTED, QOS0_DROPPED, QOS0_DROPPED, QOS0_DROPPED, QOS0_DROPPED, QOS0_DROPPED);
        verify(distClient, times(5)).unsub(
            anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt());
    }

    @Test
    public void qoS0PubAndHintChange() {
        mockAuthCheck(true);
        int messageCount = 2;
        inboxFetchConsumer.accept(fetch(messageCount, 64 * 1024, QoS.AT_MOST_ONCE), null);
        channel.runPendingTasks();
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_MOST_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), "testTopic");
        }
        verifyEvent(3, CLIENT_CONNECTED, QOS0_PUSHED, QOS0_PUSHED);
        assertEquals(fetchHints.size(), 2);
    }

    @Test
    public void qoS1PubAndAck() {
        mockAuthCheck(true);
        mockInboxCommit(AT_LEAST_ONCE);
        int messageCount = 3;
        inboxFetchConsumer.accept(fetch(messageCount, 128, AT_LEAST_ONCE), null);
        channel.runPendingTasks();
        // s2c pub received and ack
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_LEAST_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), "testTopic");
            channel.writeInbound(MQTTMessageUtils.pubAckMessage(message.variableHeader().packetId()));
        }
        verifyEvent(7, CLIENT_CONNECTED, QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_CONFIRMED, QOS1_CONFIRMED,
            QOS1_CONFIRMED);
        verify(inboxReader, times(1)).commit(anyLong(), eq(AT_LEAST_ONCE), anyLong());
    }

    @Test
    public void qoS1PubAndNotAllAck() {
        mockAuthCheck(true);
        mockInboxCommit(AT_LEAST_ONCE);
        int messageCount = 3;
        inboxFetchConsumer.accept(fetch(messageCount, 128, AT_LEAST_ONCE), null);
        channel.runPendingTasks();
        // s2c pub received and ack
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_LEAST_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), "testTopic");
            if (i != messageCount - 1) {
                channel.writeInbound(MQTTMessageUtils.pubAckMessage(message.variableHeader().packetId()));
            }
        }
        verifyEvent(6, CLIENT_CONNECTED, QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_CONFIRMED, QOS1_CONFIRMED);
        verify(inboxReader, times(0)).commit(anyLong(), eq(AT_LEAST_ONCE), anyLong());
    }

    @Test
    public void qoS1PubAuthFailed() {
        // not by pass
        mockAuthCheck(false);
        mockDistUnSub(true);
        int messageCount = 3;
        inboxFetchConsumer.accept(fetch(messageCount, 128, AT_LEAST_ONCE), null);
        channel.runPendingTasks();
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertNull(message);
        }
        verifyEvent(4, CLIENT_CONNECTED, QOS1_DROPPED, QOS1_DROPPED, QOS1_DROPPED);
        verify(distClient, times(messageCount)).unsub(
            anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt());
    }

    @Test
    public void qoS2PubAndRel() {
        mockAuthCheck(true);
        mockInboxCommit(EXACTLY_ONCE);
        int messageCount = 2;
        inboxFetchConsumer.accept(fetch(messageCount, 128, EXACTLY_ONCE), null);
        channel.runPendingTasks();
        // s2c pub received and rec
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.EXACTLY_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), "testTopic");
            channel.writeInbound(MQTTMessageUtils.publishRecMessage(message.variableHeader().packetId()));
        }
        // pubRel received and comp
        for (int i = 0; i < messageCount; i++) {
            MqttMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().messageType(), PUBREL);
            channel.writeInbound(MQTTMessageUtils.publishCompMessage(
                ((MqttMessageIdVariableHeader) message.variableHeader()).messageId()));
        }
        verifyEvent(7, CLIENT_CONNECTED, QOS2_PUSHED, QOS2_PUSHED, QOS2_RECEIVED, QOS2_RECEIVED, QOS2_CONFIRMED,
            QOS2_CONFIRMED);
        verify(inboxReader, times(1)).commit(anyLong(), eq(EXACTLY_ONCE), anyLong());
    }

    @Test
    public void qoS2PubAuthFailed() {
        // not by pass
        mockAuthCheck(false);
        mockDistUnSub(true);
        int messageCount = 3;
        inboxFetchConsumer.accept(fetch(messageCount, 128, EXACTLY_ONCE), null);
        channel.runPendingTasks();
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertNull(message);
        }
        verifyEvent(4, CLIENT_CONNECTED, QOS2_DROPPED, QOS2_DROPPED, QOS2_DROPPED);
        verify(distClient, times(messageCount))
            .unsub(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt());
    }

    @Test
    public void qoS2PubWithSameSourcePacketId() {
        mockAuthCheck(true);
        InboxMessage messagesFromClient1 = InboxMessage.newBuilder()
            .setTopicFilter("#")
            .setMsg(
                TopicMessage.newBuilder()
                    .setTopic("testTopic1")
                    .setMessage(
                        Message.newBuilder()
                            .setMessageId(1)
                            .setPayload(ByteString.copyFromUtf8("payload"))
                            .setTimestamp(System.currentTimeMillis())
                            .setPubQoS(EXACTLY_ONCE)
                            .build()
                    )
                    .setPublisher(
                        ClientInfo.newBuilder()
                            .setTenantId(tenantId)
                            .setType(MQTT_TYPE_VALUE)
                            .putMetadata(MQTT_PROTOCOL_VER_KEY, MQTT_PROTOCOL_VER_3_1_1_VALUE)
                            .putMetadata(MQTT_USER_ID_KEY, "testUser")
                            .putMetadata(MQTT_CLIENT_ID_KEY, "client1")
                            .putMetadata(MQTT_CHANNEL_ID_KEY, "channel1")
                            .putMetadata(MQTT_CLIENT_ADDRESS_KEY, "127.0.0.1:11111")
                            .build()
                    )
                    .build()
            ).build();
        InboxMessage messagesFromClient3 = InboxMessage.newBuilder()
            .setTopicFilter("#")
            .setMsg(
                TopicMessage.newBuilder()
                    .setTopic("testTopic2")
                    .setMessage(
                        Message.newBuilder()
                            .setMessageId(1)
                            .setPayload(ByteString.copyFromUtf8("payload"))
                            .setTimestamp(System.currentTimeMillis())
                            .setPubQoS(QoS.EXACTLY_ONCE)
                            .build()
                    )
                    .setPublisher(
                        ClientInfo.newBuilder()
                            .setTenantId(tenantId)
                            .setType(MQTT_TYPE_VALUE)
                            .putMetadata(MQTT_PROTOCOL_VER_KEY, MQTT_PROTOCOL_VER_3_1_1_VALUE)
                            .putMetadata(MQTT_USER_ID_KEY, "testUser")
                            .putMetadata(MQTT_CLIENT_ID_KEY, "client2")
                            .putMetadata(MQTT_CHANNEL_ID_KEY, "channel2")
                            .putMetadata(MQTT_CLIENT_ADDRESS_KEY, "127.0.0.1:22222")
                            .build()
                    )
                    .build()
            ).build();
        // four messages from two clients with same packetId
        Fetched fetched = Fetched.newBuilder()
            .addQos2Msg(messagesFromClient1)
            .addQos2Msg(messagesFromClient1)
            .addQos2Msg(messagesFromClient3)
            .addQos2Msg(messagesFromClient3)
            .addAllQos2Seq(Lists.newArrayList(1L, 2L, 3L, 4L))
            .setResult(Result.OK)
            .build();
        inboxFetchConsumer.accept(fetched, null);
        channel.runPendingTasks();
        // should receive two messages from client1 and client2
        MqttPublishMessage message = channel.readOutbound();
        assertEquals(message.fixedHeader().qosLevel().value(), QoS.EXACTLY_ONCE_VALUE);
        assertEquals(message.variableHeader().topicName(), "testTopic1");

        message = channel.readOutbound();
        assertEquals(message.fixedHeader().qosLevel().value(), QoS.EXACTLY_ONCE_VALUE);
        assertEquals(message.variableHeader().topicName(), "testTopic2");

        message = channel.readOutbound();
        assertNull(message);

        verifyEvent(3, CLIENT_CONNECTED, QOS2_PUSHED, QOS2_PUSHED);
    }

    @Test
    public void fetchError() {
        inboxFetchConsumer.accept(Fetched.newBuilder().setResult(Result.ERROR).build(), null);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        verifyEvent(2, CLIENT_CONNECTED, INBOX_TRANSIENT_ERROR);
    }


    private Fetched fetch(int count, int payloadSize, QoS qoS) {
        Builder builder = Fetched.newBuilder();
        byte[] bytes = new byte[payloadSize];
        Arrays.fill(bytes, (byte) 1);
        for (int i = 0; i < count; i++) {
            InboxMessage inboxMessage = InboxMessage.newBuilder()
                .setTopicFilter("testTopicFilter")
                .setMsg(
                    TopicMessage.newBuilder()
                        .setTopic("testTopic")
                        .setMessage(
                            Message.newBuilder()
                                .setMessageId(i)
                                .setPayload(ByteString.copyFrom(bytes))
                                .setTimestamp(System.currentTimeMillis())
                                .setPubQoS(qoS)
                                .build()
                        )
                        .setPublisher(
                            ClientInfo.newBuilder()
                                .setType(MQTT_TYPE_VALUE)
                                .build()
                        )
                        .build()
                ).build();
            switch (qoS) {
                case AT_MOST_ONCE -> builder.addQos0Msg(inboxMessage).addQos0Seq(i);
                case AT_LEAST_ONCE -> builder.addQos1Msg(inboxMessage).addQos1Seq(i);
                case EXACTLY_ONCE -> builder.addQos2Msg(inboxMessage).addQos2Seq(i);
            }
        }
        return builder.build();
    }
}
