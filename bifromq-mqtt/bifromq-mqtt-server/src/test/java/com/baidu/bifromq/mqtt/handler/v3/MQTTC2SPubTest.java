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

package com.baidu.bifromq.mqtt.handler.v3;


import static com.baidu.bifromq.plugin.eventcollector.EventType.CLIENT_CONNECTED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.DISCARD;
import static com.baidu.bifromq.plugin.eventcollector.EventType.EXCEED_PUB_RATE;
import static com.baidu.bifromq.plugin.eventcollector.EventType.INVALID_TOPIC;
import static com.baidu.bifromq.plugin.eventcollector.EventType.MALFORMED_TOPIC;
import static com.baidu.bifromq.plugin.eventcollector.EventType.NO_PUB_PERMISSION;
import static com.baidu.bifromq.plugin.eventcollector.EventType.PROTOCOL_VIOLATION;
import static com.baidu.bifromq.plugin.eventcollector.EventType.PUB_ACKED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.PUB_ACK_DROPPED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.PUB_ACTION_DISALLOW;
import static com.baidu.bifromq.plugin.eventcollector.EventType.PUB_RECED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.PUB_REC_DROPPED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS0_DIST_ERROR;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS1_DIST_ERROR;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS2_DIST_ERROR;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MsgPubPerSec;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.dist.client.DistResult;
import com.baidu.bifromq.mqtt.utils.MQTTMessageUtils;
import com.baidu.bifromq.type.ClientInfo;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class MQTTC2SPubTest extends BaseMQTTTest {

    @Test
    public void qoS0Pub() {
        setupTransientSession();
        mockAuthCheck(true);
        mockDistDist(true);
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS0Message("testTopic", 123);
        channel.writeInbound(publishMessage);
        verifyEvent(CLIENT_CONNECTED);
        verify(distClient, times(1)).pub(anyLong(), anyString(), any(), any(ClientInfo.class));
    }


    @Test
    public void qoS0PubBadMessage() {
        setupTransientSession();
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS0DupMessage("testTopic", 123);
        channel.writeInbound(publishMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        assertFalse(channel.isActive());
        verifyEvent(CLIENT_CONNECTED, PROTOCOL_VIOLATION);
    }

    @Test
    public void qoS0PubDistFailed() {
        setupTransientSession();
        mockAuthCheck(true);
        mockDistDist(false);
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS0Message("testTopic", 123);
        channel.writeInbound(publishMessage);
        verifyEvent(CLIENT_CONNECTED, QOS0_DIST_ERROR);
    }

    @Test
    public void qos0PubDistDrop() {
        setupTransientSession();
        mockAuthCheck(true);
        mockDistDrop();
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS0Message("testTopic", 123);
        channel.writeInbound(publishMessage);
        verifyEvent(CLIENT_CONNECTED, QOS0_DIST_ERROR);
    }

    @Test
    public void qoS0PubAuthFailed() {
        // not by pass
        setupTransientSession();
        mockAuthCheck(false);
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS0Message("testTopic", 123);
        channel.writeInbound(publishMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        assertFalse(channel.isActive());
        verifyEvent(CLIENT_CONNECTED, PUB_ACTION_DISALLOW, NO_PUB_PERMISSION);
    }

    @Test
    public void qoS1Pub() {
        setupTransientSession();
        mockAuthCheck(true);
        mockDistDist(true);
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS1Message("testTopic", 123);
        channel.writeInbound(publishMessage);
        MqttMessage ackMessage = channel.readOutbound();
        assertEquals(((MqttMessageIdVariableHeader) ackMessage.variableHeader()).messageId(), 123);
        verifyEvent(CLIENT_CONNECTED, PUB_ACKED);
    }

    @Test
    public void qoS1PubDistFailed() {
        setupTransientSession();
        mockAuthCheck(true);
        mockDistDist(false);
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS1Message("testTopic", 123);
        channel.writeInbound(publishMessage);
        verifyEvent(CLIENT_CONNECTED, QOS1_DIST_ERROR, PUB_ACKED);
    }

    @Test
    public void qos1PubDistDrop() {
        setupTransientSession();
        mockAuthCheck(true);
        mockDistDrop();
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS1Message("testTopic", 123);
        channel.writeInbound(publishMessage);
        verifyEvent(CLIENT_CONNECTED, QOS1_DIST_ERROR, PUB_ACKED);
    }


    @Test
    public void qoS1PubAckWithUnWritable() {
        setupTransientSession();
        mockAuthCheck(true);
        CompletableFuture<DistResult> distResult = new CompletableFuture<>();
        when(distClient.pub(anyLong(), anyString(), any(), any(ClientInfo.class))).thenReturn(distResult);
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS1Message("testTopic", 123);
        channel.writeInbound(publishMessage);
        // make channel unWritable
        channel.writeOneOutbound(MQTTMessageUtils.largeMqttMessage(250 * 1024));
        channel.writeOneOutbound(MQTTMessageUtils.largeMqttMessage(250 * 1024));
        assertFalse(channel.isWritable());
        distResult.complete(DistResult.OK);
        channel.runPendingTasks();
        verifyEvent(CLIENT_CONNECTED, PUB_ACK_DROPPED);
    }

    @Test
    public void qoS1PubAuthFailed() {
        // not by pass
        setupTransientSession();
        mockAuthCheck(false);
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS1Message("testTopic", 123);
        channel.writeInbound(publishMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        assertFalse(channel.isActive());
        verifyEvent(CLIENT_CONNECTED, PUB_ACTION_DISALLOW, NO_PUB_PERMISSION);
    }

    @Test
    public void qoS2Pub() {
        setupTransientSession();
        mockAuthCheck(true);
        mockDistDist(true);
        // publish
        channel.writeInbound(MQTTMessageUtils.publishQoS2Message("testTopic", 123));
        MqttMessage mqttMessage = channel.readOutbound();
        assertEquals(mqttMessage.fixedHeader().messageType(), MqttMessageType.PUBREC);
        assertEquals(((MqttMessageIdVariableHeader) mqttMessage.variableHeader()).messageId(), 123);
//        assertTrue(sessionContext.isConfirming(tenantId, channel.id().asLongText(), 123));
        // publish release
        channel.writeInbound(MQTTMessageUtils.publishRelMessage(123));
        mqttMessage = channel.readOutbound();
        assertEquals(mqttMessage.fixedHeader().messageType(), MqttMessageType.PUBCOMP);
        assertEquals(((MqttMessageIdVariableHeader) mqttMessage.variableHeader()).messageId(), 123);
        verifyEvent(CLIENT_CONNECTED, PUB_RECED);
//        assertFalse(sessionContext.isConfirming(tenantId, channel.id().asLongText(), 123));
    }

    @Test
    public void qoS2PubDistFailed() {
        setupTransientSession();
        mockAuthCheck(true);
        mockDistDist(false);
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS2Message("testTopic", 123);
        channel.writeInbound(publishMessage);
        verifyEvent(CLIENT_CONNECTED, QOS2_DIST_ERROR, PUB_RECED);
//        assertFalse(sessionContext.isConfirming(tenantId, channel.id().asLongText(), 123));
    }

    @Test
    public void qoS2PubDistDrop() {
        setupTransientSession();
        mockAuthCheck(true);
        mockDistDrop();
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS2Message("testTopic", 123);
        channel.writeInbound(publishMessage);
        verifyEvent(CLIENT_CONNECTED, QOS2_DIST_ERROR, PUB_RECED);
//        assertFalse(sessionContext.isConfirming(tenantId, channel.id().asLongText(), 123));
    }


    @Test
    public void qoS2PubWithUnWritable() {
        setupTransientSession();
        mockAuthCheck(true);
        CompletableFuture<DistResult> distResult = new CompletableFuture<>();
        when(distClient.pub(anyLong(), anyString(), any(), any(ClientInfo.class))).thenReturn(distResult);
        channel.writeInbound(MQTTMessageUtils.publishQoS2Message("testTopic", 123));

        // make channel unWritable and drop PubRec
        channel.writeOneOutbound(MQTTMessageUtils.largeMqttMessage(250 * 1024));
        channel.writeOneOutbound(MQTTMessageUtils.largeMqttMessage(250 * 1024));
        assertFalse(channel.isWritable());
        distResult.complete(DistResult.OK);
        channel.runPendingTasks();
        verifyEvent(CLIENT_CONNECTED, PUB_REC_DROPPED);

        // flush channel
        channel.flush();
        channel.readOutbound();
        channel.readOutbound();
        assertTrue(channel.isWritable());

        // client did not receive PubRec, resend pub and receive PubRec
        channel.writeInbound(MQTTMessageUtils.publishQoS2Message("testTopic", 123));
        channel.runPendingTasks();
        verifyEvent(CLIENT_CONNECTED, PUB_REC_DROPPED, PUB_RECED);
        MqttMessage mqttMessage = channel.readOutbound();
        assertEquals(mqttMessage.fixedHeader().messageType(), MqttMessageType.PUBREC);
        assertEquals(((MqttMessageIdVariableHeader) mqttMessage.variableHeader()).messageId(), 123);

        // continue to publish PubRel
        channel.writeInbound(MQTTMessageUtils.publishRelMessage(123));
        mqttMessage = channel.readOutbound();
        assertEquals(mqttMessage.fixedHeader().messageType(), MqttMessageType.PUBCOMP);
        assertEquals(((MqttMessageIdVariableHeader) mqttMessage.variableHeader()).messageId(), 123);
        verifyEvent(CLIENT_CONNECTED, PUB_REC_DROPPED, PUB_RECED);
    }

    @Test
    public void qoS2PubAuthFailed() {
        // not by pass
        setupTransientSession();
        mockAuthCheck(false);
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS2Message("testTopic", 123);
        channel.writeInbound(publishMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        assertFalse(channel.isActive());
        verifyEvent(CLIENT_CONNECTED, PUB_ACTION_DISALLOW, NO_PUB_PERMISSION);
//        assertFalse(sessionContext.isConfirming(tenantId, channel.id().asLongText(), 123));
    }

    @Test
    public void malformedTopic() {
        setupTransientSession();
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS0Message("topic\u0000", 123);
        channel.writeInbound(publishMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        assertFalse(channel.isActive());
        verifyEvent(CLIENT_CONNECTED, MALFORMED_TOPIC);
    }

    @Test
    public void invalidTopic() {
        setupTransientSession();
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS0Message("$share/g/testTopic", 123);
        channel.writeInbound(publishMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        assertFalse(channel.isActive());
        verifyEvent(CLIENT_CONNECTED, INVALID_TOPIC);
    }

    @Test
    public void pubTooFast() {
        when(settingProvider.provide(eq(MsgPubPerSec), anyString())).thenReturn(1);
        setupTransientSession();
        mockAuthCheck(true);
        mockDistDist(true);
        MqttPublishMessage publishMessage = MQTTMessageUtils.publishQoS1Message("testTopic", 1);
        MqttPublishMessage publishMessage2 = MQTTMessageUtils.publishQoS1Message("testTopic", 2);
        channel.writeInbound(publishMessage);
        channel.writeInbound(publishMessage2);
        MqttMessage ackMessage = channel.readOutbound();
        assertEquals(((MqttMessageIdVariableHeader) ackMessage.variableHeader()).messageId(), 1);
        verifyEvent(CLIENT_CONNECTED, PUB_ACKED, DISCARD);
    }
}
