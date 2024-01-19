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


import static com.baidu.bifromq.plugin.eventcollector.EventType.PING_REQ;
import static com.baidu.bifromq.plugin.eventcollector.EventType.SESSION_CHECK_ERROR;
import static com.baidu.bifromq.plugin.eventcollector.EventType.SESSION_CLEANUP_ERROR;
import static com.baidu.bifromq.plugin.eventcollector.EventType.SESSION_CREATE_ERROR;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_KEY;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.inbox.rpc.proto.AttachReply;
import com.baidu.bifromq.inbox.rpc.proto.CreateReply;
import com.baidu.bifromq.inbox.rpc.proto.ExpireReply;
import com.baidu.bifromq.inbox.storage.proto.InboxVersion;
import com.baidu.bifromq.mqtt.utils.MQTTMessageUtils;
import com.baidu.bifromq.plugin.authprovider.type.Reject;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.EventType;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientconnected.ClientConnected;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class MQTTConnectTest extends BaseMQTTTest {

    @Test
    public void transientSessionWithoutInbox() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExpire(ExpireReply.Code.NOT_FOUND);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);
        verifyEvent(EventType.CLIENT_CONNECTED);
    }

    @Test
    public void transientSessionWithInbox() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExpire(ExpireReply.Code.OK);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);
        verifyEvent(EventType.CLIENT_CONNECTED);
    }

    @Test
    public void cleanSessionExpireExistingError() {
        // clear failed
        mockAuthPass();
        mockSessionReg();
        mockInboxExpire(ExpireReply.Code.ERROR);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        // verifications
        Assert.assertEquals(CONNECTION_REFUSED_SERVER_UNAVAILABLE, ackMessage.variableHeader().connectReturnCode());
        verifyEvent(SESSION_CLEANUP_ERROR);
    }

    @Test
    public void attachToExistingSession() {
        mockAuthPass();
        mockSessionReg();
        mockInboxGet(InboxVersion.newBuilder().setIncarnation(1).build());
        mockAttach(AttachReply.Code.OK);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(false);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);
        assertTrue(ackMessage.variableHeader().isSessionPresent());
        verifyEvent(EventType.CLIENT_CONNECTED);
    }

    @Test
    public void attachToExistingSessionError() {
        mockAuthPass();
        mockSessionReg();
        mockInboxGet(InboxVersion.newBuilder().setIncarnation(1).build());
        mockAttach(AttachReply.Code.ERROR);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(false);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);
        verifyEvent(EventType.SESSION_CREATE_ERROR, EventType.CLIENT_CONNECTED);
    }


    @Test
    public void createNewPersistentSession() {
        mockAuthPass();
        mockSessionReg();
        mockInboxGet();
        mockInboxCreate(CreateReply.Code.OK);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(false);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);
        verifyEvent(EventType.CLIENT_CONNECTED);
    }

    @Test
    public void createPersistentSessionError() {
        mockAuthPass();
        mockSessionReg();
        mockInboxGet();
        mockInboxCreate(CreateReply.Code.ERROR);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(false);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);
        verifyEvent(SESSION_CREATE_ERROR, EventType.CLIENT_CONNECTED);
    }

    @Test
    public void persistentSessionCheckInboxError() {
        // create inbox failed
        mockAuthPass();
        mockSessionReg();
        when(inboxClient.get(any())).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Mock exception")));
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(false);
        channel.writeInbound(connectMessage);
        // verifications
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        Assert.assertEquals(CONNECTION_REFUSED_SERVER_UNAVAILABLE, ackMessage.variableHeader().connectReturnCode());
        verifyEvent(SESSION_CHECK_ERROR);
    }

    @Test
    public void authWithCustomAttrs() {
        String attrKey = "attrKey";
        String attrVal = "attrVal";
        mockAuthPass(attrKey, attrVal);
        mockSessionReg();
        mockInboxExpire(true);

        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        // verifications
        Assert.assertEquals(CONNECTION_ACCEPTED, ackMessage.variableHeader().connectReturnCode());
        ArgumentCaptor<ClientConnected> eventArgumentCaptor = ArgumentCaptor.forClass(ClientConnected.class);
        verify(eventCollector).report(eventArgumentCaptor.capture());
        ClientConnected clientConnected = eventArgumentCaptor.getValue();
        assertTrue(clientConnected.clientInfo().containsMetadata(attrKey));
        assertEquals(clientConnected.clientInfo().getMetadataMap().get(attrKey), attrVal);
    }

    @Test
    public void reservedMetadataNotOverridable() {
        String attrKey = MQTT_PROTOCOL_VER_KEY;
        String attrVal = "attrVal";
        mockAuthPass(attrKey, attrVal);
        mockSessionReg();
        mockInboxExpire(ExpireReply.Code.OK);

        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        // verifications
        Assert.assertEquals(CONNECTION_ACCEPTED, ackMessage.variableHeader().connectReturnCode());
        ArgumentCaptor<ClientConnected> eventArgumentCaptor = ArgumentCaptor.forClass(ClientConnected.class);
        verify(eventCollector).report(eventArgumentCaptor.capture());
        ClientConnected clientConnected = eventArgumentCaptor.getValue();
        assertTrue(clientConnected.clientInfo().containsMetadata(attrKey));
        assertNotEquals(clientConnected.clientInfo().getMetadataMap().get(attrKey), attrVal);
    }

    @Test
    public void authBanned() {
        mockAuthReject(Reject.Code.NotAuthorized, "");
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        // verifications
        Assert.assertEquals(CONNECTION_REFUSED_NOT_AUTHORIZED, ackMessage.variableHeader().connectReturnCode());
        verifyEvent(EventType.NOT_AUTHORIZED_CLIENT);
    }

    @Test
    public void authNotPass() {
        mockAuthReject(Reject.Code.BadPass, "");
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        // verifications
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        Assert.assertEquals(CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD,
            ackMessage.variableHeader().connectReturnCode());
        verifyEvent(EventType.UNAUTHENTICATED_CLIENT);
    }

    @Test
    public void authError() {
        mockAuthReject(Reject.Code.Error, "");
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.advanceTimeBy(disconnectDelay, TimeUnit.MILLISECONDS);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        // verifications
        Assert.assertEquals(CONNECTION_REFUSED_SERVER_UNAVAILABLE, ackMessage.variableHeader().connectReturnCode());
        ArgumentCaptor<Event> eventArgumentCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventCollector, times(1)).report(eventArgumentCaptor.capture());
        verifyEvent(EventType.AUTH_ERROR);
    }

    @Test
    public void validWillTopic() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExpire(ExpireReply.Code.OK);
        MqttConnectMessage connectMessage = MQTTMessageUtils.qoSWillMqttConnectMessage(1, true);
        channel.writeInbound(connectMessage);
        MqttConnAckMessage ackMessage = channel.readOutbound();
        // verifications
        Assert.assertEquals(CONNECTION_ACCEPTED, ackMessage.variableHeader().connectReturnCode());
        verifyEvent(EventType.CLIENT_CONNECTED);
    }

    @Test
    public void pingAndPingResp() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExpire(ExpireReply.Code.OK);
        MqttConnectMessage connectMessage = MQTTMessageUtils.qoSWillMqttConnectMessage(1, true);
        channel.writeInbound(connectMessage);
        MqttConnAckMessage ackMessage = channel.readOutbound();
        // verifications
        Assert.assertEquals(CONNECTION_ACCEPTED, ackMessage.variableHeader().connectReturnCode());

        channel.writeInbound(MqttMessage.PINGREQ);
        MqttMessage pingResp = channel.readOutbound();
        Assert.assertEquals(MqttMessage.PINGRESP, pingResp);
        verifyEvent(EventType.CLIENT_CONNECTED, PING_REQ);
    }
}
