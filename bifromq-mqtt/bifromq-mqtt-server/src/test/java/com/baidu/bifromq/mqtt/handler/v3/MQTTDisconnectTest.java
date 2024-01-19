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


import static com.baidu.bifromq.plugin.eventcollector.EventType.BAD_PACKET;
import static com.baidu.bifromq.plugin.eventcollector.EventType.BY_CLIENT;
import static com.baidu.bifromq.plugin.eventcollector.EventType.BY_SERVER;
import static com.baidu.bifromq.plugin.eventcollector.EventType.CLIENT_CONNECTED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.IDLE;
import static com.baidu.bifromq.plugin.eventcollector.EventType.PROTOCOL_VIOLATION;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.inbox.rpc.proto.AttachReply;
import com.baidu.bifromq.inbox.rpc.proto.DetachReply;
import com.baidu.bifromq.inbox.rpc.proto.ExpireReply;
import com.baidu.bifromq.inbox.storage.proto.InboxVersion;
import com.baidu.bifromq.mqtt.utils.MQTTMessageUtils;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class MQTTDisconnectTest extends BaseMQTTTest {

    @Test
    public void transientSession() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExpire(ExpireReply.Code.OK);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);

        channel.writeInbound(MQTTMessageUtils.disconnectMessage());

        Assert.assertFalse(channel.isActive());
        verifyEvent(CLIENT_CONNECTED, BY_CLIENT);
    }

    @Test
    public void persistentSession() {
        mockAuthPass();
        mockSessionReg();
        mockInboxReader();
        mockInboxGet(InboxVersion.newBuilder().setIncarnation(1).build());
        mockAttach(AttachReply.Code.OK);
        mockDetach(DetachReply.Code.OK);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(false);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);

        channel.writeInbound(MQTTMessageUtils.disconnectMessage());
        Assert.assertFalse(channel.isActive());
        verifyEvent(CLIENT_CONNECTED, BY_CLIENT);
    }

    @Test
    public void idle() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExpire(ExpireReply.Code.NOT_FOUND);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();

        channel.advanceTimeBy(50, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(50, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(CLIENT_CONNECTED, IDLE);
    }

    @Test
    public void idle2() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExpire(ExpireReply.Code.NOT_FOUND);
        // keepalive = 0
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true, "abc", 0);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();

        channel.advanceTimeBy(sessionContext.defaultKeepAliveTimeSeconds * 2L, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(sessionContext.defaultKeepAliveTimeSeconds * 2L, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(CLIENT_CONNECTED, IDLE);
    }

    @Test
    public void idle3() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExpire(ExpireReply.Code.NOT_FOUND);
        // keepalive too short, least is 5s
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true, "abc", 1);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();

        channel.advanceTimeBy(10, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(10, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(CLIENT_CONNECTED, IDLE);
    }

    @Test
    public void idle4() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExpire(ExpireReply.Code.NOT_FOUND);
        // keepalive too long, max is 7200s
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true, "abc", 100000);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();

        channel.advanceTimeBy((int) (7200 * 1.5) + 1, TimeUnit.SECONDS);
        testTicker.advanceTimeBy((int) (7200 * 1.5) + 1, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(CLIENT_CONNECTED, IDLE);
    }

    @Test
    public void connectTwice() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExpire(ExpireReply.Code.NOT_FOUND);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();

        connectMessage = MQTTMessageUtils.mqttConnectMessage(true, clientId, 0);
        channel.writeInbound(connectMessage);
        channel.advanceTimeBy(5, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(CLIENT_CONNECTED, PROTOCOL_VIOLATION);
    }

    @Test
    public void disconnectByServer() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExpire(ExpireReply.Code.NOT_FOUND);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();

        sessionRegistry.disconnectAll(1000);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(CLIENT_CONNECTED, BY_SERVER);
    }

    @Test
    public void badPacketAfterConnected() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExpire(ExpireReply.Code.NOT_FOUND);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();

        MqttMessage mqttMessage = MQTTMessageUtils.failedToDecodeMessage();
        channel.writeInbound(mqttMessage);
        channel.advanceTimeBy(5, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(CLIENT_CONNECTED, BAD_PACKET);
    }
}
