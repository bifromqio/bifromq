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

package com.baidu.bifromq.mqtt.handler;


import static com.baidu.bifromq.plugin.eventcollector.EventType.BAD_PACKET;
import static com.baidu.bifromq.plugin.eventcollector.EventType.BY_CLIENT;
import static com.baidu.bifromq.plugin.eventcollector.EventType.BY_SERVER;
import static com.baidu.bifromq.plugin.eventcollector.EventType.CLIENT_CONNECTED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.IDLE;
import static com.baidu.bifromq.plugin.eventcollector.EventType.PROTOCOL_VIOLATION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.mqtt.utils.MQTTMessageUtils;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class MQTTDisconnectTest extends BaseMQTTTest {

    @Test
    public void transientSession() {
        connectAndVerify(true, false);
        channel.writeInbound(MQTTMessageUtils.disconnectMessage());
        Assert.assertFalse(channel.isActive());
        verifyEvent(2, CLIENT_CONNECTED, BY_CLIENT);
    }

    @Test
    public void persistentSession() {
        connectAndVerify(false, true);
        channel.writeInbound(MQTTMessageUtils.disconnectMessage());
        Assert.assertFalse(channel.isActive());
        verifyEvent(2, CLIENT_CONNECTED, BY_CLIENT);
    }

    @Test
    public void idle() {
        connectAndVerify(true, false, 30);
        channel.advanceTimeBy(50, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(50, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(2, CLIENT_CONNECTED, IDLE);
    }

    @Test
    public void idle2() {
        // keepalive = 0
        connectAndVerify(true, false, 0);
        channel.advanceTimeBy(sessionContext.defaultKeepAliveTimeSeconds * 2L, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(sessionContext.defaultKeepAliveTimeSeconds * 2L, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(2, CLIENT_CONNECTED, IDLE);
    }

    @Test
    public void idle3() {
        // keepalive too short, least is 5s
        connectAndVerify(true, false, 1);
        channel.advanceTimeBy(10, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(10, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(2, CLIENT_CONNECTED, IDLE);
    }

    @Test
    public void idle4() {
        // keepalive too long, max is 7200s
        connectAndVerify(true, false, 100000);
        channel.advanceTimeBy((int) (7200 * 1.5) + 1, TimeUnit.SECONDS);
        testTicker.advanceTimeBy((int) (7200 * 1.5) + 1, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(2, CLIENT_CONNECTED, IDLE);
    }

    @Test
    public void connectTwice() {
        connectAndVerify(true, false);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true, clientId, 0);
        channel.writeInbound(connectMessage);
        channel.advanceTimeBy(5, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(2, CLIENT_CONNECTED, PROTOCOL_VIOLATION);
    }

    @Test
    public void disconnectByServer() {
        connectAndVerify(true, false);
        sessionBrokerServer.disconnectAll(1000);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(2, CLIENT_CONNECTED, BY_SERVER);
    }

    @Test
    public void badPacketAfterConnected() {
        connectAndVerify(true, false);
        MqttMessage mqttMessage = MQTTMessageUtils.failedToDecodeMessage();
        channel.writeInbound(mqttMessage);
        channel.advanceTimeBy(5, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(2, CLIENT_CONNECTED, BAD_PACKET);
    }
}
