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


import static com.baidu.bifromq.plugin.eventcollector.EventType.ACCESS_CONTROL_ERROR;
import static com.baidu.bifromq.plugin.eventcollector.EventType.CLIENT_CONNECTED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.IDLE;
import static com.baidu.bifromq.plugin.eventcollector.EventType.KICKED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.MSG_RETAINED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.MSG_RETAINED_ERROR;
import static com.baidu.bifromq.plugin.eventcollector.EventType.PUB_ACTION_DISALLOW;
import static com.baidu.bifromq.plugin.eventcollector.EventType.RETAIN_MSG_CLEARED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.WILL_DISTED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.WILL_DIST_ERROR;
import static com.baidu.bifromq.plugin.settingprovider.Setting.ByPassPermCheckError;
import static com.baidu.bifromq.retain.rpc.proto.RetainReply.Result.CLEARED;
import static com.baidu.bifromq.retain.rpc.proto.RetainReply.Result.ERROR;
import static com.baidu.bifromq.retain.rpc.proto.RetainReply.Result.RETAINED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.baidu.bifromq.mqtt.handler.BaseMQTTTest;
import com.baidu.bifromq.plugin.authprovider.CheckResult;
import com.baidu.bifromq.plugin.authprovider.CheckResult.Type;
import com.baidu.bifromq.sessiondict.rpc.proto.Quit;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.MQTT3ClientInfo;
import com.baidu.bifromq.type.QoS;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class MQTTWillMessageTest extends BaseMQTTTest {

    @Test
    public void willWhenIdle() {
        connectAndVerify(true, false, 30, true, false);
        mockAuthCheck(Type.ALLOW);
        mockDistDist(true);
        channel.advanceTimeBy(100, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(50, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(3, CLIENT_CONNECTED, IDLE, WILL_DISTED);
    }

//    @Test
//    public void willWhenSelfKick() {
//        connectAndVerify(true, false, 30, true, false);
//        mockAuthCheck(CheckResult.Type.ALLOW);
//        mockDistDist(true);
//        kickSubject.onNext(
//            Quit.newBuilder()
//                .setKiller(
//                    ClientInfo.newBuilder()
//                        .setTrafficId(trafficId)
//                        .setUserId(userId)
//                        .setMqtt3ClientInfo(
//                            MQTT3ClientInfo.newBuilder()
//                                .setClientId(clientId)
//                                .build()
//                        )
//                        .build()
//                )
//                .build()
//        );
//        channel.runPendingTasks();
//        Assert.assertFalse(channel.isActive());
//        verifyEvent(3, ClientConnected, Kicked, WillDisted);
//    }

    @Test
    public void willWhenNotSelfKick() {
        connectAndVerify(true, false, 30, true, false);
        mockAuthCheck(CheckResult.Type.ALLOW);
        mockDistDist(true);
        kickSubject.onNext(
            Quit.newBuilder()
                .setKiller(
                    ClientInfo.newBuilder()
                        .setTrafficId("sys")
                        .setUserId("sys")
                        .setMqtt3ClientInfo(
                            MQTT3ClientInfo.newBuilder()
                                .setClientId(clientId)
                                .build()
                        )
                        .build()
                )
                .build()
        );
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(3, CLIENT_CONNECTED, KICKED, WILL_DISTED);
    }

    @Test
    public void willAuthCheckError() {
        // by pass
        connectAndVerify(true, false, 30, true, false);
        mockAuthCheck(Type.ERROR);
        mockDistDist(true);
        channel.advanceTimeBy(50, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(50, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(4, CLIENT_CONNECTED, IDLE, ACCESS_CONTROL_ERROR, WILL_DISTED);
        verify(distClient, times(1)).dist(anyLong(), anyString(), any(QoS.class), any(ByteBuffer.class), anyInt(),
            any(ClientInfo.class));
    }

    @Test
    public void willAuthCheckError2() {
        // not by pass
        connectAndVerify(true, false, 30, true, false);
        Mockito.lenient().when(settingProvider.provide(eq(ByPassPermCheckError), any(ClientInfo.class)))
            .thenReturn(false);
        mockAuthCheck(Type.ERROR);
        channel.advanceTimeBy(50, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(50, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(3, CLIENT_CONNECTED, IDLE, ACCESS_CONTROL_ERROR);
        verify(distClient, times(0)).dist(anyLong(), anyString(), any(QoS.class), any(ByteBuffer.class), anyInt(),
            any(ClientInfo.class));
    }

    @Test
    public void willAuthCheckFailed() {
        connectAndVerify(true, false, 30, true, false);
        mockAuthCheck(Type.DISALLOW);
        channel.advanceTimeBy(50, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(50, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(3, CLIENT_CONNECTED, IDLE, PUB_ACTION_DISALLOW);
        verify(distClient, times(0)).dist(anyLong(), anyString(), any(QoS.class), any(ByteBuffer.class), anyInt(),
            any(ClientInfo.class));
    }

    @Test
    public void willDistError() {
        connectAndVerify(true, false, 30, true, false);
        mockAuthCheck(Type.ALLOW);
        mockDistDist(false);
        channel.advanceTimeBy(50, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(50, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(3, CLIENT_CONNECTED, IDLE, WILL_DIST_ERROR);
    }

    @Test
    public void willDistDrop() {
        connectAndVerify(true, false, 30, true, false);
        mockAuthCheck(Type.ALLOW);
        mockDistDrop();
        channel.advanceTimeBy(50, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(50, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(3, CLIENT_CONNECTED, IDLE, WILL_DIST_ERROR);
    }


    @Test
    public void willAndRetain() {
        connectAndVerify(true, false, 30, true, true);
        mockAuthCheck(Type.ALLOW);
        mockDistDist(true);
        mockRetainPipeline(RETAINED);
        channel.advanceTimeBy(50, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(50, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(4, CLIENT_CONNECTED, IDLE, WILL_DISTED, MSG_RETAINED);
    }


    @Test
    public void willAndRetainClear() {
        connectAndVerify(true, false, 30, true, true);
        mockAuthCheck(Type.ALLOW);
        mockDistDist(true);
        mockRetainPipeline(CLEARED);
        channel.advanceTimeBy(50, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(50, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(4, CLIENT_CONNECTED, IDLE, WILL_DISTED, RETAIN_MSG_CLEARED);
    }

    @Test
    public void willAndRetainError() {
        connectAndVerify(true, false, 30, true, true);
        mockAuthCheck(Type.ALLOW);
        mockDistDist(true);
        mockRetainPipeline(ERROR);
        channel.advanceTimeBy(50, TimeUnit.SECONDS);
        testTicker.advanceTimeBy(50, TimeUnit.SECONDS);
        channel.runPendingTasks();
        Assert.assertFalse(channel.isActive());
        verifyEvent(4, CLIENT_CONNECTED, IDLE, WILL_DISTED, MSG_RETAINED_ERROR);
    }
}
