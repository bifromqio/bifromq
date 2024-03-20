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


import static com.baidu.bifromq.mqtt.handler.MQTTSessionIdUtil.userSessionId;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS1_PUSHED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS2_PUSHED;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.mqtt.handler.ChannelAttrs;
import com.baidu.bifromq.mqtt.handler.TenantSettings;
import com.baidu.bifromq.mqtt.session.MQTTSessionContext;
import com.baidu.bifromq.type.QoS;
import com.google.common.collect.Lists;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class MQTT3TransientSessionHandlerTest extends BaseSessionHandlerTest {

    private MQTT3TransientSessionHandler transientSessionHandler;
    private boolean shouldCleanSubs = false;

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) {
        super.setup(method);
        int keepAlive = 2;
        sessionContext = MQTTSessionContext.builder()
            .serverId(serverId)
            .ticker(testTicker)
            .defaultKeepAliveTimeSeconds(keepAlive)
            .distClient(distClient)
            .localDistService(localDistService)
            .retainClient(retainClient)
            .authProvider(authProvider)
            .localSessionRegistry(localSessionRegistry)
            .sessionDictClient(sessionDictClient)
            .eventCollector(eventCollector)
            .resourceThrottler(resourceThrottler)
            .settingProvider(settingProvider)
            .build();
        // common mocks
        mockSettings();
        ChannelDuplexHandler sessionHandlerAdder = new ChannelDuplexHandler() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                super.channelActive(ctx);
                ctx.pipeline().addLast(MQTT3TransientSessionHandler.builder()
                    .settings(new TenantSettings(tenantId, settingProvider))
                    .userSessionId(userSessionId(clientInfo))
                    .keepAliveTimeSeconds(120)
                    .clientInfo(clientInfo)
                    .willMessage(null)
                    .ctx(ctx)
                    .build());
                ctx.pipeline().remove(this);
            }
        };
        channel = new EmbeddedChannel(true, true, new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                ch.attr(ChannelAttrs.MQTT_SESSION_CTX).set(sessionContext);
                ch.attr(ChannelAttrs.PEER_ADDR).set(new InetSocketAddress(remoteIp, remotePort));
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new ChannelTrafficShapingHandler(512 * 1024, 512 * 1024));
                pipeline.addLast(MqttDecoder.class.getName(), new MqttDecoder(256 * 1024));
                pipeline.addLast(sessionHandlerAdder);
            }
        });
        //channel.freezeTime();
        transientSessionHandler = (MQTT3TransientSessionHandler) channel.pipeline().last();
    }

    @SneakyThrows
    @AfterMethod(alwaysRun = true)
    public void tearDown(Method method) {
        super.tearDown(method);
        if (shouldCleanSubs) {
            when(distClient.unmatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(null));
            channel.close();
            verify(distClient, atLeast(1))
                .unmatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt());
        } else {
            channel.close();
        }
        shouldCleanSubs = false;
    }


//  =============================================== S2CPub ============================================================


    @Test
    public void qoS1PubAndNoAck() {
        mockCheckPermission(true);
        mockDistMatch(true);
        transientSessionHandler.subscribe(System.nanoTime(), topicFilter, QoS.AT_LEAST_ONCE);
        channel.runPendingTasks();

        int messageCount = 3;
        transientSessionHandler.publish(matchInfo(topicFilter), s2cMessageList(topic, messageCount, QoS.AT_LEAST_ONCE));
        channel.runPendingTasks();
        // s2c pub received and not ack
        List<Integer> packetIds = Lists.newArrayList();
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_LEAST_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), topic);
            packetIds.add(message.variableHeader().packetId());
        }
        // resent once
        testTicker.advanceTimeBy(11, TimeUnit.SECONDS);
        channel.advanceTimeBy(11, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        channel.flushOutbound();
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_LEAST_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), topic);
            assertEquals(message.variableHeader().packetId(), (int) packetIds.get(i));
        }
        // resent twice
        testTicker.advanceTimeBy(11, TimeUnit.SECONDS);
        channel.advanceTimeBy(11, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        channel.flushOutbound();
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_LEAST_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), topic);
            assertEquals(message.variableHeader().packetId(), (int) packetIds.get(i));
        }
        // resent three times
        testTicker.advanceTimeBy(11, TimeUnit.SECONDS);
        channel.advanceTimeBy(11, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        channel.flushOutbound();
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.AT_LEAST_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), topic);
            assertEquals(message.variableHeader().packetId(), (int) packetIds.get(i));
        }

        verifyEvent(QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED,
            QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED);
    }

    @Test
    public void qoS2PubAndNoRec() {
        mockCheckPermission(true);
        mockDistMatch(true);
        transientSessionHandler.subscribe(System.nanoTime(), topicFilter, QoS.EXACTLY_ONCE);
        channel.runPendingTasks();

        int messageCount = 1;
        transientSessionHandler.publish(matchInfo(topicFilter), s2cMessageList(topic, messageCount, QoS.EXACTLY_ONCE));
        channel.runPendingTasks();
        // s2c pub received and not ack
        List<Integer> packetIds = Lists.newArrayList();
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.EXACTLY_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), topic);
            packetIds.add(message.variableHeader().packetId());
        }
        // resent once
        testTicker.advanceTimeBy(11, TimeUnit.SECONDS);
        channel.advanceTimeBy(11, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        channel.flushOutbound();
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.EXACTLY_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), topic);
            assertEquals(message.variableHeader().packetId(), (int) packetIds.get(i));
        }
        // resent twice
        testTicker.advanceTimeBy(11, TimeUnit.SECONDS);
        channel.advanceTimeBy(11, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        channel.flushOutbound();
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.EXACTLY_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), topic);
            assertEquals(message.variableHeader().packetId(), (int) packetIds.get(i));
        }
        // resent three times
        testTicker.advanceTimeBy(11, TimeUnit.SECONDS);
        channel.advanceTimeBy(11, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        channel.flushOutbound();
        for (int i = 0; i < messageCount; i++) {
            MqttPublishMessage message = channel.readOutbound();
            assertEquals(message.fixedHeader().qosLevel().value(), QoS.EXACTLY_ONCE_VALUE);
            assertEquals(message.variableHeader().topicName(), topic);
            assertEquals(message.variableHeader().packetId(), (int) packetIds.get(i));
        }
        verifyEvent(QOS2_PUSHED, QOS2_PUSHED, QOS2_PUSHED, QOS2_PUSHED);
    }

}
