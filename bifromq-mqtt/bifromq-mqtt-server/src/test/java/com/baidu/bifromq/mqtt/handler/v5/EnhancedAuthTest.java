/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.mqtt.handler.v5;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.rpc.proto.ExpireReply;
import com.baidu.bifromq.mqtt.MockableTest;
import com.baidu.bifromq.mqtt.handler.ChannelAttrs;
import com.baidu.bifromq.mqtt.handler.ConditionalSlowDownHandler;
import com.baidu.bifromq.mqtt.handler.ConnectionRateLimitHandler;
import com.baidu.bifromq.mqtt.handler.MQTTConnectHandler;
import com.baidu.bifromq.mqtt.handler.MQTTMessageDebounceHandler;
import com.baidu.bifromq.mqtt.handler.MemPressureCondition;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5AuthReasonCode;
import com.baidu.bifromq.mqtt.service.ILocalSessionRegistry;
import com.baidu.bifromq.mqtt.service.LocalSessionRegistry;
import com.baidu.bifromq.mqtt.session.MQTTSessionContext;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.authprovider.type.Continue;
import com.baidu.bifromq.plugin.authprovider.type.Failed;
import com.baidu.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthResult;
import com.baidu.bifromq.plugin.authprovider.type.Success;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import com.bifromq.plugin.resourcethrottler.IResourceThrottler;
import com.google.protobuf.ByteString;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class EnhancedAuthTest extends MockableTest {
    private MQTTConnectHandler connectHandler;
    private EmbeddedChannel channel;
    @Mock
    private IAuthProvider authProvider;
    @Mock
    private IInboxClient inboxClient;
    @Mock
    private IEventCollector eventCollector;
    @Mock
    private IResourceThrottler resourceThrottler;
    private ILocalSessionRegistry localSessionRegistry = new LocalSessionRegistry();
    @Mock
    private ISessionDictClient sessionDictClient;
    private ISettingProvider settingProvider = Setting::current;
    private final String serverId = "serverId";
    private final int keepAlive = 2;
    private final String remoteIp = "127.0.0.1";
    private final int remotePort = 8888;
    private MQTTSessionContext sessionContext;

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        connectHandler = new MQTT5ConnectHandler();
        when(resourceThrottler.hasResource(any(), any())).thenReturn(true);
        when(inboxClient.expire(any())).thenReturn(CompletableFuture.completedFuture(
            ExpireReply.newBuilder()
                .setCode(ExpireReply.Code.OK)
                .build()));
        sessionContext = MQTTSessionContext.builder()
            .serverId(serverId)
            .defaultKeepAliveTimeSeconds(keepAlive)
            .inboxClient(inboxClient)
            .eventCollector(eventCollector)
            .resourceThrottler(resourceThrottler)
            .settingProvider(settingProvider)
            .authProvider(authProvider)
            .localSessionRegistry(localSessionRegistry)
            .sessionDictClient(sessionDictClient)
            .build();
        channel = new EmbeddedChannel(true, true, new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                ch.attr(ChannelAttrs.MQTT_SESSION_CTX).set(sessionContext);
                ch.attr(ChannelAttrs.PEER_ADDR).set(new InetSocketAddress(remoteIp, remotePort));
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast("connRateLimiter", new ConnectionRateLimitHandler(10));
                pipeline.addLast("trafficShaper",
                    new ChannelTrafficShapingHandler(512 * 1024, 512 * 1024));
                pipeline.addLast(MqttDecoder.class.getName(), new MqttDecoder());
                pipeline.addLast(MQTTMessageDebounceHandler.NAME, new MQTTMessageDebounceHandler());
                pipeline.addLast(ConditionalSlowDownHandler.NAME,
                    new ConditionalSlowDownHandler(MemPressureCondition.INSTANCE));
                pipeline.addLast(connectHandler);
            }
        });
    }

    @Test
    public void testAuthMethodNotSupport() {
        Mockito.reset(authProvider);
        when(authProvider.extendedAuth(any(MQTT5ExtendedAuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(
                MQTT5ExtendedAuthResult.newBuilder()
                    .setFailed(Failed.newBuilder()
                        .setCode(Failed.Code.NotAuthorized)
                        .setReason("Not supported")
                        .build())
                    .build()));
        MqttConnectMessage connect = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_5)
            .properties(MQTT5MessageUtils.mqttProps()
                .addAuthMethod("authMethod")
                .addAuthData(ByteString.copyFrom("authData", StandardCharsets.UTF_8))
                .build())
            .build();
        channel.writeInbound(connect);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        assertFalse(channel.isOpen());
    }

    @Test
    public void testAuthSuccess() {
        MqttConnectMessage connect = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_5)
            .cleanSession(true)
            .keepAlive(2)
            .properties(MQTT5MessageUtils.mqttProps()
                .addAuthMethod("authMethod")
                .addAuthData(ByteString.copyFrom("authData", StandardCharsets.UTF_8))
                .build())
            .build();
        Mockito.reset(authProvider);
        when(authProvider.extendedAuth(any(MQTT5ExtendedAuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(
                MQTT5ExtendedAuthResult.newBuilder()
                    .setSuccess(Success.newBuilder().build())
                    .build()));
        channel.writeInbound(connect);
        MqttConnAckMessage connAckMessage = channel.readOutbound();
        assertEquals(connAckMessage.variableHeader().connectReturnCode(), MqttConnectReturnCode.CONNECTION_ACCEPTED);
    }

    @Test
    public void testAuthSuccess2() {
        MqttConnectMessage connect = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_5)
            .cleanSession(true)
            .properties(MQTT5MessageUtils.mqttProps()
                .addAuthMethod("authMethod")
                .addAuthData(ByteString.copyFrom("authData", StandardCharsets.UTF_8))
                .build())
            .build();
        String challenge = "challenge";
        Mockito.reset(authProvider);
        when(authProvider.extendedAuth(any(MQTT5ExtendedAuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(
                MQTT5ExtendedAuthResult.newBuilder()
                    .setContinue(Continue.newBuilder()
                        .setAuthData(ByteString.copyFromUtf8(challenge))
                        .build())
                    .build()));
        channel.writeInbound(connect);
        MqttMessage authMessage = channel.readOutbound();
        MqttProperties properties =
            ((MqttReasonCodeAndPropertiesVariableHeader) authMessage.variableHeader()).properties();
        String authMethod = MQTT5MessageUtils.authMethod(properties).orElseThrow();
        ByteString authData = MQTT5MessageUtils.authData(properties).orElseThrow();
        assertEquals(authMethod, "authMethod");
        assertEquals(authData.toString(StandardCharsets.UTF_8), challenge);

        Mockito.reset(authProvider);
        when(authProvider.extendedAuth(any(MQTT5ExtendedAuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(
                MQTT5ExtendedAuthResult.newBuilder()
                    .setSuccess(Success.newBuilder().setTenantId("tenant").setUserId("user").build())
                    .build()));
        channel.writeInbound(MqttMessageBuilders.auth()
            .reasonCode(MQTT5AuthReasonCode.Continue.value())
            .properties(MQTT5MessageUtils.mqttProps()
                .addAuthMethod("authMethod")
                .addAuthData(ByteString.copyFrom("authData2", StandardCharsets.UTF_8))
                .build())
            .build());
        MqttConnAckMessage connAckMessage = channel.readOutbound();
        assertEquals(connAckMessage.variableHeader().connectReturnCode(), MqttConnectReturnCode.CONNECTION_ACCEPTED);
    }

    @Test
    public void testReasonCodeWrong() {
        MqttConnectMessage connect = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_5)
            .cleanSession(true)
            .properties(MQTT5MessageUtils.mqttProps()
                .addAuthMethod("authMethod")
                .addAuthData(ByteString.copyFrom("authData", StandardCharsets.UTF_8))
                .build())
            .build();
        String challenge = "challenge";
        Mockito.reset(authProvider);
        when(authProvider.extendedAuth(any(MQTT5ExtendedAuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(
                MQTT5ExtendedAuthResult.newBuilder()
                    .setContinue(Continue.newBuilder()
                        .setAuthData(ByteString.copyFromUtf8(challenge))
                        .build())
                    .build()));
        channel.writeInbound(connect);
        MqttMessage authMessage = channel.readOutbound();
        MqttProperties properties =
            ((MqttReasonCodeAndPropertiesVariableHeader) authMessage.variableHeader()).properties();
        String authMethod = MQTT5MessageUtils.authMethod(properties).orElseThrow();
        ByteString authData = MQTT5MessageUtils.authData(properties).orElseThrow();
        assertEquals(authMethod, "authMethod");
        assertEquals(authData.toString(StandardCharsets.UTF_8), challenge);

        channel.writeInbound(MqttMessageBuilders.auth()
            // wrong reason code!
            .reasonCode(MQTT5AuthReasonCode.Success.value())
            .properties(MQTT5MessageUtils.mqttProps()
                .addAuthMethod("authMethod")
                .addAuthData(ByteString.copyFrom("authData2", StandardCharsets.UTF_8))
                .build())
            .build());
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        assertFalse(channel.isOpen());
    }

    @Test
    public void testAuthAgainBeforeServerReply() {
        MqttConnectMessage connect = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_5)
            .cleanSession(true)
            .properties(MQTT5MessageUtils.mqttProps()
                .addAuthMethod("authMethod")
                .addAuthData(ByteString.copyFrom("authData", StandardCharsets.UTF_8))
                .build())
            .build();
        Mockito.reset(authProvider);

        when(authProvider.extendedAuth(any(MQTT5ExtendedAuthData.class))).thenReturn(new CompletableFuture<>());
        channel.writeInbound(connect);
        channel.writeInbound(MqttMessageBuilders.auth()
            .reasonCode(MQTT5AuthReasonCode.Continue.value())
            .properties(MQTT5MessageUtils.mqttProps()
                .addAuthMethod("authMethod")
                .addAuthData(ByteString.copyFrom("authData2", StandardCharsets.UTF_8))
                .build())
            .build());
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        assertFalse(channel.isOpen());
    }

    @Test
    public void testNoAuthMethod() {
        Mockito.reset(authProvider);
        when(authProvider.extendedAuth(any(MQTT5ExtendedAuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(
                MQTT5ExtendedAuthResult.newBuilder()
                    .setSuccess(Success.newBuilder().setTenantId("tenant").setUserId("user").build())
                    .build()));
        MqttConnectMessage connect = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_5)
            .properties(MQTT5MessageUtils.mqttProps()
                .addAuthData(ByteString.copyFrom("authData", StandardCharsets.UTF_8))
                .build())
            .build();
        channel.writeInbound(connect);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        assertFalse(channel.isOpen());
    }

    @Test
    public void testReAuth() {
        MqttConnectMessage connect = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_5)
            .cleanSession(true)
            .keepAlive(2)
            .properties(MQTT5MessageUtils.mqttProps()
                .addAuthMethod("authMethod")
                .addAuthData(ByteString.copyFrom("authData", StandardCharsets.UTF_8))
                .build())
            .build();
        Mockito.reset(authProvider);
        when(authProvider.extendedAuth(any(MQTT5ExtendedAuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(
                MQTT5ExtendedAuthResult.newBuilder()
                    .setSuccess(Success.newBuilder().build())
                    .build()));
        channel.writeInbound(connect);
        MqttConnAckMessage connAckMessage = channel.readOutbound();
        assertEquals(connAckMessage.variableHeader().connectReturnCode(), MqttConnectReturnCode.CONNECTION_ACCEPTED);

        channel.writeInbound(MqttMessageBuilders.auth()
            .reasonCode(MQTT5AuthReasonCode.ReAuth.value())
            .properties(MQTT5MessageUtils.mqttProps()
                .addAuthMethod("authMethod")
                .addAuthData(ByteString.copyFrom("reAuthData", StandardCharsets.UTF_8))
                .build())
            .build());
        MqttMessage reAuthMessage = channel.readOutbound();
        MqttReasonCodeAndPropertiesVariableHeader variableHeader =
            ((MqttReasonCodeAndPropertiesVariableHeader) reAuthMessage.variableHeader());
        MQTT5AuthReasonCode reasonCode = MQTT5AuthReasonCode.valueOf(variableHeader.reasonCode());
        assertEquals(reasonCode, MQTT5AuthReasonCode.Success);
    }
}
