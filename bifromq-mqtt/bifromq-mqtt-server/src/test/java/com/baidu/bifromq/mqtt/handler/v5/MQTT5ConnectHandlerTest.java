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

import static com.baidu.bifromq.plugin.eventcollector.EventType.MALFORMED_CLIENT_IDENTIFIER;
import static com.baidu.bifromq.plugin.eventcollector.EventType.MALFORMED_USERNAME;
import static com.baidu.bifromq.plugin.eventcollector.EventType.MALFORMED_WILL_TOPIC;
import static com.baidu.bifromq.plugin.eventcollector.EventType.PROTOCOL_ERROR;
import static com.baidu.bifromq.plugin.eventcollector.EventType.RESOURCE_THROTTLED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_MALFORMED_PACKET;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_PROTOCOL_ERROR;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_QUOTA_EXCEEDED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.mqtt.MockableTest;
import com.baidu.bifromq.mqtt.handler.ChannelAttrs;
import com.baidu.bifromq.mqtt.session.MQTTSessionContext;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.authprovider.type.MQTT5AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT5AuthResult;
import com.baidu.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthData;
import com.baidu.bifromq.plugin.authprovider.type.Success;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ResourceThrottled;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.bifromq.plugin.resourcethrottler.IResourceThrottler;
import com.bifromq.plugin.resourcethrottler.TenantResourceType;
import com.google.protobuf.ByteString;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttVersion;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MQTT5ConnectHandlerTest extends MockableTest {

    private MQTT5ConnectHandler connectHandler;
    private EmbeddedChannel channel;
    @Mock
    private IAuthProvider authProvider;
    @Mock
    private IInboxClient inboxClient;
    @Mock
    private IEventCollector eventCollector;

    @Mock
    private IResourceThrottler resourceThrottler;

    private ISettingProvider settingProvider = Setting::current;
    private final String serverId = "serverId";
    private final int keepAlive = 2;
    private final String remoteIp = "127.0.0.1";
    private final int remotePort = 8888;
    private MQTTSessionContext sessionContext;

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        connectHandler = new MQTT5ConnectHandler();
        when(resourceThrottler.hasResource(anyString(), any())).thenReturn(true);
        sessionContext = MQTTSessionContext.builder()
            .serverId(serverId)
            .defaultKeepAliveTimeSeconds(keepAlive)
            .authProvider(authProvider)
            .inboxClient(inboxClient)
            .eventCollector(eventCollector)
            .resourceThrottler(resourceThrottler)
            .settingProvider(settingProvider)
            .build();
        channel = new EmbeddedChannel(true, true, new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                ch.attr(ChannelAttrs.MQTT_SESSION_CTX).set(sessionContext);
                ch.attr(ChannelAttrs.PEER_ADDR).set(new InetSocketAddress(remoteIp, remotePort));
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(connectHandler);
            }
        });
        channel.freezeTime();
    }

    @Test
    public void sanityCheck_Malformed_ClientId() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("\u0000client")
            .protocolVersion(MqttVersion.MQTT_5)
            .build();
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        MqttConnAckMessage connAckMessage = channel.readOutbound();
        assertEquals(connAckMessage.variableHeader().connectReturnCode(),
            CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID);
        verify(eventCollector).report(argThat(event -> event.type() == MALFORMED_CLIENT_IDENTIFIER));
        assertFalse(channel.isOpen());
    }

    @Test
    public void sanityCheck_cleanStart_emptyClientId() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .cleanSession(true)
            .clientId("")
            .protocolVersion(MqttVersion.MQTT_5)
            .build();
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        MqttConnAckMessage connAckMessage = channel.readOutbound();
        assertEquals(connAckMessage.variableHeader().connectReturnCode(),
            CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID);
        verify(eventCollector).report(argThat(event -> event.type() == MALFORMED_CLIENT_IDENTIFIER));
        assertFalse(channel.isOpen());
    }

    @Test
    public void sanityCheck_malformed_username() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("clientId")
            .username("\u0000user")
            .protocolVersion(MqttVersion.MQTT_5)
            .build();
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        MqttConnAckMessage connAckMessage = channel.readOutbound();
        assertEquals(connAckMessage.variableHeader().connectReturnCode(),
            CONNECTION_REFUSED_MALFORMED_PACKET);
        verify(eventCollector).report(argThat(event -> event.type() == MALFORMED_USERNAME));
        assertFalse(channel.isOpen());
    }

    @Test
    public void sanityCheck_no_auth_method() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("clientId")
            .properties(MQTT5MessageUtils.mqttProps()
                .addAuthData(ByteString.copyFromUtf8("authData"))
                .build())
            .protocolVersion(MqttVersion.MQTT_5)
            .build();
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        MqttConnAckMessage connAckMessage = channel.readOutbound();
        assertEquals(connAckMessage.variableHeader().connectReturnCode(),
            CONNECTION_REFUSED_PROTOCOL_ERROR);
        verify(eventCollector).report(argThat(event -> event.type() == PROTOCOL_ERROR));
        assertFalse(channel.isOpen());
    }

    @Test
    public void sanityCheck_malformed_will_topic() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("clientId")
            .willFlag(true)
            .willTopic("\u0000topic")
            .protocolVersion(MqttVersion.MQTT_5)
            .build();
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        MqttConnAckMessage connAckMessage = channel.readOutbound();
        assertEquals(connAckMessage.variableHeader().connectReturnCode(),
            CONNECTION_REFUSED_MALFORMED_PACKET);
        verify(eventCollector).report(argThat(event -> event.type() == MALFORMED_WILL_TOPIC));
        assertFalse(channel.isOpen());
    }

    @Test
    public void authMethodCalled() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_5)
            .build();
        when(authProvider.auth(any(MQTT5AuthData.class))).thenReturn(new CompletableFuture<>());
        ArgumentCaptor<MQTT5AuthData> authDataCaptor = ArgumentCaptor.forClass(MQTT5AuthData.class);
        channel.writeInbound(connMsg);
        verify(authProvider).auth(authDataCaptor.capture());
        MQTT5AuthData authData = authDataCaptor.getValue();
        assertEquals(authData.getClientId(), "client");
    }

    @Test
    public void extAuthMethodCalled() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_5)
            .properties(MQTT5MessageUtils.mqttProps()
                .addAuthMethod("authMethod")
                .addAuthData(ByteString.copyFromUtf8("authData"))
                .build())
            .build();
        when(authProvider.extendedAuth(any(MQTT5ExtendedAuthData.class))).thenReturn(new CompletableFuture<>());
        ArgumentCaptor<MQTT5ExtendedAuthData> authDataCaptor = ArgumentCaptor.forClass(MQTT5ExtendedAuthData.class);
        channel.writeInbound(connMsg);
        verify(authProvider).extendedAuth(authDataCaptor.capture());
        MQTT5ExtendedAuthData authData = authDataCaptor.getValue();
        assertTrue(authData.hasInitial());
        assertEquals(authData.getInitial().getAuthMethod(), "authMethod");
        assertEquals(authData.getInitial().getAuthData(), ByteString.copyFromUtf8("authData"));
    }


    @Test
    public void noTotalConnectionResource() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_5)
            .build();
        when(resourceThrottler.hasResource("tenantId", TenantResourceType.TotalConnections)).thenReturn(false);
        when(authProvider.auth(any(MQTT5AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT5AuthResult.newBuilder()
                .setSuccess(Success.newBuilder().setTenantId("tenantId").build())
                .build()));
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        MqttConnAckMessage connAckMessage = channel.readOutbound();
        assertEquals(connAckMessage.variableHeader().connectReturnCode(),
            CONNECTION_REFUSED_QUOTA_EXCEEDED);
        verify(eventCollector).report(argThat(event -> event.type() == RESOURCE_THROTTLED &&
            ((ResourceThrottled) event).reason().equals(TenantResourceType.TotalConnections.name())));
        assertFalse(channel.isOpen());
    }

    @Test
    public void noSessionMemoryResource() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_5)
            .build();
        when(resourceThrottler.hasResource("tenantId", TenantResourceType.TotalSessionMemoryBytes)).thenReturn(false);
        when(authProvider.auth(any(MQTT5AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT5AuthResult.newBuilder()
                .setSuccess(Success.newBuilder().setTenantId("tenantId").build())
                .build()));
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        MqttConnAckMessage connAckMessage = channel.readOutbound();
        assertEquals(connAckMessage.variableHeader().connectReturnCode(),
            CONNECTION_REFUSED_QUOTA_EXCEEDED);
        verify(eventCollector).report(argThat(event -> event.type() == RESOURCE_THROTTLED &&
            ((ResourceThrottled) event).reason().equals(TenantResourceType.TotalSessionMemoryBytes.name())));
        assertFalse(channel.isOpen());
    }

    @Test
    public void noTotalConnectPerSecondResource() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_5)
            .build();
        when(resourceThrottler.hasResource("tenantId", TenantResourceType.TotalConnectPerSecond)).thenReturn(false);
        when(authProvider.auth(any(MQTT5AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT5AuthResult.newBuilder()
                .setSuccess(Success.newBuilder().setTenantId("tenantId").build())
                .build()));
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        MqttConnAckMessage connAckMessage = channel.readOutbound();
        assertEquals(connAckMessage.variableHeader().connectReturnCode(),
            CONNECTION_REFUSED_QUOTA_EXCEEDED);
        verify(eventCollector).report(argThat(event -> event.type() == RESOURCE_THROTTLED &&
            ((ResourceThrottled) event).reason().equals(TenantResourceType.TotalConnectPerSecond.name())));
        assertFalse(channel.isOpen());
    }
}
