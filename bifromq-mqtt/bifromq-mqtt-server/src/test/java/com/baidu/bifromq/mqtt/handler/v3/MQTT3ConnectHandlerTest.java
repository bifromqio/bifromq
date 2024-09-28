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

package com.baidu.bifromq.mqtt.handler.v3;

import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.mqtt.MockableTest;
import com.baidu.bifromq.mqtt.handler.ChannelAttrs;
import com.baidu.bifromq.mqtt.session.MQTTSessionContext;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import com.baidu.bifromq.plugin.authprovider.type.Ok;
import com.baidu.bifromq.plugin.clientbalancer.IClientBalancer;
import com.baidu.bifromq.plugin.clientbalancer.Redirection;
import com.baidu.bifromq.plugin.eventcollector.EventType;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.Redirect;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.bifromq.plugin.resourcethrottler.IResourceThrottler;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttVersion;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MQTT3ConnectHandlerTest extends MockableTest {

    private MQTT3ConnectHandler connectHandler;
    private EmbeddedChannel channel;
    @Mock
    private IClientBalancer clientBalancer;
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
        connectHandler = new MQTT3ConnectHandler();
        when(resourceThrottler.hasResource(anyString(), any())).thenReturn(true);
        when(clientBalancer.needRedirect(any())).thenReturn(Optional.empty());
        sessionContext = MQTTSessionContext.builder()
            .serverId(serverId)
            .defaultKeepAliveTimeSeconds(keepAlive)
            .authProvider(authProvider)
            .inboxClient(inboxClient)
            .eventCollector(eventCollector)
            .resourceThrottler(resourceThrottler)
            .settingProvider(settingProvider)
            .clientBalancer(clientBalancer)
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
    public void needMove() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .build();
        when(authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder().setTenantId("tenantId").build())
                .build()));
        when(clientBalancer.needRedirect(any())).thenReturn(
            Optional.of(new Redirection(true, Optional.of("server1"))));
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        MqttConnAckMessage connAckMessage = channel.readOutbound();
        assertEquals(connAckMessage.variableHeader().connectReturnCode(),
            CONNECTION_REFUSED_SERVER_UNAVAILABLE);
        assertFalse(channel.isOpen());
        verify(eventCollector).report(argThat(e -> e.type() == EventType.SERVER_REDIRECTED
            && ((Redirect) e).isPermanent() && ((Redirect) e).serverReference().equals("server1")));
    }

    @Test
    public void needUseAnotherServer() {
        MqttConnectMessage connMsg = MqttMessageBuilders.connect()
            .clientId("client")
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .build();
        when(authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder().setTenantId("tenantId").build())
                .build()));
        when(clientBalancer.needRedirect(any())).thenReturn(
            Optional.of(new Redirection(false, Optional.empty())));
        channel.writeInbound(connMsg);
        channel.advanceTimeBy(6, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        MqttConnAckMessage connAckMessage = channel.readOutbound();
        assertEquals(connAckMessage.variableHeader().connectReturnCode(),
            CONNECTION_REFUSED_SERVER_UNAVAILABLE);
        assertFalse(channel.isOpen());
        verify(eventCollector).report(argThat(e -> e.type() == EventType.SERVER_REDIRECTED
            && !((Redirect) e).isPermanent()));
    }
}
