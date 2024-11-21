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

package com.baidu.bifromq.starter.module;

import static com.baidu.bifromq.starter.module.SSLUtil.buildServerSslContext;

import com.baidu.bifromq.baserpc.server.RPCServerBuilder;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.mqtt.IMQTTBroker;
import com.baidu.bifromq.mqtt.IMQTTBrokerBuilder;
import com.baidu.bifromq.mqtt.inbox.IMqttBrokerClient;
import com.baidu.bifromq.plugin.authprovider.AuthProviderManager;
import com.baidu.bifromq.plugin.clientbalancer.ClientBalancerManager;
import com.baidu.bifromq.plugin.eventcollector.EventCollectorManager;
import com.baidu.bifromq.plugin.resourcethrottler.ResourceThrottlerManager;
import com.baidu.bifromq.plugin.settingprovider.SettingProviderManager;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import com.baidu.bifromq.starter.config.StandaloneConfig;
import com.baidu.bifromq.starter.config.model.mqtt.MQTTServerConfig;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import java.util.Optional;
import javax.inject.Named;

public class MQTTServiceModule extends AbstractModule {
    private static class MQTTBrokerClientProvider implements Provider<IMqttBrokerClient> {
        private final StandaloneConfig config;
        private final EventLoopGroup eventLoopGroup;
        private final SslContext sslContext;
        private final IRPCServiceTrafficService trafficService;

        @Inject
        private MQTTBrokerClientProvider(StandaloneConfig config,
                                         @Named("rpcClientEventLoop") EventLoopGroup eventLoopGroup,
                                         @Named("rpcClientSSLContext") Optional<SslContext> sslContext,
                                         IRPCServiceTrafficService trafficService) {
            this.config = config;
            this.eventLoopGroup = eventLoopGroup;
            this.sslContext = sslContext.orElse(null);
            this.trafficService = trafficService;
        }

        @Override
        public IMqttBrokerClient get() {
            return IMqttBrokerClient.newBuilder()
                .workerThreads(config.getMqttServiceConfig().getClient().getWorkerThreads())
                .trafficService(trafficService)
                .eventLoopGroup(eventLoopGroup)
                .sslContext(sslContext)
                .build();
        }
    }

    private static class MQTTBrokerServerProvider implements Provider<Optional<IMQTTBroker>> {
        private final StandaloneConfig config;
        private final RPCServerBuilder rpcServerBuilder;
        private final IDistClient distClient;
        private final IInboxClient inboxClient;
        private final IRetainClient retainClient;
        private final ISessionDictClient sessionDictClient;
        private final AuthProviderManager authProviderMgr;
        private final ClientBalancerManager clientBalancerMgr;
        private final EventCollectorManager eventCollectorMgr;
        private final SettingProviderManager settingProviderMgr;
        private final ResourceThrottlerManager resourceThrottlerMgr;

        @Inject
        private MQTTBrokerServerProvider(StandaloneConfig config,
                                         RPCServerBuilder rpcServerBuilder,
                                         IDistClient distClient,
                                         IInboxClient inboxClient,
                                         IRetainClient retainClient,
                                         ISessionDictClient sessionDictClient,
                                         AuthProviderManager authProviderMgr,
                                         ClientBalancerManager clientBalancerMgr,
                                         EventCollectorManager eventCollectorMgr,
                                         SettingProviderManager settingProviderMgr,
                                         ResourceThrottlerManager resourceThrottlerMgr) {
            this.config = config;
            this.rpcServerBuilder = rpcServerBuilder;
            this.distClient = distClient;
            this.inboxClient = inboxClient;
            this.retainClient = retainClient;
            this.sessionDictClient = sessionDictClient;
            this.authProviderMgr = authProviderMgr;
            this.clientBalancerMgr = clientBalancerMgr;
            this.eventCollectorMgr = eventCollectorMgr;
            this.settingProviderMgr = settingProviderMgr;
            this.resourceThrottlerMgr = resourceThrottlerMgr;
        }

        @Override
        public Optional<IMQTTBroker> get() {
            MQTTServerConfig serverConfig = config.getMqttServiceConfig().getServer();
            if (!serverConfig.isEnable()) {
                return Optional.empty();
            }
            IMQTTBrokerBuilder brokerBuilder = IMQTTBroker.builder()
                .rpcServerBuilder(rpcServerBuilder)
                .mqttBossELGThreads(serverConfig.getBossELGThreads())
                .mqttWorkerELGThreads(serverConfig.getWorkerELGThreads())
                .authProvider(authProviderMgr)
                .clientBalancer(clientBalancerMgr)
                .eventCollector(eventCollectorMgr)
                .resourceThrottler(resourceThrottlerMgr)
                .settingProvider(settingProviderMgr)
                .distClient(distClient)
                .inboxClient(inboxClient)
                .sessionDictClient(sessionDictClient)
                .retainClient(retainClient)
                .connectTimeoutSeconds(serverConfig.getConnTimeoutSec())
                .connectRateLimit(serverConfig.getMaxConnPerSec())
                .disconnectRate(serverConfig.getMaxDisconnPerSec())
                .readLimit(serverConfig.getMaxConnBandwidth())
                .writeLimit(serverConfig.getMaxConnBandwidth())
                .defaultKeepAliveSeconds(serverConfig.getDefaultKeepAliveSec())
                .maxBytesInMessage(serverConfig.getMaxMsgByteSize());
            if (serverConfig.getTcpListener().isEnable()) {
                brokerBuilder.buildTcpConnListener()
                    .host(serverConfig.getTcpListener().getHost())
                    .port(serverConfig.getTcpListener().getPort())
                    .buildListener();
            }
            if (serverConfig.getTlsListener().isEnable()) {
                brokerBuilder.buildTLSConnListener()
                    .host(serverConfig.getTlsListener().getHost())
                    .port(serverConfig.getTlsListener().getPort())
                    .sslContext(buildServerSslContext(serverConfig.getTlsListener().getSslConfig()))
                    .buildListener();
            }
            if (serverConfig.getWsListener().isEnable()) {
                brokerBuilder.buildWSConnListener()
                    .host(serverConfig.getWsListener().getHost())
                    .port(serverConfig.getWsListener().getPort())
                    .path(serverConfig.getWsListener().getWsPath())
                    .buildListener();
            }
            if (serverConfig.getWssListener().isEnable()) {
                brokerBuilder.buildWSSConnListener()
                    .host(serverConfig.getWssListener().getHost())
                    .port(serverConfig.getWssListener().getPort())
                    .path(serverConfig.getWssListener().getWsPath())
                    .sslContext(buildServerSslContext(serverConfig.getWssListener().getSslConfig()))
                    .buildListener();
            }
            return Optional.of(brokerBuilder.build());
        }

    }

    @Override
    protected void configure() {
        bind(IMqttBrokerClient.class).toProvider(MQTTBrokerClientProvider.class).asEagerSingleton();
        bind(new TypeLiteral<Optional<IMQTTBroker>>() {
        }).toProvider(MQTTBrokerServerProvider.class)
            .asEagerSingleton();
    }
}
