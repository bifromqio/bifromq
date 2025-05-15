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
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.mqtt.IMQTTBroker;
import com.baidu.bifromq.mqtt.IMQTTBrokerBuilder;
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
import jakarta.inject.Singleton;
import java.util.Optional;

public class MQTTServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(new TypeLiteral<Optional<IMQTTBroker>>() {
        }).toProvider(MQTTBrokerServerProvider.class).in(Singleton.class);
    }

    private static class MQTTBrokerServerProvider implements Provider<Optional<IMQTTBroker>> {
        private final StandaloneConfig config;
        private final ServiceInjector injector;

        @Inject
        private MQTTBrokerServerProvider(StandaloneConfig config, ServiceInjector injector) {
            this.config = config;
            this.injector = injector;
        }

        @Override
        public Optional<IMQTTBroker> get() {
            MQTTServerConfig serverConfig = config.getMqttServiceConfig().getServer();
            if (!serverConfig.isEnable()) {
                return Optional.empty();
            }
            IMQTTBrokerBuilder brokerBuilder = IMQTTBroker.builder()
                .rpcServerBuilder(injector.getInstance(RPCServerBuilder.class))
                .mqttBossELGThreads(serverConfig.getBossELGThreads())
                .mqttWorkerELGThreads(serverConfig.getWorkerELGThreads())
                .authProvider(injector.getInstance(AuthProviderManager.class))
                .clientBalancer(injector.getInstance(ClientBalancerManager.class))
                .eventCollector(injector.getInstance(EventCollectorManager.class))
                .resourceThrottler(injector.getInstance(ResourceThrottlerManager.class))
                .settingProvider(injector.getInstance(SettingProviderManager.class))
                .distClient(injector.getInstance(IDistClient.class))
                .inboxClient(injector.getInstance(IInboxClient.class))
                .sessionDictClient(injector.getInstance(ISessionDictClient.class))
                .retainClient(injector.getInstance(IRetainClient.class))
                .connectTimeoutSeconds(serverConfig.getConnTimeoutSec())
                .connectRateLimit(serverConfig.getMaxConnPerSec())
                .disconnectRate(serverConfig.getMaxDisconnPerSec())
                .readLimit(serverConfig.getMaxConnBandwidth())
                .writeLimit(serverConfig.getMaxConnBandwidth())
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
}
