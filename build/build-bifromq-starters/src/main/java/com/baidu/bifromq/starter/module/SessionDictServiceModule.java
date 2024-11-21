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

import com.baidu.bifromq.baserpc.server.RPCServerBuilder;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import com.baidu.bifromq.mqtt.inbox.IMqttBrokerClient;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import com.baidu.bifromq.sessiondict.server.ISessionDictServer;
import com.baidu.bifromq.starter.config.StandaloneConfig;
import com.baidu.bifromq.starter.config.model.dict.SessionDictServerConfig;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import java.util.Optional;
import javax.inject.Named;

public class SessionDictServiceModule extends AbstractModule {
    private static class SessionDictClientProvider implements Provider<ISessionDictClient> {
        private final StandaloneConfig config;
        private final EventLoopGroup eventLoopGroup;
        private final SslContext sslContext;
        private final IRPCServiceTrafficService trafficService;

        @Inject
        private SessionDictClientProvider(StandaloneConfig config,
                                          @Named("rpcClientEventLoop") EventLoopGroup eventLoopGroup,
                                          @Named("rpcClientSSLContext") Optional<SslContext> sslContext,
                                          IRPCServiceTrafficService trafficService) {
            this.config = config;
            this.eventLoopGroup = eventLoopGroup;
            this.sslContext = sslContext.orElse(null);
            this.trafficService = trafficService;
        }

        @Override
        public ISessionDictClient get() {
            return ISessionDictClient.newBuilder()
                .workerThreads(config.getSessionDictServiceConfig().getClient().getWorkerThreads())
                .trafficService(trafficService)
                .eventLoopGroup(eventLoopGroup)
                .sslContext(sslContext)
                .build();
        }
    }

    private static class SessionDictServerProvider implements Provider<Optional<ISessionDictServer>> {
        private final StandaloneConfig config;
        private final RPCServerBuilder rpcServerBuilder;
        private final IMqttBrokerClient mqttBrokerClient;

        @Inject
        private SessionDictServerProvider(StandaloneConfig config,
                                          RPCServerBuilder rpcServerBuilder,
                                          IMqttBrokerClient mqttBrokerClient) {
            this.config = config;
            this.rpcServerBuilder = rpcServerBuilder;
            this.mqttBrokerClient = mqttBrokerClient;
        }

        @Override
        public Optional<ISessionDictServer> get() {
            SessionDictServerConfig serverConfig = config.getSessionDictServiceConfig().getServer();
            if (!serverConfig.isEnable()) {
                return Optional.empty();
            }
            return Optional.of(ISessionDictServer.builder()
                .rpcServerBuilder(rpcServerBuilder)
                .mqttBrokerClient(mqttBrokerClient)
                .workerThreads(serverConfig.getWorkerThreads())
                .attributes(serverConfig.getAttributes())
                .defaultGroupTags(serverConfig.getDefaultGroups())
                .build());
        }
    }

    @Override
    protected void configure() {
        bind(ISessionDictClient.class).toProvider(SessionDictClientProvider.class)
            .asEagerSingleton();
        bind(new TypeLiteral<Optional<ISessionDictServer>>() {
        }).toProvider(SessionDictServerProvider.class)
            .asEagerSingleton();
    }
}
