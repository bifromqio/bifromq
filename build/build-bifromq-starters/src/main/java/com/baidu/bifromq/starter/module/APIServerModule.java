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

import com.baidu.bifromq.apiserver.APIServer;
import com.baidu.bifromq.apiserver.IAPIServer;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basekv.metaservice.IBaseKVMetaService;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.plugin.settingprovider.SettingProviderManager;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import com.baidu.bifromq.starter.config.StandaloneConfig;
import com.baidu.bifromq.starter.config.model.api.APIServerConfig;
import com.google.common.base.Strings;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import io.netty.handler.ssl.SslContext;
import java.util.Optional;

public class APIServerModule extends AbstractModule {
    private static class APIServerProvider implements Provider<Optional<IAPIServer>> {
        private final StandaloneConfig config;
        private final IAgentHost agentHost;
        private final IRPCServiceTrafficService trafficService;
        private final IBaseKVMetaService metaService;
        private final IDistClient distClient;
        private final IRetainClient retainClient;
        private final IInboxClient inboxClient;
        private final ISessionDictClient sessionDictClient;
        private final SettingProviderManager settingProviderMgr;

        @Inject
        private APIServerProvider(StandaloneConfig config,
                                  IAgentHost agentHost,
                                  IRPCServiceTrafficService trafficService,
                                  IBaseKVMetaService metaService,
                                  IDistClient distClient,
                                  IRetainClient retainClient,
                                  IInboxClient inboxClient,
                                  ISessionDictClient sessionDictClient,
                                  SettingProviderManager settingProviderMgr) {
            this.config = config;
            this.agentHost = agentHost;
            this.trafficService = trafficService;
            this.metaService = metaService;
            this.distClient = distClient;
            this.retainClient = retainClient;
            this.inboxClient = inboxClient;
            this.sessionDictClient = sessionDictClient;
            this.settingProviderMgr = settingProviderMgr;
        }

        @Override
        public Optional<IAPIServer> get() {
            APIServerConfig serverConfig = config.getApiServerConfig();
            if (!serverConfig.isEnable()) {
                return Optional.empty();
            }

            String apiHost = Strings.isNullOrEmpty(serverConfig.getHost()) ? "0.0.0.0" : serverConfig.getHost();
            SslContext sslContext = null;
            if (serverConfig.getHttpsListenerConfig().isEnable()) {
                sslContext = buildServerSslContext(serverConfig.getHttpsListenerConfig().getSslConfig());
            }
            return Optional.of(APIServer.builder()
                .host(apiHost)
                .port(serverConfig.getHttpPort())
                .tlsPort(serverConfig.getHttpsListenerConfig().getPort())
                .maxContentLength(serverConfig.getMaxContentLength())
                .workerThreads(serverConfig.getWorkerThreads())
                .sslContext(sslContext)
                .agentHost(agentHost)
                .trafficService(trafficService)
                .metaService(metaService)
                .distClient(distClient)
                .inboxClient(inboxClient)
                .sessionDictClient(sessionDictClient)
                .retainClient(retainClient)
                .settingProvider(settingProviderMgr)
                .build());
        }
    }

    @Override
    protected void configure() {
        bind(new TypeLiteral<Optional<IAPIServer>>() {
        }).toProvider(APIServerProvider.class)
            .asEagerSingleton();
    }
}
