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

import static com.baidu.bifromq.starter.module.EngineConfUtil.buildDataEngineConf;
import static com.baidu.bifromq.starter.module.EngineConfUtil.buildWALEngineConf;

import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.metaservice.IBaseKVMetaService;
import com.baidu.bifromq.basekv.store.option.KVRangeOptions;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.baserpc.server.RPCServerBuilder;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import com.baidu.bifromq.plugin.settingprovider.SettingProviderManager;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.retain.server.IRetainServer;
import com.baidu.bifromq.retain.store.IRetainStore;
import com.baidu.bifromq.starter.config.StandaloneConfig;
import com.baidu.bifromq.starter.config.model.retain.RetainServerConfig;
import com.baidu.bifromq.starter.config.model.retain.RetainStoreClientConfig;
import com.baidu.bifromq.starter.config.model.retain.RetainStoreConfig;
import com.baidu.bifromq.sysprops.props.RetainStoreLoadEstimationWindowSeconds;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import javax.inject.Named;

public class RetainServiceModule extends AbstractModule {
    private static class RetainClientProvider implements Provider<IRetainClient> {
        private final EventLoopGroup eventLoopGroup;
        private final SslContext sslContext;
        private final IRPCServiceTrafficService trafficService;

        @Inject
        private RetainClientProvider(@Named("rpcClientEventLoop") EventLoopGroup eventLoopGroup,
                                     @Named("rpcClientSSLContext") Optional<SslContext> sslContext,
                                     IRPCServiceTrafficService trafficService) {
            this.eventLoopGroup = eventLoopGroup;
            this.sslContext = sslContext.orElse(null);
            this.trafficService = trafficService;
        }

        @Override
        public IRetainClient get() {
            return IRetainClient.newBuilder()
                .trafficService(trafficService)
                .eventLoopGroup(eventLoopGroup)
                .sslContext(sslContext)
                .build();
        }
    }

    private static class RetainServerProvider implements Provider<Optional<IRetainServer>> {
        private final StandaloneConfig config;
        private final RPCServerBuilder rpcServerBuilder;
        private final IBaseKVStoreClient retainStoreClient;
        private final SettingProviderManager settingProviderMgr;
        private final ISubBrokerManager subBrokerMgr;

        @Inject
        private RetainServerProvider(StandaloneConfig config,
                                     RPCServerBuilder rpcServerBuilder,
                                     @Named("retainStoreClient") IBaseKVStoreClient retainStoreClient,
                                     SettingProviderManager settingProviderMgr,
                                     ISubBrokerManager subBrokerMgr) {
            this.config = config;
            this.rpcServerBuilder = rpcServerBuilder;
            this.retainStoreClient = retainStoreClient;
            this.settingProviderMgr = settingProviderMgr;
            this.subBrokerMgr = subBrokerMgr;
        }

        @Override
        public Optional<IRetainServer> get() {
            RetainServerConfig serverConfig = config.getRetainServiceConfig().getServer();
            if (!serverConfig.isEnable()) {
                return Optional.empty();
            }
            return Optional.of(IRetainServer.builder()
                .rpcServerBuilder(rpcServerBuilder)
                .retainStoreClient(retainStoreClient)
                .settingProvider(settingProviderMgr)
                .subBrokerManager(subBrokerMgr)
                // TODO: attributes
                // TODO: defaultGroupTags
                .build());
        }
    }

    private static class RetainStoreClientProvider implements Provider<IBaseKVStoreClient> {
        private final StandaloneConfig config;
        private final EventLoopGroup eventLoopGroup;
        private final IRPCServiceTrafficService trafficService;
        private final IBaseKVMetaService metaService;
        private final SslContext rpcClientSSLContext;

        @Inject
        private RetainStoreClientProvider(StandaloneConfig config,
                                          @Named("rpcClientEventLoop") EventLoopGroup eventLoopGroup,
                                          IRPCServiceTrafficService trafficService,
                                          IBaseKVMetaService metaService,
                                          @Named("rpcClientSSLContext") Optional<SslContext> rpcClientSSLContext) {
            this.config = config;
            this.eventLoopGroup = eventLoopGroup;
            this.trafficService = trafficService;
            this.metaService = metaService;
            this.rpcClientSSLContext = rpcClientSSLContext.orElse(null);
        }


        @Override
        public IBaseKVStoreClient get() {
            RetainStoreClientConfig clientConfig = config.getRetainServiceConfig().getStoreClient();
            return IBaseKVStoreClient.newBuilder()
                .clusterId(IRetainStore.CLUSTER_NAME)
                .trafficService(trafficService)
                .metaService(metaService)
                .eventLoopGroup(eventLoopGroup)
                .workerThreads(clientConfig.getWorkerThreads())
                .sslContext(rpcClientSSLContext)
                .queryPipelinesPerStore(clientConfig.getQueryPipelinePerStore())
                .build();
        }
    }

    private static class RetainStoreProvider implements Provider<Optional<IRetainStore>> {
        private final StandaloneConfig config;
        private final RPCServerBuilder rpcServerBuilder;
        private final IAgentHost agentHost;
        private final IBaseKVMetaService metaService;
        private final IBaseKVStoreClient retainStoreClient;
        private final ScheduledExecutorService bgTaskScheduler;

        @Inject
        private RetainStoreProvider(StandaloneConfig config,
                                    RPCServerBuilder rpcServerBuilder,
                                    IAgentHost agentHost,
                                    IBaseKVMetaService metaService,
                                    @Named("retainStoreClient") IBaseKVStoreClient retainStoreClient,
                                    @Named("bgTaskScheduler") ScheduledExecutorService bgTaskScheduler) {
            this.config = config;
            this.rpcServerBuilder = rpcServerBuilder;
            this.agentHost = agentHost;
            this.metaService = metaService;
            this.retainStoreClient = retainStoreClient;
            this.bgTaskScheduler = bgTaskScheduler;
        }

        @Override
        public Optional<IRetainStore> get() {
            RetainStoreConfig storeConfig = config.getRetainServiceConfig().getStore();
            if (!storeConfig.isEnable()) {
                return Optional.empty();
            }
            return Optional.of(IRetainStore.builder()
                .rpcServerBuilder(rpcServerBuilder)
                .agentHost(agentHost)
                .metaService(metaService)
                .storeClient(retainStoreClient)
                .workerThreads(storeConfig.getWorkerThreads())
                .tickerThreads(storeConfig.getTickerThreads())
                .bgTaskExecutor(bgTaskScheduler)
                .balancerRetryDelay(Duration.ofMillis(storeConfig.getBalanceConfig().getRetryDelayInMS()))
                .balancerFactoryConfig(storeConfig.getBalanceConfig().getBalancers())
                .loadEstimateWindow(Duration.ofSeconds(RetainStoreLoadEstimationWindowSeconds.INSTANCE.get()))
                .gcInterval(Duration.ofSeconds(storeConfig.getGcIntervalSeconds()))
                .storeOptions(new KVRangeStoreOptions()
                    .setKvRangeOptions(new KVRangeOptions()
                        .setCompactWALThreshold(storeConfig.getCompactWALThreshold()))
                    .setDataEngineConfigurator(buildDataEngineConf(storeConfig.getDataEngineConfig(), "retain_data"))
                    .setWalEngineConfigurator(buildWALEngineConf(storeConfig.getWalEngineConfig(), "retain_wal")))
                .build());
        }
    }

    @Override
    protected void configure() {
        bind(IRetainClient.class).toProvider(RetainClientProvider.class).asEagerSingleton();
        bind(IBaseKVStoreClient.class)
            .annotatedWith(Names.named("retainStoreClient"))
            .toProvider(RetainStoreClientProvider.class)
            .asEagerSingleton();
        bind(new TypeLiteral<Optional<IRetainServer>>() {
        }).toProvider(RetainServerProvider.class)
            .asEagerSingleton();
        bind(new TypeLiteral<Optional<IRetainStore>>() {
        }).toProvider(RetainStoreProvider.class)
            .asEagerSingleton();
    }
}
