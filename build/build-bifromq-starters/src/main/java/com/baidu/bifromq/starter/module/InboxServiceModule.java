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
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.server.IInboxServer;
import com.baidu.bifromq.inbox.store.IInboxStore;
import com.baidu.bifromq.plugin.eventcollector.EventCollectorManager;
import com.baidu.bifromq.plugin.resourcethrottler.ResourceThrottlerManager;
import com.baidu.bifromq.plugin.settingprovider.SettingProviderManager;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.starter.config.StandaloneConfig;
import com.baidu.bifromq.starter.config.model.inbox.InboxServerConfig;
import com.baidu.bifromq.starter.config.model.inbox.InboxStoreConfig;
import com.baidu.bifromq.sysprops.props.InboxStoreLoadEstimationWindowSeconds;
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

public class InboxServiceModule extends AbstractModule {
    private static class InboxClientProvider implements Provider<IInboxClient> {
        private final EventLoopGroup eventLoopGroup;
        private final SslContext sslContext;
        private final IRPCServiceTrafficService trafficService;

        @Inject
        private InboxClientProvider(@Named("rpcClientEventLoop") EventLoopGroup eventLoopGroup,
                                    @Named("rpcClientSSLContext") Optional<SslContext> sslContext,
                                    IRPCServiceTrafficService trafficService) {
            this.eventLoopGroup = eventLoopGroup;
            this.sslContext = sslContext.orElse(null);
            this.trafficService = trafficService;
        }

        @Override
        public IInboxClient get() {
            return IInboxClient.newBuilder()
                .trafficService(trafficService)
                .eventLoopGroup(eventLoopGroup)
                .sslContext(sslContext)
                .build();
        }
    }

    private static class InboxServerProvider implements Provider<Optional<IInboxServer>> {
        private final StandaloneConfig config;
        private final RPCServerBuilder rpcServerBuilder;
        private final IDistClient distClient;
        private final IInboxClient inboxClient;
        private final IRetainClient retainClient;
        private final IBaseKVStoreClient inboxStoreClient;
        private final EventCollectorManager eventCollectorMgr;
        private final ResourceThrottlerManager resourceThrottlerMgr;
        private final SettingProviderManager settingProviderMgr;

        @Inject
        private InboxServerProvider(StandaloneConfig config,
                                    RPCServerBuilder rpcServerBuilder,
                                    IDistClient distClient,
                                    IInboxClient inboxClient,
                                    IRetainClient retainClient,
                                    @Named("inboxStoreClient") IBaseKVStoreClient inboxStoreClient,
                                    EventCollectorManager eventCollectorMgr,
                                    ResourceThrottlerManager resourceThrottlerMgr,
                                    SettingProviderManager settingProviderMgr) {
            this.config = config;
            this.rpcServerBuilder = rpcServerBuilder;
            this.distClient = distClient;
            this.inboxClient = inboxClient;
            this.retainClient = retainClient;
            this.inboxStoreClient = inboxStoreClient;
            this.eventCollectorMgr = eventCollectorMgr;
            this.resourceThrottlerMgr = resourceThrottlerMgr;
            this.settingProviderMgr = settingProviderMgr;
        }

        @Override
        public Optional<IInboxServer> get() {
            InboxServerConfig serverConfig = config.getInboxServiceConfig().getServer();
            if (!serverConfig.isEnable()) {
                return Optional.empty();
            }
            return Optional.of(IInboxServer.builder()
                .rpcServerBuilder(rpcServerBuilder)
                .eventCollector(eventCollectorMgr)
                .resourceThrottler(resourceThrottlerMgr)
                .settingProvider(settingProviderMgr)
                .inboxClient(inboxClient)
                .distClient(distClient)
                .retainClient(retainClient)
                .inboxStoreClient(inboxStoreClient)
                .workerThreads(serverConfig.getWorkerThreads())
                .build());
        }
    }

    private static class InboxStoreClientProvider implements Provider<IBaseKVStoreClient> {
        private final StandaloneConfig config;
        private final EventLoopGroup eventLoopGroup;
        private final IRPCServiceTrafficService trafficService;
        private final IBaseKVMetaService metaService;
        private final SslContext rpcClientSSLContext;

        @Inject
        private InboxStoreClientProvider(StandaloneConfig config,
                                         IRPCServiceTrafficService trafficService,
                                         IBaseKVMetaService metaService,
                                         @Named("rpcClientSSLContext") Optional<SslContext> rpcClientSSLContext,
                                         @Named("rpcClientEventLoop") EventLoopGroup eventLoopGroup) {
            this.config = config;
            this.trafficService = trafficService;
            this.metaService = metaService;
            this.rpcClientSSLContext = rpcClientSSLContext.orElse(null);
            this.eventLoopGroup = eventLoopGroup;
        }


        @Override
        public IBaseKVStoreClient get() {
            InboxStoreConfig storeConfig = config.getInboxServiceConfig().getStore();
            return IBaseKVStoreClient.newBuilder()
                .clusterId(IInboxStore.CLUSTER_NAME)
                .trafficService(trafficService)
                .metaService(metaService)
                .workerThreads(storeConfig.getWorkerThreads())
                .eventLoopGroup(eventLoopGroup)
                .sslContext(rpcClientSSLContext)
                .queryPipelinesPerStore(storeConfig.getQueryPipelinePerStore())
                .build();
        }
    }

    private static class InboxStoreProvider implements Provider<Optional<IInboxStore>> {
        private final StandaloneConfig config;
        private final RPCServerBuilder rpcServerBuilder;
        private final IAgentHost agentHost;
        private final IBaseKVMetaService metaService;
        private final EventCollectorManager eventCollectorMgr;
        private final SettingProviderManager settingProviderMgr;
        private final IInboxClient inboxClient;
        private final IBaseKVStoreClient inboxStoreClient;
        private final ScheduledExecutorService bgTaskExecutor;

        @Inject
        private InboxStoreProvider(StandaloneConfig config,
                                   RPCServerBuilder rpcServerBuilder,
                                   IAgentHost agentHost,
                                   IBaseKVMetaService metaService,
                                   EventCollectorManager eventCollectorMgr,
                                   SettingProviderManager settingProviderMgr,
                                   IInboxClient inboxClient,
                                   @Named("inboxStoreClient") IBaseKVStoreClient inboxStoreClient,
                                   @Named("bgTaskScheduler") ScheduledExecutorService bgTaskScheduler) {
            this.config = config;
            this.rpcServerBuilder = rpcServerBuilder;
            this.agentHost = agentHost;
            this.metaService = metaService;
            this.eventCollectorMgr = eventCollectorMgr;
            this.settingProviderMgr = settingProviderMgr;
            this.inboxClient = inboxClient;
            this.inboxStoreClient = inboxStoreClient;
            this.bgTaskExecutor = bgTaskScheduler;
        }


        @Override
        public Optional<IInboxStore> get() {
            InboxStoreConfig storeConfig = config.getInboxServiceConfig().getStore();
            if (!storeConfig.isEnable()) {
                return Optional.empty();
            }
            return Optional.of(IInboxStore.builder()
                .rpcServerBuilder(rpcServerBuilder)
                .agentHost(agentHost)
                .metaService(metaService)
                .inboxClient(inboxClient)
                .storeClient(inboxStoreClient)
                .settingProvider(settingProviderMgr)
                .eventCollector(eventCollectorMgr)
                .tickerThreads(storeConfig.getTickerThreads())
                .workerThreads(storeConfig.getWorkerThreads())
                .bgTaskExecutor(bgTaskExecutor)
                .loadEstimateWindow(Duration.ofSeconds(InboxStoreLoadEstimationWindowSeconds.INSTANCE.get()))
                .gcInterval(
                    Duration.ofSeconds(storeConfig.getGcIntervalSeconds()))
                .balancerRetryDelay(Duration.ofMillis(
                    storeConfig.getBalanceConfig().getRetryDelayInMS()))
                .balancerFactoryConfig(
                    storeConfig.getBalanceConfig().getBalancers())
                .storeOptions(new KVRangeStoreOptions()
                    .setKvRangeOptions(new KVRangeOptions()
                        .setCompactWALThreshold(storeConfig
                            .getCompactWALThreshold())
                        .setEnableLoadEstimation(true))
                    .setDataEngineConfigurator(buildDataEngineConf(storeConfig.getDataEngineConfig(), "inbox_data"))
                    .setWalEngineConfigurator(buildWALEngineConf(storeConfig.getWalEngineConfig(), "inbox_wal")))
                .build());
        }
    }

    @Override
    protected void configure() {
        bind(IInboxClient.class).toProvider(InboxClientProvider.class).asEagerSingleton();
        bind(IBaseKVStoreClient.class)
            .annotatedWith(Names.named("inboxStoreClient"))
            .toProvider(InboxStoreClientProvider.class).asEagerSingleton();
        bind(new TypeLiteral<Optional<IInboxServer>>() {
        }).toProvider(InboxServerProvider.class).asEagerSingleton();
        bind(new TypeLiteral<Optional<IInboxStore>>() {
        }).toProvider(InboxStoreProvider.class).asEagerSingleton();
    }
}
