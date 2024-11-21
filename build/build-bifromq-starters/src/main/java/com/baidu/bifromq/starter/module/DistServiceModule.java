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
import com.baidu.bifromq.dist.server.IDistServer;
import com.baidu.bifromq.dist.worker.IDistWorker;
import com.baidu.bifromq.plugin.eventcollector.EventCollectorManager;
import com.baidu.bifromq.plugin.resourcethrottler.ResourceThrottlerManager;
import com.baidu.bifromq.plugin.settingprovider.SettingProviderManager;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.starter.config.StandaloneConfig;
import com.baidu.bifromq.starter.config.model.dist.DistServerConfig;
import com.baidu.bifromq.starter.config.model.dist.DistWorkerClientConfig;
import com.baidu.bifromq.starter.config.model.dist.DistWorkerConfig;
import com.baidu.bifromq.sysprops.props.DistWorkerLoadEstimationWindowSeconds;
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

public class DistServiceModule extends AbstractModule {
    private static class DistClientProvider implements Provider<IDistClient> {
        private final StandaloneConfig config;
        private final EventLoopGroup eventLoopGroup;
        private final SslContext sslContext;
        private final IRPCServiceTrafficService trafficService;

        @Inject
        private DistClientProvider(StandaloneConfig config,
                                   @Named("rpcClientEventLoop") EventLoopGroup eventLoopGroup,
                                   @Named("rpcClientSSLContext") Optional<SslContext> sslContext,
                                   IRPCServiceTrafficService trafficService) {
            this.config = config;
            this.eventLoopGroup = eventLoopGroup;
            this.sslContext = sslContext.orElse(null);
            this.trafficService = trafficService;
        }

        @Override
        public IDistClient get() {
            return IDistClient.newBuilder()
                .workerThreads(config.getDistServiceConfig().getClient().getWorkerThreads())
                .trafficService(trafficService)
                .eventLoopGroup(eventLoopGroup)
                .sslContext(sslContext)
                .build();
        }
    }

    private static class DistServerProvider implements Provider<Optional<IDistServer>> {
        private final StandaloneConfig config;
        private final RPCServerBuilder rpcServerBuilder;
        private final IBaseKVStoreClient distWorkerClient;
        private final EventCollectorManager eventCollectorMgr;
        private final SettingProviderManager settingProviderMgr;

        @Inject
        private DistServerProvider(StandaloneConfig config,
                                   RPCServerBuilder rpcServerBuilder,
                                   @Named("distWorkerClient") IBaseKVStoreClient distWorkerClient,
                                   SettingProviderManager settingProviderMgr,
                                   EventCollectorManager eventCollectorMgr) {
            this.config = config;
            this.rpcServerBuilder = rpcServerBuilder;
            this.distWorkerClient = distWorkerClient;
            this.settingProviderMgr = settingProviderMgr;
            this.eventCollectorMgr = eventCollectorMgr;
        }

        @Override
        public Optional<IDistServer> get() {
            DistServerConfig serverConfig = config.getDistServiceConfig().getServer();
            if (!serverConfig.isEnable()) {
                return Optional.empty();
            }
            return Optional.of(IDistServer.builder()
                .rpcServerBuilder(rpcServerBuilder)
                .distWorkerClient(distWorkerClient)
                .settingProvider(settingProviderMgr)
                .eventCollector(eventCollectorMgr)
                .workerThreads(serverConfig.getWorkerThreads())
                .attributes(serverConfig.getAttributes())
                .defaultGroupTags(serverConfig.getDefaultGroups())
                .build());
        }
    }

    private static class DistWorkerClientProvider implements Provider<IBaseKVStoreClient> {
        private final StandaloneConfig config;
        private final EventLoopGroup eventLoopGroup;
        private final IRPCServiceTrafficService trafficService;
        private final IBaseKVMetaService metaService;
        private final SslContext rpcClientSSLContext;

        @Inject
        private DistWorkerClientProvider(StandaloneConfig config,
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
            DistWorkerClientConfig workerConfig = config.getDistServiceConfig().getWorkerClient();
            return IBaseKVStoreClient.newBuilder()
                .clusterId(IDistWorker.CLUSTER_NAME)
                .trafficService(trafficService)
                .metaService(metaService)
                .workerThreads(workerConfig.getWorkerThreads())
                .eventLoopGroup(eventLoopGroup)
                .sslContext(rpcClientSSLContext)
                .queryPipelinesPerStore(workerConfig.getQueryPipelinePerStore())
                .build();
        }
    }

    private static class DistWorkerProvider implements Provider<Optional<IDistWorker>> {
        private final StandaloneConfig config;
        private final RPCServerBuilder rpcServerBuilder;
        private final IAgentHost agentHost;
        private final IBaseKVMetaService metaService;
        private final EventCollectorManager eventCollectorMgr;
        private final ResourceThrottlerManager resourceThrottlerMgr;
        private final IDistClient distClient;
        private final IBaseKVStoreClient distWorkerClient;
        private final ISubBrokerManager subBrokerManager;
        private final ScheduledExecutorService bgTaskExecutor;

        @Inject
        private DistWorkerProvider(StandaloneConfig config,
                                   RPCServerBuilder rpcServerBuilder,
                                   IAgentHost agentHost,
                                   IBaseKVMetaService metaService,
                                   EventCollectorManager eventCollectorMgr,
                                   ResourceThrottlerManager resourceThrottlerMgr,
                                   IDistClient distClient,
                                   @Named("distWorkerClient") IBaseKVStoreClient distWorkerClient,
                                   ISubBrokerManager subBrokerManager,
                                   @Named("bgTaskScheduler") ScheduledExecutorService bgTaskExecutor) {
            this.config = config;
            this.rpcServerBuilder = rpcServerBuilder;
            this.agentHost = agentHost;
            this.metaService = metaService;
            this.eventCollectorMgr = eventCollectorMgr;
            this.resourceThrottlerMgr = resourceThrottlerMgr;
            this.distClient = distClient;
            this.distWorkerClient = distWorkerClient;
            this.subBrokerManager = subBrokerManager;
            this.bgTaskExecutor = bgTaskExecutor;
        }

        @Override
        public Optional<IDistWorker> get() {
            DistWorkerConfig workerConfig = config.getDistServiceConfig().getWorker();
            if (!workerConfig.isEnable()) {
                return Optional.empty();
            }

            return Optional.of(IDistWorker.builder()
                .rpcServerBuilder(rpcServerBuilder)
                .agentHost(agentHost)
                .metaService(metaService)
                .eventCollector(eventCollectorMgr)
                .resourceThrottler(resourceThrottlerMgr)
                .distClient(distClient)
                .storeClient(distWorkerClient)
                .workerThreads(workerConfig.getWorkerThreads())
                .tickerThreads(workerConfig.getTickerThreads())
                .bgTaskExecutor(bgTaskExecutor)
                .storeOptions(new KVRangeStoreOptions()
                    .setKvRangeOptions(new KVRangeOptions()
                        .setCompactWALThreshold(workerConfig.getCompactWALThreshold()))
                    .setDataEngineConfigurator(buildDataEngineConf(workerConfig
                        .getDataEngineConfig(), "dist_data"))
                    .setWalEngineConfigurator(buildWALEngineConf(workerConfig
                        .getWalEngineConfig(), "dist_wal")))
                .balancerRetryDelay(Duration.ofMillis(workerConfig.getBalanceConfig().getRetryDelayInMS()))
                .balancerFactoryConfig(workerConfig.getBalanceConfig().getBalancers())
                .subBrokerManager(subBrokerManager)
                .loadEstimateWindow(Duration.ofSeconds(DistWorkerLoadEstimationWindowSeconds.INSTANCE.get()))
                .build());
        }
    }

    @Override
    protected void configure() {
        bind(IDistClient.class).toProvider(DistClientProvider.class).asEagerSingleton();
        bind(IBaseKVStoreClient.class)
            .annotatedWith(Names.named("distWorkerClient"))
            .toProvider(DistWorkerClientProvider.class).asEagerSingleton();
        bind(new TypeLiteral<Optional<IDistServer>>() {
        }).toProvider(DistServerProvider.class).asEagerSingleton();
        bind(new TypeLiteral<Optional<IDistWorker>>() {
        }).toProvider(DistWorkerProvider.class).asEagerSingleton();
    }
}
