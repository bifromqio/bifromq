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
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.server.IDistServer;
import com.baidu.bifromq.dist.worker.IDistWorker;
import com.baidu.bifromq.plugin.eventcollector.EventCollectorManager;
import com.baidu.bifromq.plugin.resourcethrottler.ResourceThrottlerManager;
import com.baidu.bifromq.plugin.settingprovider.SettingProviderManager;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.starter.config.StandaloneConfig;
import com.baidu.bifromq.starter.config.model.dist.DistServerConfig;
import com.baidu.bifromq.starter.config.model.dist.DistWorkerConfig;
import com.baidu.bifromq.sysprops.props.DistWorkerLoadEstimationWindowSeconds;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

public class DistServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(new TypeLiteral<Optional<IDistServer>>() {
        }).toProvider(DistServerProvider.class).in(Singleton.class);
        bind(new TypeLiteral<Optional<IDistWorker>>() {
        }).toProvider(DistWorkerProvider.class).in(Singleton.class);
    }

    private static class DistServerProvider implements Provider<Optional<IDistServer>> {
        private final StandaloneConfig config;
        private final ServiceInjector injector;

        @Inject
        private DistServerProvider(StandaloneConfig config, ServiceInjector injector) {
            this.config = config;
            this.injector = injector;
        }

        @Override
        public Optional<IDistServer> get() {
            DistServerConfig serverConfig = config.getDistServiceConfig().getServer();
            if (!serverConfig.isEnable()) {
                return Optional.empty();
            }
            return Optional.of(IDistServer.builder()
                .rpcServerBuilder(injector.getInstance(RPCServerBuilder.class))
                .distWorkerClient(
                    injector.getInstance(Key.get(IBaseKVStoreClient.class, Names.named("distWorkerClient"))))
                .settingProvider(injector.getInstance(SettingProviderManager.class))
                .eventCollector(injector.getInstance(EventCollectorManager.class))
                .workerThreads(serverConfig.getWorkerThreads())
                .attributes(serverConfig.getAttributes())
                .defaultGroupTags(serverConfig.getDefaultGroups())
                .build());
        }
    }

    private static class DistWorkerProvider implements Provider<Optional<IDistWorker>> {
        private final StandaloneConfig config;
        private final ServiceInjector injector;

        @Inject
        private DistWorkerProvider(StandaloneConfig config, ServiceInjector injector) {
            this.config = config;
            this.injector = injector;
        }

        @Override
        public Optional<IDistWorker> get() {
            DistWorkerConfig workerConfig = config.getDistServiceConfig().getWorker();
            if (!workerConfig.isEnable()) {
                return Optional.empty();
            }

            return Optional.of(IDistWorker.builder()
                .rpcServerBuilder(injector.getInstance(RPCServerBuilder.class))
                .agentHost(injector.getInstance(IAgentHost.class))
                .metaService(injector.getInstance(IBaseKVMetaService.class))
                .eventCollector(injector.getInstance(EventCollectorManager.class))
                .resourceThrottler(injector.getInstance(ResourceThrottlerManager.class))
                .distClient(injector.getInstance(IDistClient.class))
                .distWorkerClient(
                    injector.getInstance(Key.get(IBaseKVStoreClient.class, Names.named("distWorkerClient"))))
                .workerThreads(workerConfig.getWorkerThreads())
                .tickerThreads(workerConfig.getTickerThreads())
                .bgTaskExecutor(
                    injector.getInstance(Key.get(ScheduledExecutorService.class, Names.named("bgTaskScheduler"))))
                .storeOptions(new KVRangeStoreOptions()
                    .setKvRangeOptions(new KVRangeOptions()
                        .setMaxWALFatchBatchSize(workerConfig.getMaxWALFetchSize())
                        .setCompactWALThreshold(workerConfig.getCompactWALThreshold()))
                    .setDataEngineConfigurator(buildDataEngineConf(workerConfig
                        .getDataEngineConfig(), "dist_data"))
                    .setWalEngineConfigurator(buildWALEngineConf(workerConfig
                        .getWalEngineConfig(), "dist_wal")))
                .gcInterval(
                    Duration.ofSeconds(workerConfig.getGcIntervalSeconds()))
                .bootstrapDelay(Duration.ofMillis(workerConfig.getBalanceConfig().getBootstrapDelayInMS()))
                .zombieProbeDelay(Duration.ofMillis(workerConfig.getBalanceConfig().getZombieProbeDelayInMS()))
                .balancerRetryDelay(Duration.ofMillis(workerConfig.getBalanceConfig().getRetryDelayInMS()))
                .balancerFactoryConfig(workerConfig.getBalanceConfig().getBalancers())
                .subBrokerManager(injector.getInstance(ISubBrokerManager.class))
                .loadEstimateWindow(Duration.ofSeconds(DistWorkerLoadEstimationWindowSeconds.INSTANCE.get()))
                .attributes(workerConfig.getAttributes())
                .build());
        }
    }
}
