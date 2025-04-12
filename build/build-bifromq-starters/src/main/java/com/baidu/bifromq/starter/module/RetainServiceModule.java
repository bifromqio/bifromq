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
import com.baidu.bifromq.plugin.settingprovider.SettingProviderManager;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.retain.server.IRetainServer;
import com.baidu.bifromq.retain.store.IRetainStore;
import com.baidu.bifromq.starter.config.StandaloneConfig;
import com.baidu.bifromq.starter.config.model.retain.RetainServerConfig;
import com.baidu.bifromq.starter.config.model.retain.RetainStoreConfig;
import com.baidu.bifromq.sysprops.props.RetainStoreLoadEstimationWindowSeconds;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import javax.inject.Singleton;

public class RetainServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(new TypeLiteral<Optional<IRetainServer>>() {
        }).toProvider(RetainServerProvider.class)
            .in(Singleton.class);
        bind(new TypeLiteral<Optional<IRetainStore>>() {
        }).toProvider(RetainStoreProvider.class)
            .in(Singleton.class);
    }

    private static class RetainServerProvider implements Provider<Optional<IRetainServer>> {
        private final StandaloneConfig config;
        private final ServiceInjector injector;

        @Inject
        private RetainServerProvider(StandaloneConfig config, ServiceInjector injector) {
            this.config = config;
            this.injector = injector;
        }

        @Override
        public Optional<IRetainServer> get() {
            RetainServerConfig serverConfig = config.getRetainServiceConfig().getServer();
            if (!serverConfig.isEnable()) {
                return Optional.empty();
            }
            return Optional.of(IRetainServer.builder()
                .rpcServerBuilder(injector.getInstance(RPCServerBuilder.class))
                .distClient(injector.getInstance(IDistClient.class))
                .retainStoreClient(
                    injector.getInstance(Key.get(IBaseKVStoreClient.class, Names.named("retainStoreClient"))))
                .settingProvider(injector.getInstance(SettingProviderManager.class))
                .subBrokerManager(injector.getInstance(ISubBrokerManager.class))
                .attributes(serverConfig.getAttributes())
                .defaultGroupTags(serverConfig.getDefaultGroups())
                .build());
        }
    }

    private static class RetainStoreProvider implements Provider<Optional<IRetainStore>> {
        private final StandaloneConfig config;
        private final ServiceInjector injector;

        @Inject
        private RetainStoreProvider(StandaloneConfig config, ServiceInjector injector) {
            this.config = config;
            this.injector = injector;
        }

        @Override
        public Optional<IRetainStore> get() {
            RetainStoreConfig storeConfig = config.getRetainServiceConfig().getStore();
            if (!storeConfig.isEnable()) {
                return Optional.empty();
            }
            return Optional.of(IRetainStore.builder()
                .rpcServerBuilder(injector.getInstance(RPCServerBuilder.class))
                .agentHost(injector.getInstance(IAgentHost.class))
                .metaService(injector.getInstance(IBaseKVMetaService.class))
                .retainStoreClient(
                    injector.getInstance(Key.get(IBaseKVStoreClient.class, Names.named("retainStoreClient"))))
                .workerThreads(storeConfig.getWorkerThreads())
                .tickerThreads(storeConfig.getTickerThreads())
                .bgTaskExecutor(
                    injector.getInstance(Key.get(ScheduledExecutorService.class, Names.named("bgTaskScheduler"))))
                .bootstrapDelay(Duration.ofMillis(storeConfig.getBalanceConfig().getBootstrapDelayInMS()))
                .zombieProbeDelay(Duration.ofMillis(storeConfig.getBalanceConfig().getZombieProbeDelayInMS()))
                .balancerRetryDelay(Duration.ofMillis(storeConfig.getBalanceConfig().getRetryDelayInMS()))
                .balancerFactoryConfig(storeConfig.getBalanceConfig().getBalancers())
                .loadEstimateWindow(Duration.ofSeconds(RetainStoreLoadEstimationWindowSeconds.INSTANCE.get()))
                .gcInterval(Duration.ofSeconds(storeConfig.getGcIntervalSeconds()))
                .storeOptions(new KVRangeStoreOptions()
                    .setKvRangeOptions(new KVRangeOptions()
                        .setMaxWALFatchBatchSize(storeConfig.getMaxWALFetchSize())
                        .setCompactWALThreshold(storeConfig.getCompactWALThreshold()))
                    .setDataEngineConfigurator(buildDataEngineConf(storeConfig.getDataEngineConfig(), "retain_data"))
                    .setWalEngineConfigurator(buildWALEngineConf(storeConfig.getWalEngineConfig(), "retain_wal")))
                .attributes(storeConfig.getAttributes())
                .build());
        }
    }
}
