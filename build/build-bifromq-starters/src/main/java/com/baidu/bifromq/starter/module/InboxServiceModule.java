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
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import javax.inject.Singleton;

public class InboxServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(new TypeLiteral<Optional<IInboxServer>>() {
        }).toProvider(InboxServerProvider.class).in(Singleton.class);
        bind(new TypeLiteral<Optional<IInboxStore>>() {
        }).toProvider(InboxStoreProvider.class).in(Singleton.class);
    }

    private static class InboxServerProvider implements Provider<Optional<IInboxServer>> {
        private final StandaloneConfig config;
        private final ServiceInjector injector;

        @Inject
        private InboxServerProvider(StandaloneConfig config, ServiceInjector injector) {
            this.config = config;
            this.injector = injector;
        }

        @Override
        public Optional<IInboxServer> get() {
            InboxServerConfig serverConfig = config.getInboxServiceConfig().getServer();
            if (!serverConfig.isEnable()) {
                return Optional.empty();
            }
            return Optional.of(IInboxServer.builder()
                .rpcServerBuilder(injector.getInstance(RPCServerBuilder.class))
                .inboxClient(injector.getInstance(IInboxClient.class))
                .distClient(injector.getInstance(IDistClient.class))
                .inboxStoreClient(
                    injector.getInstance(Key.get(IBaseKVStoreClient.class, Names.named("inboxStoreClient"))))
                .workerThreads(serverConfig.getWorkerThreads())
                .attributes(serverConfig.getAttributes())
                .defaultGroupTags(serverConfig.getDefaultGroups())
                .build());
        }
    }

    private static class InboxStoreProvider implements Provider<Optional<IInboxStore>> {
        private final StandaloneConfig config;
        private final ServiceInjector injector;

        @Inject
        private InboxStoreProvider(StandaloneConfig config, ServiceInjector injector) {
            this.config = config;
            this.injector = injector;
        }

        @Override
        public Optional<IInboxStore> get() {
            InboxStoreConfig storeConfig = config.getInboxServiceConfig().getStore();
            if (!storeConfig.isEnable()) {
                return Optional.empty();
            }
            return Optional.of(IInboxStore.builder()
                .rpcServerBuilder(injector.getInstance(RPCServerBuilder.class))
                .agentHost(injector.getInstance(IAgentHost.class))
                .metaService(injector.getInstance(IBaseKVMetaService.class))
                .distClient(injector.getInstance(IDistClient.class))
                .inboxClient(injector.getInstance(IInboxClient.class))
                .retainClient(injector.getInstance(IRetainClient.class))
                .inboxStoreClient(
                    injector.getInstance(Key.get(IBaseKVStoreClient.class, Names.named("inboxStoreClient"))))
                .settingProvider(injector.getInstance(SettingProviderManager.class))
                .eventCollector(injector.getInstance(EventCollectorManager.class))
                .resourceThrottler(injector.getInstance(ResourceThrottlerManager.class))
                .tickerThreads(storeConfig.getTickerThreads())
                .workerThreads(storeConfig.getWorkerThreads())
                .bgTaskExecutor(
                    injector.getInstance(Key.get(ScheduledExecutorService.class, Names.named("bgTaskScheduler"))))
                .loadEstimateWindow(Duration.ofSeconds(InboxStoreLoadEstimationWindowSeconds.INSTANCE.get()))
                .expireRateLimit(storeConfig.getExpireRateLimit())
                .gcInterval(
                    Duration.ofSeconds(storeConfig.getGcIntervalSeconds()))
                .bootstrapDelay(Duration.ofMillis(storeConfig.getBalanceConfig().getBootstrapDelayInMS()))
                .zombieProbeDelay(Duration.ofMillis(storeConfig.getBalanceConfig().getZombieProbeDelayInMS()))
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
                .attributes(storeConfig.getAttributes())
                .build());
        }
    }
}
