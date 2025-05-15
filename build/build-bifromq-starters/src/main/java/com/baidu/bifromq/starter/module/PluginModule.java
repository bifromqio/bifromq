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

import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.mqtt.inbox.IMqttBrokerClient;
import com.baidu.bifromq.plugin.authprovider.AuthProviderManager;
import com.baidu.bifromq.plugin.clientbalancer.ClientBalancerManager;
import com.baidu.bifromq.plugin.eventcollector.EventCollectorManager;
import com.baidu.bifromq.plugin.manager.BifroMQPluginManager;
import com.baidu.bifromq.plugin.resourcethrottler.ResourceThrottlerManager;
import com.baidu.bifromq.plugin.settingprovider.SettingProviderManager;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.plugin.subbroker.SubBrokerManager;
import com.baidu.bifromq.starter.config.StandaloneConfig;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.PluginManager;

@Slf4j
public class PluginModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(PluginManager.class).toProvider(PluginManagerProvider.class).in(Singleton.class);
        bind(ISubBrokerManager.class).toProvider(SubBrokerManagerProvider.class).in(Singleton.class);
        bind(AuthProviderManager.class).toProvider(AuthProviderManagerProvider.class).in(Singleton.class);
        bind(EventCollectorManager.class).toProvider(EventCollectorManagerProvider.class).in(Singleton.class);
        bind(ResourceThrottlerManager.class).toProvider(ResourceThrottlerManagerProvider.class).in(Singleton.class);
        bind(SettingProviderManager.class).toProvider(SettingProviderManagerProvider.class).in(Singleton.class);
        bind(ClientBalancerManager.class).toProvider(ClientBalancerManagerProvider.class).in(Singleton.class);
    }

    private static class PluginManagerProvider extends SharedResourceProvider<BifroMQPluginManager> {

        @Inject
        private PluginManagerProvider(SharedResourcesHolder sharedResourcesHolder) {
            super(sharedResourcesHolder);
        }

        @Override
        public BifroMQPluginManager share() {
            BifroMQPluginManager pluginMgr = new BifroMQPluginManager();
            pluginMgr.getPlugins().forEach(
                plugin -> log.info("Loaded plugin: {}@{}",
                    plugin.getDescriptor().getPluginId(), plugin.getDescriptor().getVersion()));
            return pluginMgr;
        }
    }

    private static class AuthProviderManagerProvider extends SharedResourceProvider<AuthProviderManager> {
        private final StandaloneConfig config;
        private final PluginManager pluginManager;
        private final SettingProviderManager settingProviderManager;
        private final EventCollectorManager eventCollectorManager;

        @Inject
        private AuthProviderManagerProvider(StandaloneConfig config,
                                            PluginManager pluginManager,
                                            SettingProviderManager settingProviderManager,
                                            EventCollectorManager eventCollectorManager,
                                            SharedResourcesHolder sharedResourcesHolder) {
            super(sharedResourcesHolder);
            this.config = config;
            this.pluginManager = pluginManager;
            this.settingProviderManager = settingProviderManager;
            this.eventCollectorManager = eventCollectorManager;
        }

        @Override
        public AuthProviderManager share() {
            return new AuthProviderManager(config.getAuthProviderFQN(),
                pluginManager,
                settingProviderManager,
                eventCollectorManager);
        }
    }

    private static class EventCollectorManagerProvider extends SharedResourceProvider<EventCollectorManager> {
        private final PluginManager pluginManager;

        @Inject
        private EventCollectorManagerProvider(PluginManager pluginManager,
                                              SharedResourcesHolder sharedResourcesHolder) {
            super(sharedResourcesHolder);
            this.pluginManager = pluginManager;
        }

        @Override
        public EventCollectorManager share() {
            return new EventCollectorManager(pluginManager);
        }
    }

    private static class ResourceThrottlerManagerProvider extends SharedResourceProvider<ResourceThrottlerManager> {
        private final StandaloneConfig config;
        private final PluginManager pluginManager;

        @Inject
        private ResourceThrottlerManagerProvider(StandaloneConfig config,
                                                 PluginManager pluginManager,
                                                 SharedResourcesHolder sharedResourcesHolder) {
            super(sharedResourcesHolder);
            this.config = config;
            this.pluginManager = pluginManager;
        }

        @Override
        public ResourceThrottlerManager share() {
            return new ResourceThrottlerManager(config.getResourceThrottlerFQN(), pluginManager);
        }
    }

    private static class SettingProviderManagerProvider extends SharedResourceProvider<SettingProviderManager> {
        private final StandaloneConfig config;
        private final PluginManager pluginManager;

        @Inject
        private SettingProviderManagerProvider(StandaloneConfig config,
                                               PluginManager pluginManager,
                                               SharedResourcesHolder sharedResourcesHolder) {
            super(sharedResourcesHolder);
            this.config = config;
            this.pluginManager = pluginManager;
        }


        @Override
        public SettingProviderManager share() {
            return new SettingProviderManager(config.getSettingProviderFQN(), pluginManager);
        }
    }

    private static class ClientBalancerManagerProvider extends SharedResourceProvider<ClientBalancerManager> {
        private final PluginManager pluginManager;

        @Inject
        private ClientBalancerManagerProvider(PluginManager pluginManager,
                                              SharedResourcesHolder sharedResourcesHolder) {
            super(sharedResourcesHolder);
            this.pluginManager = pluginManager;
        }

        @Override
        public ClientBalancerManager share() {
            return new ClientBalancerManager(pluginManager);
        }
    }

    private static class SubBrokerManagerProvider extends SharedResourceProvider<ISubBrokerManager> {
        private final PluginManager pluginManager;
        private final IMqttBrokerClient mqttBrokerClient;
        private final IInboxClient inboxClient;

        @Inject
        private SubBrokerManagerProvider(PluginManager pluginManager,
                                         IMqttBrokerClient mqttBrokerClient,
                                         IInboxClient inboxClient,
                                         SharedResourcesHolder sharedResourcesHolder) {
            super(sharedResourcesHolder);
            this.pluginManager = pluginManager;
            this.mqttBrokerClient = mqttBrokerClient;
            this.inboxClient = inboxClient;
        }

        @Override
        public ISubBrokerManager share() {
            return new SubBrokerManager(pluginManager, mqttBrokerClient, inboxClient);
        }
    }
}
