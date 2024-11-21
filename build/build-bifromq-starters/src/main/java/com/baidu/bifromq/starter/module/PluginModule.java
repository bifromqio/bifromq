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
import com.google.inject.Provider;
import org.pf4j.PluginManager;
import org.slf4j.Logger;

public class PluginModule extends AbstractModule {
    private static class PluginManagerProvider implements Provider<PluginManager> {
        private final Logger log;

        @Inject
        private PluginManagerProvider(Logger log) {
            this.log = log;
        }

        @Override
        public PluginManager get() {
            PluginManager pluginMgr = new BifroMQPluginManager();
            pluginMgr.loadPlugins();
            pluginMgr.startPlugins();
            pluginMgr.getPlugins().forEach(
                plugin -> log.info("Loaded plugin: {}@{}",
                    plugin.getDescriptor().getPluginId(), plugin.getDescriptor().getVersion()));
            return pluginMgr;
        }
    }

    private static class AuthProviderManagerProvider implements Provider<AuthProviderManager> {
        private final StandaloneConfig config;
        private final PluginManager pluginManager;
        private final SettingProviderManager settingProviderManager;
        private final EventCollectorManager eventCollectorManager;

        @Inject
        private AuthProviderManagerProvider(StandaloneConfig config,
                                            PluginManager pluginManager,
                                            SettingProviderManager settingProviderManager,
                                            EventCollectorManager eventCollectorManager) {
            this.config = config;
            this.pluginManager = pluginManager;
            this.settingProviderManager = settingProviderManager;
            this.eventCollectorManager = eventCollectorManager;
        }

        @Override
        public AuthProviderManager get() {
            return new AuthProviderManager(config.getAuthProviderFQN(),
                pluginManager,
                settingProviderManager,
                eventCollectorManager);
        }
    }

    private static class EventCollectorManagerProvider implements Provider<EventCollectorManager> {
        private final PluginManager pluginManager;

        @Inject
        private EventCollectorManagerProvider(PluginManager pluginManager) {
            this.pluginManager = pluginManager;
        }

        @Override
        public EventCollectorManager get() {
            return new EventCollectorManager(pluginManager);
        }
    }

    private static class ResourceThrottlerManagerProvider implements Provider<ResourceThrottlerManager> {
        private final StandaloneConfig config;
        private final PluginManager pluginManager;

        @Inject
        private ResourceThrottlerManagerProvider(StandaloneConfig config, PluginManager pluginManager) {
            this.config = config;
            this.pluginManager = pluginManager;
        }

        @Override
        public ResourceThrottlerManager get() {
            return new ResourceThrottlerManager(config.getResourceThrottlerFQN(), pluginManager);
        }
    }

    private static class SettingProviderManagerProvider implements Provider<SettingProviderManager> {
        private final StandaloneConfig config;
        private final PluginManager pluginManager;

        @Inject
        private SettingProviderManagerProvider(StandaloneConfig config, PluginManager pluginManager) {
            this.config = config;
            this.pluginManager = pluginManager;
        }


        @Override
        public SettingProviderManager get() {
            return new SettingProviderManager(config.getSettingProviderFQN(), pluginManager);
        }
    }

    private static class ClientBalancerManagerProvider implements Provider<ClientBalancerManager> {
        private final PluginManager pluginManager;

        @Inject
        private ClientBalancerManagerProvider(PluginManager pluginManager) {
            this.pluginManager = pluginManager;
        }

        @Override
        public ClientBalancerManager get() {
            return new ClientBalancerManager(pluginManager);
        }
    }

    private static class SubBrokerManagerProvider implements Provider<ISubBrokerManager> {
        private final PluginManager pluginManager;
        private final IMqttBrokerClient mqttBrokerClient;
        private final IInboxClient inboxClient;

        @Inject
        private SubBrokerManagerProvider(PluginManager pluginManager,
                                         IMqttBrokerClient mqttBrokerClient,
                                         IInboxClient inboxClient) {
            this.pluginManager = pluginManager;
            this.mqttBrokerClient = mqttBrokerClient;
            this.inboxClient = inboxClient;
        }

        @Override
        public ISubBrokerManager get() {
            return new SubBrokerManager(pluginManager, mqttBrokerClient, inboxClient);
        }
    }

    @Override
    protected void configure() {
        bind(PluginManager.class).toProvider(PluginManagerProvider.class).asEagerSingleton();
        bind(ISubBrokerManager.class).toProvider(SubBrokerManagerProvider.class).asEagerSingleton();
        bind(AuthProviderManager.class).toProvider(AuthProviderManagerProvider.class).asEagerSingleton();
        bind(EventCollectorManager.class).toProvider(EventCollectorManagerProvider.class).asEagerSingleton();
        bind(ResourceThrottlerManager.class).toProvider(ResourceThrottlerManagerProvider.class).asEagerSingleton();
        bind(SettingProviderManager.class).toProvider(SettingProviderManagerProvider.class).asEagerSingleton();
        bind(ClientBalancerManager.class).toProvider(ClientBalancerManagerProvider.class).asEagerSingleton();
    }
}
