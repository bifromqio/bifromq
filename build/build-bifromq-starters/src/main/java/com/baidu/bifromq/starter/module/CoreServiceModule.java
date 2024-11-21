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

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.metaservice.IBaseKVMetaService;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import com.baidu.bifromq.starter.config.StandaloneConfig;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;

public class CoreServiceModule extends AbstractModule {
    private static class AgentHostProvider implements Provider<IAgentHost> {
        private final StandaloneConfig config;

        @Inject
        private AgentHostProvider(StandaloneConfig config) {
            this.config = config;
        }

        @Override
        public IAgentHost get() {
            AgentHostOptions agentHostOptions = AgentHostOptions.builder()
                .env(config.getClusterConfig().getEnv())
                .addr(config.getClusterConfig().getHost())
                .port(config.getClusterConfig().getPort())
                .build();
            return IAgentHost.newInstance(agentHostOptions);
        }
    }

    private static class CRDTServiceProvider implements Provider<ICRDTService> {
        private final StandaloneConfig config;
        private final IAgentHost agentHost;

        @Inject
        private CRDTServiceProvider(StandaloneConfig config, IAgentHost agentHost) {
            this.config = config;
            this.agentHost = agentHost;
        }

        @Override
        public ICRDTService get() {
            return ICRDTService.newInstance(agentHost, CRDTServiceOptions.builder().build());
        }
    }

    private static class BaseKVMetaServiceProvider implements Provider<IBaseKVMetaService> {
        private final ICRDTService crdtService;

        @Inject
        private BaseKVMetaServiceProvider(ICRDTService crdtService) {
            this.crdtService = crdtService;
        }

        @Override
        public IBaseKVMetaService get() {
            return IBaseKVMetaService.newInstance(crdtService);
        }
    }

    private static class RPCServiceTrafficServiceProvider implements Provider<IRPCServiceTrafficService> {
        private final ICRDTService crdtService;

        @Inject
        private RPCServiceTrafficServiceProvider(ICRDTService crdtService) {
            this.crdtService = crdtService;
        }

        @Override
        public IRPCServiceTrafficService get() {
            return IRPCServiceTrafficService.newInstance(crdtService);
        }
    }

    @Override
    protected void configure() {
        bind(IAgentHost.class).toProvider(AgentHostProvider.class).asEagerSingleton();
        bind(ICRDTService.class).toProvider(CRDTServiceProvider.class).asEagerSingleton();
        bind(IBaseKVMetaService.class).toProvider(BaseKVMetaServiceProvider.class).asEagerSingleton();
        bind(IRPCServiceTrafficService.class).toProvider(RPCServiceTrafficServiceProvider.class).asEagerSingleton();
    }
}
