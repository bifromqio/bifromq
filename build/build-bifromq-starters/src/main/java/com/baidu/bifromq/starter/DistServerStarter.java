/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

package com.baidu.bifromq.starter;

import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.dist.server.IDistServer;
import com.baidu.bifromq.plugin.eventcollector.EventCollectorManager;
import com.baidu.bifromq.plugin.manager.BifroMQPluginManager;
import com.baidu.bifromq.plugin.settingprovider.SettingProviderManager;
import com.baidu.bifromq.starter.config.DistServerConfig;
import com.baidu.bifromq.starter.config.model.StoreClientConfig;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.PluginManager;

@Slf4j
public class DistServerStarter extends BaseStarter<DistServerConfig> {

    private PluginManager pluginMgr;

    private SettingProviderManager settingProviderMgr;

    private EventCollectorManager eventCollectorMgr;

    private IAgentHost agentHost;

    private ICRDTService crdtService;

    private IDistServer distServer;

    private IBaseKVStoreClient distWorkerClient;

    @Override
    protected Class<DistServerConfig> configClass() {
        return DistServerConfig.class;
    }

    @Override
    protected void init(DistServerConfig config) {
        pluginMgr = new BifroMQPluginManager();
        pluginMgr.loadPlugins();
        pluginMgr.startPlugins();

        settingProviderMgr = new SettingProviderManager(config.getSettingProviderFQN(), pluginMgr);
        eventCollectorMgr = new EventCollectorManager(pluginMgr);

        agentHost = initAgentHost(config.getAgentHostConfig());
        log.info("Agent host started");

        crdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        crdtService.start(agentHost);
        log.info("CRDT service started");

        distWorkerClient = buildDistWorkerClient(config.getDistWorkerStoreClientConfig());
        distServer = buildDistServer(config);
    }

    public void start() {
        setupMetrics();
        distServer.start();
        log.info("Dist server started");
    }

    public void stop() {
        distServer.shutdown();
        log.info("Dist server shut down");

        distWorkerClient.stop();
        log.info("DistStore client stopped");

        crdtService.stop();
        log.debug("CRDT service stopped");

        agentHost.shutdown();
        log.debug("Agent host stopped");

        eventCollectorMgr.close();
        log.debug("Event collector manager stopped");

        settingProviderMgr.close();
        log.debug("Setting provider manager stopped");

        pluginMgr.stopPlugins();
        pluginMgr.unloadPlugins();
    }

    public static void main(String[] args) {
        StarterRunner.run(DistServerStarter.class, args);
    }

    private IBaseKVStoreClient buildDistWorkerClient(StoreClientConfig config) {
        if (config.getSslContextConfig().isEnableSSL()) {
            return IBaseKVStoreClient.sslClientBuilder()
                .clusterId(config.getClusterName())
                .crdtService(crdtService)
                .executor(MoreExecutors.directExecutor())
                .queryPipelinesPerServer(config.getQueryPipelinePerServer())
                .execPipelinesPerServer(config.getExecPipelinePerServer())
                .trustCertsFile(loadFromConfDir(config.getSslContextConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(config.getSslContextConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(config.getSslContextConfig().getKeyFile()))
                .build();
        }
        return IBaseKVStoreClient.nonSSLClientBuilder()
            .clusterId(config.getClusterName())
            .crdtService(crdtService)
            .executor(MoreExecutors.directExecutor())
            .queryPipelinesPerServer(config.getQueryPipelinePerServer())
            .execPipelinesPerServer(config.getExecPipelinePerServer())
            .build();
    }

    private IDistServer buildDistServer(DistServerConfig config) {
        if (config.getServerSSLCtxConfig().isEnableSSL()) {
            return IDistServer.sslBuilder()
                .id(UUID.randomUUID().toString())
                .host(config.getHost())
                .port(config.getPort())
                .crdtService(crdtService)
                .settingProvider(settingProviderMgr)
                .eventCollector(eventCollectorMgr)
                .storeClient(distWorkerClient)
                .ioExecutor(MoreExecutors.directExecutor())
                .distCallPreSchedulerFactoryClass(config.getDistCallPreBatchSchedulerFactoryClass())
                .trustCertsFile(loadFromConfDir(config.getServerSSLCtxConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(config.getServerSSLCtxConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(config.getServerSSLCtxConfig().getKeyFile()))
                .build();
        }
        return IDistServer.nonSSLBuilder()
            .id(UUID.randomUUID().toString())
            .host(config.getHost())
            .port(config.getPort())
            .crdtService(crdtService)
            .settingProvider(settingProviderMgr)
            .eventCollector(eventCollectorMgr)
            .storeClient(distWorkerClient)
            .ioExecutor(MoreExecutors.directExecutor())
            .distCallPreSchedulerFactoryClass(config.getDistCallPreBatchSchedulerFactoryClass())
            .build();
    }


}
