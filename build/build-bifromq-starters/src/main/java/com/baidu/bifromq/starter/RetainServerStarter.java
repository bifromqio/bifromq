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
import com.baidu.bifromq.plugin.manager.BifroMQPluginManager;
import com.baidu.bifromq.plugin.settingprovider.SettingProviderManager;
import com.baidu.bifromq.retain.server.IRetainServer;
import com.baidu.bifromq.starter.config.RetainServerConfig;
import com.baidu.bifromq.starter.config.model.StoreClientConfig;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.PluginManager;

@Slf4j
public class RetainServerStarter extends BaseStarter<RetainServerConfig> {

    private PluginManager pluginMgr;

    private SettingProviderManager settingProviderMgr;

    private IAgentHost agentHost;

    private ICRDTService crdtService;

    private IRetainServer retainServer;

    private IBaseKVStoreClient retainStoreClient;

    @Override
    protected void init(RetainServerConfig config) {
        pluginMgr = new BifroMQPluginManager();
        pluginMgr.loadPlugins();
        pluginMgr.startPlugins();

        settingProviderMgr = new SettingProviderManager(config.getSettingProviderFQN(), pluginMgr);

        agentHost = initAgentHost(config.getAgentHostConfig());
        log.info("Agent host started");

        crdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        crdtService.start(agentHost);
        log.info("CRDT service started");

        retainStoreClient = buildRetainStoreClient(config.getRetainStoreClientConfig());
        retainServer = buildRetainServer(config);
    }

    @Override
    protected Class<RetainServerConfig> configClass() {
        return RetainServerConfig.class;
    }

    public void start() {
        setupMetrics();
        retainServer.start();
        log.info("Retain server started");
    }

    public void stop() {
        retainServer.shutdown();
        log.info("Retain server shutdown");

        retainStoreClient.stop();
        log.info("RetainStore client stopped");

        crdtService.stop();
        log.debug("CRDT service stopped");

        agentHost.shutdown();
        log.debug("Agent host stopped");

        settingProviderMgr.close();
        log.debug("Setting provider manager stopped");

        pluginMgr.stopPlugins();
        pluginMgr.unloadPlugins();
    }

    public static void main(String[] args) {
        StarterRunner.run(RetainServerStarter.class, args);
    }

    private IBaseKVStoreClient buildRetainStoreClient(StoreClientConfig config) {
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

    private IRetainServer buildRetainServer(RetainServerConfig config) {
        if (config.getServerSSLCtxConfig().isEnableSSL()) {
            return IRetainServer.sslBuilder()
                .id(UUID.randomUUID().toString())
                .host(config.getHost())
                .port(config.getPort())
                .crdtService(crdtService)
                .settingProvider(settingProviderMgr)
                .storeClient(retainStoreClient)
                .ioExecutor(MoreExecutors.directExecutor())
                .trustCertsFile(loadFromConfDir(config.getServerSSLCtxConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(config.getServerSSLCtxConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(config.getServerSSLCtxConfig().getKeyFile()))
                .build();
        }
        return IRetainServer.nonSSLBuilder()
            .id(UUID.randomUUID().toString())
            .host(config.getHost())
            .port(config.getPort())
            .crdtService(crdtService)
            .settingProvider(settingProviderMgr)
            .storeClient(retainStoreClient)
            .ioExecutor(MoreExecutors.directExecutor())
            .build();
    }

}
