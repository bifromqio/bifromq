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
import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.plugin.manager.BifroMQPluginManager;
import com.baidu.bifromq.retain.store.IRetainStore;
import com.baidu.bifromq.retain.store.RetainStoreBuilder;
import com.baidu.bifromq.starter.config.RetainStoreConfig;
import com.baidu.bifromq.starter.config.model.StoreClientConfig;
import com.google.common.util.concurrent.MoreExecutors;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.PluginManager;

@Slf4j
public class RetainStoreStarter extends BaseEngineStarter<RetainStoreConfig> {

    private PluginManager pluginMgr;
    private ScheduledExecutorService bgTaskExecutor;

    private IAgentHost agentHost;

    private ICRDTService crdtService;

    private IBaseKVStoreClient retainStoreClient;

    private IRetainStore retainStore;

    private boolean bootstrap;

    @Override
    protected void init(RetainStoreConfig config) {
        bootstrap = config.isBootstrap();
        pluginMgr = new BifroMQPluginManager();
        pluginMgr.loadPlugins();
        pluginMgr.startPlugins();

        bgTaskExecutor = ExecutorServiceMetrics
            .monitor(Metrics.globalRegistry, new ScheduledThreadPoolExecutor(config.getBgWorkerThreads(),
                EnvProvider.INSTANCE.newThreadFactory("bg-job-executor")), "bgTaskExecutor");

        agentHost = initAgentHost(config.getAgentHostConfig());
        log.info("Agent host started");

        crdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        crdtService.start(agentHost);
        log.info("CRDT service started");

        retainStoreClient = buildRetainStoreClient(config.getRetainStoreClientConfig());
        retainStore = buildRetainStore(config);
    }

    @Override
    protected Class<RetainStoreConfig> configClass() {
        return RetainStoreConfig.class;
    }

    public void start() {
        setupMetrics();
        retainStore.start(bootstrap);
        log.info("Retain store started");
    }

    public void stop() {
        retainStore.stop();
        log.info("Retain store shutdown");

        retainStoreClient.stop();
        log.info("RetainStore client stopped");

        crdtService.stop();
        log.debug("CRDT service stopped");

        agentHost.shutdown();
        log.debug("Agent host stopped");

        MoreExecutors.shutdownAndAwaitTermination(bgTaskExecutor, 5, TimeUnit.SECONDS);
        log.debug("Shutdown bg task executor");

        pluginMgr.stopPlugins();
        pluginMgr.unloadPlugins();
    }

    public static void main(String[] args) {
        StarterRunner.run(RetainStoreStarter.class, args);
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

    private IRetainStore buildRetainStore(RetainStoreConfig config) {
        RetainStoreBuilder retainStoreBuilder;
        if (config.getServerSSLCtxConfig().isEnableSSL()) {
            retainStoreBuilder = IRetainStore.sslBuilder()
                .bindAddr(config.getHost())
                .bindPort(config.getPort())
                .trustCertsFile(loadFromConfDir(config.getServerSSLCtxConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(config.getServerSSLCtxConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(config.getServerSSLCtxConfig().getKeyFile()));
        } else {
            retainStoreBuilder = IRetainStore.nonSSLBuilder()
                .bindAddr(config.getHost())
                .bindPort(config.getPort());
        }
        return retainStoreBuilder
            .agentHost(agentHost)
            .crdtService(crdtService)
            .storeClient(retainStoreClient)
            .ioExecutor(MoreExecutors.directExecutor())
            .bgTaskExecutor(bgTaskExecutor)
            .kvRangeStoreOptions(new KVRangeStoreOptions()
                .setOverrideIdentity(config.getOverrideIdentity())
                .setWalFlushBufferSize(config.getWalFlushBufferSize())
                .setDataEngineConfigurator(
                    buildEngineConf(config.getDataEngineConfig(), "retain_data"))
                .setWalEngineConfigurator(
                    buildEngineConf(config.getWalEngineConfig(), "retain_wal")))
            .build();
    }

}
