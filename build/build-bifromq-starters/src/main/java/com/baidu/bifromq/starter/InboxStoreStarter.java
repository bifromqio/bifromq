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
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.inbox.store.IInboxStore;
import com.baidu.bifromq.inbox.store.InboxStoreBuilder;
import com.baidu.bifromq.plugin.eventcollector.EventCollectorManager;
import com.baidu.bifromq.plugin.manager.BifroMQPluginManager;
import com.baidu.bifromq.starter.config.InboxStoreConfig;
import com.baidu.bifromq.starter.config.model.StoreClientConfig;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.PluginManager;

@Slf4j
public class InboxStoreStarter extends BaseEngineStarter<InboxStoreConfig> {

    private PluginManager pluginMgr;
    private ScheduledExecutorService bgTaskExecutor;

    private EventCollectorManager eventCollectorMgr;

    private IAgentHost agentHost;

    private ICRDTService crdtService;

    private IBaseKVStoreClient inboxStoreClient;

    private IInboxStore inboxStore;

    private boolean bootstrap;

    @Override
    protected void init(InboxStoreConfig config) {
        bootstrap = config.isBootstrap();
        pluginMgr = new BifroMQPluginManager();
        pluginMgr.loadPlugins();
        pluginMgr.startPlugins();

        bgTaskExecutor = ExecutorServiceMetrics
            .monitor(Metrics.globalRegistry, new ScheduledThreadPoolExecutor(config.getBgWorkerThreads(),
                new ThreadFactoryBuilder().setNameFormat("bg-job-executor-%d").build()), "bgTaskExecutor");
        eventCollectorMgr = new EventCollectorManager(pluginMgr);


        agentHost = initAgentHost(config.getAgentHostConfig());
        log.info("Agent host started");

        crdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        crdtService.start(agentHost);
        log.info("CRDT service started");

        inboxStoreClient = buildInboxStoreClient(config.getInboxStoreClientConfig());
        inboxStore = buildInboxStore(config);
    }

    @Override
    protected Class<InboxStoreConfig> configClass() {
        return InboxStoreConfig.class;
    }

    public void start() {
        inboxStore.start(bootstrap);
        log.info("Inbox store started");
        setupMetrics();
    }

    public void stop() {
        inboxStore.stop();
        log.info("Inbox store shutdown");

        inboxStoreClient.stop();
        log.info("InboxStore client stopped");

        crdtService.stop();
        log.debug("CRDT service stopped");

        agentHost.shutdown();
        log.debug("Agent host stopped");

        eventCollectorMgr.close();
        log.debug("Event collector manager stopped");

        MoreExecutors.shutdownAndAwaitTermination(bgTaskExecutor, 5, TimeUnit.SECONDS);
        log.debug("Shutdown bg task executor");

        pluginMgr.stopPlugins();
        pluginMgr.unloadPlugins();
    }

    public static void main(String[] args) {
        StarterRunner.run(InboxStoreStarter.class, args);
    }

    private IBaseKVStoreClient buildInboxStoreClient(StoreClientConfig config) {
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

    private IInboxStore buildInboxStore(InboxStoreConfig config) {
        InboxStoreBuilder inboxStoreBuilder;
        if (config.getServerSSLCtxConfig().isEnableSSL()) {
            inboxStoreBuilder = IInboxStore.sslBuilder()
                .bindAddr(config.getHost())
                .bindPort(config.getPort())
                .trustCertsFile(loadFromConfDir(config.getServerSSLCtxConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(config.getServerSSLCtxConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(config.getServerSSLCtxConfig().getKeyFile()));
        } else {
            inboxStoreBuilder = IInboxStore.nonSSLBuilder()
                .bindAddr(config.getHost())
                .bindPort(config.getPort());
        }
        return inboxStoreBuilder
            .agentHost(agentHost)
            .crdtService(crdtService)
            .storeClient(inboxStoreClient)
            .eventCollector(eventCollectorMgr)
            .ioExecutor(MoreExecutors.directExecutor())
            .bgTaskExecutor(bgTaskExecutor)
            .kvRangeStoreOptions(new KVRangeStoreOptions()
                .setOverrideIdentity(config.getOverrideIdentity())
                .setWalFlushBufferSize(config.getWalFlushBufferSize())
                .setDataEngineConfigurator(
                    buildEngineConf(config.getDataEngineConfig(), "inbox_data"))
                .setWalEngineConfigurator(
                    buildEngineConf(config.getWalEngineConfig(), "inbox_wal")))
            .build();
    }


}
