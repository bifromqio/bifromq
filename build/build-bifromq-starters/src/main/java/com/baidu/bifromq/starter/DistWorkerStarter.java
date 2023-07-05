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
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.worker.DistWorkerBuilder;
import com.baidu.bifromq.dist.worker.IDistWorker;
import com.baidu.bifromq.inbox.client.IInboxBrokerClient;
import com.baidu.bifromq.mqtt.inbox.IMqttBrokerClient;
import com.baidu.bifromq.plugin.eventcollector.EventCollectorManager;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.plugin.subbroker.SubBrokerManager;
import com.baidu.bifromq.plugin.manager.BifroMQPluginManager;
import com.baidu.bifromq.plugin.settingprovider.SettingProviderManager;
import com.baidu.bifromq.starter.config.DistWorkerConfig;
import com.baidu.bifromq.starter.config.model.RPCClientConfig;
import com.baidu.bifromq.starter.config.model.StoreClientConfig;
import com.google.common.util.concurrent.MoreExecutors;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.PluginManager;

@Slf4j
public class DistWorkerStarter extends BaseEngineStarter<DistWorkerConfig> {

    private ExecutorService pushExecutor;

    private ScheduledExecutorService bgTaskExecutor;

    private PluginManager pluginMgr;

    private SettingProviderManager settingProviderMgr;

    private EventCollectorManager eventCollectorMgr;

    private ISubBrokerManager inboxBrokerMgr;

    private IAgentHost agentHost;

    private ICRDTService crdtService;

    private IDistClient distClient;

    private IBaseKVStoreClient distWorkerClient;

    private IDistWorker distWorker;

    private boolean bootstrap;

    @Override
    protected void init(DistWorkerConfig config) {
        bootstrap = config.isBootstrap();

        pushExecutor = ExecutorServiceMetrics
            .monitor(Metrics.globalRegistry,
                new ForkJoinPool(config.getPushIOThreads(), new ForkJoinPool.ForkJoinWorkerThreadFactory() {
                    final AtomicInteger index = new AtomicInteger(0);

                    @Override
                    public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                        ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                        worker.setName(String.format("push-io-executor-%d", index.incrementAndGet()));
                        worker.setDaemon(false);
                        return worker;
                    }
                }, null, true), "push-io-executor");
        bgTaskExecutor = ExecutorServiceMetrics
            .monitor(Metrics.globalRegistry, new ScheduledThreadPoolExecutor(config.getBgWorkerThreads(),
                EnvProvider.INSTANCE.newThreadFactory("bg-job-executor")), "bgTaskExecutor");
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

        distClient = buildDistClient(config.getDistClientConfig());

        IMqttBrokerClient mqttBrokerClient = buildMqttBrokerClient(config.getMqttBrokerClientConfig());

        IInboxBrokerClient inboxBrokerClient = buildInboxBrokerClient(config.getInboxBrokerClientConfig());

        distWorkerClient = buildDistWorkerClient(config.getDistWorkerClientConfig());

        inboxBrokerMgr = new SubBrokerManager(pluginMgr, mqttBrokerClient, inboxBrokerClient);

        distWorker = buildDistWorker(config);
    }

    @Override
    protected Class<DistWorkerConfig> configClass() {
        return DistWorkerConfig.class;
    }

    public void start() {
        setupMetrics();
        distWorker.start(bootstrap);
        log.info("Dist worker started");
    }

    public void stop() {
        distClient.stop();
        log.debug("Dist client stopped");

        distWorkerClient.stop();
        log.info("Dist worker store client stopped");

        distWorker.stop();
        log.debug("Dist worker stopped");

        crdtService.stop();
        log.debug("CRDT service stopped");

        agentHost.shutdown();
        log.debug("Agent host stopped");

        eventCollectorMgr.close();
        log.debug("Event collector manager stopped");

        settingProviderMgr.close();
        log.debug("Setting provider manager stopped");

        inboxBrokerMgr.stop();
        log.debug("Inbox broker manager stopped");

        MoreExecutors.shutdownAndAwaitTermination(pushExecutor, 5, TimeUnit.SECONDS);
        log.debug("Shutdown push executor");

        MoreExecutors.shutdownAndAwaitTermination(bgTaskExecutor, 5, TimeUnit.SECONDS);
        log.debug("Shutdown bgTask executor");

        pluginMgr.stopPlugins();
        pluginMgr.unloadPlugins();
    }

    public static void main(String[] args) {
        StarterRunner.run(DistWorkerStarter.class, args);
    }

    private IInboxBrokerClient buildInboxBrokerClient(RPCClientConfig config) {
        if (config.getSslContextConfig().isEnableSSL()) {
            return IInboxBrokerClient.sslClientBuilder()
                .trustCertsFile(loadFromConfDir(config.getSslContextConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(config.getSslContextConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(config.getSslContextConfig().getKeyFile()))
                .crdtService(crdtService)
                .executor(MoreExecutors.directExecutor())
                .build();
        }
        return IInboxBrokerClient.nonSSLClientBuilder()
            .crdtService(crdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
    }

    private IMqttBrokerClient buildMqttBrokerClient(RPCClientConfig config) {
        if (config.getSslContextConfig().isEnableSSL()) {
            return IMqttBrokerClient.sslClientBuilder()
                .trustCertsFile(loadFromConfDir(config.getSslContextConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(config.getSslContextConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(config.getSslContextConfig().getKeyFile()))
                .crdtService(crdtService)
                .executor(MoreExecutors.directExecutor())
                .build();
        }
        return IMqttBrokerClient.nonSSLClientBuilder()
            .crdtService(crdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
    }

    private IDistClient buildDistClient(RPCClientConfig config) {
        if (config.getSslContextConfig().isEnableSSL()) {
            return IDistClient.sslClientBuilder()
                .trustCertsFile(loadFromConfDir(config.getSslContextConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(config.getSslContextConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(config.getSslContextConfig().getKeyFile()))
                .crdtService(crdtService)
                .executor(MoreExecutors.directExecutor())
                .build();
        }
        return IDistClient.nonSSLClientBuilder()
            .crdtService(crdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
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

    private IDistWorker buildDistWorker(DistWorkerConfig config) {
        DistWorkerBuilder distWorkerBuilder;
        if (config.getServerSSLCtxConfig().isEnableSSL()) {
            distWorkerBuilder = IDistWorker.sslBuilder()
                .bindAddr(config.getHost())
                .bindPort(config.getPort())
                .trustCertsFile(loadFromConfDir(config.getServerSSLCtxConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(config.getServerSSLCtxConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(config.getServerSSLCtxConfig().getKeyFile()));
        } else {
            distWorkerBuilder = IDistWorker.nonSSLBuilder()
                .bindAddr(config.getHost())
                .bindPort(config.getPort());
        }
        return distWorkerBuilder.agentHost(agentHost)
            .crdtService(crdtService)
            .bgTaskExecutor(bgTaskExecutor)
            .ioExecutor(MoreExecutors.directExecutor())
            .distClient(distClient)
            .storeClient(distWorkerClient)
            .kvRangeStoreOptions(new KVRangeStoreOptions()
                .setOverrideIdentity(config.getOverrideIdentity())
                .setWalFlushBufferSize(config.getWalFlushBufferSize())
                .setDataEngineConfigurator(
                    buildEngineConf(config.getDataEngineConfig(), "dist_data"))
                .setWalEngineConfigurator(
                    buildEngineConf(config.getWalEngineConfig(), "dist_wal")))
            .balanceControllerOptions(config.getBalanceConfig())
            .eventCollector(eventCollectorMgr)
            .settingProvider(settingProviderMgr)
            .subBrokerManager(inboxBrokerMgr)
            .build();
    }


}
