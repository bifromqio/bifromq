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

import static com.baidu.bifromq.baseutils.ThreadUtil.threadFactory;

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.balance.option.KVRangeBalanceControllerOptions;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.baserpc.utils.NettyUtil;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.server.IDistServer;
import com.baidu.bifromq.dist.worker.IDistWorker;
import com.baidu.bifromq.inbox.client.IInboxBrokerClient;
import com.baidu.bifromq.inbox.client.IInboxReaderClient;
import com.baidu.bifromq.inbox.server.IInboxServer;
import com.baidu.bifromq.inbox.store.IInboxStore;
import com.baidu.bifromq.mqtt.IMQTTBroker;
import com.baidu.bifromq.mqtt.MQTTBrokerBuilder;
import com.baidu.bifromq.mqtt.inbox.IMqttBrokerClient;
import com.baidu.bifromq.plugin.authprovider.AuthProviderManager;
import com.baidu.bifromq.plugin.eventcollector.EventCollectorManager;
import com.baidu.bifromq.plugin.inboxbroker.IInboxBrokerManager;
import com.baidu.bifromq.plugin.inboxbroker.InboxBrokerManager;
import com.baidu.bifromq.plugin.manager.BifroMQPluginManager;
import com.baidu.bifromq.plugin.settingprovider.SettingProviderManager;
import com.baidu.bifromq.retain.client.IRetainServiceClient;
import com.baidu.bifromq.retain.server.IRetainServer;
import com.baidu.bifromq.retain.store.IRetainStore;
import com.baidu.bifromq.sessiondict.client.ISessionDictionaryClient;
import com.baidu.bifromq.sessiondict.server.ISessionDictionaryServer;
import com.baidu.bifromq.starter.config.StandaloneConfig;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.PluginManager;

@Slf4j
public class StandaloneStarter extends BaseEngineStarter<StandaloneConfig> {
    private PluginManager pluginMgr;
    private Executor ioClientExecutor;
    private Executor ioServerExecutor;
    private Executor queryExecutor;
    private ExecutorService mutationExecutor;
    private ScheduledExecutorService tickTaskExecutor;
    private ScheduledExecutorService bgTaskExecutor;
    private AuthProviderManager authProviderMgr;
    private EventCollectorManager eventCollectorMgr;
    private SettingProviderManager settingProviderMgr;
    private IAgentHost agentHost;
    private ICRDTService clientCrdtService;
    private ICRDTService serverCrdtService;
    private IMqttBrokerClient mqttBrokerClient;
    private ISessionDictionaryClient sessionDictClient;
    private ISessionDictionaryServer sessionDictServer;
    private IDistClient distClient;
    private IBaseKVStoreClient distWorkerClient;
    private IDistWorker distWorker;
    private IDistServer distServer;
    private IInboxReaderClient inboxReaderClient;
    private IInboxBrokerClient inboxBrokerClient;
    private IBaseKVStoreClient inboxStoreClient;
    private IInboxStore inboxStore;
    private IInboxServer inboxServer;
    private IRetainServiceClient retainClient;
    private IBaseKVStoreClient retainStoreClient;
    private IRetainStore retainStore;
    private IRetainServer retainServer;
    private IMQTTBroker mqttBroker;
    private IInboxBrokerManager inboxBrokerMgr;


    @Override
    protected void init(StandaloneConfig config) {
        pluginMgr = new BifroMQPluginManager();

        pluginMgr.loadPlugins();

        pluginMgr.startPlugins();

        ioClientExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(config.getIoClientParallelism(), config.getIoClientParallelism(), 0L,
                TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(), threadFactory("io-client-executor-%d")),
            "io-client-executor");
        ioServerExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(config.getIoServerParallelism(), config.getIoServerParallelism(), 0L,
                TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(), threadFactory("io-server-executor-%d")),
            "io-server-executor");
        queryExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(config.getQueryThreads(), config.getQueryThreads(), 0L,
                TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(), threadFactory("query-executor-%d")),
            "query-executor");
        mutationExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(config.getMutationThreads(), config.getMutationThreads(), 0L,
                TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(), threadFactory("mutation-executor-%d")),
            "mutation-executor");
        tickTaskExecutor = ExecutorServiceMetrics
            .monitor(Metrics.globalRegistry, new ScheduledThreadPoolExecutor(config.getTickerThreads(),
                threadFactory("tick-task-executor-%d")), "tick-task-executor");
        bgTaskExecutor = ExecutorServiceMetrics
            .monitor(Metrics.globalRegistry, new ScheduledThreadPoolExecutor(config.getBgWorkerThreads(),
                threadFactory("bg-task-executor-%d")), "bg-task-executor");

        eventCollectorMgr = new EventCollectorManager(pluginMgr);

        settingProviderMgr = new SettingProviderManager(config.getSettingProviderFQN(), pluginMgr);

        authProviderMgr = new AuthProviderManager(config.getAuthProviderFQN(),
            pluginMgr, settingProviderMgr, eventCollectorMgr);

        AgentHostOptions agentHostOpts = AgentHostOptions.builder().addr("127.0.0.1").port(freePort()).build();
        agentHost = IAgentHost.newInstance(agentHostOpts);
        agentHost.start();
        log.debug("Agent host started");

        clientCrdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        clientCrdtService.start(agentHost);
        serverCrdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        serverCrdtService.start(agentHost);
        log.debug("CRDT service started");

        sessionDictClient = ISessionDictionaryClient.inProcBuilder()
            .executor(MoreExecutors.directExecutor())
            .build();
        sessionDictServer = ISessionDictionaryServer.inProcServerBuilder()
            .executor(MoreExecutors.directExecutor())
            .build();
        inboxReaderClient = IInboxReaderClient.inProcClientBuilder()
            .executor(MoreExecutors.directExecutor())
            .build();
        inboxBrokerClient = IInboxBrokerClient.inProcClientBuilder()
            .executor(MoreExecutors.directExecutor())
            .build();
        inboxStoreClient = IBaseKVStoreClient.inProcClientBuilder()
            .clusterId(IInboxStore.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .executor(ioClientExecutor)
            .queryPipelinesPerServer(config.getInboxStoreConfig().getInboxStoreClientConfig()
                .getQueryPipelinePerServer())
            .execPipelinesPerServer(config.getInboxStoreConfig().getInboxStoreClientConfig()
                .getExecPipelinePerServer())
            .build();
        inboxStore = IInboxStore.inProcBuilder()
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .storeClient(inboxStoreClient)
            .eventCollector(eventCollectorMgr)
            .ioExecutor(MoreExecutors.directExecutor())
            .queryExecutor(queryExecutor)
            .mutationExecutor(mutationExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .kvRangeStoreOptions(new KVRangeStoreOptions()
                .setDataEngineConfigurator(
                    buildEngineConf(config.getInboxStoreConfig().getDataEngineConfig(), "inbox_data"))
                .setWalEngineConfigurator(
                    buildEngineConf(config.getInboxStoreConfig().getWalEngineConfig(), "inbox_wal")))
            .build();
        inboxServer = IInboxServer.inProcBuilder()
            .settingProvider(settingProviderMgr)
            .ioExecutor(ioServerExecutor)
            .storeClient(inboxStoreClient)
            .bgTaskExecutor(bgTaskExecutor)
            .build();
        retainClient = IRetainServiceClient.inProcClientBuilder()
            .executor(MoreExecutors.directExecutor())
            .build();
        retainStoreClient = IBaseKVStoreClient
            .inProcClientBuilder()
            .clusterId(IRetainStore.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .executor(ioClientExecutor)
            .queryPipelinesPerServer(config.getRetainStoreConfig().getRetainStoreClientConfig()
                .getQueryPipelinePerServer())
            .execPipelinesPerServer(config.getRetainStoreConfig().getRetainStoreClientConfig()
                .getExecPipelinePerServer())
            .build();
        retainStore = IRetainStore
            .inProcBuilder()
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .storeClient(retainStoreClient)
            .ioExecutor(MoreExecutors.directExecutor())
            .queryExecutor(queryExecutor)
            .mutationExecutor(mutationExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .kvRangeStoreOptions(new KVRangeStoreOptions()
                .setDataEngineConfigurator(
                    buildEngineConf(config.getRetainStoreConfig().getDataEngineConfig(), "retain_data"))
                .setWalEngineConfigurator(
                    buildEngineConf(config.getRetainStoreConfig().getWalEngineConfig(), "retain_wal")))
            .build();
        retainServer = IRetainServer
            .inProcBuilder()
            .settingProvider(settingProviderMgr)
            .storeClient(retainStoreClient)
            .ioExecutor(ioServerExecutor)
            .build();

        distClient = IDistClient.inProcClientBuilder()
            .executor(MoreExecutors.directExecutor())
            .build();

        mqttBrokerClient = IMqttBrokerClient.inProcClientBuilder()
            .executor(MoreExecutors.directExecutor())
            .build();

        inboxBrokerMgr = new InboxBrokerManager(pluginMgr, mqttBrokerClient, inboxBrokerClient);

        distWorkerClient = IBaseKVStoreClient.inProcClientBuilder()
            .clusterId(IDistWorker.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .executor(ioClientExecutor)
            .queryPipelinesPerServer(config.getDistWorkerConfig().getDistWorkerClientConfig()
                .getQueryPipelinePerServer())
            .execPipelinesPerServer(config.getDistWorkerConfig().getDistWorkerClientConfig()
                .getExecPipelinePerServer())
            .build();

        distWorker = IDistWorker.inProcBuilder()
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .settingProvider(settingProviderMgr)
            .eventCollector(eventCollectorMgr)
            .distClient(distClient)
            .storeClient(distWorkerClient)
            .ioExecutor(MoreExecutors.directExecutor())
            .queryExecutor(queryExecutor)
            .mutationExecutor(mutationExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .kvRangeStoreOptions(new KVRangeStoreOptions()
                .setDataEngineConfigurator(
                    buildEngineConf(config.getDistWorkerConfig().getDataEngineConfig(), "dist_data"))
                .setWalEngineConfigurator(
                    buildEngineConf(config.getDistWorkerConfig().getWalEngineConfig(), "dist_wal")))
            .balanceControllerOptions(new KVRangeBalanceControllerOptions())
            .inboxBrokerManager(inboxBrokerMgr)
            .build();

        distServer = IDistServer.inProcBuilder()
            .storeClient(distWorkerClient)
            .settingProvider(settingProviderMgr)
            .eventCollector(eventCollectorMgr)
            .crdtService(clientCrdtService)
            .ioExecutor(ioServerExecutor)
            .build();

        MQTTBrokerBuilder.InProcBrokerBuilder brokerBuilder = IMQTTBroker.inProcBrokerBuilder()
            .host(config.getHost())
            .bossGroup(NettyUtil.createEventLoopGroup(1))
            .workerGroup(NettyUtil.createEventLoopGroup(config.getMqttWorkerThreads()))
            .ioExecutor(ioServerExecutor)
            .authProvider(authProviderMgr)
            .eventCollector(eventCollectorMgr)
            .settingProvider(settingProviderMgr)
            .distClient(distClient)
            .inboxReader(inboxReaderClient)
            .sessionDictClient(sessionDictClient)
            .retainClient(retainClient)
            .connectTimeoutSeconds(config.getConnTimeoutSec())
            .connectRateLimit(config.getMaxConnPerSec())
            .disconnectRate(config.getMaxDisconnPerSec())
            .readLimit(config.getMaxConnBandwidth())
            .writeLimit(config.getMaxConnBandwidth())
            .defaultKeepAliveSeconds(config.getDefaultKeepAliveSec())
            .maxBytesInMessage(config.getMaxMsgByteSize())
            .maxResendTimes(config.getMaxResendTimes())
            .qos2ConfirmWindowSeconds(config.getQos2ConfirmWindowSec());
        if (config.isTcpEnabled()) {
            brokerBuilder.buildTcpConnListener()
                .port(config.getTcpPort())
                .buildListener();
        }
        if (config.isTlsEnabled()) {
            Preconditions.checkNotNull(config.getBrokerSSLCtxConfig());
            brokerBuilder.buildTLSConnListener()
                .port(config.getTlsPort())
                .sslContext(buildServerSslContext(config.getBrokerSSLCtxConfig()))
                .buildListener();
        }
        if (config.isWsEnabled()) {
            brokerBuilder.buildWSConnListener()
                .path(config.getWsPath())
                .port(config.getWsPort())
                .buildListener();
        }
        if (config.isWssEnabled()) {
            brokerBuilder.buildWSSConnListener()
                .path(config.getWsPath())
                .port(config.getWssPort())
                .sslContext(buildServerSslContext(config.getBrokerSSLCtxConfig()))
                .buildListener();
        }

        mqttBroker = brokerBuilder.build();
    }

    @Override
    protected Class<StandaloneConfig> configClass() {
        return StandaloneConfig.class;
    }

    public void start() {
        sessionDictServer.start();

        inboxStore.start(true);

        inboxServer.start();
        inboxStoreClient.join();

        retainStore.start(true);
        retainServer.start();
        retainStoreClient.join();

        distWorker.start(true);
        distServer.start();
        distWorkerClient.join();

        mqttBroker.start();
        log.info("Standalone broker started");
        setupMetrics();
    }

    public void stop() {
        mqttBroker.shutdown();

        distClient.stop();
        distServer.shutdown();

        distWorkerClient.stop();
        distWorker.stop();

        inboxReaderClient.stop();
        inboxServer.shutdown();

        inboxStoreClient.stop();
        inboxStore.stop();

        retainClient.stop();
        retainServer.shutdown();

        retainStoreClient.stop();
        retainStore.stop();

        sessionDictClient.stop();
        sessionDictServer.shutdown();

        clientCrdtService.stop();
        serverCrdtService.stop();

        agentHost.shutdown();
        log.debug("Agent host stopped");

        authProviderMgr.close();

        eventCollectorMgr.close();

        settingProviderMgr.close();

        inboxBrokerMgr.stop();

        if (ioClientExecutor instanceof ExecutorService) {
            ((ExecutorService) ioClientExecutor).shutdownNow();
            log.debug("Shutdown io client executor");
        }
        if (ioServerExecutor instanceof ExecutorService) {
            ((ExecutorService) ioServerExecutor).shutdownNow();
            log.debug("Shutdown io server executor");
        }
        if (queryExecutor instanceof ExecutorService) {
            ((ExecutorService) queryExecutor).shutdownNow();
            log.debug("Shutdown query executor");
        }
        if (mutationExecutor instanceof ExecutorService) {
            mutationExecutor.shutdownNow();
            log.debug("Shutdown mutation executor");
        }
        tickTaskExecutor.shutdownNow();
        log.debug("Shutdown tick task executor");
        bgTaskExecutor.shutdownNow();
        log.debug("Shutdown bg task executor");

        pluginMgr.stopPlugins();
        pluginMgr.unloadPlugins();
    }

    public static void main(String[] args) {
        StarterRunner.run(StandaloneStarter.class, args);
    }


    private int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
