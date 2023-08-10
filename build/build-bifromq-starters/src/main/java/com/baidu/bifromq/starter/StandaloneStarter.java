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

import static com.baidu.bifromq.sysprops.BifroMQSysProp.INBOX_LOAD_TRACKING_SECONDS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.INBOX_MAX_RANGE_LOAD;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.INBOX_SPLIT_KEY_EST_THRESHOLD;

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.balance.option.KVRangeBalanceControllerOptions;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.option.KVRangeOptions;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.baserpc.IRPCServer;
import com.baidu.bifromq.baserpc.RPCServerBuilder;
import com.baidu.bifromq.baserpc.utils.NettyUtil;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.server.IDistServer;
import com.baidu.bifromq.dist.worker.IDistWorker;
import com.baidu.bifromq.inbox.client.IInboxBrokerClient;
import com.baidu.bifromq.inbox.client.IInboxReaderClient;
import com.baidu.bifromq.inbox.server.IInboxServer;
import com.baidu.bifromq.inbox.store.IInboxStore;
import com.baidu.bifromq.mqtt.IMQTTBroker;
import com.baidu.bifromq.mqtt.IMQTTBrokerBuilder;
import com.baidu.bifromq.mqtt.inbox.IMqttBrokerClient;
import com.baidu.bifromq.plugin.authprovider.AuthProviderManager;
import com.baidu.bifromq.plugin.eventcollector.EventCollectorManager;
import com.baidu.bifromq.plugin.manager.BifroMQPluginManager;
import com.baidu.bifromq.plugin.settingprovider.SettingProviderManager;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.plugin.subbroker.SubBrokerManager;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.retain.server.IRetainServer;
import com.baidu.bifromq.retain.store.IRetainStore;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import com.baidu.bifromq.sessiondict.server.ISessionDictServer;
import com.baidu.bifromq.starter.config.standalone.StandaloneConfig;
import com.baidu.bifromq.starter.config.standalone.StandaloneConfigConsolidator;
import com.baidu.bifromq.starter.config.standalone.model.StateStoreConfig;
import com.baidu.bifromq.starter.config.standalone.model.mqttserver.MQTTServerConfig;
import com.baidu.bifromq.starter.utils.ConfigUtil;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.netty.handler.ssl.SslContext;
import io.reactivex.rxjava3.core.Observable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.PluginManager;

@Slf4j
public class StandaloneStarter extends BaseEngineStarter<StandaloneConfig> {
    private PluginManager pluginMgr;
    private ExecutorService ioClientExecutor;
    private ExecutorService ioServerExecutor;
    private ExecutorService queryExecutor;
    private ExecutorService mutationExecutor;
    private ScheduledExecutorService tickTaskExecutor;
    private ScheduledExecutorService bgTaskExecutor;
    private AuthProviderManager authProviderMgr;
    private EventCollectorManager eventCollectorMgr;
    private SettingProviderManager settingProviderMgr;
    private IAgentHost agentHost;
    private ICRDTService clientCrdtService;
    private ICRDTService serverCrdtService;
    private IRPCServer sharedIORpcServer;
    private IRPCServer sharedBaseKVRpcServer;
    private ISessionDictClient sessionDictClient;
    private ISessionDictServer sessionDictServer;
    private IDistClient distClient;
    private IBaseKVStoreClient distWorkerClient;
    private IDistWorker distWorker;
    private IDistServer distServer;
    private IInboxReaderClient inboxReaderClient;
    private IInboxBrokerClient inboxBrokerClient;
    private IBaseKVStoreClient inboxStoreClient;
    private IInboxStore inboxStore;
    private IInboxServer inboxServer;
    private IRetainClient retainClient;
    private IBaseKVStoreClient retainStoreClient;
    private IRetainStore retainStore;
    private IRetainServer retainServer;
    private IMqttBrokerClient mqttBrokerClient;
    private IMQTTBroker mqttBroker;
    private ISubBrokerManager subBrokerManager;

    @Override
    protected void init(StandaloneConfig config) {
        StandaloneConfigConsolidator.consolidate(config);
        log.info("Consolidated Config: \n{}", ConfigUtil.serialize(config));
        pluginMgr = new BifroMQPluginManager();
        pluginMgr.loadPlugins();
        pluginMgr.startPlugins();

        ioClientExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(config.getRpcClientConfig().getWorkerThreads(),
                config.getRpcClientConfig().getWorkerThreads(), 0L,
                TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                EnvProvider.INSTANCE.newThreadFactory("rpc-client-executor")), "rpc-client-executor");
        ioServerExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(config.getRpcServerConfig().getWorkerThreads(),
                config.getRpcServerConfig().getWorkerThreads(), 0L,
                TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                EnvProvider.INSTANCE.newThreadFactory("rpc-server-executor")), "rpc-server-executor");
        queryExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(config.getStateStoreConfig().getQueryThreads(),
                config.getStateStoreConfig().getQueryThreads(), 0L,
                TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                EnvProvider.INSTANCE.newThreadFactory("query-executor")), "query-executor");
        mutationExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(config.getStateStoreConfig().getMutationThreads(),
                config.getStateStoreConfig().getMutationThreads(), 0L,
                TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                EnvProvider.INSTANCE.newThreadFactory("mutation-executor")), "mutation-executor");
        tickTaskExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ScheduledThreadPoolExecutor(config.getStateStoreConfig().getTickerThreads(),
                EnvProvider.INSTANCE.newThreadFactory("tick-task-executor")), "tick-task-executor");
        bgTaskExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ScheduledThreadPoolExecutor(config.getStateStoreConfig().getBgWorkerThreads(),
                EnvProvider.INSTANCE.newThreadFactory("bg-task-executor")), "bg-task-executor");

        eventCollectorMgr = new EventCollectorManager(pluginMgr);

        settingProviderMgr = new SettingProviderManager(config.getSettingProviderFQN(), pluginMgr);

        authProviderMgr = new AuthProviderManager(config.getAuthProviderFQN(),
            pluginMgr, settingProviderMgr, eventCollectorMgr);

        AgentHostOptions agentHostOptions = AgentHostOptions.builder()
            .clusterDomainName(config.getClusterConfig().getClusterDomainName())
            .addr(config.getClusterConfig().getHost())
            .port(config.getClusterConfig().getPort())
            .build();
        agentHost = IAgentHost.newInstance(agentHostOptions);
        agentHost.start();
        String seeds = config.getClusterConfig().getSeedEndpoints();
        if (!Strings.isNullOrEmpty(seeds)) {
            log.debug("AgentHost join seedEndpoints: {}", seeds);
            Set<InetSocketAddress> seedEndpoints = Arrays.stream(seeds.split(","))
                .map(endpoint -> {
                    String[] hostPort = endpoint.trim().split(":");
                    return new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1]));
                }).collect(Collectors.toSet());
            agentHost.join(seedEndpoints)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        log.warn("AgentHost failed to join seedEndpoint: {}", seeds, e);
                    } else {
                        log.info("AgentHost joined seedEndpoint: {}", seeds);
                    }
                });
        }
        log.debug("Agent host started");

        clientCrdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        clientCrdtService.start(agentHost);
        serverCrdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        serverCrdtService.start(agentHost);
        log.debug("CRDT service started");

        RPCServerBuilder sharedIORPCServerBuilder = IRPCServer.newBuilder()
            .host(config.getRpcServerConfig().getHost())
            .port(config.getRpcServerConfig().getPort())
            .crdtService(serverCrdtService)
            .executor(ioServerExecutor);
        RPCServerBuilder sharedBaseKVRPCServerBuilder = IRPCServer.newBuilder()
            .host(config.getRpcServerConfig().getHost())
            .port(config.getBaseKVRpcServerConfig().getPort())
            .crdtService(serverCrdtService)
            .executor(MoreExecutors.directExecutor());
        if (config.getRpcServerConfig().getSslConfig() != null) {
            sharedIORPCServerBuilder.sslContext(buildServerSslContext(config.getRpcServerConfig().getSslConfig()));
            sharedBaseKVRPCServerBuilder.sslContext(buildServerSslContext(config.getRpcServerConfig().getSslConfig()));
        }
        SslContext clientSslContext = config.getRpcClientConfig().getSslConfig() != null ?
            buildClientSslContext(config.getRpcClientConfig().getSslConfig()) : null;
        sessionDictClient = ISessionDictClient.newBuilder()
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .sslContext(clientSslContext)
            .build();
        sessionDictServer = ISessionDictServer.nonStandaloneBuilder()
            .rpcServerBuilder(sharedIORPCServerBuilder)
            .build();
        inboxReaderClient = IInboxReaderClient.newBuilder()
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .sslContext(clientSslContext)
            .build();
        inboxBrokerClient = IInboxBrokerClient.newBuilder()
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .sslContext(clientSslContext)
            .build();
        inboxStoreClient = IBaseKVStoreClient.newBuilder()
            .clusterId(IInboxStore.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .executor(ioClientExecutor)
            .sslContext(clientSslContext)
            .queryPipelinesPerStore(config.getStateStoreConfig().getInboxStoreConfig()
                .getQueryPipelinePerStore())
            .build();
        inboxStore = IInboxStore.nonStandaloneBuilder()
            .rpcServerBuilder(sharedBaseKVRPCServerBuilder)
            .bootstrap(config.isBootstrap())
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .storeClient(inboxStoreClient)
            .eventCollector(eventCollectorMgr)
            .queryExecutor(queryExecutor)
            .mutationExecutor(mutationExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .balanceControllerOptions(
                toControllerOptions(config.getStateStoreConfig().getInboxStoreConfig().getBalanceConfig())
            )
            .storeOptions(new KVRangeStoreOptions()
                .setKvRangeOptions(new KVRangeOptions()
                    .setMaxRangeLoad(INBOX_MAX_RANGE_LOAD.get())
                    .setSplitKeyThreshold(INBOX_SPLIT_KEY_EST_THRESHOLD.get())
                    .setLoadTrackingWindowSec(INBOX_LOAD_TRACKING_SECONDS.get()))
                .setDataEngineConfigurator(buildEngineConf(config
                    .getStateStoreConfig()
                    .getInboxStoreConfig()
                    .getDataEngineConfig(), "inbox_data"))
                .setWalEngineConfigurator(buildEngineConf(config
                    .getStateStoreConfig()
                    .getInboxStoreConfig()
                    .getWalEngineConfig(), "inbox_wal")))
            .build();
        inboxServer = IInboxServer.nonStandaloneBuilder()
            .rpcServerBuilder(sharedIORPCServerBuilder)
            .settingProvider(settingProviderMgr)
            .inboxStoreClient(inboxStoreClient)
            .bgTaskExecutor(bgTaskExecutor)
            .build();
        retainClient = IRetainClient.newBuilder()
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .sslContext(clientSslContext)
            .build();
        retainStoreClient = IBaseKVStoreClient.newBuilder()
            .clusterId(IRetainStore.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .executor(ioClientExecutor)
            .sslContext(clientSslContext)
            .queryPipelinesPerStore(config
                .getStateStoreConfig()
                .getRetainStoreConfig()
                .getQueryPipelinePerStore())
            .build();
        retainStore = IRetainStore.nonStandaloneBuilder()
            .rpcServerBuilder(sharedBaseKVRPCServerBuilder)
            .bootstrap(config.isBootstrap())
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .storeClient(retainStoreClient)
            .queryExecutor(queryExecutor)
            .mutationExecutor(mutationExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .balanceControllerOptions(
                toControllerOptions(config.getStateStoreConfig().getRetainStoreConfig().getBalanceConfig())
            )
            .storeOptions(new KVRangeStoreOptions()
                .setDataEngineConfigurator(
                    buildEngineConf(config
                        .getStateStoreConfig()
                        .getRetainStoreConfig()
                        .getDataEngineConfig(), "retain_data"))
                .setWalEngineConfigurator(
                    buildEngineConf(config
                        .getStateStoreConfig()
                        .getRetainStoreConfig()
                        .getWalEngineConfig(), "retain_wal")))
            .build();
        retainServer = IRetainServer.nonStandaloneBuilder()
            .rpcServerBuilder(sharedIORPCServerBuilder)
            .settingProvider(settingProviderMgr)
            .retainStoreClient(retainStoreClient)
            .build();

        distClient = IDistClient.newBuilder()
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .sslContext(clientSslContext)
            .build();

        mqttBrokerClient = IMqttBrokerClient.newBuilder()
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .sslContext(clientSslContext)
            .build();

        subBrokerManager = new SubBrokerManager(pluginMgr, mqttBrokerClient, inboxBrokerClient);

        distWorkerClient = IBaseKVStoreClient.newBuilder()
            .clusterId(IDistWorker.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .executor(ioClientExecutor)
            .sslContext(clientSslContext)
            .queryPipelinesPerStore(config
                .getStateStoreConfig()
                .getDistWorkerConfig()
                .getQueryPipelinePerStore())
            .build();

        distWorker = IDistWorker.nonStandaloneBuilder()
            .rpcServerBuilder(sharedBaseKVRPCServerBuilder)
            .bootstrap(config.isBootstrap())
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .settingProvider(settingProviderMgr)
            .eventCollector(eventCollectorMgr)
            .distClient(distClient)
            .storeClient(distWorkerClient)
            .queryExecutor(queryExecutor)
            .mutationExecutor(mutationExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .storeOptions(new KVRangeStoreOptions()
                .setDataEngineConfigurator(
                    buildEngineConf(config
                        .getStateStoreConfig()
                        .getDistWorkerConfig()
                        .getDataEngineConfig(), "dist_data"))
                .setWalEngineConfigurator(
                    buildEngineConf(config
                        .getStateStoreConfig()
                        .getDistWorkerConfig()
                        .getWalEngineConfig(), "dist_wal")))
            .balanceControllerOptions(
                toControllerOptions(config.getStateStoreConfig().getDistWorkerConfig().getBalanceConfig()))
            .subBrokerManager(subBrokerManager)
            .build();

        distServer = IDistServer.nonStandaloneBuilder()
            .rpcServerBuilder(sharedIORPCServerBuilder)
            .crdtService(serverCrdtService)
            .distWorkerClient(distWorkerClient)
            .settingProvider(settingProviderMgr)
            .eventCollector(eventCollectorMgr)
            .build();

        MQTTServerConfig mqttServerConfig = config.getMqttServerConfig();
        IMQTTBrokerBuilder<?> brokerBuilder = IMQTTBroker.nonStandaloneBuilder()
            .rpcServerBuilder(sharedIORPCServerBuilder)
            .mqttBossGroup(NettyUtil.createEventLoopGroup(mqttServerConfig.getBossELGThreads(),
                EnvProvider.INSTANCE.newThreadFactory("mqtt-boss-elg")))
            .mqttWorkerGroup(NettyUtil.createEventLoopGroup(mqttServerConfig.getWorkerELGThreads(),
                EnvProvider.INSTANCE.newThreadFactory("mqtt-worker-elg")))
            .authProvider(authProviderMgr)
            .eventCollector(eventCollectorMgr)
            .settingProvider(settingProviderMgr)
            .distClient(distClient)
            .inboxClient(inboxReaderClient)
            .sessionDictClient(sessionDictClient)
            .retainClient(retainClient)
            .connectTimeoutSeconds(mqttServerConfig.getConnTimeoutSec())
            .connectRateLimit(mqttServerConfig.getMaxConnPerSec())
            .disconnectRate(mqttServerConfig.getMaxDisconnPerSec())
            .readLimit(mqttServerConfig.getMaxConnBandwidth())
            .writeLimit(mqttServerConfig.getMaxConnBandwidth())
            .defaultKeepAliveSeconds(mqttServerConfig.getDefaultKeepAliveSec())
            .maxBytesInMessage(mqttServerConfig.getMaxMsgByteSize())
            .maxResendTimes(mqttServerConfig.getMaxResendTimes())
            .qos2ConfirmWindowSeconds(mqttServerConfig.getQos2ConfirmWindowSec());
        if (mqttServerConfig.getTcpListener().isEnable()) {
            brokerBuilder.buildTcpConnListener()
                .host(mqttServerConfig.getTcpListener().getHost())
                .port(mqttServerConfig.getTcpListener().getPort())
                .buildListener();
        }
        if (mqttServerConfig.getTlsListener().isEnable()) {
            brokerBuilder.buildTLSConnListener()
                .host(mqttServerConfig.getTlsListener().getHost())
                .port(mqttServerConfig.getTlsListener().getPort())
                .sslContext(buildServerSslContext(mqttServerConfig.getTlsListener().getSslConfig()))
                .buildListener();
        }
        if (mqttServerConfig.getWsListener().isEnable()) {
            brokerBuilder.buildWSConnListener()
                .host(mqttServerConfig.getWsListener().getHost())
                .port(mqttServerConfig.getWsListener().getPort())
                .path(mqttServerConfig.getWsListener().getWsPath())
                .buildListener();
        }
        if (mqttServerConfig.getWssListener().isEnable()) {
            brokerBuilder.buildWSSConnListener()
                .host(mqttServerConfig.getWssListener().getHost())
                .port(mqttServerConfig.getWssListener().getPort())
                .path(mqttServerConfig.getWssListener().getWsPath())
                .sslContext(buildServerSslContext(mqttServerConfig.getWssListener().getSslConfig()))
                .buildListener();
        }
        sharedBaseKVRpcServer = sharedBaseKVRPCServerBuilder.build();
        mqttBroker = brokerBuilder.build();
        sharedIORpcServer = sharedIORPCServerBuilder.build();
    }

    private KVRangeBalanceControllerOptions toControllerOptions(StateStoreConfig.BalancerOptions options) {
        return new KVRangeBalanceControllerOptions()
            .setScheduleIntervalInMs(options.getScheduleIntervalInMs())
            .setBalancers(options.getBalancers());
    }

    @Override
    protected Class<StandaloneConfig> configClass() {
        return StandaloneConfig.class;
    }

    public void start() {
        sessionDictServer.start();

        inboxStore.start();

        inboxServer.start();

        retainStore.start();
        retainServer.start();

        distWorker.start();
        distServer.start();

        sharedIORpcServer.start();
        sharedBaseKVRpcServer.start();

        mqttBroker.start();

        Observable.combineLatest(
                distWorkerClient.connState(),
                inboxBrokerClient.connState(),
                retainStoreClient.connState(),
                mqttBrokerClient.connState(),
                inboxBrokerClient.connState(),
                sessionDictClient.connState(),
                retainClient.connState(),
                inboxReaderClient.connState(),
                distClient.connState(),
                Sets::newHashSet
            )
            .mapOptional(states -> {
                if (states.size() > 1) {
                    return Optional.empty();
                }
                return states.stream().findFirst();
            })
            .filter(state -> state == IRPCClient.ConnState.READY)
            .firstElement()
            .blockingSubscribe();

        inboxStoreClient.join();
        retainStoreClient.join();
        distWorkerClient.join();
        log.info("Standalone broker started");
        setupMetrics();
    }

    public void stop() {
        mqttBroker.shutdown();
        sharedIORpcServer.shutdown();
        sharedBaseKVRpcServer.shutdown();

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

        authProviderMgr.close();

        eventCollectorMgr.close();

        settingProviderMgr.close();

        subBrokerManager.stop();

        if (ioClientExecutor != null) {
            ioClientExecutor.shutdownNow();
            log.debug("Shutdown io client executor");
        }
        if (ioServerExecutor != null) {
            ioServerExecutor.shutdownNow();
            log.debug("Shutdown io server executor");
        }
        if (queryExecutor != null) {
            queryExecutor.shutdownNow();
            log.debug("Shutdown query executor");
        }
        if (mutationExecutor != null) {
            mutationExecutor.shutdownNow();
            log.debug("Shutdown mutation executor");
        }
        if (tickTaskExecutor != null) {
            tickTaskExecutor.shutdownNow();
            log.debug("Shutdown tick task executor");
        }
        if (bgTaskExecutor != null) {
            bgTaskExecutor.shutdownNow();
            log.debug("Shutdown bg task executor");
        }
        pluginMgr.stopPlugins();
        pluginMgr.unloadPlugins();
        log.info("Standalone broker stopped");
    }

    public static void main(String[] args) {
        StarterRunner.run(StandaloneStarter.class, args);
    }
}
