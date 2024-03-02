/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

import static com.baidu.bifromq.sysprops.BifroMQSysProp.DIST_WORKER_LOAD_EST_WINDOW_SECONDS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.INBOX_STORE_LOAD_EST_WINDOW_SECONDS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.RETAIN_STORE_LOAD_EST_WINDOW_SECONDS;

import com.baidu.bifromq.apiserver.APIServer;
import com.baidu.bifromq.apiserver.IAPIServer;
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
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.server.IInboxServer;
import com.baidu.bifromq.inbox.store.IInboxStore;
import com.baidu.bifromq.mqtt.IMQTTBroker;
import com.baidu.bifromq.mqtt.IMQTTBrokerBuilder;
import com.baidu.bifromq.mqtt.inbox.IMqttBrokerClient;
import com.baidu.bifromq.plugin.authprovider.AuthProviderManager;
import com.baidu.bifromq.plugin.eventcollector.EventCollectorManager;
import com.baidu.bifromq.plugin.manager.BifroMQPluginManager;
import com.baidu.bifromq.plugin.resourcethrottler.ResourceThrottlerManager;
import com.baidu.bifromq.plugin.settingprovider.Setting;
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
import com.baidu.bifromq.starter.config.standalone.model.apiserver.APIServerConfig;
import com.baidu.bifromq.starter.config.standalone.model.mqttserver.MQTTServerConfig;
import com.baidu.bifromq.starter.utils.ConfigUtil;
import com.baidu.bifromq.sysprops.BifroMQSysProp;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.micrometer.core.instrument.binder.netty4.NettyEventExecutorMetrics;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.reactivex.rxjava3.core.Observable;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
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
    private ScheduledExecutorService tickTaskExecutor;
    private ScheduledExecutorService bgTaskExecutor;
    private AuthProviderManager authProviderMgr;
    private ResourceThrottlerManager resourceThrottlerMgr;
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
    private IInboxClient inboxClient;
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
    private IAPIServer apiServer;

    @Override
    protected void init(StandaloneConfig config) {
        StandaloneConfigConsolidator.consolidate(config);
        printConfigs(config);

        if (!Strings.isNullOrEmpty(config.getClusterConfig().getEnv())) {
            Metrics.globalRegistry.config()
                .commonTags("env", config.getClusterConfig().getEnv());
        }

        pluginMgr = new BifroMQPluginManager();
        pluginMgr.loadPlugins();
        pluginMgr.startPlugins();
        pluginMgr.getPlugins().forEach(
            plugin -> log.info("Loaded plugin: {}@{}",
                plugin.getDescriptor().getPluginId(), plugin.getDescriptor().getVersion()));

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
        tickTaskExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ScheduledThreadPoolExecutor(config.getStateStoreConfig().getTickerThreads(),
                EnvProvider.INSTANCE.newThreadFactory("tick-task-executor")), "tick-task-executor");
        bgTaskExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ScheduledThreadPoolExecutor(config.getStateStoreConfig().getBgWorkerThreads(),
                EnvProvider.INSTANCE.newThreadFactory("bg-task-executor")), "bg-task-executor");

        eventCollectorMgr = new EventCollectorManager(pluginMgr);
        resourceThrottlerMgr = new ResourceThrottlerManager(config.getResourceThrottlerFQN(), pluginMgr);

        settingProviderMgr = new SettingProviderManager(config.getSettingProviderFQN(), pluginMgr);

        authProviderMgr = new AuthProviderManager(config.getAuthProviderFQN(),
            pluginMgr, settingProviderMgr, eventCollectorMgr);

        AgentHostOptions agentHostOptions = AgentHostOptions.builder()
            .clusterDomainName(config.getClusterConfig().getClusterDomainName())
            .env(config.getClusterConfig().getEnv())
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

        EventLoopGroup rpcServerBossELG =
            NettyUtil.createEventLoopGroup(1, EnvProvider.INSTANCE.newThreadFactory("rpc-boss-elg"));
        new NettyEventExecutorMetrics(rpcServerBossELG).bindTo(Metrics.globalRegistry);

        EventLoopGroup ioRPCWorkerELG =
            NettyUtil.createEventLoopGroup(0, EnvProvider.INSTANCE.newThreadFactory("io-rpc-worker-elg"));
        new NettyEventExecutorMetrics(ioRPCWorkerELG).bindTo(Metrics.globalRegistry);

        EventLoopGroup kvRPCWorkerELG =
            NettyUtil.createEventLoopGroup(0, EnvProvider.INSTANCE.newThreadFactory("kv-rpc-worker-elg"));
        new NettyEventExecutorMetrics(ioRPCWorkerELG).bindTo(Metrics.globalRegistry);

        RPCServerBuilder sharedIORPCServerBuilder = IRPCServer.newBuilder()
            .host(config.getRpcServerConfig().getHost())
            .port(config.getRpcServerConfig().getPort())
            .bossEventLoopGroup(rpcServerBossELG)
            .workerEventLoopGroup(ioRPCWorkerELG)
            .crdtService(serverCrdtService)
            .executor(ioServerExecutor);
        RPCServerBuilder sharedBaseKVRPCServerBuilder = IRPCServer.newBuilder()
            .host(config.getRpcServerConfig().getHost())
            .port(config.getBaseKVRpcServerConfig().getPort())
            .bossEventLoopGroup(rpcServerBossELG)
            .workerEventLoopGroup(kvRPCWorkerELG)
            .crdtService(serverCrdtService)
            .executor(MoreExecutors.directExecutor());
        if (config.getRpcServerConfig().getSslConfig() != null) {
            sharedIORPCServerBuilder.sslContext(buildServerSslContext(config.getRpcServerConfig().getSslConfig()));
            sharedBaseKVRPCServerBuilder.sslContext(buildServerSslContext(config.getRpcServerConfig().getSslConfig()));
        }
        SslContext clientSslContext = config.getRpcClientConfig().getSslConfig() != null ?
            buildClientSslContext(config.getRpcClientConfig().getSslConfig()) : null;
        distClient = IDistClient.newBuilder()
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .sslContext(clientSslContext)
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
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .loadEstimateWindow(Duration.ofSeconds(RETAIN_STORE_LOAD_EST_WINDOW_SECONDS.get()))
            .gcInterval(Duration.ofSeconds(config.getStateStoreConfig().getRetainStoreConfig().getGcIntervalSeconds()))
            .balanceControllerOptions(
                toControllerOptions(config.getStateStoreConfig().getRetainStoreConfig().getBalanceConfig())
            )
            .storeOptions(new KVRangeStoreOptions()
                .setKvRangeOptions(new KVRangeOptions()
                    .setCompactWALThreshold(config.getStateStoreConfig()
                        .getRetainStoreConfig()
                        .getCompactWALThreshold()))
                .setDataEngineConfigurator(
                    buildDataEngineConf(config
                        .getStateStoreConfig()
                        .getRetainStoreConfig()
                        .getDataEngineConfig(), "retain_data"))
                .setWalEngineConfigurator(
                    buildWALEngineConf(config
                        .getStateStoreConfig()
                        .getRetainStoreConfig()
                        .getWalEngineConfig(), "retain_wal")))
            .build();
        inboxClient = IInboxClient.newBuilder()
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
            .inboxClient(inboxClient)
            .storeClient(inboxStoreClient)
            .settingProvider(settingProviderMgr)
            .eventCollector(eventCollectorMgr)
            .queryExecutor(queryExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .loadEstimateWindow(Duration.ofSeconds(INBOX_STORE_LOAD_EST_WINDOW_SECONDS.get()))
            .gcInterval(Duration.ofSeconds(config.getStateStoreConfig().getInboxStoreConfig().getGcIntervalSeconds()))
            .balanceControllerOptions(
                toControllerOptions(config.getStateStoreConfig().getInboxStoreConfig().getBalanceConfig())
            )
            .storeOptions(new KVRangeStoreOptions()
                .setKvRangeOptions(new KVRangeOptions()
                    .setCompactWALThreshold(config.getStateStoreConfig()
                        .getInboxStoreConfig()
                        .getCompactWALThreshold())
                    .setEnableLoadEstimation(true))
                .setDataEngineConfigurator(
                    buildDataEngineConf(config
                        .getStateStoreConfig()
                        .getInboxStoreConfig()
                        .getDataEngineConfig(), "inbox_data"))
                .setWalEngineConfigurator(
                    buildWALEngineConf(config
                        .getStateStoreConfig()
                        .getInboxStoreConfig()
                        .getWalEngineConfig(), "inbox_wal")))
            .build();
        inboxServer = IInboxServer.nonStandaloneBuilder()
            .rpcServerBuilder(sharedIORPCServerBuilder)
            .eventCollector(eventCollectorMgr)
            .resourceThrottler(resourceThrottlerMgr)
            .settingProvider(settingProviderMgr)
            .inboxClient(inboxClient)
            .distClient(distClient)
            .retainClient(retainClient)
            .inboxStoreClient(inboxStoreClient)
            .bgTaskExecutor(bgTaskExecutor)
            .build();
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
        mqttBrokerClient = IMqttBrokerClient.newBuilder()
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .sslContext(clientSslContext)
            .build();

        subBrokerManager = new SubBrokerManager(pluginMgr, mqttBrokerClient, inboxClient);

        sessionDictClient = ISessionDictClient.newBuilder()
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .sslContext(clientSslContext)
            .build();

        sessionDictServer = ISessionDictServer.nonStandaloneBuilder()
            .rpcServerBuilder(sharedIORPCServerBuilder)
            .mqttBrokerClient(mqttBrokerClient)
            .build();
        retainServer = IRetainServer.nonStandaloneBuilder()
            .rpcServerBuilder(sharedIORPCServerBuilder)
            .retainStoreClient(retainStoreClient)
            .settingProvider(settingProviderMgr)
            .subBrokerManager(subBrokerManager)
            .build();
        distWorker = IDistWorker.nonStandaloneBuilder()
            .rpcServerBuilder(sharedBaseKVRPCServerBuilder)
            .bootstrap(config.isBootstrap())
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .eventCollector(eventCollectorMgr)
            .resourceThrottler(resourceThrottlerMgr)
            .distClient(distClient)
            .storeClient(distWorkerClient)
            .queryExecutor(queryExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .storeOptions(new KVRangeStoreOptions()
                .setKvRangeOptions(new KVRangeOptions()
                    .setCompactWALThreshold(config.getStateStoreConfig()
                        .getRetainStoreConfig()
                        .getCompactWALThreshold()))
                .setDataEngineConfigurator(
                    buildDataEngineConf(config
                        .getStateStoreConfig()
                        .getDistWorkerConfig()
                        .getDataEngineConfig(), "dist_data"))
                .setWalEngineConfigurator(
                    buildWALEngineConf(config
                        .getStateStoreConfig()
                        .getDistWorkerConfig()
                        .getWalEngineConfig(), "dist_wal")))
            .balanceControllerOptions(
                toControllerOptions(config.getStateStoreConfig().getDistWorkerConfig().getBalanceConfig()))
            .subBrokerManager(subBrokerManager)
            .loadEstimateWindow(Duration.ofSeconds(DIST_WORKER_LOAD_EST_WINDOW_SECONDS.get()))
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
            .mqttBossELGThreads(mqttServerConfig.getBossELGThreads())
            .mqttWorkerELGThreads(mqttServerConfig.getWorkerELGThreads())
            .authProvider(authProviderMgr)
            .eventCollector(eventCollectorMgr)
            .resourceThrottler(resourceThrottlerMgr)
            .settingProvider(settingProviderMgr)
            .distClient(distClient)
            .inboxClient(inboxClient)
            .sessionDictClient(sessionDictClient)
            .retainClient(retainClient)
            .connectTimeoutSeconds(mqttServerConfig.getConnTimeoutSec())
            .connectRateLimit(mqttServerConfig.getMaxConnPerSec())
            .disconnectRate(mqttServerConfig.getMaxDisconnPerSec())
            .readLimit(mqttServerConfig.getMaxConnBandwidth())
            .writeLimit(mqttServerConfig.getMaxConnBandwidth())
            .defaultKeepAliveSeconds(mqttServerConfig.getDefaultKeepAliveSec())
            .maxBytesInMessage(mqttServerConfig.getMaxMsgByteSize());
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

        APIServerConfig apiServerConfig = config.getApiServerConfig();
        if (apiServerConfig.isEnable()) {
            apiServer = buildAPIServer(apiServerConfig);
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
        super.start();
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
        if (apiServer != null) {
            apiServer.start();
        }
        Observable.combineLatest(
                distWorkerClient.connState(),
                inboxStoreClient.connState(),
                retainStoreClient.connState(),
                mqttBrokerClient.connState(),
                inboxClient.connState(),
                sessionDictClient.connState(),
                retainClient.connState(),
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
    }

    public void stop() {
        mqttBroker.shutdown();
        if (apiServer != null) {
            apiServer.shutdown();
        }
        sharedIORpcServer.shutdown();
        sharedBaseKVRpcServer.shutdown();

        distClient.stop();
        distServer.shutdown();

        distWorkerClient.stop();
        distWorker.stop();

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
        super.stop();
    }

    private IAPIServer buildAPIServer(APIServerConfig apiServerConfig) {
        String apiHost = Strings.isNullOrEmpty(apiServerConfig.getHost()) ?
            "0.0.0.0" : apiServerConfig.getHost();
        EventLoopGroup bossELG = NettyUtil.createEventLoopGroup(apiServerConfig.getApiBossThreads(),
            EnvProvider.INSTANCE.newThreadFactory("api-server-boss-elg"));
        EventLoopGroup workerELG = NettyUtil.createEventLoopGroup(apiServerConfig.getApiWorkerThreads(),
            EnvProvider.INSTANCE.newThreadFactory("api-server-worker-elg"));
        SslContext sslContext = null;
        if (apiServerConfig.getHttpsListenerConfig().isEnable()) {
            sslContext = buildServerSslContext(apiServerConfig.getHttpsListenerConfig().getSslConfig());
        }
        return new APIServer(apiHost, apiServerConfig.getHttpPort(), apiServerConfig.getHttpsListenerConfig().getPort(),
            bossELG, workerELG, sslContext, distClient,
            inboxClient, sessionDictClient, retainClient, settingProviderMgr);
    }

    private void printConfigs(StandaloneConfig config) {
        List<String> arguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
        log.info("JVM arguments: \n  {}", String.join("\n  ", arguments));

        log.info("Settings, which can be modified at runtime, allowing for dynamic adjustment of BifroMQ's " +
            "service behavior per tenant. See https://bifromq.io/docs/plugin/setting_provider/");
        log.info("The initial value of each setting could be overridden by JVM arguments like: '-DMQTT5Enabled=false'");
        for (Setting setting : Setting.values()) {
            log.info("Setting: {}={}", setting.name(), setting.current(""));
        }

        log.info("BifroMQ system properties: ");
        for (BifroMQSysProp prop : BifroMQSysProp.values()) {
            log.info("BifroMQSysProp: {}={}", prop.propKey, prop.get());
        }

        log.info("Consolidated Config(YAML): \n{}", ConfigUtil.serialize(config));
    }

    public static void main(String[] args) {
        StarterRunner.run(StandaloneStarter.class, args);
    }
}
