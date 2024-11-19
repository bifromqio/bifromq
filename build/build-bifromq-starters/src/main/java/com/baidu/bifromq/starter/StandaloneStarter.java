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

import static com.baidu.bifromq.starter.utils.ClusterDomainUtil.resolve;

import com.baidu.bifromq.apiserver.APIServer;
import com.baidu.bifromq.apiserver.IAPIServer;
import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.metaservice.IBaseKVMetaService;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.option.KVRangeOptions;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.baserpc.client.IRPCClient;
import com.baidu.bifromq.baserpc.server.IRPCServer;
import com.baidu.bifromq.baserpc.server.RPCServerBuilder;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
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
import com.baidu.bifromq.plugin.clientbalancer.ClientBalancerManager;
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
import com.baidu.bifromq.starter.config.standalone.model.apiserver.APIServerConfig;
import com.baidu.bifromq.starter.config.standalone.model.mqttserver.MQTTServerConfig;
import com.baidu.bifromq.starter.utils.ConfigUtil;
import com.baidu.bifromq.sysprops.BifroMQSysProp;
import com.baidu.bifromq.sysprops.props.ClusterDomainResolveTimeoutSeconds;
import com.baidu.bifromq.sysprops.props.DistWorkerLoadEstimationWindowSeconds;
import com.baidu.bifromq.sysprops.props.InboxStoreLoadEstimationWindowSeconds;
import com.baidu.bifromq.sysprops.props.RetainStoreLoadEstimationWindowSeconds;
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
import org.reflections.Reflections;

@Slf4j
public class StandaloneStarter extends BaseEngineStarter<StandaloneConfig> {
    private StandaloneConfig config;
    private PluginManager pluginMgr;
    private ExecutorService rpcClientExecutor;
    private ExecutorService rpcServerExecutor;
    private ExecutorService storeClientExecutor;
    private ExecutorService storeServerExecutor;
    private ScheduledExecutorService bgTaskExecutor;
    private AuthProviderManager authProviderMgr;
    private ClientBalancerManager clientBalancerMgr;
    private ResourceThrottlerManager resourceThrottlerMgr;
    private EventCollectorManager eventCollectorMgr;
    private SettingProviderManager settingProviderMgr;
    private IAgentHost agentHost;
    private ICRDTService crdtService;
    private IRPCServiceTrafficService trafficService;
    private IBaseKVMetaService metaService;
    private IRPCServer rpcServer;
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
        this.config = config;
        defaultSysProps();
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

        if (config.getRpcClientConfig().getWorkerThreads() == null) {
            rpcClientExecutor = MoreExecutors.newDirectExecutorService();
        } else {
            rpcClientExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ThreadPoolExecutor(config.getRpcClientConfig().getWorkerThreads(),
                    config.getRpcClientConfig().getWorkerThreads(), 0L,
                    TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                    EnvProvider.INSTANCE.newThreadFactory("rpc-client-executor")), "rpc-client-executor");
        }
        if (config.getRpcServerConfig().getWorkerThreads() == null) {
            rpcServerExecutor = MoreExecutors.newDirectExecutorService();
        } else {
            rpcServerExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ThreadPoolExecutor(config.getRpcServerConfig().getWorkerThreads(),
                    config.getRpcServerConfig().getWorkerThreads(), 0L,
                    TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                    EnvProvider.INSTANCE.newThreadFactory("rpc-server-executor")), "rpc-server-executor");
        }
        if (config.getBaseKVClientConfig().getWorkerThreads() == null) {
            storeClientExecutor = MoreExecutors.newDirectExecutorService();
        } else {
            storeClientExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ThreadPoolExecutor(config.getBaseKVClientConfig().getWorkerThreads(),
                    config.getBaseKVClientConfig().getWorkerThreads(), 0L,
                    TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                    EnvProvider.INSTANCE.newThreadFactory("basekv-client-executor")), "basekv-client-executor");
        }
        if (config.getBaseKVServerConfig().getWorkerThreads() == null) {
            storeServerExecutor = MoreExecutors.newDirectExecutorService();
        } else {
            storeServerExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ThreadPoolExecutor(config.getBaseKVServerConfig().getWorkerThreads(),
                    config.getBaseKVServerConfig().getWorkerThreads(), 0L,
                    TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                    EnvProvider.INSTANCE.newThreadFactory("basekv-server-executor")), "basekv-server-executor");
        }
        bgTaskExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ScheduledThreadPoolExecutor(config.getStateStoreConfig().getBgWorkerThreads(),
                EnvProvider.INSTANCE.newThreadFactory("bg-task-executor")), "bg-task-executor");

        eventCollectorMgr = new EventCollectorManager(pluginMgr);
        resourceThrottlerMgr = new ResourceThrottlerManager(config.getResourceThrottlerFQN(), pluginMgr);

        settingProviderMgr = new SettingProviderManager(config.getSettingProviderFQN(), pluginMgr);

        authProviderMgr = new AuthProviderManager(config.getAuthProviderFQN(),
            pluginMgr, settingProviderMgr, eventCollectorMgr);

        clientBalancerMgr = new ClientBalancerManager(pluginMgr);

        AgentHostOptions agentHostOptions = AgentHostOptions.builder()
            .env(config.getClusterConfig().getEnv())
            .addr(config.getClusterConfig().getHost())
            .port(config.getClusterConfig().getPort())
            .build();
        agentHost = IAgentHost.newInstance(agentHostOptions);
        crdtService = ICRDTService.newInstance(agentHost, CRDTServiceOptions.builder().build());

        trafficService = IRPCServiceTrafficService.newInstance(crdtService);
        metaService = IBaseKVMetaService.newInstance(crdtService);

        EventLoopGroup rpcServerBossELG =
            NettyUtil.createEventLoopGroup(1, EnvProvider.INSTANCE.newThreadFactory("rpc-boss-elg"));
        new NettyEventExecutorMetrics(rpcServerBossELG).bindTo(Metrics.globalRegistry);

        EventLoopGroup ioRPCWorkerELG =
            NettyUtil.createEventLoopGroup(0, EnvProvider.INSTANCE.newThreadFactory("io-rpc-worker-elg"));
        new NettyEventExecutorMetrics(ioRPCWorkerELG).bindTo(Metrics.globalRegistry);

        RPCServerBuilder rpcServerBuilder = IRPCServer.newBuilder()
            .host(config.getRpcServerConfig().getHost())
            .port(config.getRpcServerConfig().getPort())
            .trafficService(trafficService)
            .bossEventLoopGroup(rpcServerBossELG)
            .workerEventLoopGroup(ioRPCWorkerELG);
        if (config.getRpcServerConfig().isEnableSSL()) {
            rpcServerBuilder.sslContext(buildRPCServerSslContext(config.getRpcServerConfig().getSslConfig()));
        }
        SslContext rpcClientSslContext = config.getRpcClientConfig().isEnableSSL()
            ? buildRPCClientSslContext(config.getRpcClientConfig().getSslConfig()) : null;
        distClient = IDistClient.newBuilder()
            .trafficService(trafficService)
            .executor(rpcClientExecutor)
            .sslContext(rpcClientSslContext)
            .build();
        retainClient = IRetainClient.newBuilder()
            .trafficService(trafficService)
            .executor(rpcClientExecutor)
            .sslContext(rpcClientSslContext)
            .build();
        retainStoreClient = IBaseKVStoreClient.newBuilder()
            .clusterId(IRetainStore.CLUSTER_NAME)
            .trafficService(trafficService)
            .metaService(metaService)
            .executor(storeClientExecutor)
            .sslContext(rpcClientSslContext)
            .queryPipelinesPerStore(config
                .getStateStoreConfig()
                .getRetainStoreConfig()
                .getQueryPipelinePerStore())
            .build();
        retainStore = IRetainStore.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .bootstrap(config.isBootstrap())
            .agentHost(agentHost)
            .metaService(metaService)
            .storeClient(retainStoreClient)
            .queryExecutor(MoreExecutors.directExecutor())
            .rpcExecutor(storeServerExecutor)
            .tickerThreads(config.getStateStoreConfig().getTickerThreads())
            .bgTaskExecutor(bgTaskExecutor)
            .balancerRetryDelay(Duration.ofMillis(
                config.getStateStoreConfig().getRetainStoreConfig().getBalanceConfig().getRetryDelayInMS()))
            .balancerFactoryConfig(
                config.getStateStoreConfig().getRetainStoreConfig().getBalanceConfig().getBalancers())
            .loadEstimateWindow(Duration.ofSeconds(RetainStoreLoadEstimationWindowSeconds.INSTANCE.get()))
            .gcInterval(Duration.ofSeconds(config.getStateStoreConfig().getRetainStoreConfig().getGcIntervalSeconds()))
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
            .trafficService(trafficService)
            .executor(rpcClientExecutor)
            .sslContext(rpcClientSslContext)
            .build();
        inboxStoreClient = IBaseKVStoreClient.newBuilder()
            .clusterId(IInboxStore.CLUSTER_NAME)
            .trafficService(trafficService)
            .metaService(metaService)
            .executor(storeClientExecutor)
            .sslContext(rpcClientSslContext)
            .queryPipelinesPerStore(config.getStateStoreConfig().getInboxStoreConfig()
                .getQueryPipelinePerStore())
            .build();
        inboxStore = IInboxStore.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .bootstrap(config.isBootstrap())
            .agentHost(agentHost)
            .metaService(metaService)
            .inboxClient(inboxClient)
            .storeClient(inboxStoreClient)
            .settingProvider(settingProviderMgr)
            .eventCollector(eventCollectorMgr)
            .queryExecutor(MoreExecutors.directExecutor())
            .rpcExecutor(storeServerExecutor)
            .tickerThreads(config.getStateStoreConfig().getTickerThreads())
            .bgTaskExecutor(bgTaskExecutor)
            .loadEstimateWindow(Duration.ofSeconds(InboxStoreLoadEstimationWindowSeconds.INSTANCE.get()))
            .gcInterval(Duration.ofSeconds(config.getStateStoreConfig().getInboxStoreConfig().getGcIntervalSeconds()))
            .balancerRetryDelay(Duration.ofMillis(
                config.getStateStoreConfig().getInboxStoreConfig().getBalanceConfig().getRetryDelayInMS()))
            .balancerFactoryConfig(config.getStateStoreConfig().getInboxStoreConfig().getBalanceConfig().getBalancers())
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
        inboxServer = IInboxServer.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .eventCollector(eventCollectorMgr)
            .resourceThrottler(resourceThrottlerMgr)
            .settingProvider(settingProviderMgr)
            .inboxClient(inboxClient)
            .distClient(distClient)
            .retainClient(retainClient)
            .inboxStoreClient(inboxStoreClient)
            .build();
        distWorkerClient = IBaseKVStoreClient.newBuilder()
            .clusterId(IDistWorker.CLUSTER_NAME)
            .trafficService(trafficService)
            .metaService(metaService)
            .executor(storeClientExecutor)
            .sslContext(rpcClientSslContext)
            .queryPipelinesPerStore(config
                .getStateStoreConfig()
                .getDistWorkerConfig()
                .getQueryPipelinePerStore())
            .build();
        mqttBrokerClient = IMqttBrokerClient.newBuilder()
            .trafficService(trafficService)
            .executor(rpcClientExecutor)
            .sslContext(rpcClientSslContext)
            .build();

        subBrokerManager = new SubBrokerManager(pluginMgr, mqttBrokerClient, inboxClient);

        sessionDictClient = ISessionDictClient.newBuilder()
            .trafficService(trafficService)
            .executor(rpcClientExecutor)
            .sslContext(rpcClientSslContext)
            .build();

        sessionDictServer = ISessionDictServer.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .mqttBrokerClient(mqttBrokerClient)
            // TODO: attributes
            // TODO: defaultGroupTags
            .build();
        retainServer = IRetainServer.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .retainStoreClient(retainStoreClient)
            .settingProvider(settingProviderMgr)
            .subBrokerManager(subBrokerManager)
            // TODO: attributes
            // TODO: defaultGroupTags
            .build();
        distWorker = IDistWorker.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .bootstrap(config.isBootstrap())
            .agentHost(agentHost)
            .metaService(metaService)
            .eventCollector(eventCollectorMgr)
            .resourceThrottler(resourceThrottlerMgr)
            .distClient(distClient)
            .storeClient(distWorkerClient)
            .queryExecutor(MoreExecutors.directExecutor())
            .rpcExecutor(storeServerExecutor)
            .tickerThreads(config.getStateStoreConfig().getTickerThreads())
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
            .balancerRetryDelay(Duration.ofMillis(
                config.getStateStoreConfig().getDistWorkerConfig().getBalanceConfig().getRetryDelayInMS()))
            .balancerFactoryConfig(config.getStateStoreConfig().getDistWorkerConfig().getBalanceConfig().getBalancers())
            .subBrokerManager(subBrokerManager)
            .loadEstimateWindow(Duration.ofSeconds(DistWorkerLoadEstimationWindowSeconds.INSTANCE.get()))
            .build();

        distServer = IDistServer.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .distWorkerClient(distWorkerClient)
            .settingProvider(settingProviderMgr)
            .eventCollector(eventCollectorMgr)
            // TODO: attributes
            // TODO: defaultGroupTags
            .build();

        MQTTServerConfig mqttServerConfig = config.getMqttServerConfig();
        IMQTTBrokerBuilder brokerBuilder = IMQTTBroker.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .mqttBossELGThreads(mqttServerConfig.getBossELGThreads())
            .mqttWorkerELGThreads(mqttServerConfig.getWorkerELGThreads())
            .authProvider(authProviderMgr)
            .clientBalancer(clientBalancerMgr)
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
        mqttBroker = brokerBuilder.build();
        rpcServer = rpcServerBuilder.build();
    }

    @Override
    protected Class<StandaloneConfig> configClass() {
        return StandaloneConfig.class;
    }

    @Override
    public void start() {
        super.start();
        join();
        log.info("Start RPC server");
        rpcServer.start();
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
        mqttBroker.close();
        if (apiServer != null) {
            apiServer.close();
        }
        rpcServer.shutdown();

        distClient.close();
        distServer.close();

        distWorkerClient.close();
        distWorker.close();

        inboxServer.close();

        inboxStoreClient.close();
        inboxStore.close();

        retainClient.close();
        retainServer.close();

        retainStoreClient.close();
        retainStore.close();

        sessionDictClient.close();
        sessionDictServer.close();

        metaService.close();

        trafficService.close();

        crdtService.close();

        agentHost.close();

        authProviderMgr.close();

        resourceThrottlerMgr.close();

        clientBalancerMgr.close();

        eventCollectorMgr.close();

        settingProviderMgr.close();

        subBrokerManager.close();

        if (rpcClientExecutor != null) {
            rpcClientExecutor.shutdownNow();
            log.debug("Shutdown rpc client executor");
        }
        if (rpcServerExecutor != null) {
            rpcServerExecutor.shutdownNow();
            log.debug("Shutdown rpc server executor");
        }
        if (storeClientExecutor != null) {
            storeClientExecutor.shutdownNow();
            log.debug("Shutdown baseKV client executor");
        }
        if (storeServerExecutor != null) {
            storeServerExecutor.shutdownNow();
            log.debug("Shutdown baseKV server executor");
        }
        if (bgTaskExecutor != null) {
            bgTaskExecutor.shutdownNow();
            log.debug("Shutdown Shared bg task executor");
        }
        pluginMgr.stopPlugins();
        pluginMgr.unloadPlugins();
        log.info("Standalone broker stopped");
        super.stop();
    }

    private IAPIServer buildAPIServer(APIServerConfig apiServerConfig) {
        String apiHost = Strings.isNullOrEmpty(apiServerConfig.getHost()) ? "0.0.0.0" : apiServerConfig.getHost();
        EventLoopGroup bossELG = NettyUtil.createEventLoopGroup(apiServerConfig.getApiBossThreads(),
            EnvProvider.INSTANCE.newThreadFactory("api-server-boss-elg"));
        EventLoopGroup workerELG = NettyUtil.createEventLoopGroup(apiServerConfig.getApiWorkerThreads(),
            EnvProvider.INSTANCE.newThreadFactory("api-server-worker-elg"));
        SslContext sslContext = null;
        if (apiServerConfig.getHttpsListenerConfig().isEnable()) {
            sslContext = buildServerSslContext(apiServerConfig.getHttpsListenerConfig().getSslConfig());
        }
        return APIServer.builder()
            .host(apiHost)
            .port(apiServerConfig.getHttpPort())
            .tlsPort(apiServerConfig.getHttpsListenerConfig().getPort())
            .maxContentLength(apiServerConfig.getMaxContentLength())
            .bossGroup(bossELG)
            .workerGroup(workerELG)
            .sslContext(sslContext)
            .agentHost(agentHost)
            .trafficService(trafficService)
            .metaService(metaService)
            .distClient(distClient)
            .inboxClient(inboxClient)
            .sessionDictClient(sessionDictClient)
            .retainClient(retainClient)
            .settingProvider(settingProviderMgr)
            .build();
    }

    private void printConfigs(StandaloneConfig config) {
        log.info("Available Processors: {}", EnvProvider.INSTANCE.availableProcessors());
        List<String> arguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
        log.info("JVM arguments: \n  {}", String.join("\n  ", arguments));

        log.info("Settings, which can be modified at runtime, allowing for dynamic adjustment of BifroMQ's "
            + "service behavior per tenant. See https://bifromq.io/docs/plugin/setting_provider/");
        log.info("The initial value of each setting could be overridden by JVM arguments like: '-DMQTT5Enabled=false'");
        for (Setting setting : Setting.values()) {
            log.info("Setting: {}={}", setting.name(), setting.current(""));
        }

        log.info("BifroMQ system properties: ");
        Reflections reflections = new Reflections(BifroMQSysProp.class.getPackageName());
        for (Class<? extends BifroMQSysProp> subclass : reflections.getSubTypesOf(BifroMQSysProp.class)) {
            try {
                BifroMQSysProp<?, ?> prop = (BifroMQSysProp<?, ?>) subclass.getField("INSTANCE").get(null);
                log.info("BifroMQSysProp: {}={}", prop.propKey(), prop.get());
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException("Failed to access INSTANCE field of subclass: " + subclass.getName(), e);
            }
        }
        log.info("Consolidated Config(YAML): \n{}", ConfigUtil.serialize(config));
    }

    private void join() {
        String env = config.getClusterConfig().getEnv();
        String clusterDomainName = config.getClusterConfig().getClusterDomainName();
        String seeds = config.getClusterConfig().getSeedEndpoints();
        if (!Strings.isNullOrEmpty(clusterDomainName)) {
            log.debug("AgentHost[{}] join clusterDomainName: {}", env, clusterDomainName);
            resolve(clusterDomainName, Duration.ofSeconds(ClusterDomainResolveTimeoutSeconds.INSTANCE.get()))
                .thenApply(seedAddrs ->
                    Arrays.stream(seedAddrs)
                        .map(addr -> new InetSocketAddress(addr, config.getClusterConfig().getPort()))
                        .collect(Collectors.toSet()))
                .whenComplete((seedEndpoints, e) -> {
                    if (e != null) {
                        log.warn("ClusterDomainName[{}] is unresolvable, due to {}", clusterDomainName, e.getMessage());
                    } else {
                        log.info("ClusterDomainName[{}] resolved to seedEndpoints: {}",
                            clusterDomainName, seedEndpoints);
                        joinSeeds(seedEndpoints);
                    }
                });
        }
        if (!Strings.isNullOrEmpty(seeds)) {
            log.debug("AgentHost[{}] join seedEndpoints: {}", env, seeds);
            Set<InetSocketAddress> seedEndpoints = Arrays.stream(seeds.split(","))
                .map(endpoint -> {
                    String[] hostPort = endpoint.trim().split(":");
                    return new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1]));
                }).collect(Collectors.toSet());
            joinSeeds(seedEndpoints);
        }
    }

    private void joinSeeds(Set<InetSocketAddress> seeds) {
        agentHost.join(seeds)
            .whenComplete((v, e) -> {
                if (e != null) {
                    log.warn("AgentHost failed to join seedEndpoint: {}", seeds, e);
                } else {
                    log.info("AgentHost joined seedEndpoint: {}", seeds);
                }
            });
    }

    private void defaultSysProps() {
        // force using a single ByteBufAllocator for netty in both grpc and mqtt broker, to make memory tuning easier
        System.setProperty("io.grpc.netty.useCustomAllocator", "false");
    }

    public static void main(String[] args) {
        StarterRunner.run(StandaloneStarter.class, args);
    }
}
