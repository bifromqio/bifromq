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

import static com.baidu.bifromq.inbox.store.IInboxStore.CLUSTER_NAME;

import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.server.IDistServer;
import com.baidu.bifromq.dist.worker.DistWorkerBuilder;
import com.baidu.bifromq.dist.worker.IDistWorker;
import com.baidu.bifromq.inbox.client.IInboxBrokerClient;
import com.baidu.bifromq.inbox.client.IInboxReaderClient;
import com.baidu.bifromq.inbox.server.IInboxServer;
import com.baidu.bifromq.inbox.store.IInboxStore;
import com.baidu.bifromq.inbox.store.InboxStoreBuilder;
import com.baidu.bifromq.mqtt.IMQTTBroker;
import com.baidu.bifromq.mqtt.MQTTBrokerBuilder;
import com.baidu.bifromq.mqtt.inbox.IMqttBrokerClient;
import com.baidu.bifromq.plugin.authprovider.AuthProviderManager;
import com.baidu.bifromq.plugin.eventcollector.EventCollectorManager;
import com.baidu.bifromq.plugin.manager.BifroMQPluginManager;
import com.baidu.bifromq.plugin.settingprovider.SettingProviderManager;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.plugin.subbroker.SubBrokerManager;
import com.baidu.bifromq.retain.client.IRetainServiceClient;
import com.baidu.bifromq.retain.server.IRetainServer;
import com.baidu.bifromq.retain.store.IRetainStore;
import com.baidu.bifromq.retain.store.RetainStoreBuilder;
import com.baidu.bifromq.sessiondict.client.ISessionDictionaryClient;
import com.baidu.bifromq.sessiondict.server.ISessionDictionaryServer;
import com.baidu.bifromq.starter.config.DistServerConfig;
import com.baidu.bifromq.starter.config.DistWorkerConfig;
import com.baidu.bifromq.starter.config.InboxServerConfig;
import com.baidu.bifromq.starter.config.InboxStoreConfig;
import com.baidu.bifromq.starter.config.RetainServerConfig;
import com.baidu.bifromq.starter.config.RetainStoreConfig;
import com.baidu.bifromq.starter.config.StandardNodeConfig;
import com.baidu.bifromq.starter.config.model.RPCClientConfig;
import com.baidu.bifromq.starter.config.model.ServerSSLContextConfig;
import com.baidu.bifromq.starter.config.model.StoreClientConfig;
import com.google.common.base.Preconditions;
import io.netty.channel.EventLoopGroup;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.PluginManager;

@Slf4j
public class StandardStarter extends BaseEngineStarter<StandardNodeConfig> {
    private PluginManager pluginMgr;
    private EventLoopGroup rpcBossGroup;
    private EventLoopGroup rpcWorkerGroup;
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
    private ISessionDictionaryClient sessionDictClient;
    private ISessionDictionaryServer sessionDictServer;
    private IDistClient distClient;
    private IBaseKVStoreClient distWorkerClient;
    private IDistWorker distWorker;
    private IDistServer distServer;
    private IInboxReaderClient inboxReaderClient;
    private IBaseKVStoreClient inboxStoreClient;
    private IInboxStore inboxStore;
    private IInboxServer inboxServer;
    private IRetainServiceClient retainClient;
    private IBaseKVStoreClient retainStoreClient;
    private IRetainStore retainStore;
    private IRetainServer retainServer;
    private IMQTTBroker mqttBroker;
    private ISubBrokerManager subBrokerManager;
    private boolean kvStoreBootstrap;

    @Override
    protected void init(StandardNodeConfig config) {
        kvStoreBootstrap = config.isKvStoreBootstrap();
        pluginMgr = new BifroMQPluginManager();
        pluginMgr.loadPlugins();
        pluginMgr.startPlugins();

        rpcBossGroup = config.getExecutorConfig().rpcBossGroup();
        rpcWorkerGroup = config.getExecutorConfig().rpcWorkerGroup();
        ioClientExecutor = config.getExecutorConfig().ioClientExecutor();
        ioServerExecutor = config.getExecutorConfig().ioServerExecutor();
        queryExecutor = config.getExecutorConfig().queryExecutor();
        mutationExecutor = config.getExecutorConfig().mutationExecutor();
        tickTaskExecutor = config.getExecutorConfig().tickTaskExecutor();
        bgTaskExecutor = config.getExecutorConfig().bgTaskExecutor();

        eventCollectorMgr = new EventCollectorManager(pluginMgr);
        settingProviderMgr = new SettingProviderManager(config.getSettingProviderFQN(), pluginMgr);
        authProviderMgr = new AuthProviderManager(config.getAuthProviderFQN(),
            pluginMgr, settingProviderMgr, eventCollectorMgr);

        agentHost = initAgentHost(config.getAgentHostConfig());
        log.info("Agent host started");

        clientCrdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        clientCrdtService.start(agentHost);
        serverCrdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        serverCrdtService.start(agentHost);
        log.info("CRDT service started");

        sessionDictClient = buildSessionDictionaryClient(config.getRpcClientConfig());
        sessionDictServer = buildSessionDictionaryServer(config);

        inboxReaderClient = buildInboxReaderClient(config.getRpcClientConfig());
        inboxStoreClient = buildBaseKVStoreClient(IInboxStore.CLUSTER_NAME, config.getInboxStoreClientConfig(),
            config.getRpcClientConfig());
        inboxStore = buildInboxStore(config.getInboxStoreConfig(), config.getRpcServerSSLCtxConfig());
        inboxServer = buildInboxServer(config.getInboxServerConfig(), config.getRpcServerSSLCtxConfig());

        IInboxBrokerClient inboxBrokerClient = buildInboxBrokerClient(config.getRpcClientConfig());
        IMqttBrokerClient mqttBrokerClient = buildMqttBrokerClient(config.getRpcClientConfig());
        subBrokerManager = new SubBrokerManager(pluginMgr, mqttBrokerClient, inboxBrokerClient);

        retainClient = buildRetainServiceClient(config.getRpcClientConfig());
        retainStoreClient = buildBaseKVStoreClient(IRetainStore.CLUSTER_NAME, config.getRetainStoreClientConfig(),
            config.getRpcClientConfig());
        retainStore = buildRetainStore(config.getRetainStoreConfig(), config.getRpcServerSSLCtxConfig());
        retainServer = buildRetainServer(config.getRetainServerConfig(), config.getRpcServerSSLCtxConfig());

        distClient = buildDistClient(config.getRpcClientConfig());
        distWorkerClient = buildBaseKVStoreClient(IDistWorker.CLUSTER_NAME, config.getDistStoreClientConfig(),
            config.getRpcClientConfig());
        distWorker = buildDistWorker(config.getDistWorkerConfig(), config.getRpcServerSSLCtxConfig());
        distServer = buildDistServer(config.getDistServerConfig(), config.getRpcServerSSLCtxConfig());


        mqttBroker = buildMQTTBuilder(config);
    }

    @Override
    protected Class<StandardNodeConfig> configClass() {
        return StandardNodeConfig.class;
    }

    public void start() {
        sessionDictServer.start();

        inboxStore.start(kvStoreBootstrap);
        inboxStoreClient.join();
        inboxServer.start();

        retainStore.start(kvStoreBootstrap);
        retainStoreClient.join();
        retainServer.start();

        distWorker.start(kvStoreBootstrap);
        distWorkerClient.join();
        distServer.start();

        mqttBroker.start();
        log.info("Standard bifromq started");
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
    }

    private IBaseKVStoreClient buildBaseKVStoreClient(String clusterName, StoreClientConfig clientConfig,
                                                      RPCClientConfig rpcClientConfig) {
        if (rpcClientConfig.getSslContextConfig().isEnableSSL()) {
            return IBaseKVStoreClient.sslClientBuilder()
                .clusterId(clusterName)
                .crdtService(clientCrdtService)
                .executor(ioClientExecutor)
                .eventLoopGroup(rpcWorkerGroup)
                .queryPipelinesPerServer(clientConfig.getQueryPipelinePerServer())
                .execPipelinesPerServer(clientConfig.getExecPipelinePerServer())
                .trustCertsFile(loadFromConfDir(rpcClientConfig.getSslContextConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(rpcClientConfig.getSslContextConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(rpcClientConfig.getSslContextConfig().getKeyFile()))
                .build();
        }
        return IBaseKVStoreClient.nonSSLClientBuilder()
            .clusterId(clusterName)
            .crdtService(clientCrdtService)
            .executor(ioClientExecutor)
            .eventLoopGroup(rpcWorkerGroup)
            .queryPipelinesPerServer(clientConfig.getQueryPipelinePerServer())
            .execPipelinesPerServer(clientConfig.getExecPipelinePerServer())
            .build();
    }

    private ISessionDictionaryClient buildSessionDictionaryClient(RPCClientConfig config) {
        if (config.getSslContextConfig().isEnableSSL()) {
            return ISessionDictionaryClient.sslBuilder()
                .trustCertsFile(loadFromConfDir(config.getSslContextConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(config.getSslContextConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(config.getSslContextConfig().getKeyFile()))
                .eventLoopGroup(rpcWorkerGroup)
                .crdtService(clientCrdtService)
                .executor(ioClientExecutor)
                .build();
        }
        return ISessionDictionaryClient.nonSSLBuilder()
            .eventLoopGroup(rpcWorkerGroup)
            .crdtService(clientCrdtService)
            .executor(ioClientExecutor)
            .build();
    }

    private IInboxReaderClient buildInboxReaderClient(RPCClientConfig config) {
        if (config.getSslContextConfig().isEnableSSL()) {
            return IInboxReaderClient.sslClientBuilder()
                .trustCertsFile(loadFromConfDir(config.getSslContextConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(config.getSslContextConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(config.getSslContextConfig().getKeyFile()))
                .eventLoopGroup(rpcWorkerGroup)
                .crdtService(clientCrdtService)
                .executor(ioClientExecutor)
                .build();
        }
        return IInboxReaderClient.nonSSLClientBuilder()
            .eventLoopGroup(rpcWorkerGroup)
            .crdtService(clientCrdtService)
            .executor(ioClientExecutor)
            .build();
    }

    private IRetainServiceClient buildRetainServiceClient(RPCClientConfig config) {
        if (config.getSslContextConfig().isEnableSSL()) {
            return IRetainServiceClient.sslClientBuilder()
                .trustCertsFile(loadFromConfDir(config.getSslContextConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(config.getSslContextConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(config.getSslContextConfig().getKeyFile()))
                .eventLoopGroup(rpcWorkerGroup)
                .crdtService(clientCrdtService)
                .executor(ioClientExecutor)
                .build();
        }
        return IRetainServiceClient.nonSSLClientBuilder()
            .eventLoopGroup(rpcWorkerGroup)
            .crdtService(clientCrdtService)
            .executor(ioClientExecutor)
            .build();
    }

    private IInboxBrokerClient buildInboxBrokerClient(RPCClientConfig config) {
        if (config.getSslContextConfig().isEnableSSL()) {
            return IInboxBrokerClient.sslClientBuilder()
                .trustCertsFile(loadFromConfDir(config.getSslContextConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(config.getSslContextConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(config.getSslContextConfig().getKeyFile()))
                .crdtService(clientCrdtService)
                .eventLoopGroup(rpcWorkerGroup)
                .executor(ioClientExecutor)
                .build();
        }
        return IInboxBrokerClient.nonSSLClientBuilder()
            .crdtService(clientCrdtService)
            .eventLoopGroup(rpcWorkerGroup)
            .executor(ioClientExecutor)
            .build();
    }

    private IMqttBrokerClient buildMqttBrokerClient(RPCClientConfig config) {
        if (config.getSslContextConfig().isEnableSSL()) {
            return IMqttBrokerClient.sslClientBuilder()
                .trustCertsFile(loadFromConfDir(config.getSslContextConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(config.getSslContextConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(config.getSslContextConfig().getKeyFile()))
                .crdtService(clientCrdtService)
                .eventLoopGroup(rpcWorkerGroup)
                .executor(ioClientExecutor)
                .build();
        }
        return IMqttBrokerClient.nonSSLClientBuilder()
            .crdtService(clientCrdtService)
            .eventLoopGroup(rpcWorkerGroup)
            .executor(ioClientExecutor)
            .build();
    }

    private IDistClient buildDistClient(RPCClientConfig config) {
        if (config.getSslContextConfig().isEnableSSL()) {
            return IDistClient.sslClientBuilder()
                .trustCertsFile(loadFromConfDir(config.getSslContextConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(config.getSslContextConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(config.getSslContextConfig().getKeyFile()))
                .eventLoopGroup(rpcWorkerGroup)
                .crdtService(clientCrdtService)
                .executor(ioClientExecutor)
                .build();
        }
        return IDistClient.nonSSLClientBuilder()
            .eventLoopGroup(rpcWorkerGroup)
            .crdtService(clientCrdtService)
            .executor(ioClientExecutor)
            .build();
    }

    private ISessionDictionaryServer buildSessionDictionaryServer(StandardNodeConfig nodeConfig) {
        if (nodeConfig.getRpcServerSSLCtxConfig().isEnableSSL()) {
            return ISessionDictionaryServer.sslServerBuilder()
                .id(UUID.randomUUID().toString())
                .host(nodeConfig.getSessionDictServerConfig().getBindAddress())
                .port(nodeConfig.getSessionDictServerConfig().getPort())
                .crdtService(serverCrdtService)
                .bossEventLoopGroup(rpcBossGroup)
                .workerEventLoopGroup(rpcWorkerGroup)
                .executor(ioServerExecutor)
                .trustCertsFile(loadFromConfDir(nodeConfig.getRpcServerSSLCtxConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(nodeConfig.getRpcServerSSLCtxConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(nodeConfig.getRpcServerSSLCtxConfig().getKeyFile()))
                .build();
        }
        return ISessionDictionaryServer.nonSSLServerBuilder()
            .id(UUID.randomUUID().toString())
            .host(nodeConfig.getSessionDictServerConfig().getBindAddress())
            .port(nodeConfig.getSessionDictServerConfig().getPort())
            .crdtService(serverCrdtService)
            .bossEventLoopGroup(rpcBossGroup)
            .workerEventLoopGroup(rpcWorkerGroup)
            .executor(ioServerExecutor)
            .build();
    }

    private IDistServer buildDistServer(DistServerConfig serverConfig,
                                        ServerSSLContextConfig rpcServerSSLCtxConfig) {
        if (rpcServerSSLCtxConfig.isEnableSSL()) {
            return IDistServer.sslBuilder()
                .id(serverConfig.getServerId())
                .host(serverConfig.getBindAddress())
                .port(serverConfig.getPort())
                .crdtService(serverCrdtService)
                .settingProvider(settingProviderMgr)
                .eventCollector(eventCollectorMgr)
                .storeClient(distWorkerClient)
                .bossEventLoopGroup(rpcBossGroup)
                .workerEventLoopGroup(rpcWorkerGroup)
                .ioExecutor(ioServerExecutor)
                .distCallPreSchedulerFactoryClass(serverConfig.getDistCallPreBatchSchedulerFactoryClass())
                .trustCertsFile(loadFromConfDir(rpcServerSSLCtxConfig.getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(rpcServerSSLCtxConfig.getCertFile()))
                .privateKeyFile(loadFromConfDir(rpcServerSSLCtxConfig.getKeyFile()))
                .build();
        }
        return IDistServer.nonSSLBuilder()
            .id(serverConfig.getServerId())
            .host(serverConfig.getBindAddress())
            .port(serverConfig.getPort())
            .crdtService(serverCrdtService)
            .settingProvider(settingProviderMgr)
            .eventCollector(eventCollectorMgr)
            .storeClient(distWorkerClient)
            .bossEventLoopGroup(rpcBossGroup)
            .workerEventLoopGroup(rpcWorkerGroup)
            .ioExecutor(ioServerExecutor)
            .distCallPreSchedulerFactoryClass(serverConfig.getDistCallPreBatchSchedulerFactoryClass())
            .build();
    }

    private IDistWorker buildDistWorker(DistWorkerConfig workerConfig,
                                        ServerSSLContextConfig rpcServerSSLCtxConfig) {
        DistWorkerBuilder distWorkerBuilder;
        if (rpcServerSSLCtxConfig.isEnableSSL()) {
            distWorkerBuilder = IDistWorker.sslBuilder()
                .bindAddr(workerConfig.getBindAddress())
                .bindPort(workerConfig.getPort())
                .bossEventLoopGroup(rpcBossGroup)
                .workerEventLoopGroup(rpcWorkerGroup)
                .trustCertsFile(loadFromConfDir(rpcServerSSLCtxConfig.getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(rpcServerSSLCtxConfig.getCertFile()))
                .privateKeyFile(loadFromConfDir(rpcServerSSLCtxConfig.getKeyFile()));
        } else {
            distWorkerBuilder = IDistWorker.nonSSLBuilder()
                .bindAddr(workerConfig.getBindAddress())
                .bindPort(workerConfig.getPort())
                .bossEventLoopGroup(rpcBossGroup)
                .workerEventLoopGroup(rpcWorkerGroup);
        }
        return distWorkerBuilder.agentHost(agentHost)
            .crdtService(serverCrdtService)
            .ioExecutor(ioServerExecutor)
            .queryExecutor(queryExecutor)
            .mutationExecutor(mutationExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .distClient(distClient)
            .storeClient(distWorkerClient)
            .kvRangeStoreOptions(new KVRangeStoreOptions()
                .setOverrideIdentity(workerConfig.getOverrideIdentity())
                .setWalFlushBufferSize(workerConfig.getWalFlushBufferSize())
                .setDataEngineConfigurator(
                    buildEngineConf(workerConfig.getDataEngineConfig(), "dist_data"))
                .setWalEngineConfigurator(
                    buildEngineConf(workerConfig.getWalEngineConfig(), "dist_wal")))
            .balanceControllerOptions(workerConfig.getBalanceConfig())
            .eventCollector(eventCollectorMgr)
            .settingProvider(settingProviderMgr)
            .subBrokerManager(subBrokerManager)
            .build();
    }

    private IInboxServer buildInboxServer(InboxServerConfig serverConfig,
                                          ServerSSLContextConfig rpcServerSSLCtxConfig) {
        if (rpcServerSSLCtxConfig.isEnableSSL()) {
            return IInboxServer.sslBuilder()
                .id(serverConfig.getServerId())
                .host(serverConfig.getBindAddress())
                .port(serverConfig.getPort())
                .crdtService(serverCrdtService)
                .settingProvider(settingProviderMgr)
                .storeClient(inboxStoreClient)
                .bossEventLoopGroup(rpcBossGroup)
                .workerEventLoopGroup(rpcWorkerGroup)
                .executor(ioServerExecutor)
                .bgTaskExecutor(bgTaskExecutor)
                .trustCertsFile(loadFromConfDir(rpcServerSSLCtxConfig.getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(rpcServerSSLCtxConfig.getCertFile()))
                .privateKeyFile(loadFromConfDir(rpcServerSSLCtxConfig.getKeyFile()))
                .build();
        }
        return IInboxServer.nonSSLBuilder()
            .id(serverConfig.getServerId())
            .host(serverConfig.getBindAddress())
            .port(serverConfig.getPort())
            .crdtService(serverCrdtService)
            .settingProvider(settingProviderMgr)
            .storeClient(inboxStoreClient)
            .bossEventLoopGroup(rpcBossGroup)
            .workerEventLoopGroup(rpcWorkerGroup)
            .executor(ioServerExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .build();
    }

    private IInboxStore buildInboxStore(InboxStoreConfig storeConfig,
                                        ServerSSLContextConfig rpcServerSSLCtxConfig) {
        InboxStoreBuilder inboxStoreBuilder;
        if (rpcServerSSLCtxConfig.isEnableSSL()) {
            inboxStoreBuilder = IInboxStore.sslBuilder()
                .bindAddr(storeConfig.getBindAddress())
                .bindPort(storeConfig.getPort())
                .bossEventLoopGroup(rpcBossGroup)
                .workerEventLoopGroup(rpcWorkerGroup)
                .trustCertsFile(loadFromConfDir(rpcServerSSLCtxConfig.getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(rpcServerSSLCtxConfig.getCertFile()))
                .privateKeyFile(loadFromConfDir(rpcServerSSLCtxConfig.getKeyFile()));
        } else {
            inboxStoreBuilder = IInboxStore.nonSSLBuilder()
                .bindAddr(storeConfig.getBindAddress())
                .bindPort(storeConfig.getPort())
                .bossEventLoopGroup(rpcBossGroup)
                .workerEventLoopGroup(rpcWorkerGroup);
        }
        return inboxStoreBuilder
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .storeClient(inboxStoreClient)
            .eventCollector(eventCollectorMgr)
            .ioExecutor(ioServerExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .queryExecutor(queryExecutor)
            .mutationExecutor(mutationExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .balanceControllerOptions(storeConfig.getBalanceConfig())
            .kvRangeStoreOptions(new KVRangeStoreOptions()
                .setOverrideIdentity(storeConfig.getOverrideIdentity())
                .setWalFlushBufferSize(storeConfig.getWalFlushBufferSize())
                .setDataEngineConfigurator(
                    buildEngineConf(storeConfig.getDataEngineConfig(), "inbox_data"))
                .setWalEngineConfigurator(
                    buildEngineConf(storeConfig.getWalEngineConfig(), "inbox_wal")))
            .build();
    }

    private IRetainServer buildRetainServer(RetainServerConfig serverConfig,
                                            ServerSSLContextConfig rpcServerSSLCtxConfig) {
        if (rpcServerSSLCtxConfig.isEnableSSL()) {
            return IRetainServer.sslBuilder()
                .id(serverConfig.getServerId())
                .host(serverConfig.getBindAddress())
                .port(serverConfig.getPort())
                .crdtService(serverCrdtService)
                .settingProvider(settingProviderMgr)
                .storeClient(retainStoreClient)
                .bossEventLoopGroup(rpcBossGroup)
                .workerEventLoopGroup(rpcWorkerGroup)
                .ioExecutor(ioServerExecutor)
                .trustCertsFile(loadFromConfDir(rpcServerSSLCtxConfig.getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(rpcServerSSLCtxConfig.getCertFile()))
                .privateKeyFile(loadFromConfDir(rpcServerSSLCtxConfig.getKeyFile()))
                .build();
        }
        return IRetainServer.nonSSLBuilder()
            .id(serverConfig.getServerId())
            .host(serverConfig.getBindAddress())
            .port(serverConfig.getPort())
            .crdtService(serverCrdtService)
            .settingProvider(settingProviderMgr)
            .storeClient(retainStoreClient)
            .bossEventLoopGroup(rpcBossGroup)
            .workerEventLoopGroup(rpcWorkerGroup)
            .ioExecutor(ioServerExecutor)
            .build();
    }

    private IRetainStore buildRetainStore(RetainStoreConfig storeConfig,
                                          ServerSSLContextConfig rpcServerSSLCtxConfig) {
        RetainStoreBuilder retainStoreBuilder;
        if (rpcServerSSLCtxConfig.isEnableSSL()) {
            retainStoreBuilder = IRetainStore.sslBuilder()
                .bindAddr(storeConfig.getBindAddress())
                .bindPort(storeConfig.getPort())
                .bossEventLoopGroup(rpcBossGroup)
                .workerEventLoopGroup(rpcWorkerGroup)
                .trustCertsFile(loadFromConfDir(rpcServerSSLCtxConfig.getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(rpcServerSSLCtxConfig.getCertFile()))
                .privateKeyFile(loadFromConfDir(rpcServerSSLCtxConfig.getKeyFile()));
        } else {
            retainStoreBuilder = IRetainStore.nonSSLBuilder()
                .bindAddr(storeConfig.getBindAddress())
                .bindPort(storeConfig.getPort())
                .bossEventLoopGroup(rpcBossGroup)
                .workerEventLoopGroup(rpcWorkerGroup);
        }
        return retainStoreBuilder
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .storeClient(retainStoreClient)
            .ioExecutor(ioServerExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .queryExecutor(queryExecutor)
            .mutationExecutor(mutationExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .balanceControllerOptions(storeConfig.getBalanceConfig())
            .kvRangeStoreOptions(new KVRangeStoreOptions()
                .setOverrideIdentity(storeConfig.getOverrideIdentity())
                .setWalFlushBufferSize(storeConfig.getWalFlushBufferSize())
                .setDataEngineConfigurator(
                    buildEngineConf(storeConfig.getDataEngineConfig(), "retain_data"))
                .setWalEngineConfigurator(
                    buildEngineConf(storeConfig.getWalEngineConfig(), "retain_wal")))
            .build();
    }

    private IMQTTBroker buildMQTTBuilder(StandardNodeConfig config) {
        MQTTBrokerBuilder brokerBuilder;
        if (config.getRpcServerSSLCtxConfig().isEnableSSL()) {
            brokerBuilder = IMQTTBroker.sslBrokerBuilder()
                .serverId(config.getLocalSessionServerConfig().getServerId())
                .rpcBindAddr(config.getLocalSessionServerConfig().getBindAddress())
                .port(config.getLocalSessionServerConfig().getPort())
                .rpcWorkerGroup(rpcWorkerGroup)
                .crdtService(serverCrdtService)
                .trustCertsFile(loadFromConfDir(config.getRpcServerSSLCtxConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(config.getRpcServerSSLCtxConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(config.getRpcServerSSLCtxConfig().getKeyFile()));
        } else {
            brokerBuilder = IMQTTBroker.nonSSLBrokerBuilder()
                .serverId(config.getLocalSessionServerConfig().getServerId())
                .rpcBindAddr(config.getLocalSessionServerConfig().getBindAddress())
                .port(config.getLocalSessionServerConfig().getPort())
                .rpcWorkerGroup(rpcWorkerGroup)
                .crdtService(serverCrdtService);
        }
        brokerBuilder.host(config.getHost())
            .bossGroup(config.getExecutorConfig().mqttBossGroup())
            .workerGroup(config.getExecutorConfig().mqttWorkerGroup())
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
        return brokerBuilder.build();
    }

    public static void main(String[] args) {
        StarterRunner.run(StandardStarter.class, args);
    }
}
