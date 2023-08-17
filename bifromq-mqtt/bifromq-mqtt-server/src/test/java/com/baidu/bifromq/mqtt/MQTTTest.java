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

package com.baidu.bifromq.mqtt;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.balance.option.KVRangeBalanceControllerOptions;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.localengine.InMemoryKVEngineConfigurator;
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
import com.baidu.bifromq.mqtt.inbox.IMqttBrokerClient;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.plugin.subbroker.SubBrokerManager;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.retain.server.IRetainServer;
import com.baidu.bifromq.retain.store.IRetainStore;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import com.baidu.bifromq.sessiondict.server.ISessionDictServer;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import io.reactivex.rxjava3.core.Observable;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pf4j.DefaultPluginManager;
import org.pf4j.PluginManager;
import org.testng.annotations.AfterSuite;

@Slf4j
class MQTTTest {
    protected static final String brokerURI = "tcp://127.0.0.1:1883";
    @Mock
    protected IAuthProvider authProvider;

    @Mock
    protected IEventCollector eventCollector;
    @Mock
    protected ISettingProvider settingProvider;

    private IAgentHost agentHost;
    private ICRDTService clientCrdtService;
    private ICRDTService serverCrdtService;
    private IRPCServer sharedRpcServer;
    private IMqttBrokerClient onlineInboxBrokerClient;
    protected ISessionDictClient sessionDictClient;
    private ISessionDictServer sessionDictServer;
    private IDistClient distClient;
    private IBaseKVStoreClient distWorkerStoreClient;
    private IDistWorker distWorker;
    private IDistServer distServer;
    private IInboxClient inboxClient;
    private IBaseKVStoreClient inboxStoreKVStoreClient;
    private IInboxStore inboxStore;
    private IInboxServer inboxServer;
    private IRetainClient retainClient;
    private IBaseKVStoreClient retainStoreKVStoreClient;
    private IRetainStore retainStore;
    private IRetainServer retainServer;
    private IMQTTBroker mqttBroker;
    private ISubBrokerManager inboxBrokerMgr;
    private PluginManager pluginMgr;
    private ExecutorService queryExecutor;
    private ExecutorService mutationExecutor;
    private ScheduledExecutorService tickTaskExecutor;
    private ScheduledExecutorService bgTaskExecutor;
    private AutoCloseable closeable;

    private static MQTTTest singleInstance;

    public static MQTTTest getInstance() {
        if (singleInstance == null) {
            singleInstance = new MQTTTest();
            singleInstance.setup();
        }
        return singleInstance;
    }

    private MQTTTest() {
    }

    private void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        System.setProperty("distservice_topic_match_expiry_seconds", "1");
        pluginMgr = new DefaultPluginManager();
        bgTaskExecutor = new ScheduledThreadPoolExecutor(1,
            EnvProvider.INSTANCE.newThreadFactory("bg-task-executor"));
        tickTaskExecutor = new ScheduledThreadPoolExecutor(2,
            EnvProvider.INSTANCE.newThreadFactory("tick-task-executor"));
        queryExecutor = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(20_000),
            EnvProvider.INSTANCE.newThreadFactory("query-executor"));
        mutationExecutor = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(20_000),
            EnvProvider.INSTANCE.newThreadFactory("mutation-executor"));
        AgentHostOptions agentHostOpts = AgentHostOptions.builder()
            .addr("127.0.0.1")
            .baseProbeInterval(Duration.ofSeconds(10))
            .joinRetryInSec(5)
            .joinTimeout(Duration.ofMinutes(5))
            .build();
        agentHost = IAgentHost.newInstance(agentHostOpts);
        agentHost.start();
        log.info("Agent host started");

        clientCrdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        clientCrdtService.start(agentHost);

        serverCrdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        serverCrdtService.start(agentHost);
        log.info("CRDT service started");

        RPCServerBuilder rpcServerBuilder = IRPCServer.newBuilder()
            .host("127.0.0.1")
            .crdtService(serverCrdtService)
            .executor(MoreExecutors.directExecutor());

        onlineInboxBrokerClient = IMqttBrokerClient.newBuilder()
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
        sessionDictClient = ISessionDictClient.newBuilder()
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
        sessionDictServer = ISessionDictServer.nonStandaloneBuilder()
            .rpcServerBuilder(rpcServerBuilder)
            .build();
        inboxClient = IInboxClient.newBuilder()
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
        inboxStoreKVStoreClient = IBaseKVStoreClient.newBuilder()
            .clusterId(IInboxStore.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
        inboxStore = IInboxStore.nonStandaloneBuilder()
            .rpcServerBuilder(rpcServerBuilder)
            .bootstrap(true)
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .inboxClient(inboxClient)
            .storeClient(inboxStoreKVStoreClient)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .queryExecutor(queryExecutor)
            .mutationExecutor(mutationExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .balanceControllerOptions(new KVRangeBalanceControllerOptions())
            .storeOptions(new KVRangeStoreOptions()
                .setDataEngineConfigurator(new InMemoryKVEngineConfigurator())
                .setWalEngineConfigurator(new InMemoryKVEngineConfigurator()))
            .build();
        distClient = IDistClient.newBuilder()
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
        inboxServer = IInboxServer.nonStandaloneBuilder()
            .rpcServerBuilder(rpcServerBuilder)
            .distClient(distClient)
            .settingProvider(settingProvider)
            .inboxStoreClient(inboxStoreKVStoreClient)
            .build();
        retainClient = IRetainClient
            .newBuilder()
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
        retainStoreKVStoreClient = IBaseKVStoreClient
            .newBuilder()
            .clusterId(IRetainStore.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
        retainStore = IRetainStore.nonStandaloneBuilder()
            .rpcServerBuilder(rpcServerBuilder)
            .bootstrap(true)
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .settingProvider(settingProvider)
            .storeClient(retainStoreKVStoreClient)
            .queryExecutor(queryExecutor)
            .mutationExecutor(mutationExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .balanceControllerOptions(new KVRangeBalanceControllerOptions())
            .storeOptions(new KVRangeStoreOptions()
                .setDataEngineConfigurator(new InMemoryKVEngineConfigurator())
                .setWalEngineConfigurator(new InMemoryKVEngineConfigurator()))
            .build();
        retainServer = IRetainServer.nonStandaloneBuilder()
            .rpcServerBuilder(rpcServerBuilder)
            .retainStoreClient(retainStoreKVStoreClient)
            .build();

        distWorkerStoreClient = IBaseKVStoreClient.newBuilder()
            .clusterId(IDistWorker.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .build();

        inboxBrokerMgr = new SubBrokerManager(pluginMgr, onlineInboxBrokerClient, inboxClient);

        KVRangeBalanceControllerOptions balanceControllerOptions = new KVRangeBalanceControllerOptions();
        distWorker = IDistWorker.nonStandaloneBuilder()
            .rpcServerBuilder(rpcServerBuilder)
            .bootstrap(true)
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .distClient(distClient)
            .storeClient(distWorkerStoreClient)
            .queryExecutor(queryExecutor)
            .mutationExecutor(mutationExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .balanceControllerOptions(new KVRangeBalanceControllerOptions())
            .storeOptions(new KVRangeStoreOptions()
                .setDataEngineConfigurator(new InMemoryKVEngineConfigurator())
                .setWalEngineConfigurator(new InMemoryKVEngineConfigurator()))
            .balanceControllerOptions(balanceControllerOptions)
            .subBrokerManager(inboxBrokerMgr)
            .build();
        distServer = IDistServer.nonStandaloneBuilder()
            .rpcServerBuilder(rpcServerBuilder)
            .distWorkerClient(distWorkerStoreClient)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .crdtService(serverCrdtService)
            .build();

        mqttBroker = IMQTTBroker.nonStandaloneBuilder()
            .rpcServerBuilder(rpcServerBuilder)
            .mqttBossGroup(NettyUtil.createEventLoopGroup(1))
            .mqttWorkerGroup(NettyUtil.createEventLoopGroup())
            .authProvider(authProvider)
            .eventCollector(eventCollector)
            .settingProvider(settingProvider)
            .distClient(distClient)
            .inboxClient(inboxClient)
            .sessionDictClient(sessionDictClient)
            .retainClient(retainClient)
            .buildTcpConnListener()
            .host("127.0.0.1")
            .buildListener()
            .build();

        sessionDictServer.start();
        log.info("Session dict server started");
        inboxStore.start();
        log.info("Inbox store started");
        inboxServer.start();
        log.info("Inbox server started");

        retainStore.start();
        log.info("Retain store started");
        retainServer.start();
        log.info("Retain server started");

        distWorker.start();
        log.info("Dist worker started");
        distServer.start();
        log.info("Dist server started");
        sharedRpcServer = rpcServerBuilder.build();
        sharedRpcServer.start();
        log.info("Shared RPC server started");
        mqttBroker.start();
        log.info("Mqtt broker started");

        Observable.combineLatest(
                distWorkerStoreClient.connState(),
                inboxStoreKVStoreClient.connState(),
                retainStoreKVStoreClient.connState(),
                onlineInboxBrokerClient.connState(),
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
        distWorkerStoreClient.join();
        inboxStoreKVStoreClient.join();
        retainStoreKVStoreClient.join();
        lenient().when(settingProvider.provide(any(), anyString()))
            .thenAnswer(invocation -> {
                Setting setting = invocation.getArgument(0);
                return setting.current(invocation.getArgument(1));
            });
    }

    @AfterSuite(groups = "integration")
    public static void teardown() throws Exception {
        if (singleInstance != null) {
            singleInstance.shutdown();
        }
    }

    public void shutdown() throws Exception {
        log.info("Start to tearing down");
        mqttBroker.shutdown();
        log.info("Mqtt broker shut down");
        sharedRpcServer.shutdown();
        log.info("Shared rpc server shutdown");

        distClient.stop();
        log.info("Dist client stopped");
        distServer.shutdown();
        log.info("Dist worker stopped");
        distWorkerStoreClient.stop();
        log.info("Dist server shut down");
        distWorker.stop();

        inboxBrokerMgr.stop();
        inboxClient.close();
        log.info("Inbox client stopped");
        inboxServer.shutdown();
        log.info("Inbox server shut down");

        inboxStoreKVStoreClient.stop();
        inboxStore.stop();
        log.info("Inbox store closed");

        retainClient.stop();
        log.info("Retain client stopped");
        retainServer.shutdown();
        log.info("Retain server shut down");

        retainStoreKVStoreClient.stop();
        retainStore.stop();
        log.info("Retain store closed");

        sessionDictClient.stop();
        log.info("Session dict client stopped");
        sessionDictServer.shutdown();
        log.info("Session dict server shut down");

        clientCrdtService.stop();
        serverCrdtService.stop();
        log.info("CRDT service stopped");
        agentHost.shutdown();
        log.info("Agent host stopped");

        log.info("Shutdown work executor");
        queryExecutor.shutdownNow();
        log.info("Shutdown tick task executor");
        tickTaskExecutor.shutdownNow();
        log.info("Shutdown bg task executor");
        bgTaskExecutor.shutdownNow();
        closeable.close();

        singleInstance = null;
    }
}
