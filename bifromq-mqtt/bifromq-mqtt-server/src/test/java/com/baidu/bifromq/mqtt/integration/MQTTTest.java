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

package com.baidu.bifromq.mqtt.integration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.balance.option.KVRangeBalanceControllerOptions;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.localengine.memory.InMemKVEngineConfigurator;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.baserpc.IRPCServer;
import com.baidu.bifromq.baserpc.RPCServerBuilder;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.server.IDistServer;
import com.baidu.bifromq.dist.worker.IDistWorker;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.server.IInboxServer;
import com.baidu.bifromq.inbox.store.IInboxStore;
import com.baidu.bifromq.mqtt.IMQTTBroker;
import com.baidu.bifromq.mqtt.inbox.IMqttBrokerClient;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.plugin.subbroker.SubBrokerManager;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.retain.server.IRetainServer;
import com.baidu.bifromq.retain.store.IRetainStore;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import com.baidu.bifromq.sessiondict.rpc.proto.KillReply;
import com.baidu.bifromq.sessiondict.server.ISessionDictServer;
import com.baidu.bifromq.type.ClientInfo;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import io.reactivex.rxjava3.core.Observable;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.pf4j.DefaultPluginManager;
import org.pf4j.PluginManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

@Slf4j
public abstract class MQTTTest {
    protected static final String BROKER_URI = "tcp://127.0.0.1:1883";
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
    private ISessionDictClient sessionDictClient;
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
    private ScheduledExecutorService tickTaskExecutor;
    private ScheduledExecutorService bgTaskExecutor;
    private AutoCloseable closeable;
    protected String tenantId;

    protected MQTTTest() {
    }

    @BeforeClass(alwaysRun = true, groups = "integration")
    public final void setupClass() {
        closeable = MockitoAnnotations.openMocks(this);

        System.setProperty("distservice_topic_match_expiry_seconds", "1");
        pluginMgr = new DefaultPluginManager();
        queryExecutor = Executors.newFixedThreadPool(2);
        tickTaskExecutor = Executors.newScheduledThreadPool(2);
        bgTaskExecutor = Executors.newSingleThreadScheduledExecutor();

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
            .mqttBrokerClient(onlineInboxBrokerClient)
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
            .storeClient(inboxStoreKVStoreClient)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .queryExecutor(queryExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .balanceControllerOptions(new KVRangeBalanceControllerOptions())
            .storeOptions(new KVRangeStoreOptions()
                .setDataEngineConfigurator(new InMemKVEngineConfigurator())
                .setWalEngineConfigurator(new InMemKVEngineConfigurator()))
            .build();
        distClient = IDistClient.newBuilder()
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
        retainClient = IRetainClient
            .newBuilder()
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
        inboxServer = IInboxServer.nonStandaloneBuilder()
            .rpcServerBuilder(rpcServerBuilder)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .inboxClient(inboxClient)
            .distClient(distClient)
            .retainClient(retainClient)
            .inboxStoreClient(inboxStoreKVStoreClient)
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
            .storeClient(retainStoreKVStoreClient)
            .queryExecutor(queryExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .balanceControllerOptions(new KVRangeBalanceControllerOptions())
            .storeOptions(new KVRangeStoreOptions()
                .setDataEngineConfigurator(new InMemKVEngineConfigurator())
                .setWalEngineConfigurator(new InMemKVEngineConfigurator()))
            .build();

        distWorkerStoreClient = IBaseKVStoreClient.newBuilder()
            .clusterId(IDistWorker.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .build();

        inboxBrokerMgr = new SubBrokerManager(pluginMgr, onlineInboxBrokerClient, inboxClient);
        retainServer = IRetainServer.nonStandaloneBuilder()
            .rpcServerBuilder(rpcServerBuilder)
            .subBrokerManager(inboxBrokerMgr)
            .retainStoreClient(retainStoreKVStoreClient)
            .settingProvider(settingProvider)
            .build();
        KVRangeBalanceControllerOptions balanceControllerOptions = new KVRangeBalanceControllerOptions();
        distWorker = IDistWorker.nonStandaloneBuilder()
            .rpcServerBuilder(rpcServerBuilder)
            .bootstrap(true)
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .eventCollector(eventCollector)
            .distClient(distClient)
            .storeClient(distWorkerStoreClient)
            .queryExecutor(queryExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .balanceControllerOptions(new KVRangeBalanceControllerOptions())
            .storeOptions(new KVRangeStoreOptions()
                .setDataEngineConfigurator(new InMemKVEngineConfigurator())
                .setWalEngineConfigurator(new InMemKVEngineConfigurator()))
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
            .mqttBossELGThreads(1)
            .mqttWorkerELGThreads(4)
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

    @AfterClass(alwaysRun = true, groups = "integration")
    public final void tearDownClass() throws Exception {
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
    }

    @BeforeMethod(groups = "integration")
    public final void setupTest(Method method) {
        log.info("Test case[{}.{}] start", method.getDeclaringClass().getName(), method.getName());
        tenantId = System.nanoTime() + "";
        lenient().doAnswer(invocationOnMock -> {
            Event event = invocationOnMock.getArgument(0);
            event.clone(event.getClass().getConstructor().newInstance());
            return null;
        }).when(eventCollector).report(any(Event.class));
        doSetup(method);
    }

    protected CompletableFuture<KillReply.Result> kill(String userId, String clientId) {
        return sessionDictClient.kill(System.nanoTime(), tenantId, userId, clientId, ClientInfo.newBuilder()
            .setTenantId(tenantId)
            .setType("Killer")
            .build()).thenApply(KillReply::getResult);
    }

    protected void doSetup(Method method) {
    }

    @AfterMethod(groups = "integration")
    private void tearDownTest(Method method) {
        log.info("Test case[{}.{}] finished", method.getDeclaringClass().getName(), method.getName());
        doTearDown(method);
        Mockito.reset(authProvider, eventCollector, settingProvider);
    }

    protected void doTearDown(Method method) {
    }
}
