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

package com.baidu.bifromq.inbox.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.localengine.memory.InMemKVEngineConfigurator;
import com.baidu.bifromq.basekv.metaservice.IBaseKVMetaService;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.baserpc.client.IRPCClient;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.client.MatchResult;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.store.IInboxStore;
import com.baidu.bifromq.inbox.store.balance.RangeBootstrapBalancerFactory;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.bifromq.plugin.resourcethrottler.IResourceThrottler;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Struct;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

@Slf4j
public abstract class InboxServiceTest {
    protected IInboxClient inboxClient;
    private IAgentHost agentHost;
    private ICRDTService crdtService;
    private IRPCServiceTrafficService trafficService;
    private IBaseKVMetaService metaService;
    @Mock
    protected IEventCollector eventCollector;
    @Mock
    protected IResourceThrottler resourceThrottler;
    @Mock
    protected ISettingProvider settingProvider;
    @Mock
    protected IDistClient distClient;
    @Mock
    protected IRetainClient retainClient;
    private IBaseKVStoreClient inboxStoreClient;
    private IInboxStore inboxStore;
    private IInboxServer inboxServer;
    private ExecutorService queryExecutor;
    private int tickerThreads = 2;
    private ScheduledExecutorService bgTaskExecutor;
    private AutoCloseable closeable;

    @BeforeClass(alwaysRun = true)
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        when(resourceThrottler.hasResource(anyString(), any())).thenReturn(true);
        when(settingProvider.provide(any(), anyString())).thenAnswer(
            invocation -> ((Setting) invocation.getArgument(0)).current(invocation.getArgument(1)));
        when(distClient.match(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        when(distClient.unmatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(UnmatchResult.OK));
        AgentHostOptions agentHostOpts = AgentHostOptions.builder()
            .addr("127.0.0.1")
            .baseProbeInterval(Duration.ofSeconds(10))
            .joinRetryInSec(5)
            .joinTimeout(Duration.ofMinutes(5))
            .build();
        agentHost = IAgentHost.newInstance(agentHostOpts);
        agentHost.start();

        crdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        crdtService.start(agentHost);

        trafficService = IRPCServiceTrafficService.newInstance(crdtService);

        metaService = IBaseKVMetaService.newInstance(crdtService);
        inboxClient = IInboxClient.newBuilder().trafficService(trafficService).build();

        KVRangeStoreOptions kvRangeStoreOptions = new KVRangeStoreOptions();
        kvRangeStoreOptions.setDataEngineConfigurator(new InMemKVEngineConfigurator());
        kvRangeStoreOptions.setWalEngineConfigurator(new InMemKVEngineConfigurator());
        queryExecutor = Executors.newFixedThreadPool(2);
        bgTaskExecutor = Executors.newSingleThreadScheduledExecutor();
        inboxStoreClient = IBaseKVStoreClient.newBuilder()
            .clusterId(IInboxStore.CLUSTER_NAME)
            .trafficService(trafficService)
            .metaService(metaService)
            .build();
        inboxStore = IInboxStore.standaloneBuilder()
            .bootstrap(true)
            .host("127.0.0.1")
            .agentHost(agentHost)
            .trafficService(trafficService)
            .metaService(metaService)
            .storeClient(inboxStoreClient)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .storeOptions(kvRangeStoreOptions)
            .queryExecutor(queryExecutor)
            .rpcExecutor(MoreExecutors.directExecutor())
            .tickerThreads(tickerThreads)
            .bgTaskExecutor(bgTaskExecutor)
            .balancerFactoryConfig(
                Map.of(RangeBootstrapBalancerFactory.class.getName(), Struct.getDefaultInstance()))
            .build();
        inboxServer = IInboxServer.standaloneBuilder()
            .host("127.0.0.1")
            .trafficService(trafficService)
            .inboxClient(inboxClient)
            .distClient(distClient)
            .retainClient(retainClient)
            .resourceThrottler(resourceThrottler)
            .eventCollector(eventCollector)
            .settingProvider(settingProvider)
            .inboxStoreClient(inboxStoreClient)
            .build();
        inboxStore.start();
        inboxServer.start();
        inboxStoreClient.join();
        inboxClient.connState().filter(s -> s == IRPCClient.ConnState.READY).blockingFirst();
        log.info("Setup finished, and start testing");
    }

    @SneakyThrows
    @AfterClass(alwaysRun = true)
    public void tearDown() {
        log.info("Finish testing, and tearing down");
        inboxServer.shutdown();
        inboxStore.stop();
        inboxClient.close();
        inboxStoreClient.stop();
        metaService.stop();
        trafficService.stop();
        crdtService.stop();
        agentHost.shutdown();
        queryExecutor.shutdown();
        bgTaskExecutor.shutdown();
        closeable.close();
    }

    @BeforeMethod(alwaysRun = true)
    public void beforeCaseStart(Method method) {
        log.info("Test case[{}.{}] start", method.getDeclaringClass().getName(), method.getName());
    }

    @SneakyThrows
    @AfterMethod(alwaysRun = true)
    public void afterCaseFinish(Method method) {
        log.info("Test case[{}.{}] finished, doing teardown",
            method.getDeclaringClass().getName(), method.getName());
    }
}
