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
import com.baidu.bifromq.basekv.balance.option.KVRangeBalanceControllerOptions;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.localengine.memory.InMemKVEngineConfigurator;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.client.MatchResult;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.store.IInboxStore;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

@Slf4j
public abstract class InboxServiceTest {
    protected IInboxClient inboxClient;
    private IAgentHost agentHost;
    private ICRDTService clientCrdtService;
    private ICRDTService serverCrdtService;
    private ISettingProvider settingProvider = Setting::current;
    private IEventCollector eventCollector = event -> {

    };

    @Mock
    private IDistClient distClient;
    private IBaseKVStoreClient inboxStoreClient;
    private IInboxStore inboxStore;
    private IInboxServer inboxServer;
    private ExecutorService queryExecutor;
    private ScheduledExecutorService tickTaskExecutor;
    private ScheduledExecutorService bgTaskExecutor;
    private AutoCloseable closeable;

    @BeforeClass(alwaysRun = true)
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        when(distClient.match(anyLong(), anyString(), anyString(), any(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        AgentHostOptions agentHostOpts = AgentHostOptions.builder()
            .addr("127.0.0.1")
            .baseProbeInterval(Duration.ofSeconds(10))
            .joinRetryInSec(5)
            .joinTimeout(Duration.ofMinutes(5))
            .build();
        agentHost = IAgentHost.newInstance(agentHostOpts);
        agentHost.start();
        clientCrdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        clientCrdtService.start(agentHost);

        serverCrdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        serverCrdtService.start(agentHost);
        inboxClient = IInboxClient.newBuilder().crdtService(clientCrdtService).build();

        KVRangeBalanceControllerOptions controllerOptions = new KVRangeBalanceControllerOptions();
        KVRangeStoreOptions kvRangeStoreOptions = new KVRangeStoreOptions();
        kvRangeStoreOptions.setDataEngineConfigurator(new InMemKVEngineConfigurator());
        kvRangeStoreOptions.setWalEngineConfigurator(new InMemKVEngineConfigurator());
        queryExecutor = Executors.newFixedThreadPool(2);
        tickTaskExecutor = Executors.newScheduledThreadPool(2);
        bgTaskExecutor = Executors.newSingleThreadScheduledExecutor();
        inboxStoreClient = IBaseKVStoreClient.newBuilder()
            .clusterId(IInboxStore.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .build();
        inboxStore = IInboxStore.standaloneBuilder()
            .bootstrap(true)
            .host("127.0.0.1")
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .distClient(distClient)
            .storeClient(inboxStoreClient)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .storeOptions(kvRangeStoreOptions)
            .balanceControllerOptions(controllerOptions)
            .queryExecutor(queryExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .build();
        inboxServer = IInboxServer.standaloneBuilder()
            .host("127.0.0.1")
            .crdtService(serverCrdtService)
            .distClient(distClient)
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
    public void teardown() {
        log.info("Finish testing, and tearing down");
        inboxServer.shutdown();
        inboxStore.stop();
        inboxClient.close();
        inboxStoreClient.stop();
        new Thread(() -> {
            clientCrdtService.stop();
            serverCrdtService.stop();
            agentHost.shutdown();
            queryExecutor.shutdown();
            tickTaskExecutor.shutdown();
            bgTaskExecutor.shutdown();
        }).start();
        closeable.close();
    }
}
