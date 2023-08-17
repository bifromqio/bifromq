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

package com.baidu.bifromq.dist.server;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

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
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.worker.IDistWorker;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.plugin.subbroker.CheckResult;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.baidu.bifromq.plugin.subbroker.ISubBroker;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

@Slf4j
public abstract class DistServiceTest {
    private IAgentHost agentHost;
    private ICRDTService clientCrdtService;
    private ICRDTService serverCrdtService;
    private IDistWorker distWorker;
    private IDistServer distServer;
    private IDistClient distClient;
    private IBaseKVStoreClient workerClient;
    private ExecutorService queryExecutor;
    private ExecutorService mutationExecutor;
    private ScheduledExecutorService tickTaskExecutor;
    private ScheduledExecutorService bgTaskExecutor;
    private ISettingProvider settingProvider = Setting::current;

    private IEventCollector eventCollector = new IEventCollector() {
        @Override
        public void report(Event<?> event) {
            log.debug("event {}", event);
        }
    };

    @Mock
    protected ISubBroker inboxBroker;
    @Mock
    protected IDeliverer inboxDeliverer;
    @Mock
    private ISubBrokerManager subBrokerMgr;

    private AutoCloseable closeable;

    @BeforeClass(alwaysRun = true)
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        when(subBrokerMgr.get(anyInt())).thenReturn(inboxBroker);
        when(inboxBroker.open(anyString())).thenReturn(inboxDeliverer);
        queryExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(2, 2, 0L,
                TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                EnvProvider.INSTANCE.newThreadFactory("query-executor")),
            "query-executor");
        mutationExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(2, 2, 0L,
                TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                EnvProvider.INSTANCE.newThreadFactory("mutation-executor")),
            "mutation-executor");
        tickTaskExecutor = ExecutorServiceMetrics
            .monitor(Metrics.globalRegistry, new ScheduledThreadPoolExecutor(2,
                EnvProvider.INSTANCE.newThreadFactory("tick-task-executor")), "tick-task-executor");
        bgTaskExecutor = ExecutorServiceMetrics
            .monitor(Metrics.globalRegistry, new ScheduledThreadPoolExecutor(1,
                EnvProvider.INSTANCE.newThreadFactory("bg-task-executor")), "bg-task-executor");
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

        distClient = IDistClient.newBuilder().crdtService(clientCrdtService).build();

        KVRangeStoreOptions kvRangeStoreOptions = new KVRangeStoreOptions();
        kvRangeStoreOptions.setDataEngineConfigurator(new InMemoryKVEngineConfigurator());
        kvRangeStoreOptions.setWalEngineConfigurator(new InMemoryKVEngineConfigurator());

        KVRangeBalanceControllerOptions balanceControllerOptions = new KVRangeBalanceControllerOptions();
        workerClient = IBaseKVStoreClient
            .newBuilder()
            .clusterId(IDistWorker.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .build();
        distWorker = IDistWorker
            .standaloneBuilder()
            .bootstrap(true)
            .host("127.0.0.1")
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .distClient(distClient)
            .storeClient(workerClient)
            .statsInterval(Duration.ofSeconds(5))
            .queryExecutor(queryExecutor)
            .mutationExecutor(mutationExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .storeOptions(kvRangeStoreOptions)
            .balanceControllerOptions(balanceControllerOptions)
            .subBrokerManager(subBrokerMgr)
            .build();
        distServer = IDistServer.standaloneBuilder()
            .host("127.0.0.1")
            .distWorkerClient(workerClient)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .crdtService(clientCrdtService)
            .build();

        distWorker.start();
        distServer.start();
        workerClient.join();
        distClient.connState().filter(s -> s == IRPCClient.ConnState.READY).blockingFirst();
        log.info("Setup finished, and start testing");
    }

    @AfterClass(alwaysRun = true)
    public void teardown() throws Exception {
        log.info("Finish testing, and tearing down");
        new Thread(() -> {
            workerClient.stop();
            distWorker.stop();
            distClient.stop();
            distServer.shutdown();
            clientCrdtService.stop();
            serverCrdtService.stop();
            agentHost.shutdown();
            queryExecutor.shutdown();
            mutationExecutor.shutdown();
            tickTaskExecutor.shutdown();
            bgTaskExecutor.shutdown();
        }).start();
        closeable.close();
    }

    protected final IDistClient distClient() {
        return distClient;
    }
}
