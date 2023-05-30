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

import static com.baidu.bifromq.baseutils.ThreadUtil.threadFactory;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.balance.option.KVRangeBalanceControllerOptions;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.localengine.InMemoryKVEngineConfigurator;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.baseutils.PortUtil;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.worker.IDistWorker;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.inboxbroker.HasResult;
import com.baidu.bifromq.plugin.inboxbroker.IInboxBrokerManager;
import com.baidu.bifromq.plugin.inboxbroker.IInboxWriter;
import com.baidu.bifromq.plugin.inboxbroker.WriteResult;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;

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
        public <T extends Event> void report(T event) {
            log.debug("event {}", event);
        }
    };

    @Mock
    protected IInboxWriter inboxWriter;
    private IInboxBrokerManager receiverManager = new IInboxBrokerManager() {
        @Override
        public boolean hasBroker(int brokerId) {
            return true;
        }

        @Override
        public IInboxWriter openWriter(String inboxGroupKey, int brokerId) {
            return inboxWriter;
        }

        @Override
        public CompletableFuture<HasResult> hasInbox(long reqId,
                                                     String trafficId,
                                                     String inboxId,
                                                     String inboxGroupKey,
                                                     int brokerId) {
            return CompletableFuture.completedFuture(HasResult.YES);
        }

        @Override
        public void stop() {

        }
    };

    @Before
    public void setup() {
        queryExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(2, 2, 0L,
                TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(), threadFactory("query-executor-%d")),
            "query-executor");
        mutationExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(2, 2, 0L,
                TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(), threadFactory("mutation-executor-%d")),
            "mutation-executor");
        tickTaskExecutor = ExecutorServiceMetrics
            .monitor(Metrics.globalRegistry, new ScheduledThreadPoolExecutor(2,
                threadFactory("tick-task-executor-%d")), "tick-task-executor");
        bgTaskExecutor = ExecutorServiceMetrics
            .monitor(Metrics.globalRegistry, new ScheduledThreadPoolExecutor(1,
                threadFactory("bg-task-executor-%d")), "bg-task-executor");
        AgentHostOptions agentHostOpts = AgentHostOptions.builder()
            .addr("127.0.0.1")
            .port(PortUtil.freePort())
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

        distClient = IDistClient.inProcClientBuilder().build();

        KVRangeStoreOptions kvRangeStoreOptions = new KVRangeStoreOptions();
        kvRangeStoreOptions.setDataEngineConfigurator(new InMemoryKVEngineConfigurator());
        kvRangeStoreOptions.setWalEngineConfigurator(new InMemoryKVEngineConfigurator());

        KVRangeBalanceControllerOptions balanceControllerOptions = new KVRangeBalanceControllerOptions();
        workerClient = IBaseKVStoreClient
            .inProcClientBuilder()
            .clusterId(IDistWorker.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .build();
        distWorker = IDistWorker
            .inProcBuilder()
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
            .kvRangeStoreOptions(kvRangeStoreOptions)
            .balanceControllerOptions(balanceControllerOptions)
            .inboxBrokerManager(receiverManager)
            .build();
        distServer = IDistServer.inProcBuilder()
            .storeClient(workerClient)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .crdtService(clientCrdtService)
            .build();

        distWorker.start(true);
        distServer.start();
        workerClient.join();
        distClient.connState().filter(s -> s == IRPCClient.ConnState.READY).blockingFirst();
        log.info("Setup finished, and start testing");
        when(inboxWriter.write(anyMap()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                Map<TopicMessagePack, List<SubInfo>> msgPack = invocation.getArgument(0);
                return CompletableFuture.completedFuture(msgPack.values().stream().flatMap(l -> l.stream())
                    .collect(Collectors.toMap(s -> s, s -> WriteResult.OK)));
            });
    }

    @After
    public void teardown() {
        log.info("Finish testing, and tearing down");
        workerClient.stop();
        distWorker.stop();
        distClient.stop();
        distServer.shutdown();
        clientCrdtService.stop();
        serverCrdtService.stop();
        agentHost.shutdown();
//        queryExecutor.shutdown();
//        mutationExecutor.shutdown();
//        tickTaskExecutor.shutdown();
//        bgTaskExecutor.shutdown();
    }

    protected final IDistClient distClient() {
        return distClient;
    }
}
