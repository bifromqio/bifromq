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

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.localengine.InMemoryKVEngineConfigurator;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.inbox.client.IInboxBrokerClient;
import com.baidu.bifromq.inbox.client.IInboxReaderClient;
import com.baidu.bifromq.inbox.store.IInboxStore;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

@Slf4j
public abstract class InboxServiceTest {
    protected IInboxReaderClient inboxReaderClient;
    protected IInboxBrokerClient inboxBrokerClient;
    private IAgentHost agentHost;
    private ICRDTService clientCrdtService;
    private ICRDTService serverCrdtService;
    private ISettingProvider settingProvider = Setting::current;

    private IEventCollector eventCollector = new IEventCollector() {
        @Override
        public void report(Event<?> event) {

        }
    };
    private IBaseKVStoreClient inboxStoreClient;

    private IInboxStore inboxStore;

    private IInboxServer inboxServer;
    private ExecutorService queryExecutor;
    private ExecutorService mutationExecutor;
    private ScheduledExecutorService tickTaskExecutor;
    private ScheduledExecutorService bgTaskExecutor;

    @BeforeClass(alwaysRun = true)
    public void setup() {
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
        inboxBrokerClient = IInboxBrokerClient.inProcClientBuilder().build();
        inboxReaderClient = IInboxReaderClient.inProcClientBuilder().build();


        KVRangeStoreOptions kvRangeStoreOptions = new KVRangeStoreOptions();
        kvRangeStoreOptions.setDataEngineConfigurator(new InMemoryKVEngineConfigurator());
        kvRangeStoreOptions.setWalEngineConfigurator(new InMemoryKVEngineConfigurator());
        queryExecutor = new ThreadPoolExecutor(2, 2, 0L,
            TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
            EnvProvider.INSTANCE.newThreadFactory("query-executor"));
        mutationExecutor = new ThreadPoolExecutor(2, 2, 0L,
            TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
            EnvProvider.INSTANCE.newThreadFactory("mutation-executor"));
        tickTaskExecutor = new ScheduledThreadPoolExecutor(2,
            EnvProvider.INSTANCE.newThreadFactory("tick-task-executor"));
        bgTaskExecutor = new ScheduledThreadPoolExecutor(1,
            EnvProvider.INSTANCE.newThreadFactory("bg-task-executor"));

        inboxStoreClient = IBaseKVStoreClient
            .inProcClientBuilder()
            .clusterId(IInboxStore.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .build();
        inboxStore = IInboxStore
            .inProcBuilder()
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .storeClient(inboxStoreClient)
            .eventCollector(eventCollector)
            .kvRangeStoreOptions(kvRangeStoreOptions)
            .queryExecutor(queryExecutor)
            .mutationExecutor(mutationExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .build();
        inboxServer = IInboxServer.inProcBuilder()
            .settingProvider(settingProvider)
            .storeClient(inboxStoreClient)
            .build();
        inboxStore.start(true);
        inboxServer.start();
        inboxStoreClient.join();
        inboxReaderClient.connState().filter(s -> s == IRPCClient.ConnState.READY).blockingFirst();
        log.info("Setup finished, and start testing");
    }

    @AfterClass(alwaysRun = true)
    public void teardown() {
        log.info("Finish testing, and tearing down");
        inboxBrokerClient.close();
        inboxReaderClient.stop();
        inboxServer.shutdown();
        inboxStoreClient.stop();
        inboxStore.stop();
        new Thread(() -> {
            clientCrdtService.stop();
            serverCrdtService.stop();
            agentHost.shutdown();
            queryExecutor.shutdown();
            mutationExecutor.shutdown();
            tickTaskExecutor.shutdown();
            bgTaskExecutor.shutdown();
        }).start();
    }
}
