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

package com.baidu.bifromq.dist.server.benchmark;

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.localengine.InMemoryKVEngineConfigurator;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.baseutils.PortUtil;
import com.baidu.bifromq.dist.client.ClearResult;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.server.IDistServer;
import com.baidu.bifromq.dist.worker.IDistWorker;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.inboxbroker.HasResult;
import com.baidu.bifromq.plugin.inboxbroker.IInboxBrokerManager;
import com.baidu.bifromq.plugin.inboxbroker.IInboxWriter;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.SysClientInfo;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@Slf4j
@State(Scope.Benchmark)
public class DistServiceState {
    public static final int MqttBroker = 0;
    public static final int InboxService = 1;
    public final ClientInfo clientInfo = ClientInfo.newBuilder()
        .setTrafficId("DevOnly")
        .setUserId("benchmark")
        .setSysClientInfo(SysClientInfo.newBuilder()
            .setType("benchmark")
            .build())
        .build();

    private IAgentHost agentHost;
    private ICRDTService crdtService;

    private ISettingProvider settingProvider = Setting::current;

    private IEventCollector eventCollector = new IEventCollector() {
        @Override
        public <T extends Event> void report(T event) {

        }
    };
    private IDistClient distClient;
    private IInboxBrokerManager receiverManager = new IInboxBrokerManager() {
        @Override
        public boolean hasBroker(int brokerId) {
            return true;
        }

        @Override
        public IInboxWriter openWriter(String inboxGroupKey, int brokerId) {
            return null;
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

    private IDistWorker distWorker;
    private IDistServer distServer;

    private IBaseKVStoreClient storeClient;

    private AtomicLong seqNo = new AtomicLong(10000);

    public DistServiceState() {
        AgentHostOptions agentHostOpts = AgentHostOptions.builder()
            .addr("127.0.0.1")
            .port(PortUtil.freePort())
            .baseProbeInterval(Duration.ofSeconds(10))
            .joinRetryInSec(5)
            .joinTimeout(Duration.ofMinutes(5))
            .build();
        agentHost = IAgentHost.newInstance(agentHostOpts);
        agentHost.start();
        CRDTServiceOptions crdtServiceOptions = CRDTServiceOptions.builder().build();
        crdtService = ICRDTService.newInstance(crdtServiceOptions);
        crdtService.start(agentHost);
        distClient = IDistClient.inProcClientBuilder().build();

        KVRangeStoreOptions kvRangeStoreOptions = new KVRangeStoreOptions();
        kvRangeStoreOptions.setDataEngineConfigurator(new InMemoryKVEngineConfigurator());
        kvRangeStoreOptions.setWalEngineConfigurator(new InMemoryKVEngineConfigurator());
        storeClient = IBaseKVStoreClient
            .inProcClientBuilder()
            .clusterId(IDistWorker.CLUSTER_NAME)
            .crdtService(crdtService)
            .build();
        distWorker = IDistWorker
            .inProcBuilder()
            .agentHost(agentHost)
            .crdtService(crdtService)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .distClient(distClient)
            .storeClient(storeClient)
            .kvRangeStoreOptions(kvRangeStoreOptions)
            .inboxBrokerManager(receiverManager)
            .build();
        distServer = IDistServer.inProcBuilder()
            .storeClient(storeClient)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .crdtService(crdtService)
            .build();
    }

    @Setup(Level.Trial)
    public void setup() {
        distWorker.start(true);
        distServer.start();
        storeClient.join();
        log.info("Setup finished, and start testing");
    }

    @TearDown(Level.Trial)
    public void teardown() {
        log.info("Finish testing, and tearing down");
        storeClient.stop();
        distServer.shutdown();
        distWorker.stop();
        crdtService.stop();
        agentHost.shutdown();
    }

    public ClearResult requestClear(String inboxId, String inboxGroupKey, int brokerId, ClientInfo clientInfo) {
        long reqId = seqNo.incrementAndGet();
        return distClient.clear(reqId, inboxId, inboxGroupKey, brokerId, clientInfo)
            .join();
    }
}
