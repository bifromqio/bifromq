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
import com.baidu.bifromq.basekv.balance.option.KVRangeBalanceControllerOptions;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.localengine.InMemoryKVEngineConfigurator;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.server.IDistServer;
import com.baidu.bifromq.dist.worker.IDistWorker;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.plugin.subbroker.CheckResult;
import com.baidu.bifromq.plugin.subbroker.DeliveryPack;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.baidu.bifromq.plugin.subbroker.ISubBroker;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.SubInfo;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
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
        .setTenantId("DevOnly")
        .putMetadata("type", "benchmark")
        .build();

    private final IAgentHost agentHost;
    private final ICRDTService crdtService;
    private final IDistClient distClient;
    private final IDistWorker distWorker;
    private final IDistServer distServer;
    private final IBaseKVStoreClient storeClient;
    private final AtomicLong seqNo = new AtomicLong(10000);

    public DistServiceState() {
        AgentHostOptions agentHostOpts = AgentHostOptions.builder()
            .addr("127.0.0.1")
            .baseProbeInterval(Duration.ofSeconds(10))
            .joinRetryInSec(5)
            .joinTimeout(Duration.ofMinutes(5))
            .build();
        agentHost = IAgentHost.newInstance(agentHostOpts);
        agentHost.start();
        CRDTServiceOptions crdtServiceOptions = CRDTServiceOptions.builder().build();
        crdtService = ICRDTService.newInstance(crdtServiceOptions);
        crdtService.start(agentHost);
        distClient = IDistClient.newBuilder()
            .crdtService(crdtService)
            .build();

        KVRangeStoreOptions kvRangeStoreOptions = new KVRangeStoreOptions();
        kvRangeStoreOptions.setDataEngineConfigurator(new InMemoryKVEngineConfigurator());
        kvRangeStoreOptions.setWalEngineConfigurator(new InMemoryKVEngineConfigurator());
        storeClient = IBaseKVStoreClient
            .newBuilder()
            .clusterId(IDistWorker.CLUSTER_NAME)
            .crdtService(crdtService)
            .build();
        ISettingProvider settingProvider = Setting::current;
        IEventCollector eventCollector = event -> {

        };
        ISubBrokerManager subBrokerMgr = new ISubBrokerManager() {
            public ISubBroker get(int subBrokerId) {
                return new ISubBroker() {
                    @Override
                    public int id() {
                        return 0;
                    }

                    @Override
                    public IDeliverer open(String delivererKey) {
                        return new IDeliverer() {
                            @Override
                            public CompletableFuture<Map<SubInfo, DeliveryResult>> deliver(
                                Iterable<DeliveryPack> packs) {
                                return CompletableFuture.completedFuture(Collections.emptyMap());
                            }

                            @Override
                            public void close() {

                            }
                        };
                    }

                    @Override
                    public CompletableFuture<CheckResult> hasInbox(long reqId, String tenantId, String inboxId,
                                                                   String delivererKey) {
                        return CompletableFuture.completedFuture(CheckResult.EXIST);
                    }

                    @Override
                    public void close() {

                    }
                };
            }

            @Override
            public void stop() {

            }
        };
        distWorker = IDistWorker.standaloneBuilder()
            .bootstrap(true)
            .host("127.0.0.1")
            .agentHost(agentHost)
            .crdtService(crdtService)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .distClient(distClient)
            .storeClient(storeClient)
            .queryExecutor(Executors.newWorkStealingPool())
            .mutationExecutor(Executors.newWorkStealingPool())
            .bgTaskExecutor(Executors.newSingleThreadScheduledExecutor())
            .tickTaskExecutor(Executors.newSingleThreadScheduledExecutor())
            .balanceControllerOptions(new KVRangeBalanceControllerOptions())
            .storeOptions(kvRangeStoreOptions)
            .subBrokerManager(subBrokerMgr)
            .build();
        distServer = IDistServer.standaloneBuilder()
            .distWorkerClient(storeClient)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .crdtService(crdtService)
            .build();
    }

    @Setup(Level.Trial)
    public void setup() {
        distWorker.start();
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

    public void requestClear(String inboxId, String delivererKey, int brokerId, ClientInfo clientInfo) {
        long reqId = seqNo.incrementAndGet();
        distClient.clear(reqId, clientInfo.getTenantId(), inboxId, delivererKey, brokerId).join();
    }
}
