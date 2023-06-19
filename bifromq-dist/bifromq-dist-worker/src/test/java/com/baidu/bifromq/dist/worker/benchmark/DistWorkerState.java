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

package com.baidu.bifromq.dist.worker.benchmark;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.localengine.InMemoryKVEngineConfigurator;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.entity.EntityUtil;
import com.baidu.bifromq.dist.rpc.proto.ClearSubInfoReply;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcInput;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcOutput;
import com.baidu.bifromq.dist.rpc.proto.UpdateReply;
import com.baidu.bifromq.dist.util.MessageUtil;
import com.baidu.bifromq.dist.worker.IDistWorker;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.inboxbroker.HasResult;
import com.baidu.bifromq.plugin.inboxbroker.IInboxBrokerManager;
import com.baidu.bifromq.plugin.inboxbroker.IInboxWriter;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
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
public class DistWorkerState {
    public static final int MqttBroker = 0;
    public static final int InboxService = 1;

    private IAgentHost agentHost;
    private ICRDTService crdtService;

    private ISettingProvider settingProvider = Setting::current;
    private IEventCollector eventCollector = new IEventCollector() {
        @Override
        public void report(Event<?> event) {

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

    private IDistWorker testWorker;

    private IBaseKVStoreClient storeClient;

    private AtomicLong seqNo = new AtomicLong(10000);

    public DistWorkerState() {
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
        distClient = IDistClient.inProcClientBuilder().build();

        KVRangeStoreOptions kvRangeStoreOptions = new KVRangeStoreOptions();
        kvRangeStoreOptions.setDataEngineConfigurator(new InMemoryKVEngineConfigurator());
        kvRangeStoreOptions.setWalEngineConfigurator(new InMemoryKVEngineConfigurator());
        storeClient = IBaseKVStoreClient
            .inProcClientBuilder()
            .clusterId(IDistWorker.CLUSTER_NAME)
            .crdtService(crdtService)
            .build();
        testWorker = IDistWorker
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
    }

    @Setup(Level.Trial)
    public void setup() {
        testWorker.start(true);
        storeClient.join();
        log.info("Setup finished, and start testing");
    }

    @TearDown(Level.Trial)
    public void teardown() {
        log.info("Finish testing, and tearing down");
        storeClient.stop();
        testWorker.stop();
        crdtService.stop();
        agentHost.shutdown();
    }

    public ClearSubInfoReply requestClearSubInfo(String trafficId, int subBroker, String inboxId,
                                                 String serverId) {
        try {
            long reqId = seqNo.incrementAndGet();
            ByteString subInfoKey =
                EntityUtil.subInfoKey(trafficId, EntityUtil.toQualifiedInboxId(subBroker, inboxId, serverId));
            KVRangeSetting s = storeClient.findByKey(subInfoKey).get();
            DistServiceRWCoProcInput input = MessageUtil.buildClearSubInfoRequest(reqId, subInfoKey);
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reqId, reply.getReqId());
            assertEquals(ReplyCode.Ok, reply.getCode());
            UpdateReply updateReply = DistServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult()).getUpdateReply();
            assertTrue(updateReply.hasClearSubInfo());
            assertEquals(reqId, updateReply.getReqId());
            return updateReply.getClearSubInfo();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }
}
