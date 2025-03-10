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

package com.baidu.bifromq.retain.store;

import static com.baidu.bifromq.basekv.client.KVRangeRouterUtil.findByBoundary;
import static com.baidu.bifromq.basekv.client.KVRangeRouterUtil.findByKey;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static com.baidu.bifromq.metrics.TenantMetric.MqttRetainNumGauge;
import static com.baidu.bifromq.metrics.TenantMetric.MqttRetainSpaceGauge;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.KVRangeSetting;
import com.baidu.bifromq.basekv.localengine.rocksdb.RocksDBCPableKVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.rocksdb.RocksDBWALableKVEngineConfigurator;
import com.baidu.bifromq.basekv.metaservice.IBaseKVMetaService;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.basekv.store.proto.KVRangeROReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.baserpc.server.IRPCServer;
import com.baidu.bifromq.baserpc.server.RPCServerBuilder;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import com.baidu.bifromq.metrics.TenantMetric;
import com.baidu.bifromq.retain.rpc.proto.BatchMatchRequest;
import com.baidu.bifromq.retain.rpc.proto.BatchRetainRequest;
import com.baidu.bifromq.retain.rpc.proto.GCReply;
import com.baidu.bifromq.retain.rpc.proto.GCRequest;
import com.baidu.bifromq.retain.rpc.proto.MatchParam;
import com.baidu.bifromq.retain.rpc.proto.MatchResult;
import com.baidu.bifromq.retain.rpc.proto.RetainMessage;
import com.baidu.bifromq.retain.rpc.proto.RetainParam;
import com.baidu.bifromq.retain.rpc.proto.RetainResult;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceROCoProcInput;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceROCoProcOutput;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceRWCoProcInput;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceRWCoProcOutput;
import com.baidu.bifromq.retain.store.balance.RangeBootstrapBalancerFactory;
import com.baidu.bifromq.retain.store.schema.KVSchemaUtil;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.TopicMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

@Slf4j
public class RetainStoreTest {
    private static final String DB_NAME = "testDB";
    private static final String DB_CHECKPOINT_DIR_NAME = "testDB_cp";

    private static final String DB_WAL_NAME = "testWAL";
    private final int tickerThreads = 2;
    protected SimpleMeterRegistry meterRegistry;
    protected IRPCServer rpcServer;
    protected IRetainStore testStore;
    protected IBaseKVStoreClient storeClient;
    private IAgentHost agentHost;
    private ICRDTService crdtService;
    private IRPCServiceTrafficService trafficService;
    private IBaseKVMetaService metaService;
    private ScheduledExecutorService bgTaskExecutor;
    private KVRangeStoreOptions options;
    private Path dbRootDir;
    private AutoCloseable closeable;

    @BeforeClass(alwaysRun = true)
    public void setup() throws IOException {
        closeable = MockitoAnnotations.openMocks(this);
        meterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(meterRegistry);
        dbRootDir = Files.createTempDirectory("");

        AgentHostOptions agentHostOpts =
            AgentHostOptions.builder().addr("127.0.0.1").baseProbeInterval(Duration.ofSeconds(10)).joinRetryInSec(5)
                .joinTimeout(Duration.ofMinutes(5)).build();
        agentHost = IAgentHost.newInstance(agentHostOpts);

        crdtService = ICRDTService.newInstance(agentHost, CRDTServiceOptions.builder().build());

        trafficService = IRPCServiceTrafficService.newInstance(crdtService);
        metaService = IBaseKVMetaService.newInstance(crdtService);

        String uuid = UUID.randomUUID().toString();
        options = new KVRangeStoreOptions();
        ((RocksDBCPableKVEngineConfigurator) options.getDataEngineConfigurator()).dbCheckpointRootDir(
                Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR_NAME, uuid).toString())
            .dbRootDir(Paths.get(dbRootDir.toString(), DB_NAME, uuid).toString());
        ((RocksDBWALableKVEngineConfigurator) options.getWalEngineConfigurator()).dbRootDir(
            Paths.get(dbRootDir.toString(), DB_WAL_NAME, uuid).toString());
        bgTaskExecutor = new ScheduledThreadPoolExecutor(1, EnvProvider.INSTANCE.newThreadFactory("bg-task-executor"));

        storeClient =
            IBaseKVStoreClient.newBuilder().clusterId(IRetainStore.CLUSTER_NAME).trafficService(trafficService)
                .metaService(metaService).build();
        buildStoreServer();
        rpcServer.start();
        storeClient.join();
        log.info("Setup finished, and start testing");
    }

    private void buildStoreServer() {
        RPCServerBuilder rpcServerBuilder = IRPCServer.newBuilder().host("127.0.0.1").trafficService(trafficService);
        testStore =
            IRetainStore.builder().rpcServerBuilder(rpcServerBuilder).agentHost(agentHost).metaService(metaService)
                .retainStoreClient(storeClient).storeOptions(options).tickerThreads(tickerThreads)
                .bgTaskExecutor(bgTaskExecutor).gcInterval(Duration.ofSeconds(60)).balancerFactoryConfig(
                    Map.of(RangeBootstrapBalancerFactory.class.getName(), Struct.getDefaultInstance())).build();
        rpcServer = rpcServerBuilder.build();
    }

    protected void restartStoreServer() {
        log.info("Restarting test store server");
        rpcServer.shutdown();
        testStore.close();
        buildStoreServer();
        rpcServer.start();
        log.info("Test store server restarted");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() throws Exception {
        log.info("Finish testing, and tearing down");
        storeClient.close();
        rpcServer.shutdown();
        testStore.close();
        trafficService.close();
        metaService.close();
        crdtService.close();
        agentHost.close();
        try {
            Files.walk(dbRootDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        } catch (IOException e) {
            log.error("Failed to delete db root dir", e);
        }
        bgTaskExecutor.shutdown();
        closeable.close();
    }


    protected RetainResult.Code requestRetain(String tenantId, TopicMessage topicMsg) {
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString tenantNS = KVSchemaUtil.tenantBeginKey(tenantId);
        KVRangeSetting s = findByKey(tenantNS, storeClient.latestEffectiveRouter()).get();
        String topic = topicMsg.getTopic();
        Message message = topicMsg.getMessage();
        BatchRetainRequest request = BatchRetainRequest.newBuilder().setReqId(message.getMessageId())
            .putParams(tenantId, RetainParam.newBuilder().putTopicMessages(topic,
                RetainMessage.newBuilder().setMessage(message).setPublisher(topicMsg.getPublisher()).build()).build())
            .build();
        RetainServiceRWCoProcInput input = buildRetainRequest(request);
        KVRangeRWReply reply = storeClient.execute(s.leader,
            KVRangeRWRequest.newBuilder().setReqId(reqId).setVer(s.ver).setKvRangeId(s.id)
                .setRwCoProc(RWCoProcInput.newBuilder().setRetainService(input).build()).build()).join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        RetainServiceRWCoProcOutput output = reply.getRwCoProcResult().getRetainService();
        assertTrue(output.hasBatchRetain());
        assertEquals(output.getBatchRetain().getReqId(), message.getMessageId());
        return output.getBatchRetain().getResultsMap().get(tenantId).getResultsMap().get(topic);
    }

    protected MatchResult requestMatch(String tenantId, String topicFilter, int limit) {
        return requestMatch(tenantId, HLC.INST.getPhysical(), topicFilter, limit);
    }

    protected MatchResult requestMatch(String tenantId, long now, String topicFilter, int limit) {
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString tenantNS = KVSchemaUtil.tenantBeginKey(tenantId);
        KVRangeSetting s = findByKey(tenantNS, storeClient.latestEffectiveRouter()).get();
        BatchMatchRequest request = BatchMatchRequest.newBuilder().setReqId(reqId)
            .putMatchParams(tenantId, MatchParam.newBuilder().setNow(now).putTopicFilters(topicFilter, limit).build())
            .build();
        RetainServiceROCoProcInput input = buildMatchRequest(request);
        KVRangeROReply reply = storeClient.query(s.leader,
            KVRangeRORequest.newBuilder().setReqId(reqId).setVer(s.ver).setKvRangeId(s.id)
                .setRoCoProc(ROCoProcInput.newBuilder().setRetainService(input).build()).build()).join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        RetainServiceROCoProcOutput output = reply.getRoCoProcResult().getRetainService();
        assertTrue(output.hasBatchMatch());
        assertEquals(output.getBatchMatch().getReqId(), reqId);
        return output.getBatchMatch().getResultPackMap().get(tenantId).getResultsMap().get(topicFilter);
    }

    protected GCReply requestGC(long now, String tenantId, Integer expirySeconds) {
        long reqId = ThreadLocalRandom.current().nextInt();
        KVRangeSetting s = findByBoundary(FULL_BOUNDARY, storeClient.latestEffectiveRouter()).get(0);
        RetainServiceRWCoProcInput input = buildGCRequest(reqId, now, tenantId, expirySeconds);
        KVRangeRWReply reply = storeClient.execute(s.leader,
            KVRangeRWRequest.newBuilder().setReqId(reqId).setVer(s.ver).setKvRangeId(s.id)
                .setRwCoProc(RWCoProcInput.newBuilder().setRetainService(input).build()).build()).join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        RetainServiceRWCoProcOutput output = reply.getRwCoProcResult().getRetainService();
        assertTrue(output.hasGc());
        assertEquals(output.getGc().getReqId(), reqId);
        return output.getGc();
    }

    protected Gauge getSpaceUsageGauge(String tenantId) {
        return getGauge(tenantId, MqttRetainSpaceGauge);
    }

    protected Gauge getRetainCountGauge(String tenantId) {
        return getGauge(tenantId, MqttRetainNumGauge);
    }

    protected void assertNoGauge(String tenantId, TenantMetric gaugeMetric) {
        await().until(() -> {
            boolean found = false;
            for (Meter meter : meterRegistry.getMeters()) {
                if (meter.getId().getType() == Meter.Type.GAUGE &&
                    meter.getId().getName().equals(gaugeMetric.metricName) &&
                    Objects.equals(meter.getId().getTag("tenantId"), tenantId)) {
                    found = true;
                }
            }
            return !found;
        });
    }

    protected Gauge getGauge(String tenantId, TenantMetric gaugeMetric) {
        AtomicReference<Gauge> holder = new AtomicReference<>();
        await().until(() -> {
            for (Meter meter : meterRegistry.getMeters()) {
                if (meter.getId().getType() == Meter.Type.GAUGE &&
                    meter.getId().getName().equals(gaugeMetric.metricName) &&
                    Objects.equals(meter.getId().getTag("tenantId"), tenantId)) {
                    holder.set((Gauge) meter);
                    break;
                }
            }
            return holder.get() != null;
        });
        return holder.get();
    }

    protected TopicMessage message(String topic, String payload) {
        return message(topic, payload, HLC.INST.getPhysical(), Integer.MAX_VALUE);
    }

    protected TopicMessage message(String topic, String payload, long timestamp, int expirySeconds) {
        return TopicMessage.newBuilder().setTopic(topic).setMessage(
                Message.newBuilder().setMessageId(System.nanoTime()).setPayload(ByteString.copyFromUtf8(payload))
                    .setTimestamp(timestamp).setExpiryInterval(expirySeconds).build())
            .setPublisher(ClientInfo.getDefaultInstance()).build();
    }

    private RetainServiceRWCoProcInput buildGCRequest(long reqId, long now, String tenantId, Integer expirySeconds) {
        GCRequest.Builder reqBuilder = GCRequest.newBuilder().setReqId(reqId).setNow(now);
        if (tenantId != null) {
            reqBuilder.setTenantId(tenantId);
        }
        if (expirySeconds != null) {
            reqBuilder.setExpirySeconds(expirySeconds);
        }
        return RetainServiceRWCoProcInput.newBuilder().setGc(reqBuilder.build()).build();
    }

    private RetainServiceRWCoProcInput buildRetainRequest(BatchRetainRequest request) {
        return RetainServiceRWCoProcInput.newBuilder().setBatchRetain(request).build();
    }

    private RetainServiceROCoProcInput buildMatchRequest(BatchMatchRequest request) {
        return RetainServiceROCoProcInput.newBuilder().setBatchMatch(request).build();
    }
}
