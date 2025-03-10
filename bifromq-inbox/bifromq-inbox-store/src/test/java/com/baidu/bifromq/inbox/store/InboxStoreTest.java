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

package com.baidu.bifromq.inbox.store;

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.baseenv.EnvProvider;
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
import com.baidu.bifromq.baserpc.client.IConnectable;
import com.baidu.bifromq.baserpc.server.IRPCServer;
import com.baidu.bifromq.baserpc.server.RPCServerBuilder;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.storage.proto.BatchAttachReply;
import com.baidu.bifromq.inbox.storage.proto.BatchAttachRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCommitReply;
import com.baidu.bifromq.inbox.storage.proto.BatchCommitRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchDeleteReply;
import com.baidu.bifromq.inbox.storage.proto.BatchDeleteRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchDetachReply;
import com.baidu.bifromq.inbox.storage.proto.BatchDetachRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchFetchRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchGetReply;
import com.baidu.bifromq.inbox.storage.proto.BatchGetRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchInsertReply;
import com.baidu.bifromq.inbox.storage.proto.BatchInsertRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchSubReply;
import com.baidu.bifromq.inbox.storage.proto.BatchSubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchReply;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubReply;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubRequest;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.GCReply;
import com.baidu.bifromq.inbox.storage.proto.GCRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.InboxSubMessagePack;
import com.baidu.bifromq.inbox.store.balance.RangeBootstrapBalancerFactory;
import com.baidu.bifromq.metrics.TenantMetric;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import static com.baidu.bifromq.basekv.client.KVRangeRouterUtil.findByBoundary;
import static com.baidu.bifromq.basekv.client.KVRangeRouterUtil.findByKey;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static com.baidu.bifromq.inbox.store.schema.KVSchemaUtil.inboxStartKeyPrefix;
import static com.baidu.bifromq.metrics.TenantMetric.MqttPersistentSessionNumGauge;
import static com.baidu.bifromq.metrics.TenantMetric.MqttPersistentSessionSpaceGauge;
import static com.baidu.bifromq.metrics.TenantMetric.MqttPersistentSubCountGauge;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Slf4j
abstract class InboxStoreTest {
    private static final String DB_NAME = "testDB";
    private static final String DB_CHECKPOINT_DIR_NAME = "testDB_cp";
    private static final String DB_WAL_NAME = "testWAL";
    private final int tickerThreads = 2;
    public Path dbRootDir;
    @Mock
    protected IInboxClient inboxClient;
    @Mock
    protected ISettingProvider settingProvider;
    @Mock
    protected IEventCollector eventCollector;
    protected SimpleMeterRegistry meterRegistry;
    protected IBaseKVStoreClient storeClient;
    protected IInboxStore testStore;
    private IAgentHost agentHost;
    private ICRDTService crdtService;
    private IRPCServiceTrafficService trafficService;
    private IRPCServer rpcServer;
    private IBaseKVMetaService metaService;
    private ExecutorService queryExecutor;
    private ScheduledExecutorService bgTaskExecutor;
    private KVRangeStoreOptions options;
    private AutoCloseable closeable;

    @BeforeClass(groups = "integration")
    public void setup() throws IOException {
        closeable = MockitoAnnotations.openMocks(this);
        dbRootDir = Files.createTempDirectory("");
        when(settingProvider.provide(any(Setting.class), anyString())).thenAnswer(
            invocation -> ((Setting) invocation.getArgument(0)).current(invocation.getArgument(1)));
        meterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(meterRegistry);
        AgentHostOptions agentHostOpts = AgentHostOptions.builder()
            .addr("127.0.0.1")
            .baseProbeInterval(Duration.ofSeconds(10))
            .joinRetryInSec(5)
            .joinTimeout(Duration.ofMinutes(5))
            .build();
        agentHost = IAgentHost.newInstance(agentHostOpts);

        crdtService = ICRDTService.newInstance(agentHost, CRDTServiceOptions.builder().build());

        trafficService = IRPCServiceTrafficService.newInstance(crdtService);
        metaService = IBaseKVMetaService.newInstance(crdtService);

        String uuid = UUID.randomUUID().toString();
        options = new KVRangeStoreOptions();
        ((RocksDBCPableKVEngineConfigurator) options.getDataEngineConfigurator())
            .dbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR_NAME, uuid)
                .toString())
            .dbRootDir(Paths.get(dbRootDir.toString(), DB_NAME, uuid).toString());
        ((RocksDBWALableKVEngineConfigurator) options.getWalEngineConfigurator())
            .dbRootDir(Paths.get(dbRootDir.toString(), DB_WAL_NAME, uuid).toString());
        queryExecutor = new ThreadPoolExecutor(2, 2, 0L,
            TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
            EnvProvider.INSTANCE.newThreadFactory("query-executor"));
        bgTaskExecutor = new ScheduledThreadPoolExecutor(1,
            EnvProvider.INSTANCE.newThreadFactory("bg-task-executor"));

        storeClient = IBaseKVStoreClient
            .newBuilder()
            .clusterId(IInboxStore.CLUSTER_NAME)
            .trafficService(trafficService)
            .metaService(metaService)
            .build();
        buildStoreServer();
        rpcServer.start();

        storeClient.connState().filter(connState -> connState == IConnectable.ConnState.READY).blockingFirst();
        storeClient.join();
        log.info("Setup finished, and start testing");
    }

    private void buildStoreServer() {
        RPCServerBuilder rpcServerBuilder = IRPCServer.newBuilder().host("127.0.0.1").trafficService(trafficService);
        testStore = IInboxStore.builder()
            .rpcServerBuilder(rpcServerBuilder)
            .agentHost(agentHost)
            .metaService(metaService)
            .inboxClient(inboxClient)
            .inboxStoreClient(storeClient)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .storeOptions(options)
            .tickerThreads(tickerThreads)
            .balancerFactoryConfig(Map.of(RangeBootstrapBalancerFactory.class.getName(), Struct.getDefaultInstance()))
            .bgTaskExecutor(bgTaskExecutor)
            .gcInterval(Duration.ofSeconds(1))
            .build();
        rpcServer = rpcServerBuilder.build();
    }

    protected void restartStoreServer() {
        rpcServer.shutdown();
        testStore.close();
        buildStoreServer();
        rpcServer.start();
    }

    @AfterClass(groups = "integration")
    public void tearDown() throws Exception {
        log.info("Finish testing, and tearing down");
        inboxClient.close();
        storeClient.close();
        testStore.close();
        rpcServer.shutdown();
        trafficService.close();
        metaService.close();
        crdtService.close();
        agentHost.close();
        try {
            Files.walk(dbRootDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        } catch (IOException e) {
            log.error("Failed to delete db root dir", e);
        }
        queryExecutor.shutdown();
        bgTaskExecutor.shutdown();
        closeable.close();
    }

    @BeforeMethod(alwaysRun = true)
    public void beforeCastStart(Method method) {
        log.info("Test case[{}.{}] start", method.getDeclaringClass().getName(), method.getName());
        reset(eventCollector);
    }

    @AfterMethod(alwaysRun = true)
    public void afterCaseFinish(Method method) {
        log.info("Test case[{}.{}] finished, doing teardown",
            method.getDeclaringClass().getName(), method.getName());
    }

    private InboxServiceROCoProcOutput query(ByteString routeKey, InboxServiceROCoProcInput input) {
        KVRangeSetting s = findByKey(routeKey, storeClient.latestEffectiveRouter()).get();
        return query(s, input);
    }

    private InboxServiceROCoProcOutput query(KVRangeSetting s, InboxServiceROCoProcInput input) {
        KVRangeROReply reply = storeClient.query(s.leader, KVRangeRORequest.newBuilder()
            .setReqId(input.getReqId())
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRoCoProc(ROCoProcInput.newBuilder().setInboxService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), input.getReqId());
        assertEquals(reply.getCode(), ReplyCode.Ok);
        return reply.getRoCoProcResult().getInboxService();
    }


    private InboxServiceRWCoProcOutput mutate(ByteString routeKey, InboxServiceRWCoProcInput input) {
        KVRangeSetting s = findByKey(routeKey, storeClient.latestEffectiveRouter()).get();
        KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
            .setReqId(input.getReqId())
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRwCoProc(RWCoProcInput.newBuilder().setInboxService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), input.getReqId());
        assertEquals(reply.getCode(), ReplyCode.Ok);
        return reply.getRwCoProcResult().getInboxService();
    }

    protected GCReply requestGCScan(GCRequest request) {
        long reqId = ThreadLocalRandom.current().nextInt();
        InboxServiceROCoProcInput input = InboxServiceROCoProcInput.newBuilder()
            .setReqId(reqId)
            .setGc(request)
            .build();
        List<KVRangeSetting> rangeSettings = findByBoundary(FULL_BOUNDARY, storeClient.latestEffectiveRouter());
        assert !rangeSettings.isEmpty();
        InboxServiceROCoProcOutput output = query(rangeSettings.get(0), input);
        assertTrue(output.hasGc());
        assertEquals(output.getReqId(), reqId);
        return output.getGc();
    }

    protected List<BatchGetReply.Result> requestGet(BatchGetRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxStartKeyPrefix(params[0].getTenantId(), params[0].getInboxId());
        InboxServiceROCoProcInput input = MessageUtil.buildGetRequest(reqId, BatchGetRequest.newBuilder()
            .addAllParams(List.of(params))
            .build());
        InboxServiceROCoProcOutput output = query(routeKey, input);
        assertTrue(output.hasBatchGet());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchGet().getResultList();
    }

    protected List<Fetched> requestFetch(BatchFetchRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxStartKeyPrefix(params[0].getTenantId(), params[0].getInboxId());
        InboxServiceROCoProcInput input = MessageUtil.buildFetchRequest(reqId, BatchFetchRequest.newBuilder()
            .addAllParams(List.of(params))
            .build());
        InboxServiceROCoProcOutput output = query(routeKey, input);
        assertTrue(output.hasBatchFetch());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchFetch().getResultList();
    }

    protected List<BatchAttachReply.Result> requestAttach(BatchAttachRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxStartKeyPrefix(params[0].getClient().getTenantId(), params[0].getInboxId());
        InboxServiceRWCoProcInput input = MessageUtil.buildAttachRequest(reqId,
            BatchAttachRequest.newBuilder().addAllParams(List.of(params)).build());
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchAttach());
        assertEquals(output.getReqId(), reqId);
        assertEquals(params.length, output.getBatchAttach().getResultCount());
        return output.getBatchAttach().getResultList();
    }

    protected List<BatchDetachReply.Result> requestDetach(BatchDetachRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxStartKeyPrefix(params[0].getTenantId(), params[0].getInboxId());
        InboxServiceRWCoProcInput input = MessageUtil.buildDetachRequest(reqId,
            BatchDetachRequest.newBuilder().addAllParams(List.of(params)).build());
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchDetach());
        assertEquals(output.getReqId(), reqId);
        assertEquals(params.length, output.getBatchDetach().getResultCount());
        return output.getBatchDetach().getResultList();
    }

    protected List<Boolean> requestCreate(BatchCreateRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxStartKeyPrefix(params[0].getClient().getTenantId(), params[0].getInboxId());
        InboxServiceRWCoProcInput input = MessageUtil.buildCreateRequest(reqId,
            BatchCreateRequest.newBuilder().addAllParams(List.of(params)).build());
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchCreate());
        assertEquals(output.getReqId(), reqId);
        assertEquals(params.length, output.getBatchCreate().getSucceedCount());
        return output.getBatchCreate().getSucceedList();
    }

    protected List<BatchDeleteReply.Result> requestDelete(BatchDeleteRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxStartKeyPrefix(params[0].getTenantId(), params[0].getInboxId());

        InboxServiceRWCoProcInput input = MessageUtil.buildDeleteRequest(reqId, BatchDeleteRequest.newBuilder()
            .addAllParams(List.of(params)).build());
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchDelete());
        assertEquals(output.getReqId(), reqId);
        assertEquals(params.length, output.getBatchDelete().getResultCount());
        return output.getBatchDelete().getResultList();
    }

    protected List<BatchSubReply.Code> requestSub(BatchSubRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxStartKeyPrefix(params[0].getTenantId(), params[0].getInboxId());
        InboxServiceRWCoProcInput input = MessageUtil
            .buildSubRequest(reqId, BatchSubRequest.newBuilder().addAllParams(List.of(params)).build());
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchSub());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchSub().getCodeList();
    }

    protected List<BatchUnsubReply.Result> requestUnsub(BatchUnsubRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxStartKeyPrefix(params[0].getTenantId(), params[0].getInboxId());
        InboxServiceRWCoProcInput input = MessageUtil.buildUnsubRequest(reqId, BatchUnsubRequest.newBuilder()
            .addAllParams(List.of(params))
            .build());
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchUnsub());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchUnsub().getResultList();
    }

    protected List<BatchTouchReply.Code> requestTouch(BatchTouchRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxStartKeyPrefix(params[0].getTenantId(), params[0].getInboxId());
        InboxServiceRWCoProcInput input = MessageUtil.buildTouchRequest(reqId, BatchTouchRequest.newBuilder()
            .addAllParams(List.of(params))
            .build());
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchTouch());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchTouch().getCodeList();
    }

    protected List<BatchInsertReply.Result> requestInsert(InboxSubMessagePack... inboxSubMessagePack) {
        assert inboxSubMessagePack.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey =
            inboxStartKeyPrefix(inboxSubMessagePack[0].getTenantId(), inboxSubMessagePack[0].getInboxId());
        InboxServiceRWCoProcInput input = MessageUtil.buildInsertRequest(reqId, BatchInsertRequest.newBuilder()
            .addAllInboxSubMsgPack(List.of(inboxSubMessagePack))
            .build());
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchInsert());
        assertEquals(output.getReqId(), reqId);
        assertEquals(inboxSubMessagePack.length, output.getBatchInsert().getResultCount());
        return output.getBatchInsert().getResultList();
    }

    protected List<BatchCommitReply.Code> requestCommit(BatchCommitRequest.Params... params) {
        assert params.length > 0;
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxStartKeyPrefix(params[0].getTenantId(), params[0].getInboxId());

        InboxServiceRWCoProcInput input = MessageUtil.buildCommitRequest(reqId, BatchCommitRequest.newBuilder()
            .addAllParams(List.of(params))
            .build());

        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchCommit());
        assertEquals(output.getReqId(), reqId);
        assertEquals(params.length, output.getBatchCommit().getCodeCount());
        return output.getBatchCommit().getCodeList();
    }

    protected Gauge getPSessionGauge(String tenantId) {
        return getGauge(tenantId, MqttPersistentSessionNumGauge);
    }

    protected Gauge getPSessionSpaceGauge(String tenantId) {
        return getGauge(tenantId, MqttPersistentSessionSpaceGauge);
    }

    protected Gauge getSubCountGauge(String tenantId) {
        return getGauge(tenantId, MqttPersistentSubCountGauge);
    }

    protected void assertNoGauge(String tenantId, TenantMetric gaugeMetric) {
        await().until(() -> {
            boolean found = false;
            for (Meter meter : meterRegistry.getMeters()) {
                if (meter.getId().getType() == Meter.Type.GAUGE
                    && meter.getId().getName().equals(gaugeMetric.metricName)
                    && Objects.equals(meter.getId().getTag("tenantId"), tenantId)) {
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
                if (meter.getId().getType() == Meter.Type.GAUGE
                    && meter.getId().getName().equals(gaugeMetric.metricName)
                    && Objects.equals(meter.getId().getTag("tenantId"), tenantId)) {
                    holder.set((Gauge) meter);
                    break;
                }
            }
            return holder.get() != null;
        });
        return holder.get();
    }

    protected TopicMessagePack.PublisherPack message(QoS qos, String payload) {
        return TopicMessagePack.PublisherPack.newBuilder()
            .addMessage(Message.newBuilder()
                .setMessageId(System.nanoTime())
                .setPubQoS(qos)
                .setPayload(ByteString.copyFromUtf8(payload))
                .build())
            .build();
    }

    protected TopicMessagePack.PublisherPack message(int messageId, QoS qos, String payload, ClientInfo publisher) {
        return TopicMessagePack.PublisherPack.newBuilder()
            .addMessage(Message.newBuilder()
                .setMessageId(messageId)
                .setPubQoS(qos)
                .setPayload(ByteString.copyFromUtf8(payload))
                .build())
            .setPublisher(publisher)
            .build();
    }
}
