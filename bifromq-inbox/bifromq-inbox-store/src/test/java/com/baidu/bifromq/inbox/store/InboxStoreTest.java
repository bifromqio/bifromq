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

package com.baidu.bifromq.inbox.store;

import static com.baidu.bifromq.inbox.util.KeyUtil.scopedTopicFilter;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.balance.option.KVRangeBalanceControllerOptions;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.localengine.memory.InMemKVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.rocksdb.RocksDBKVEngineConfigurator;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.basekv.store.proto.KVRangeROReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.baserpc.IConnectable;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.storage.proto.BatchCheckReply;
import com.baidu.bifromq.inbox.storage.proto.BatchCheckRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCommitReply;
import com.baidu.bifromq.inbox.storage.proto.BatchCommitRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateReply;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchFetchReply;
import com.baidu.bifromq.inbox.storage.proto.BatchFetchRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchInsertReply;
import com.baidu.bifromq.inbox.storage.proto.BatchInsertRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchSubReply;
import com.baidu.bifromq.inbox.storage.proto.BatchSubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchReply;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubReply;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubRequest;
import com.baidu.bifromq.inbox.storage.proto.CommitParams;
import com.baidu.bifromq.inbox.storage.proto.CreateParams;
import com.baidu.bifromq.inbox.storage.proto.FetchParams;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.MessagePack;
import com.baidu.bifromq.inbox.util.KeyUtil;
import com.baidu.bifromq.inbox.util.MessageUtil;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

@Slf4j
abstract class InboxStoreTest {
    private static final String DB_NAME = "testDB";
    private static final String DB_CHECKPOINT_DIR_NAME = "testDB_cp";
    private static final String DB_WAL_NAME = "testWAL";
    private static final String DB_WAL_CHECKPOINT_DIR = "testWAL_cp";

    protected SimpleMeterRegistry meterRegistry;

    @Mock
    protected IDistClient distClient;
    @Mock
    protected ISettingProvider settingProvider;
    @Mock
    protected IEventCollector eventCollector;
    private IAgentHost agentHost;
    private ICRDTService clientCrdtService;
    private ICRDTService serverCrdtService;
    private ExecutorService queryExecutor;
    private ScheduledExecutorService tickTaskExecutor;
    private ScheduledExecutorService bgTaskExecutor;
    public Path dbRootDir;

    protected IBaseKVStoreClient storeClient;
    protected StandaloneInboxStore testStore;

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
        agentHost.start();

        clientCrdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        clientCrdtService.start(agentHost);

        serverCrdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        serverCrdtService.start(agentHost);

        String uuid = UUID.randomUUID().toString();
        KVRangeStoreOptions options = new KVRangeStoreOptions();
        if (!runOnMac()) {
            options.setDataEngineConfigurator(new InMemKVEngineConfigurator());
            options.setWalEngineConfigurator(new InMemKVEngineConfigurator());
        } else {
            ((RocksDBKVEngineConfigurator) options.getDataEngineConfigurator())
                .setDbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR_NAME, uuid)
                    .toString())
                .setDbRootDir(Paths.get(dbRootDir.toString(), DB_NAME, uuid).toString());
            ((RocksDBKVEngineConfigurator) options.getWalEngineConfigurator())
                .setDbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_WAL_CHECKPOINT_DIR, uuid)
                    .toString())
                .setDbRootDir(Paths.get(dbRootDir.toString(), DB_WAL_NAME, uuid).toString());
        }
        KVRangeBalanceControllerOptions controllerOptions = new KVRangeBalanceControllerOptions();
        queryExecutor = new ThreadPoolExecutor(2, 2, 0L,
            TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
            EnvProvider.INSTANCE.newThreadFactory("query-executor"));
        tickTaskExecutor = new ScheduledThreadPoolExecutor(2,
            EnvProvider.INSTANCE.newThreadFactory("tick-task-executor"));
        bgTaskExecutor = new ScheduledThreadPoolExecutor(1,
            EnvProvider.INSTANCE.newThreadFactory("bg-task-executor"));

        storeClient = IBaseKVStoreClient
            .newBuilder()
            .clusterId(IInboxStore.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .build();
        testStore = (StandaloneInboxStore) IInboxStore.standaloneBuilder()
            .bootstrap(true)
            .host("127.0.0.1")
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .distClient(distClient)
            .storeClient(storeClient)
            .settingProvider(settingProvider)
            .eventCollector(eventCollector)
            .purgeDelay(Duration.ZERO)
            .clock(getClock())
            .storeOptions(options)
            .balanceControllerOptions(controllerOptions)
            .queryExecutor(queryExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .gcInterval(Duration.ofSeconds(1))
            .statsInterval(Duration.ofSeconds(1))
            .build();
        testStore.start();

        storeClient.connState().filter(connState -> connState == IConnectable.ConnState.READY).blockingFirst();
        storeClient.join();
        log.info("Setup finished, and start testing");
    }

    @AfterClass(groups = "integration")
    public void teardown() throws Exception {
        log.info("Finish testing, and tearing down");
        testStore.stop();
        new Thread(() -> {
            clientCrdtService.stop();
            serverCrdtService.stop();
            agentHost.shutdown();
            try {
                Files.walk(dbRootDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            } catch (IOException e) {
                log.error("Failed to delete db root dir", e);
            }
            queryExecutor.shutdown();
            tickTaskExecutor.shutdown();
            bgTaskExecutor.shutdown();
        }).start();
        closeable.close();
    }

    protected Clock getClock() {
        return Clock.systemUTC();
    }

    private static boolean runOnMac() {
        String osName = System.getProperty("os.name");
        return osName != null && osName.startsWith("Mac");
    }

    protected BatchCreateReply requestCreate(String tenantId, String inboxId,
                                             int limit, int expireSeconds, boolean dropOldest) {
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString scopedInboxId = KeyUtil.scopedInboxId(tenantId, inboxId);
        KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
        BatchCreateRequest request = BatchCreateRequest.newBuilder()
            .putInboxes(scopedInboxId.toStringUtf8(), CreateParams.newBuilder()
                .setExpireSeconds(expireSeconds)
                .setLimit(limit)
                .setDropOldest(dropOldest)
                .setClient(ClientInfo.newBuilder().setTenantId(tenantId).build())
                .build())
            .build();
        InboxServiceRWCoProcInput input = MessageUtil.buildCreateRequest(reqId, request);
        KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
            .setReqId(reqId)
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRwCoProc(RWCoProcInput.newBuilder().setInboxService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        InboxServiceRWCoProcOutput output = reply.getRwCoProcResult().getInboxService();
        assertTrue(output.hasBatchCreate());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchCreate();
    }

    protected BatchSubReply.Result requestSub(String tenantId, String inboxId, String topicFilter, QoS subQoS) {
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString scopedInboxId = KeyUtil.scopedInboxId(tenantId, inboxId);
        KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
        String scopedTopicFilter = scopedTopicFilter(tenantId, inboxId, topicFilter).toStringUtf8();
        BatchSubRequest request = BatchSubRequest.newBuilder()
            .setReqId(reqId)
            .putTopicFilters(scopedTopicFilter, subQoS)
            .build();
        InboxServiceRWCoProcInput input = MessageUtil.buildBatchSubRequest(reqId, request);
        KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
            .setReqId(reqId)
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRwCoProc(RWCoProcInput.newBuilder().setInboxService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        InboxServiceRWCoProcOutput output = reply.getRwCoProcResult().getInboxService();
        assertTrue(output.hasBatchSub());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchSub().getResultsMap().get(scopedTopicFilter);
    }

    protected BatchUnsubReply.Result requestUnsub(String tenantId, String inboxId, String topicFilter) {
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString scopedInboxId = KeyUtil.scopedInboxId(tenantId, inboxId);
        KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
        ByteString scopedTopicFilter = scopedTopicFilter(tenantId, inboxId, topicFilter);
        BatchUnsubRequest request = BatchUnsubRequest.newBuilder()
            .setReqId(reqId)
            .addTopicFilters(scopedTopicFilter)
            .build();
        InboxServiceRWCoProcInput input = MessageUtil.buildBatchUnsubRequest(reqId, request);
        KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
            .setReqId(reqId)
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRwCoProc(RWCoProcInput.newBuilder().setInboxService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        InboxServiceRWCoProcOutput output = reply.getRwCoProcResult().getInboxService();
        assertTrue(output.hasBatchUnsub());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchUnsub().getResultsMap().get(scopedTopicFilter.toStringUtf8());
    }


    protected BatchCheckReply requestHas(String tenantId, String inboxId) {
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString scopedInboxId = KeyUtil.scopedInboxId(tenantId, inboxId);
        KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
        BatchCheckRequest request = BatchCheckRequest.newBuilder().addScopedInboxId(scopedInboxId).build();
        InboxServiceROCoProcInput input = MessageUtil.buildHasRequest(reqId, request);
        KVRangeROReply reply = storeClient.linearizedQuery(s.leader, KVRangeRORequest.newBuilder()
            .setReqId(reqId)
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRoCoProc(ROCoProcInput.newBuilder().setInboxService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        InboxServiceROCoProcOutput output = reply.getRoCoProcResult().getInboxService();
        assertTrue(output.hasBatchCheck());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchCheck();
    }

    protected BatchTouchReply requestDelete(String tenantId, String inboxId) {
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString scopedInboxId = KeyUtil.scopedInboxId(tenantId, inboxId);
        KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
        BatchTouchRequest request = BatchTouchRequest.newBuilder()
            .putScopedInboxId(scopedInboxId.toStringUtf8(), false)
            .build();
        InboxServiceRWCoProcInput input = MessageUtil.buildTouchRequest(reqId, request);
        KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
            .setReqId(reqId)
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRwCoProc(RWCoProcInput.newBuilder().setInboxService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        InboxServiceRWCoProcOutput output = reply.getRwCoProcResult().getInboxService();
        assertTrue(output.hasBatchTouch());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchTouch();
    }

    protected BatchInsertReply requestInsert(SubInfo subInfo, String topic,
                                             TopicMessagePack.PublisherPack... messages) {
        return requestInsert(subInfo.getTenantId(), subInfo.getInboxId(), MessagePack.newBuilder()
            .setSubInfo(subInfo)
            .addMessages(TopicMessagePack.newBuilder()
                .setTopic(topic)
                .addAllMessage(Arrays.stream(messages).collect(Collectors.toList()))
                .build())
            .build());
    }

    protected BatchInsertReply requestInsert(String tenantId, String inboxId, MessagePack... subMsgPacks) {
        BatchInsertRequest request = BatchInsertRequest.newBuilder()
            .addAllSubMsgPack(Arrays.stream(subMsgPacks).collect(Collectors.toList()))
            .build();
        ByteString scopedInboxId = KeyUtil.scopedInboxId(tenantId, inboxId);
        long reqId = ThreadLocalRandom.current().nextInt();
        KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
        InboxServiceRWCoProcInput input = MessageUtil.buildBatchInboxInsertRequest(reqId, request);
        KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
            .setReqId(reqId)
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRwCoProc(RWCoProcInput.newBuilder().setInboxService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        InboxServiceRWCoProcOutput output = reply.getRwCoProcResult().getInboxService();
        assertTrue(output.hasBatchInsert());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchInsert();
    }

    protected BatchFetchReply requestFetchQoS0(String tenantId,
                                               String inboxId,
                                               int maxFetch,
                                               Long startAfterSeq) {
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString scopedInboxId = KeyUtil.scopedInboxId(tenantId, inboxId);
        KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
        BatchFetchRequest.Builder request = BatchFetchRequest.newBuilder()
            .putInboxFetch(scopedInboxId.toStringUtf8(), FetchParams.newBuilder()
                .setMaxFetch(maxFetch)
                .build());
        if (startAfterSeq != null) {
            request.putInboxFetch(scopedInboxId.toStringUtf8(), FetchParams.newBuilder()
                .setMaxFetch(maxFetch)
                .setQos0StartAfter(startAfterSeq)
                .build());
        }
        InboxServiceROCoProcInput input = MessageUtil.buildInboxFetchRequest(reqId, request.build());
        KVRangeROReply reply = storeClient.linearizedQuery(s.leader, KVRangeRORequest.newBuilder()
            .setReqId(reqId)
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRoCoProc(ROCoProcInput.newBuilder().setInboxService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        InboxServiceROCoProcOutput output = reply.getRoCoProcResult().getInboxService();
        assertTrue(output.hasBatchFetch());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchFetch();
    }

    protected BatchFetchReply requestFetchQoS0(String tenantId, String inboxId, int maxFetch) {
        return requestFetchQoS0(tenantId, inboxId, maxFetch, null);
    }

    protected BatchFetchReply requestFetchQoS1(String tenantId, String inboxId, int maxFetch,
                                               Long startAfterSeq) {
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString scopedInboxId = KeyUtil.scopedInboxId(tenantId, inboxId);
        KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
        BatchFetchRequest.Builder request = BatchFetchRequest.newBuilder()
            .putInboxFetch(scopedInboxId.toStringUtf8(), FetchParams.newBuilder()
                .setMaxFetch(maxFetch)
                .build());
        if (startAfterSeq != null) {
            request.putInboxFetch(scopedInboxId.toStringUtf8(), FetchParams.newBuilder()
                .setMaxFetch(maxFetch)
                .setQos1StartAfter(startAfterSeq)
                .build());
        }
        InboxServiceROCoProcInput input = MessageUtil.buildInboxFetchRequest(reqId, request.build());
        KVRangeROReply reply = storeClient.linearizedQuery(s.leader, KVRangeRORequest.newBuilder()
            .setReqId(reqId)
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRoCoProc(ROCoProcInput.newBuilder().setInboxService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        InboxServiceROCoProcOutput output = reply.getRoCoProcResult().getInboxService();
        assertTrue(output.hasBatchFetch());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchFetch();
    }

    protected BatchFetchReply requestFetchQoS1(String tenantId, String inboxId, int maxFetch) {
        return requestFetchQoS1(tenantId, inboxId, maxFetch, null);
    }

    protected BatchFetchReply requestFetchQoS2(String tenantId, String inboxId, int maxFetch,
                                               Long startAfterSeq) {
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString scopedInboxId = KeyUtil.scopedInboxId(tenantId, inboxId);
        KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
        BatchFetchRequest.Builder request = BatchFetchRequest.newBuilder()
            .putInboxFetch(scopedInboxId.toStringUtf8(), FetchParams.newBuilder()
                .setMaxFetch(maxFetch)
                .build());
        if (startAfterSeq != null) {
            request.putInboxFetch(scopedInboxId.toStringUtf8(), FetchParams.newBuilder()
                .setMaxFetch(maxFetch)
                .setQos2StartAfter(startAfterSeq)
                .build());
        }
        InboxServiceROCoProcInput input = MessageUtil.buildInboxFetchRequest(reqId, request.build());
        KVRangeROReply reply = storeClient.linearizedQuery(s.leader, KVRangeRORequest.newBuilder()
            .setReqId(reqId)
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRoCoProc(ROCoProcInput.newBuilder().setInboxService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        InboxServiceROCoProcOutput output = reply.getRoCoProcResult().getInboxService();
        assertTrue(output.hasBatchFetch());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchFetch();
    }

    protected BatchTouchReply requestTouch(String tenantId, String inboxId) {
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString scopedInboxId = KeyUtil.scopedInboxId(tenantId, inboxId);
        KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
        InboxServiceRWCoProcInput input = MessageUtil.buildTouchRequest(reqId, BatchTouchRequest.newBuilder()
            .putScopedInboxId(scopedInboxId.toStringUtf8(), true)
            .build());
        KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
            .setReqId(reqId)
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRwCoProc(RWCoProcInput.newBuilder().setInboxService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        InboxServiceRWCoProcOutput output = reply.getRwCoProcResult().getInboxService();
        assertTrue(output.hasBatchTouch());
        assertEquals(output.getReqId(), reqId);
        return output.getBatchTouch();
    }

    protected BatchCommitReply requestCommitQoS0(String tenantId, String inboxId, long upToSeq) {
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString scopedInboxId = KeyUtil.scopedInboxId(tenantId, inboxId);
        KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
        InboxServiceRWCoProcInput input = MessageUtil.buildBatchCommitRequest(reqId, BatchCommitRequest.newBuilder()
            .putInboxCommit(scopedInboxId.toStringUtf8(), CommitParams.newBuilder()
                .setQos0UpToSeq(upToSeq).build())
            .build());

        KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(RWCoProcInput.newBuilder().setInboxService(input).build())
                .build())
            .join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        InboxServiceRWCoProcOutput output = reply.getRwCoProcResult().getInboxService();
        assertTrue(output.hasBatchCommit());
        assertEquals(output.getReqId(), reqId);
        assertTrue(output.getBatchCommit().getResultMap().get(scopedInboxId.toStringUtf8()));
        return output.getBatchCommit();
    }


    protected BatchCommitReply requestCommitQoS1(String tenantId, String inboxId, long upToSeq) {
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString scopedInboxId = KeyUtil.scopedInboxId(tenantId, inboxId);
        KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
        InboxServiceRWCoProcInput input = MessageUtil.buildBatchCommitRequest(reqId, BatchCommitRequest.newBuilder()
            .putInboxCommit(scopedInboxId.toStringUtf8(), CommitParams.newBuilder()
                .setQos1UpToSeq(upToSeq).build())
            .build());

        KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
            .setReqId(reqId)
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRwCoProc(RWCoProcInput.newBuilder().setInboxService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        InboxServiceRWCoProcOutput output = reply.getRwCoProcResult().getInboxService();
        assertTrue(output.hasBatchCommit());
        assertEquals(output.getReqId(), reqId);
        assertTrue(output.getBatchCommit().getResultMap().get(scopedInboxId.toStringUtf8()));
        return output.getBatchCommit();
    }

    protected BatchCommitReply requestCommitQoS2(String tenantId, String inboxId, long upToSeq) {
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString scopedInboxId = KeyUtil.scopedInboxId(tenantId, inboxId);
        KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
        InboxServiceRWCoProcInput input = MessageUtil.buildBatchCommitRequest(reqId, BatchCommitRequest.newBuilder()
            .putInboxCommit(scopedInboxId.toStringUtf8(), CommitParams.newBuilder()
                .setQos2UpToSeq(upToSeq)
                .build())
            .build());
        KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
            .setReqId(reqId)
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRwCoProc(RWCoProcInput.newBuilder().setInboxService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        InboxServiceRWCoProcOutput output = reply.getRwCoProcResult().getInboxService();
        assertTrue(output.hasBatchCommit());
        assertEquals(output.getReqId(), reqId);
        assertTrue(output.getBatchCommit().getResultMap().get(scopedInboxId.toStringUtf8()));
        return output.getBatchCommit();
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
