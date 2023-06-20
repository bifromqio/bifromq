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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.localengine.InMemoryKVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.RocksDBKVEngineConfigurator;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.basekv.store.proto.KVRangeROReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.inbox.storage.proto.CreateParams;
import com.baidu.bifromq.inbox.storage.proto.CreateReply;
import com.baidu.bifromq.inbox.storage.proto.CreateRequest;
import com.baidu.bifromq.inbox.storage.proto.FetchParams;
import com.baidu.bifromq.inbox.storage.proto.HasReply;
import com.baidu.bifromq.inbox.storage.proto.HasRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxCommit;
import com.baidu.bifromq.inbox.storage.proto.InboxCommitReply;
import com.baidu.bifromq.inbox.storage.proto.InboxCommitRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxFetchReply;
import com.baidu.bifromq.inbox.storage.proto.InboxFetchRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxInsertReply;
import com.baidu.bifromq.inbox.storage.proto.InboxInsertRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.MessagePack;
import com.baidu.bifromq.inbox.storage.proto.TouchReply;
import com.baidu.bifromq.inbox.storage.proto.TouchRequest;
import com.baidu.bifromq.inbox.util.KeyUtil;
import com.baidu.bifromq.inbox.util.MessageUtil;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

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
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.mockito.Mock;

@Slf4j
abstract class InboxStoreTest {
    private static final String DB_NAME = "testDB";
    private static final String DB_CHECKPOINT_DIR_NAME = "testDB_cp";

    private static final String DB_WAL_NAME = "testWAL";
    private static final String DB_WAL_CHECKPOINT_DIR = "testWAL_cp";
//    @Rule
//    public TestRule watcher = new TestWatcher() {
//        protected void starting(Description description) {
//            log.info("Starting test: " + description.getMethodName());
//        }
//    };

    @Mock
    protected IEventCollector eventCollector;
    private IAgentHost agentHost;
    private ICRDTService clientCrdtService;
    private ICRDTService serverCrdtService;
    private ExecutorService queryExecutor;
    private ExecutorService mutationExecutor;
    private ScheduledExecutorService tickTaskExecutor;
    private ScheduledExecutorService bgTaskExecutor;
    public Path dbRootDir;

    protected IBaseKVStoreClient storeClient;
    protected InboxStore testStore;

    private AutoCloseable closeable;
    @BeforeMethod
    public void setup() throws IOException {
        closeable = MockitoAnnotations.openMocks(this);
        dbRootDir = Files.createTempDirectory("");
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
            options.setDataEngineConfigurator(new InMemoryKVEngineConfigurator());
            options.setWalEngineConfigurator(new InMemoryKVEngineConfigurator());
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
        queryExecutor = new ThreadPoolExecutor(2, 2, 0L,
            TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("query-executor-%d").build());
        mutationExecutor = new ThreadPoolExecutor(2, 2, 0L,
            TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("mutation-executor-%d").build());
        tickTaskExecutor = new ScheduledThreadPoolExecutor(2,
            new ThreadFactoryBuilder().setNameFormat("tick-task-executor-%d").build());
        bgTaskExecutor = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryBuilder().setNameFormat("bg-task-executor-%d").build());

        storeClient = IBaseKVStoreClient
            .inProcClientBuilder()
            .clusterId(IInboxStore.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .build();
        testStore = (InboxStore) IInboxStore.
            inProcBuilder()
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .storeClient(storeClient)
            .eventCollector(eventCollector)
            .purgeDelay(Duration.ZERO)
            .clock(getClock())
            .kvRangeStoreOptions(options)
            .queryExecutor(queryExecutor)
            .mutationExecutor(mutationExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .build();
        testStore.start(true);

        storeClient.join();
        log.info("Setup finished, and start testing");
    }

    @AfterMethod
    public void teardown() throws Exception {
        log.info("Finish testing, and tearing down");
        storeClient.stop();
        testStore.stop();
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
        mutationExecutor.shutdown();
        tickTaskExecutor.shutdown();
        bgTaskExecutor.shutdown();
        closeable.close();
    }

    protected Clock getClock() {
        return Clock.systemUTC();
    }

    private static boolean runOnMac() {
        String osName = System.getProperty("os.name");
        return osName != null && osName.startsWith("Mac");
    }

    protected CreateReply requestCreate(String trafficId, String inboxId,
                                        int limit, int expireSeconds, boolean dropOldest) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString scopedInboxId = KeyUtil.scopedInboxId(trafficId, inboxId);
            KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
            CreateRequest request = CreateRequest.newBuilder()
                .putInboxes(scopedInboxId.toStringUtf8(), CreateParams.newBuilder()
                    .setExpireSeconds(expireSeconds)
                    .setLimit(limit)
                    .setDropOldest(dropOldest)
                    .build())
                .build();
            InboxServiceRWCoProcInput input = MessageUtil.buildCreateRequest(reqId, request);
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reqId, reply.getReqId());
            assertEquals(ReplyCode.Ok, reply.getCode());
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult());
            assertTrue(output.getReply().hasCreateInbox());
            assertEquals(reqId, output.getReply().getReqId());
            return output.getReply().getCreateInbox();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected HasReply requestHas(String trafficId, String inboxId) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString scopedInboxId = KeyUtil.scopedInboxId(trafficId, inboxId);
            KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
            HasRequest request = HasRequest.newBuilder().addScopedInboxId(scopedInboxId).build();
            InboxServiceROCoProcInput input = MessageUtil.buildHasRequest(reqId, request);
            KVRangeROReply reply = storeClient.linearizedQuery(s.leader, KVRangeRORequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRoCoProcInput(input.toByteString())
                .build()).join();
            assertEquals(reqId, reply.getReqId());
            assertEquals(ReplyCode.Ok, reply.getCode());
            InboxServiceROCoProcOutput output = InboxServiceROCoProcOutput.parseFrom(reply.getRoCoProcResult());
            assertTrue(output.getReply().hasHas());
            assertEquals(reqId, output.getReply().getReqId());
            return output.getReply().getHas();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected TouchReply requestDelete(String trafficId, String inboxId) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString scopedInboxId = KeyUtil.scopedInboxId(trafficId, inboxId);
            KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
            TouchRequest request = TouchRequest.newBuilder()
                .putScopedInboxId(scopedInboxId.toStringUtf8(), false)
                .build();
            InboxServiceRWCoProcInput input = MessageUtil.buildTouchRequest(reqId, request);
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reqId, reply.getReqId());
            assertEquals(ReplyCode.Ok, reply.getCode());
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult());
            assertTrue(output.getReply().hasTouch());
            assertEquals(reqId, output.getReply().getReqId());
            return output.getReply().getTouch();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected InboxInsertReply requestInsert(SubInfo subInfo, String topic,
                                             TopicMessagePack.SenderMessagePack... messages) {
        return requestInsert(subInfo.getTrafficId(), subInfo.getInboxId(), MessagePack.newBuilder()
            .setSubInfo(subInfo)
            .addMessages(TopicMessagePack.newBuilder()
                .setTopic(topic)
                .addAllMessage(Arrays.stream(messages).collect(Collectors.toList()))
                .build())
            .build());
    }

    protected InboxInsertReply requestInsert(String trafficId, String inboxId, MessagePack... subMsgPacks) {
        try {
            InboxInsertRequest request = InboxInsertRequest.newBuilder()
                .addAllSubMsgPack(Arrays.stream(subMsgPacks).collect(Collectors.toList()))
                .build();
            ByteString scopedInboxId = KeyUtil.scopedInboxId(trafficId, inboxId);
            long reqId = ThreadLocalRandom.current().nextInt();
            KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
            InboxServiceRWCoProcInput input = MessageUtil.buildBatchInboxInsertRequest(reqId, request);
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reqId, reply.getReqId());
            assertEquals(ReplyCode.Ok, reply.getCode());
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult());
            assertTrue(output.getReply().hasInsert());
            assertEquals(reqId, output.getReply().getReqId());
            return output.getReply().getInsert();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected InboxFetchReply requestFetchQoS0(String trafficId,
                                               String inboxId,
                                               int maxFetch,
                                               Long startAfterSeq) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString scopedInboxId = KeyUtil.scopedInboxId(trafficId, inboxId);
            KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
            InboxFetchRequest.Builder request = InboxFetchRequest.newBuilder()
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
                .setRoCoProcInput(input.toByteString())
                .build()).join();
            assertEquals(reqId, reply.getReqId());
            assertEquals(ReplyCode.Ok, reply.getCode());
            InboxServiceROCoProcOutput output = InboxServiceROCoProcOutput.parseFrom(reply.getRoCoProcResult());
            assertTrue(output.getReply().hasFetch());
            assertEquals(reqId, output.getReply().getReqId());
            return output.getReply().getFetch();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected InboxFetchReply requestFetchQoS0(String trafficId, String inboxId, int maxFetch) {
        return requestFetchQoS0(trafficId, inboxId, maxFetch, null);
    }

    protected InboxFetchReply requestFetchQoS1(String trafficId, String inboxId, int maxFetch,
                                               Long startAfterSeq) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString scopedInboxId = KeyUtil.scopedInboxId(trafficId, inboxId);
            KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
            InboxFetchRequest.Builder request = InboxFetchRequest.newBuilder()
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
                .setRoCoProcInput(input.toByteString())
                .build()).join();
            assertEquals(reqId, reply.getReqId());
            assertEquals(ReplyCode.Ok, reply.getCode());
            InboxServiceROCoProcOutput output = InboxServiceROCoProcOutput.parseFrom(reply.getRoCoProcResult());
            assertTrue(output.getReply().hasFetch());
            assertEquals(reqId, output.getReply().getReqId());
            return output.getReply().getFetch();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected InboxFetchReply requestFetchQoS1(String trafficId, String inboxId, int maxFetch) {
        return requestFetchQoS1(trafficId, inboxId, maxFetch, null);
    }

    protected InboxFetchReply requestFetchQoS2(String trafficId, String inboxId, int maxFetch,
                                               Long startAfterSeq) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString scopedInboxId = KeyUtil.scopedInboxId(trafficId, inboxId);
            KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
            InboxFetchRequest.Builder request = InboxFetchRequest.newBuilder()
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
                .setRoCoProcInput(input.toByteString())
                .build()).join();
            assertEquals(reqId, reply.getReqId());
            assertEquals(ReplyCode.Ok, reply.getCode());
            InboxServiceROCoProcOutput output = InboxServiceROCoProcOutput.parseFrom(reply.getRoCoProcResult());
            assertTrue(output.getReply().hasFetch());
            assertEquals(reqId, output.getReply().getReqId());
            return output.getReply().getFetch();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected TouchReply requestTouch(String trafficId, String inboxId) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString scopedInboxId = KeyUtil.scopedInboxId(trafficId, inboxId);
            KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
            InboxServiceRWCoProcInput input = MessageUtil.buildTouchRequest(reqId, TouchRequest.newBuilder()
                .putScopedInboxId(scopedInboxId.toStringUtf8(), true)
                .build());
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reqId, reply.getReqId());
            assertEquals(ReplyCode.Ok, reply.getCode());
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult());
            assertTrue(output.getReply().hasTouch());
            assertEquals(reqId, output.getReply().getReqId());
            return output.getReply().getTouch();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }

    }

    protected InboxCommitReply requestCommitQoS0(String trafficId, String inboxId, long upToSeq) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString scopedInboxId = KeyUtil.scopedInboxId(trafficId, inboxId);
            KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
            InboxServiceRWCoProcInput input = MessageUtil.buildBatchCommitRequest(reqId, InboxCommitRequest.newBuilder()
                .putInboxCommit(scopedInboxId.toStringUtf8(), InboxCommit.newBuilder()
                    .setQos0UpToSeq(upToSeq).build())
                .build());

            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reqId, reply.getReqId());
            assertEquals(ReplyCode.Ok, reply.getCode());
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(reply
                .getRwCoProcResult());
            assertTrue(output.getReply().hasCommit());
            assertEquals(reqId, output.getReply().getReqId());
            assertTrue(output.getReply().getCommit().getResultMap().get(scopedInboxId.toStringUtf8()));
            return output.getReply().getCommit();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }


    protected InboxCommitReply requestCommitQoS1(String trafficId, String inboxId, long upToSeq) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString scopedInboxId = KeyUtil.scopedInboxId(trafficId, inboxId);
            KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
            InboxServiceRWCoProcInput input = MessageUtil.buildBatchCommitRequest(reqId, InboxCommitRequest.newBuilder()
                .putInboxCommit(scopedInboxId.toStringUtf8(), InboxCommit.newBuilder()
                    .setQos1UpToSeq(upToSeq).build())
                .build());

            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reqId, reply.getReqId());
            assertEquals(ReplyCode.Ok, reply.getCode());
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult());
            assertTrue(output.getReply().hasCommit());
            assertEquals(reqId, output.getReply().getReqId());
            assertTrue(output.getReply().getCommit().getResultMap().get(scopedInboxId.toStringUtf8()));
            return output.getReply().getCommit();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected InboxCommitReply requestCommitQoS2(String trafficId, String inboxId, long upToSeq) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString scopedInboxId = KeyUtil.scopedInboxId(trafficId, inboxId);
            KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
            InboxServiceRWCoProcInput input = MessageUtil.buildBatchCommitRequest(reqId, InboxCommitRequest.newBuilder()
                .putInboxCommit(scopedInboxId.toStringUtf8(), InboxCommit.newBuilder()
                    .setQos2UpToSeq(upToSeq)
                    .build())
                .build());
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reqId, reply.getReqId());
            assertEquals(ReplyCode.Ok, reply.getCode());
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult());
            assertTrue(output.getReply().hasCommit());
            assertEquals(reqId, output.getReply().getReqId());
            assertTrue(output.getReply().getCommit().getResultMap().get(scopedInboxId.toStringUtf8()));
            return output.getReply().getCommit();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }


    protected TopicMessagePack.SenderMessagePack message(QoS qos, String payload) {
        return TopicMessagePack.SenderMessagePack.newBuilder()
            .addMessage(Message.newBuilder()
                .setMessageId(System.nanoTime())
                .setPubQoS(qos)
                .setPayload(ByteString.copyFromUtf8(payload))
                .build())
            .build();
    }

    protected TopicMessagePack.SenderMessagePack message(int messageId, QoS qos, String payload, ClientInfo sender) {
        return TopicMessagePack.SenderMessagePack.newBuilder()
            .addMessage(Message.newBuilder()
                .setMessageId(messageId)
                .setPubQoS(qos)
                .setPayload(ByteString.copyFromUtf8(payload))
                .build())
            .setSender(sender)
            .build();
    }
}
