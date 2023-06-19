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

package com.baidu.bifromq.inbox.store.benchmark;

import static com.baidu.bifromq.inbox.util.KeyUtil.scopedInboxId;
import static com.baidu.bifromq.inbox.util.MessageUtil.buildBatchInboxInsertRequest;
import static com.baidu.bifromq.inbox.util.MessageUtil.buildCreateRequest;
import static com.baidu.bifromq.inbox.util.MessageUtil.buildHasRequest;
import static com.baidu.bifromq.inbox.util.MessageUtil.buildTouchRequest;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
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
import com.baidu.bifromq.inbox.storage.proto.HasReply;
import com.baidu.bifromq.inbox.storage.proto.HasRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxInsertReply;
import com.baidu.bifromq.inbox.storage.proto.InboxInsertRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.MessagePack;
import com.baidu.bifromq.inbox.storage.proto.TouchReply;
import com.baidu.bifromq.inbox.storage.proto.TouchRequest;
import com.baidu.bifromq.inbox.store.IInboxStore;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.pf4j.util.FileUtils;

@Slf4j
abstract class InboxStoreState {
    private static final String DB_NAME = "testDB";
    private static final String DB_CHECKPOINT_DIR_NAME = "testDB_cp";

    private static final String DB_WAL_NAME = "testWAL";
    private static final String DB_WAL_CHECKPOINT_DIR = "testWAL_cp";
    public Path dbRootDir;
    protected IBaseKVStoreClient storeClient;
    private IAgentHost agentHost;
    private ICRDTService crdtService;
    protected IInboxStore testStore;

    private IEventCollector eventCollector = new IEventCollector() {
        @Override
        public void report(Event<?> event) {

        }
    };

    public InboxStoreState() {
        try {
            dbRootDir = Files.createTempDirectory("");
        } catch (IOException e) {
        }

        String uuid = UUID.randomUUID().toString();
        AgentHostOptions agentHostOpts = AgentHostOptions.builder()
            .addr("127.0.0.1")
            .baseProbeInterval(Duration.ofSeconds(10))
            .joinRetryInSec(5)
            .joinTimeout(Duration.ofMinutes(5))
            .build();
        agentHost = IAgentHost.newInstance(agentHostOpts);
        agentHost.start();
        crdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        crdtService.start(agentHost);

        KVRangeStoreOptions options = new KVRangeStoreOptions();
//        options.setWalEngineConfigurator(new InMemoryKVEngineConfigurator());
//        options.setDataEngineConfigurator(new InMemoryKVEngineConfigurator());
        ((RocksDBKVEngineConfigurator) options.getDataEngineConfigurator())
            .setDbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR_NAME, uuid)
                .toString())
            .setDbRootDir(Paths.get(dbRootDir.toString(), DB_NAME, uuid).toString());
        ((RocksDBKVEngineConfigurator) options.getWalEngineConfigurator())
            .setDbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_WAL_CHECKPOINT_DIR, uuid)
                .toString())
            .setDbRootDir(Paths.get(dbRootDir.toString(), DB_WAL_NAME, uuid).toString());

        storeClient = IBaseKVStoreClient
            .inProcClientBuilder()
            .clusterId(IInboxStore.CLUSTER_NAME)
            .crdtService(crdtService)
            .build();
        testStore = IInboxStore.
            inProcBuilder()
            .agentHost(agentHost)
            .crdtService(crdtService)
            .storeClient(storeClient)
            .eventCollector(eventCollector)
            .purgeDelay(Duration.ZERO)
            .clock(Clock.systemUTC())
            .kvRangeStoreOptions(options)
            .build();
    }

    @Setup(Level.Trial)
    public void setup() {
        testStore.start(true);
        storeClient.join();
        afterSetup();
        log.info("Setup finished, and start testing");
    }

    abstract void afterSetup();

    @TearDown
    public void teardown() {
        log.info("Finish testing, and tearing down");
        beforeTeardown();
        storeClient.stop();
        testStore.stop();
        crdtService.stop();
        agentHost.shutdown();
        try {
            FileUtils.delete(dbRootDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    abstract void beforeTeardown();

    protected HasReply requestHas(String trafficId, String inboxId) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString scopedInboxId = scopedInboxId(trafficId, inboxId);
            KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
            HasRequest request = HasRequest.newBuilder().addScopedInboxId(scopedInboxId).build();
            InboxServiceROCoProcInput input = buildHasRequest(reqId, request);
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

    protected CreateReply requestCreate(String trafficId, String inboxId,
                                        int limit, int expireSeconds, boolean dropOldest) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString scopedInboxId = scopedInboxId(trafficId, inboxId);
            KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
            CreateRequest request = CreateRequest.newBuilder()
                .putInboxes(scopedInboxId.toStringUtf8(), CreateParams.newBuilder()
                    .setExpireSeconds(expireSeconds)
                    .setLimit(limit)
                    .setDropOldest(dropOldest)
                    .build())
                .build();
            InboxServiceRWCoProcInput input = buildCreateRequest(reqId, request);
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

    protected TouchReply requestDelete(String trafficId, String inboxId) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString scopedInboxId = scopedInboxId(trafficId, inboxId);
            KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
            TouchRequest request = TouchRequest.newBuilder()
                .putScopedInboxId(scopedInboxId.toStringUtf8(), false)
                .build();
            InboxServiceRWCoProcInput input = buildTouchRequest(reqId, request);
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

    protected InboxInsertReply requestInsert(String trafficId, String inboxId, InboxInsertRequest request) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString scopedInboxId = scopedInboxId(trafficId, inboxId);
            KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
            InboxServiceRWCoProcInput input = buildBatchInboxInsertRequest(reqId, request);
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
            assertEquals(reqId, output.getReply().getInsert());
            return output.getReply().getInsert();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected InboxInsertReply requestInsert(String trafficId, String inboxId, String topic, Message... messages) {

        try {
            InboxInsertRequest.Builder builder = InboxInsertRequest.newBuilder();
            InboxInsertRequest request = builder
                .addSubMsgPack(MessagePack.newBuilder()
                    .setSubInfo(SubInfo.newBuilder()
                        .setTrafficId(trafficId)
                        .setInboxId(inboxId)
                        .setSubQoS(messages[0].getPubQoS())
                        .build())
                    .addMessages(TopicMessagePack.newBuilder()
                        .setTopic(topic)
                        .addMessage(TopicMessagePack.SenderMessagePack.newBuilder()
                            .addAllMessage(Arrays.stream(messages).collect(Collectors.toList()))
                            .build())
                        .build())
                    .build())
                .build();
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString scopedInboxId = scopedInboxId(trafficId, inboxId);
            KVRangeSetting s = storeClient.findByKey(scopedInboxId).get();
            InboxServiceRWCoProcInput input = buildBatchInboxInsertRequest(reqId, request);
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build(), inboxId).join();
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

    protected Message message(QoS qos, String payload) {
        return Message.newBuilder()
            .setMessageId(System.nanoTime())
            .setPubQoS(qos)
            .setPayload(ByteString.copyFromUtf8(payload))
            .build();
    }
}
