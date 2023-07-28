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

package com.baidu.bifromq.dist.worker;

import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ADDRESS_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_3_1_1_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_TYPE_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
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
import com.baidu.bifromq.basekv.localengine.InMemoryKVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.RocksDBKVEngineConfigurator;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.basekv.store.proto.KVRangeROReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.baserpc.utils.NettyUtil;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.entity.EntityUtil;
import com.baidu.bifromq.dist.rpc.proto.AddTopicFilterReply;
import com.baidu.bifromq.dist.rpc.proto.BatchDist;
import com.baidu.bifromq.dist.rpc.proto.BatchDistReply;
import com.baidu.bifromq.dist.rpc.proto.ClearSubInfoReply;
import com.baidu.bifromq.dist.rpc.proto.DeleteMatchRecordReply;
import com.baidu.bifromq.dist.rpc.proto.DistPack;
import com.baidu.bifromq.dist.rpc.proto.DistServiceROCoProcInput;
import com.baidu.bifromq.dist.rpc.proto.DistServiceROCoProcOutput;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcInput;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcOutput;
import com.baidu.bifromq.dist.rpc.proto.InsertMatchRecordReply;
import com.baidu.bifromq.dist.rpc.proto.JoinMatchGroupReply;
import com.baidu.bifromq.dist.rpc.proto.LeaveMatchGroupReply;
import com.baidu.bifromq.dist.rpc.proto.RemoveTopicFilterReply;
import com.baidu.bifromq.dist.rpc.proto.SubRequest;
import com.baidu.bifromq.dist.rpc.proto.UnsubRequest;
import com.baidu.bifromq.dist.rpc.proto.UpdateReply;
import com.baidu.bifromq.dist.util.MessageUtil;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.baidu.bifromq.plugin.subbroker.ISubBroker;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

@Slf4j
public abstract class DistWorkerTest {
    private static final String DB_NAME = "testDB";
    private static final String DB_CHECKPOINT_DIR_NAME = "testDB_cp";
    private static final String DB_WAL_NAME = "testWAL";
    private static final String DB_WAL_CHECKPOINT_DIR = "testWAL_cp";

    protected static final int MqttBroker = 0;
    protected static final int InboxService = 1;

    private IAgentHost agentHost;
    private ICRDTService clientCrdtService;
    private ICRDTService serverCrdtService;
    @Mock
    protected IEventCollector eventCollector;
    @Mock
    protected ISettingProvider settingProvider;
    @Mock
    protected IDistClient distClient;
    @Mock
    protected ISubBrokerManager receiverManager;

    @Mock
    protected ISubBroker mqttBroker;

    @Mock
    protected ISubBroker inboxBroker;
    @Mock
    protected IDeliverer writer1;
    @Mock
    protected IDeliverer writer2;
    @Mock
    protected IDeliverer writer3;
    protected SimpleMeterRegistry meterRegistry;
    protected IDistWorker testWorker;
    protected IBaseKVStoreClient storeClient;

    protected String tenantA = "tenantA";
    protected String tenantB = "tenantB";
    private ExecutorService queryExecutor;
    private ExecutorService mutationExecutor;
    private ScheduledExecutorService tickTaskExecutor;
    private ScheduledExecutorService bgTaskExecutor;
    private Path dbRootDir;

    private static boolean runOnMac() {
        String osName = System.getProperty("os.name");
        return osName != null && osName.startsWith("Mac");
    }

    private AutoCloseable closeable;

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        System.setProperty("dist_worker_topic_match_expiry_seconds", "1");
        meterRegistry = new SimpleMeterRegistry();
        try {
            dbRootDir = Files.createTempDirectory("");
        } catch (IOException e) {
        }
        lenient().when(settingProvider.provide(Setting.MaxTopicFiltersPerInbox, tenantA)).thenReturn(200);
        lenient().when(settingProvider.provide(Setting.MaxTopicFiltersPerInbox, tenantB)).thenReturn(200);
        lenient().when(settingProvider.provide(Setting.MaxSharedGroupMembers, tenantA)).thenReturn(200);
        lenient().when(settingProvider.provide(Setting.MaxSharedGroupMembers, tenantB)).thenReturn(200);
        lenient().when(receiverManager.get(MqttBroker)).thenReturn(mqttBroker);
        lenient().when(receiverManager.get(InboxService)).thenReturn(inboxBroker);
        lenient().when(mqttBroker.hasInbox(anyLong(), anyString(), anyString(), anyString()))
            .thenReturn(CompletableFuture.completedFuture(true));
        lenient().when(inboxBroker.hasInbox(anyLong(), anyString(), anyString(), anyString()))
            .thenReturn(CompletableFuture.completedFuture(true));
        lenient().when(distClient.clear(anyLong(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(null));

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

        KVRangeBalanceControllerOptions balanceControllerOptions = new KVRangeBalanceControllerOptions();

        storeClient = IBaseKVStoreClient
            .newBuilder()
            .eventLoopGroup(NettyUtil.createEventLoopGroup())
            .clusterId(IDistWorker.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
        testWorker = IDistWorker.standaloneBuilder()
            .host("127.0.0.1")
            .bossEventLoopGroup(NettyUtil.createEventLoopGroup(1))
            .workerEventLoopGroup(NettyUtil.createEventLoopGroup())
            .ioExecutor(MoreExecutors.directExecutor())
            .bootstrap(true)
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .eventCollector(eventCollector)
            .settingProvider(settingProvider)
            .distClient(distClient)
            .storeClient(storeClient)
            .queryExecutor(queryExecutor)
            .mutationExecutor(mutationExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .balanceControllerOptions(balanceControllerOptions)
            .gcInterval(Duration.ofSeconds(1))
            .statsInterval(Duration.ofSeconds(1))
            .storeOptions(options)
            .subBrokerManager(receiverManager)
            .build();
        testWorker.start();

        storeClient.join();
        log.info("Setup finished, and start testing");
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        log.info("Finish testing, and tearing down");
        storeClient.stop();
        testWorker.stop();
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

    protected AddTopicFilterReply addTopicFilter(String tenantId, String topicFilter, QoS subQoS,
                                                 int subBroker, String inboxId, String delivererKey) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString subInfoKey =
                EntityUtil.subInfoKey(tenantId, EntityUtil.toQualifiedInboxId(subBroker, inboxId, delivererKey));
            KVRangeSetting s = storeClient.findByKey(subInfoKey).get();
            SubRequest request = SubRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setTopicFilter(topicFilter)
                .setSubQoS(subQoS)
                .setInboxId(inboxId)
                .setBroker(subBroker)
                .setDelivererKey(delivererKey)
                .build();
            DistServiceRWCoProcInput input = MessageUtil.buildAddTopicFilterRequest(request);
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reply.getReqId(), reqId);
            assertEquals(reply.getCode(), ReplyCode.Ok);
            UpdateReply updateReply = DistServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult()).getUpdateReply();
            assertTrue(updateReply.hasAddTopicFilter());
            assertEquals(updateReply.getReqId(), reqId);
            return updateReply.getAddTopicFilter();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected RemoveTopicFilterReply removeTopicFilter(String tenantId, String topicFilter,
                                                       int subBroker, String inboxId, String delivererKey) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString subInfoKey =
                EntityUtil.subInfoKey(tenantId, EntityUtil.toQualifiedInboxId(subBroker, inboxId, delivererKey));
            KVRangeSetting s = storeClient.findByKey(subInfoKey).get();
            UnsubRequest request = UnsubRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setTopicFilter(topicFilter)
                .setBroker(subBroker)
                .setInboxId(inboxId)
                .setDelivererKey(delivererKey)
                .build();
            DistServiceRWCoProcInput input = MessageUtil.buildRemoveTopicFilterRequest(request);
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reply.getReqId(), reqId);
            assertEquals(reply.getCode(), ReplyCode.Ok);
            UpdateReply updateReply = DistServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult()).getUpdateReply();
            assertTrue(updateReply.hasRemoveTopicFilter());
            assertEquals(updateReply.getReqId(), reqId);
            return updateReply.getRemoveTopicFilter();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected InsertMatchRecordReply insertMatchRecord(String tenantId, String topicFilter, QoS subQoS,
                                                       int subBroker, String inboxId, String delivererKey) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString matchRecordKey = EntityUtil.matchRecordKey(tenantId, topicFilter,
                EntityUtil.toQualifiedInboxId(subBroker, inboxId, delivererKey));
            KVRangeSetting s = storeClient.findByKey(matchRecordKey).get();
            SubRequest.Builder reqBuilder = SubRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setTopicFilter(topicFilter)
                .setInboxId(inboxId)
                .setBroker(subBroker)
                .setSubQoS(subQoS)
                .setDelivererKey(delivererKey);

            DistServiceRWCoProcInput input = MessageUtil.buildInsertMatchRecordRequest(reqBuilder.build());
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reply.getReqId(), reqId);
            assertEquals(reply.getCode(), ReplyCode.Ok);
            UpdateReply updateReply = DistServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult()).getUpdateReply();
            assertTrue(updateReply.hasInsertMatchRecord());
            assertEquals(updateReply.getReqId(), reqId);
            return updateReply.getInsertMatchRecord();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected DeleteMatchRecordReply deleteMatchRecord(String tenantId, String topicFilter,
                                                       int subBroker, String inboxId, String serverId) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            String scopedInboxId = EntityUtil.toQualifiedInboxId(subBroker, inboxId, serverId);
            ByteString matchRecordKey = EntityUtil.matchRecordKey(tenantId, topicFilter, scopedInboxId);
            KVRangeSetting s = storeClient.findByKey(matchRecordKey).get();
            DistServiceRWCoProcInput input = MessageUtil.buildDeleteMatchRecordRequest(reqId, tenantId, scopedInboxId,
                topicFilter);
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reply.getReqId(), reqId);
            assertEquals(reply.getCode(), ReplyCode.Ok);
            UpdateReply updateReply = DistServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult()).getUpdateReply();
            assertTrue(updateReply.hasDeleteMatchRecord());
            assertEquals(updateReply.getReqId(), reqId);
            return updateReply.getDeleteMatchRecord();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected JoinMatchGroupReply joinMatchGroup(String tenantId, String topicFilter, QoS subQoS,
                                                 int subBroker, String inboxId, String delivererKey) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString matchRecordKey = EntityUtil.matchRecordKey(tenantId, topicFilter,
                EntityUtil.toQualifiedInboxId(subBroker, inboxId, delivererKey));
            KVRangeSetting s = storeClient.findByKey(matchRecordKey).get();
            SubRequest.Builder reqBuilder = SubRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setTopicFilter(topicFilter)
                .setInboxId(inboxId)
                .setBroker(subBroker)
                .setSubQoS(subQoS)
                .setDelivererKey(delivererKey);

            DistServiceRWCoProcInput input = MessageUtil.buildJoinMatchGroupRequest(reqBuilder.build());
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reply.getReqId(), reqId);
            assertEquals(reply.getCode(), ReplyCode.Ok);
            UpdateReply updateReply = DistServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult()).getUpdateReply();
            assertTrue(updateReply.hasJoinMatchGroup());
            assertEquals(updateReply.getReqId(), reqId);
            return updateReply.getJoinMatchGroup();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected LeaveMatchGroupReply leaveMatchGroup(String tenantId, String topicFilter,
                                                   int subBroker, String inboxId, String serverId) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            String scopedInboxId = EntityUtil.toQualifiedInboxId(subBroker, inboxId, serverId);
            ByteString matchRecordKey = EntityUtil.matchRecordKey(tenantId, topicFilter, scopedInboxId);
            KVRangeSetting s = storeClient.findByKey(matchRecordKey).get();
            DistServiceRWCoProcInput input = MessageUtil.buildLeaveMatchGroupRequest(reqId, tenantId, scopedInboxId,
                topicFilter);
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reply.getReqId(), reqId);
            assertEquals(reply.getCode(), ReplyCode.Ok);
            UpdateReply updateReply = DistServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult()).getUpdateReply();
            assertTrue(updateReply.hasLeaveMatchGroup());
            assertEquals(updateReply.getReqId(), reqId);
            return updateReply.getLeaveMatchGroup();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected ClearSubInfoReply clearSubInfo(String tenantId,
                                             int subBroker,
                                             String inboxId,
                                             String serverId) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString subInfoKey =
                EntityUtil.subInfoKey(tenantId, EntityUtil.toQualifiedInboxId(subBroker, inboxId, serverId));
            KVRangeSetting s = storeClient.findByKey(subInfoKey).get();
            DistServiceRWCoProcInput input = MessageUtil.buildClearSubInfoRequest(reqId, subInfoKey);
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reply.getReqId(), reqId);
            assertEquals(reply.getCode(), ReplyCode.Ok);
            UpdateReply updateReply = DistServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult()).getUpdateReply();
            assertTrue(updateReply.hasClearSubInfo());
            assertEquals(updateReply.getReqId(), reqId);
            return updateReply.getClearSubInfo();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected BatchDistReply dist(String tenantId, List<TopicMessagePack> msgs, String orderKey) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            KVRangeSetting s = storeClient.findByKey(EntityUtil.matchRecordKeyPrefix(tenantId)).get();
            BatchDist request = BatchDist.newBuilder()
                .setReqId(reqId)
                .addDistPack(DistPack.newBuilder()
                    .setTenantId(tenantId)
                    .addAllMsgPack(msgs)
                    .build())
                .setOrderKey(orderKey)
                .build();
            DistServiceROCoProcInput input = MessageUtil.buildBatchDistRequest(request);
            KVRangeROReply reply = storeClient.query(s.leader, KVRangeRORequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRoCoProcInput(input.toByteString())
                .build()).join();
            assertEquals(reply.getReqId(), reqId);
            assertEquals(reply.getCode(), ReplyCode.Ok);
            DistServiceROCoProcOutput output = DistServiceROCoProcOutput.parseFrom(reply.getRoCoProcResult());
            assertTrue(output.hasDistReply());
            assertEquals(output.getDistReply().getReqId(), reqId);
            return output.getDistReply();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected BatchDistReply dist(String tenantId, QoS qos, String topic, ByteString payload, String orderKey) {
        return dist(tenantId, List.of(TopicMessagePack.newBuilder()
            .setTopic(topic)
            .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                .setPublisher(ClientInfo.newBuilder()
                    .setTenantId(tenantId)
                    .setType(MQTT_TYPE_VALUE)
                    .putMetadata(MQTT_PROTOCOL_VER_KEY, MQTT_PROTOCOL_VER_3_1_1_VALUE)
                    .putMetadata(MQTT_USER_ID_KEY, "testUser")
                    .putMetadata(MQTT_CLIENT_ID_KEY, "testClientId")
                    .putMetadata(MQTT_CLIENT_ADDRESS_KEY, "127.0.0.1:8080")
                    .build())
                .addMessage(Message.newBuilder()
                    .setMessageId(ThreadLocalRandom.current().nextInt())
                    .setPubQoS(qos)
                    .setPayload(payload)
                    .setTimestamp(System.currentTimeMillis())
                    .setExpireTimestamp(Long.MAX_VALUE)
                    .build())
                .build())
            .build()), orderKey);
    }
}
