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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
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
import com.baidu.bifromq.dist.client.ClearResult;
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
import com.baidu.bifromq.plugin.inboxbroker.HasResult;
import com.baidu.bifromq.plugin.inboxbroker.IInboxBrokerManager;
import com.baidu.bifromq.plugin.inboxbroker.IInboxWriter;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.MQTT3ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
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
import org.mockito.MockitoAnnotations;
import org.pf4j.util.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.mockito.Mock;

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
    protected IDistClient distClient;
    @Mock
    protected IInboxBrokerManager receiverManager;
    @Mock
    protected IInboxWriter writer1;
    @Mock
    protected IInboxWriter writer2;
    @Mock
    protected IInboxWriter writer3;
    protected SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    protected IDistWorker testWorker;
    protected IBaseKVStoreClient storeClient;
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
    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        try {
            dbRootDir = Files.createTempDirectory("");
        } catch (IOException e) {
        }
        lenient().when(receiverManager.hasBroker(MqttBroker)).thenReturn(true);
        lenient().when(receiverManager.hasBroker(InboxService)).thenReturn(true);
        lenient().when(receiverManager.hasInbox(anyLong(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(HasResult.YES));
        lenient().when(distClient.clear(anyLong(), anyString(), anyString(), anyInt(), any(ClientInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(ClearResult.OK));

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
            .nonSSLClientBuilder()
            .eventLoopGroup(NettyUtil.createEventLoopGroup())
//                .inProcClientBuilder()
            .clusterId(IDistWorker.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
        testWorker = IDistWorker
            .nonSSLBuilder()
            .bindAddr("127.0.0.1")
            .bindPort(8080)
            .bossEventLoopGroup(NettyUtil.createEventLoopGroup(1))
            .workerEventLoopGroup(NettyUtil.createEventLoopGroup())
            .ioExecutor(MoreExecutors.directExecutor())
//                .inProcBuilder()
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .eventCollector(eventCollector)
            .distClient(distClient)
            .storeClient(storeClient)
            .queryExecutor(queryExecutor)
            .mutationExecutor(mutationExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .balanceControllerOptions(balanceControllerOptions)
            .gcInterval(Duration.ofSeconds(1))
            .statsInterval(Duration.ofSeconds(1))
            .kvRangeStoreOptions(options)
            .inboxBrokerManager(receiverManager)
            .build();
        testWorker.start(true);

        storeClient.join();
        log.info("Setup finished, and start testing");
    }

    @AfterMethod
    public void teardown() throws Exception {
        log.info("Finish testing, and tearing down");
        storeClient.stop();
        testWorker.stop();
        clientCrdtService.stop();
        serverCrdtService.stop();
        agentHost.shutdown();
        try {
            FileUtils.delete(dbRootDir);
        } catch(IOException e) {
            e.printStackTrace();
        }
        queryExecutor.shutdown();
        mutationExecutor.shutdown();
        tickTaskExecutor.shutdown();
        bgTaskExecutor.shutdown();
        closeable.close();
    }

    protected AddTopicFilterReply addTopicFilter(String trafficId, String topicFilter, QoS subQoS,
                                                 int subBroker, String inboxId, String inboxGroupKey) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString subInfoKey =
                EntityUtil.subInfoKey(trafficId, EntityUtil.toQualifiedInboxId(subBroker, inboxId, inboxGroupKey));
            KVRangeSetting s = storeClient.findByKey(subInfoKey).get();
            SubRequest request = SubRequest.newBuilder()
                .setReqId(reqId)
                .setTopicFilter(topicFilter)
                .setSubQoS(subQoS)
                .setInboxId(inboxId)
                .setBroker(subBroker)
                .setInboxGroupKey(inboxGroupKey)
                .setClient(ClientInfo.newBuilder()
                    .setTrafficId(trafficId)
                    .build())
                .build();
            DistServiceRWCoProcInput input = MessageUtil.buildAddTopicFilterRequest(request);
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reqId, reply.getReqId());
            assertEquals(ReplyCode.Ok, reply.getCode());
            UpdateReply updateReply = DistServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult()).getUpdateReply();
            assertTrue(updateReply.hasAddTopicFilter());
            assertEquals(reqId, updateReply.getReqId());
            return updateReply.getAddTopicFilter();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected RemoveTopicFilterReply removeTopicFilter(String trafficId, String topicFilter,
                                                       int subBroker, String inboxId, String inboxGroupKey) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString subInfoKey =
                EntityUtil.subInfoKey(trafficId, EntityUtil.toQualifiedInboxId(subBroker, inboxId, inboxGroupKey));
            KVRangeSetting s = storeClient.findByKey(subInfoKey).get();
            UnsubRequest request = UnsubRequest.newBuilder()
                .setReqId(reqId)
                .setTopicFilter(topicFilter)
                .setBroker(subBroker)
                .setInboxId(inboxId)
                .setInboxGroupKey(inboxGroupKey)
                .setClient(ClientInfo.newBuilder()
                    .setTrafficId(trafficId)
                    .build())
                .build();
            DistServiceRWCoProcInput input = MessageUtil.buildRemoveTopicFilterRequest(request);
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reqId, reply.getReqId());
            assertEquals(ReplyCode.Ok, reply.getCode());
            UpdateReply updateReply = DistServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult()).getUpdateReply();
            assertTrue(updateReply.hasRemoveTopicFilter());
            assertEquals(reqId, updateReply.getReqId());
            return updateReply.getRemoveTopicFilter();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected InsertMatchRecordReply insertMatchRecord(String trafficId, String topicFilter, QoS subQoS,
                                                       int subBroker, String inboxId, String inboxGroupKey) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString matchRecordKey = EntityUtil.matchRecordKey(trafficId, topicFilter,
                EntityUtil.toQualifiedInboxId(subBroker, inboxId, inboxGroupKey));
            KVRangeSetting s = storeClient.findByKey(matchRecordKey).get();
            SubRequest.Builder reqBuilder = SubRequest.newBuilder()
                .setReqId(reqId)
                .setTopicFilter(topicFilter)
                .setInboxId(inboxId)
                .setBroker(subBroker)
                .setSubQoS(subQoS)
                .setInboxGroupKey(inboxGroupKey)
                .setClient(ClientInfo.newBuilder()
                    .setTrafficId(trafficId)
                    .build());

            DistServiceRWCoProcInput input = MessageUtil.buildInsertMatchRecordRequest(reqBuilder.build());
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reqId, reply.getReqId());
            assertEquals(ReplyCode.Ok, reply.getCode());
            UpdateReply updateReply = DistServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult()).getUpdateReply();
            assertTrue(updateReply.hasInsertMatchRecord());
            assertEquals(reqId, updateReply.getReqId());
            return updateReply.getInsertMatchRecord();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected DeleteMatchRecordReply deleteMatchRecord(String trafficId, String topicFilter,
                                                       int subBroker, String inboxId, String serverId) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            String scopedInboxId = EntityUtil.toQualifiedInboxId(subBroker, inboxId, serverId);
            ByteString matchRecordKey = EntityUtil.matchRecordKey(trafficId, topicFilter, scopedInboxId);
            KVRangeSetting s = storeClient.findByKey(matchRecordKey).get();
            DistServiceRWCoProcInput input = MessageUtil.buildDeleteMatchRecordRequest(reqId, trafficId, scopedInboxId,
                topicFilter);
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reqId, reply.getReqId());
            assertEquals(ReplyCode.Ok, reply.getCode());
            UpdateReply updateReply = DistServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult()).getUpdateReply();
            assertTrue(updateReply.hasDeleteMatchRecord());
            assertEquals(reqId, updateReply.getReqId());
            return updateReply.getDeleteMatchRecord();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected JoinMatchGroupReply joinMatchGroup(String trafficId, String topicFilter, QoS subQoS,
                                                 int subBroker, String inboxId, String inboxGroupKey) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            ByteString matchRecordKey = EntityUtil.matchRecordKey(trafficId, topicFilter,
                EntityUtil.toQualifiedInboxId(subBroker, inboxId, inboxGroupKey));
            KVRangeSetting s = storeClient.findByKey(matchRecordKey).get();
            SubRequest.Builder reqBuilder = SubRequest.newBuilder()
                .setReqId(reqId)
                .setTopicFilter(topicFilter)
                .setInboxId(inboxId)
                .setBroker(subBroker)
                .setSubQoS(subQoS)
                .setInboxGroupKey(inboxGroupKey)
                .setClient(ClientInfo.newBuilder()
                    .setTrafficId(trafficId)
                    .build());

            DistServiceRWCoProcInput input = MessageUtil.buildJoinMatchGroupRequest(reqBuilder.build());
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reqId, reply.getReqId());
            assertEquals(ReplyCode.Ok, reply.getCode());
            UpdateReply updateReply = DistServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult()).getUpdateReply();
            assertTrue(updateReply.hasJoinMatchGroup());
            assertEquals(reqId, updateReply.getReqId());
            return updateReply.getJoinMatchGroup();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected LeaveMatchGroupReply leaveMatchGroup(String trafficId, String topicFilter,
                                                   int subBroker, String inboxId, String serverId) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            String scopedInboxId = EntityUtil.toQualifiedInboxId(subBroker, inboxId, serverId);
            ByteString matchRecordKey = EntityUtil.matchRecordKey(trafficId, topicFilter, scopedInboxId);
            KVRangeSetting s = storeClient.findByKey(matchRecordKey).get();
            DistServiceRWCoProcInput input = MessageUtil.buildLeaveMatchGroupRequest(reqId, trafficId, scopedInboxId,
                topicFilter);
            KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver)
                .setKvRangeId(s.id)
                .setRwCoProc(input.toByteString())
                .build()).join();
            assertEquals(reqId, reply.getReqId());
            assertEquals(ReplyCode.Ok, reply.getCode());
            UpdateReply updateReply = DistServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult()).getUpdateReply();
            assertTrue(updateReply.hasLeaveMatchGroup());
            assertEquals(reqId, updateReply.getReqId());
            return updateReply.getLeaveMatchGroup();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected ClearSubInfoReply clearSubInfo(String trafficId,
                                             int subBroker,
                                             String inboxId,
                                             String serverId) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
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

    protected BatchDistReply dist(String trafficId, List<TopicMessagePack> msgs, String orderKey) {
        try {
            long reqId = ThreadLocalRandom.current().nextInt();
            KVRangeSetting s = storeClient.findByKey(EntityUtil.matchRecordKeyPrefix(trafficId)).get();
            BatchDist request = BatchDist.newBuilder()
                .setReqId(reqId)
                .addDistPack(DistPack.newBuilder()
                    .setTrafficId(trafficId)
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
            assertEquals(reqId, reply.getReqId());
            assertEquals(ReplyCode.Ok, reply.getCode());
            DistServiceROCoProcOutput output = DistServiceROCoProcOutput.parseFrom(reply.getRoCoProcResult());
            assertTrue(output.hasDistReply());
            assertEquals(reqId, output.getDistReply().getReqId());
            return output.getDistReply();
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    protected BatchDistReply dist(String trafficId, QoS qos, String topic, ByteString payload, String orderKey) {
        return dist(trafficId, List.of(TopicMessagePack.newBuilder()
            .setTopic(topic)
            .addMessage(TopicMessagePack.SenderMessagePack.newBuilder()
                .setSender(ClientInfo.newBuilder()
                    .setTrafficId(trafficId)
                    .setUserId("testUser")
                    .setMqtt3ClientInfo(MQTT3ClientInfo.newBuilder()
                        .setClientId("testClientId")
                        .setIp("127.0.0.1")
                        .setPort(8080)
                        .build())
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
