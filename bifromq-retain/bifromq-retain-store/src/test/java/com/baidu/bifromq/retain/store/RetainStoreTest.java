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
import com.baidu.bifromq.basekv.localengine.rocksdb.RocksDBCPableKVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.rocksdb.RocksDBWALableKVEngineConfigurator;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.basekv.store.proto.KVRangeROReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.retain.rpc.proto.BatchMatchRequest;
import com.baidu.bifromq.retain.rpc.proto.BatchRetainRequest;
import com.baidu.bifromq.retain.rpc.proto.GCReply;
import com.baidu.bifromq.retain.rpc.proto.MatchParam;
import com.baidu.bifromq.retain.rpc.proto.MatchResult;
import com.baidu.bifromq.retain.rpc.proto.RetainMessage;
import com.baidu.bifromq.retain.rpc.proto.RetainMessagePack;
import com.baidu.bifromq.retain.rpc.proto.RetainResult;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceROCoProcInput;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceROCoProcOutput;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceRWCoProcInput;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceRWCoProcOutput;
import com.baidu.bifromq.retain.utils.KeyUtil;
import com.baidu.bifromq.retain.utils.MessageUtil;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.TopicMessage;
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
import java.util.Comparator;
import java.util.UUID;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

@Slf4j
public class RetainStoreTest {
    private static final String DB_NAME = "testDB";
    private static final String DB_CHECKPOINT_DIR_NAME = "testDB_cp";

    private static final String DB_WAL_NAME = "testWAL";
    private static final String DB_WAL_CHECKPOINT_DIR = "testWAL_cp";
    private IAgentHost agentHost;
    private ICRDTService clientCrdtService;
    private ICRDTService serverCrdtService;
    protected SimpleMeterRegistry meterRegistry;
    @Mock
    protected ISettingProvider settingProvider;
    protected IRetainStore testStore;
    protected IBaseKVStoreClient storeClient;
    private ExecutorService queryExecutor;
    private ScheduledExecutorService tickTaskExecutor;
    private ScheduledExecutorService bgTaskExecutor;
    private Path dbRootDir;
    private AutoCloseable closeable;

    @BeforeClass(alwaysRun = true)
    public void setup() throws IOException {
        closeable = MockitoAnnotations.openMocks(this);
        meterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(meterRegistry);
        when(settingProvider.provide(any(Setting.class), anyString())).thenAnswer(
            invocation -> ((Setting) invocation.getArguments()[0]).current(invocation.getArgument(1)));
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
            options.setDataEngineConfigurator(new InMemKVEngineConfigurator());
            options.setWalEngineConfigurator(new InMemKVEngineConfigurator());
        } else {
            ((RocksDBCPableKVEngineConfigurator) options.getDataEngineConfigurator())
                .dbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR_NAME, uuid)
                    .toString())
                .dbRootDir(Paths.get(dbRootDir.toString(), DB_NAME, uuid).toString());
            ((RocksDBWALableKVEngineConfigurator) options.getWalEngineConfigurator())
                .dbRootDir(Paths.get(dbRootDir.toString(), DB_WAL_NAME, uuid).toString());
        }
        queryExecutor = new ThreadPoolExecutor(2, 2, 0L,
            TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
            EnvProvider.INSTANCE.newThreadFactory("query-executor"));
        tickTaskExecutor = new ScheduledThreadPoolExecutor(2,
            EnvProvider.INSTANCE.newThreadFactory("tick-task-executor"));
        bgTaskExecutor = new ScheduledThreadPoolExecutor(1,
            EnvProvider.INSTANCE.newThreadFactory("bg-task-executor"));

        storeClient = IBaseKVStoreClient
            .newBuilder()
            .clusterId(IRetainStore.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .build();
        testStore = IRetainStore.standaloneBuilder()
            .bootstrap(true)
            .host("127.0.0.1")
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .settingProvider(settingProvider)
            .storeClient(storeClient)
            .clock(getClock())
            .storeOptions(options)
            .balanceControllerOptions(new KVRangeBalanceControllerOptions())
            .queryExecutor(queryExecutor)
            .tickTaskExecutor(tickTaskExecutor)
            .bgTaskExecutor(bgTaskExecutor)
            .gcInterval(Duration.ofSeconds(1))
            .statsInterval(Duration.ofSeconds(1))
            .build();
        testStore.start();

        storeClient.join();
        log.info("Setup finished, and start testing");
    }

    @AfterClass(alwaysRun = true)
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

    protected RetainResult requestRetain(String tenantId, TopicMessage topicMsg) {
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString tenantNS = KeyUtil.tenantNS(tenantId);
        KVRangeSetting s = storeClient.findByKey(tenantNS).get();
        String topic = topicMsg.getTopic();
        Message message = topicMsg.getMessage();
        BatchRetainRequest request = BatchRetainRequest.newBuilder()
            .setReqId(message.getMessageId())
            .putRetainMessagePack(tenantId, RetainMessagePack.newBuilder()
                .putTopicMessages(topic, RetainMessage.newBuilder()
                    .setMessage(message)
                    .setPublisher(topicMsg.getPublisher())
                    .build())
                .build())
            .build();
        RetainServiceRWCoProcInput input = MessageUtil.buildRetainRequest(request);
        KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
            .setReqId(reqId)
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRwCoProc(RWCoProcInput.newBuilder().setRetainService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        RetainServiceRWCoProcOutput output = reply.getRwCoProcResult().getRetainService();
        assertTrue(output.hasBatchRetain());
        assertEquals(output.getBatchRetain().getReqId(), message.getMessageId());
        return output.getBatchRetain().getResultsMap().get(tenantId).getResultsMap().get(topic);
    }

    protected MatchResult requestMatch(String tenantId, String topicFilter, int limit) {
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString tenantNS = KeyUtil.tenantNS(tenantId);
        KVRangeSetting s = storeClient.findByKey(tenantNS).get();
        BatchMatchRequest request = BatchMatchRequest.newBuilder()
            .setReqId(reqId)
            .putMatchParams(tenantId, MatchParam.newBuilder()
                .putTopicFilters(topicFilter, limit)
                .build())
            .build();
        RetainServiceROCoProcInput input = MessageUtil.buildMatchRequest(request);
        KVRangeROReply reply = storeClient.query(s.leader, KVRangeRORequest.newBuilder()
            .setReqId(reqId)
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRoCoProc(ROCoProcInput.newBuilder().setRetainService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        RetainServiceROCoProcOutput output = reply.getRoCoProcResult().getRetainService();
        assertTrue(output.hasBatchMatch());
        assertEquals(output.getBatchMatch().getReqId(), reqId);
        return output.getBatchMatch().getResultPackMap().get(tenantId).getResultsMap().get(topicFilter);
    }

    protected GCReply requestGC(String tenantId) {
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString tenantNS = KeyUtil.tenantNS(tenantId);
        KVRangeSetting s = storeClient.findByKey(tenantNS).get();
        RetainServiceRWCoProcInput input = MessageUtil.buildGCRequest(reqId);
        KVRangeRWReply reply = storeClient.execute(s.leader, KVRangeRWRequest.newBuilder()
            .setReqId(reqId)
            .setVer(s.ver)
            .setKvRangeId(s.id)
            .setRwCoProc(RWCoProcInput.newBuilder().setRetainService(input).build())
            .build()).join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        RetainServiceRWCoProcOutput output = reply.getRwCoProcResult().getRetainService();
        assertTrue(output.hasGc());
        assertEquals(output.getGc().getReqId(), reqId);
        return output.getGc();
    }

    protected void clearMessage(String tenantId, String topic) {
        requestRetain(tenantId, message(topic, ""));
    }

    protected TopicMessage message(String topic, String payload) {
        return message(topic, payload, System.currentTimeMillis(), Integer.MAX_VALUE);
    }

    protected TopicMessage message(String topic, String payload, long timestamp, int expirySeconds) {
        return TopicMessage.newBuilder()
            .setTopic(topic)
            .setMessage(Message.newBuilder()
                .setMessageId(System.nanoTime())
                .setPayload(ByteString.copyFromUtf8(payload))
                .setTimestamp(timestamp)
                .setExpireTimestamp(expirySeconds == Integer.MAX_VALUE ? Long.MAX_VALUE :
                    timestamp + Duration.ofSeconds(expirySeconds).toMillis())
                .build())
            .setPublisher(ClientInfo.getDefaultInstance())
            .build();
    }
}
