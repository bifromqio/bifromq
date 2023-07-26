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

package com.baidu.bifromq.mqtt.handler;

import static com.baidu.bifromq.plugin.settingprovider.Setting.ByPassPermCheckError;
import static com.baidu.bifromq.plugin.settingprovider.Setting.DebugModeEnabled;
import static com.baidu.bifromq.plugin.settingprovider.Setting.ForceTransient;
import static com.baidu.bifromq.plugin.settingprovider.Setting.InBoundBandWidth;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicFiltersPerSub;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicLength;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicLevelLength;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicLevels;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxUserPayloadBytes;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MsgPubPerSec;
import static com.baidu.bifromq.plugin.settingprovider.Setting.OutBoundBandWidth;
import static com.baidu.bifromq.plugin.settingprovider.Setting.RetainEnabled;
import static com.baidu.bifromq.plugin.settingprovider.Setting.RetainMessageMatchLimit;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.basescheduler.exception.DropException;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxReaderClient;
import com.baidu.bifromq.inbox.client.IInboxReaderClient.IInboxReader;
import com.baidu.bifromq.inbox.rpc.proto.CommitReply;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.DeleteInboxReply;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.mqtt.service.ILocalSessionBrokerServer;
import com.baidu.bifromq.mqtt.session.IMQTTSession;
import com.baidu.bifromq.mqtt.session.MQTTSessionContext;
import com.baidu.bifromq.mqtt.utils.MQTTMessageUtils;
import com.baidu.bifromq.mqtt.utils.TestTicker;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import com.baidu.bifromq.plugin.authprovider.type.Ok;
import com.baidu.bifromq.plugin.authprovider.type.Reject;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.EventType;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.retain.client.IRetainServiceClient;
import com.baidu.bifromq.retain.client.IRetainServiceClient.IClientPipeline;
import com.baidu.bifromq.retain.rpc.proto.MatchReply;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import com.baidu.bifromq.sessiondict.rpc.proto.Ping;
import com.baidu.bifromq.sessiondict.rpc.proto.Quit;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.OngoingStubbing;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public abstract class BaseMQTTTest {

    @Mock
    protected IAuthProvider authProvider;
    @Mock
    protected IEventCollector eventCollector;
    @Mock
    protected ISettingProvider settingProvider;
    @Mock
    protected IDistClient distClient;
    @Mock
    protected IInboxReaderClient inboxClient;
    @Mock
    protected IRetainServiceClient retainClient;
    @Mock
    protected ISessionDictClient sessionDictClient;
    @Mock
    protected IRPCClient.IMessageStream<Quit, Ping> kickStream;
    @Mock
    protected IInboxReader inboxReader;
    @Mock
    protected IClientPipeline retainPipeline;
    protected TestTicker testTicker;
    protected MQTTSessionContext sessionContext;
    protected ILocalSessionBrokerServer sessionBrokerServer = new ILocalSessionBrokerServer() {
        private Map<String, IMQTTSession> sessions = new HashMap<>();

        @Override
        public String id() {
            return null;
        }

        @Override
        public void start() {

        }

        @Override
        public CompletableFuture<Void> disconnectAll(int disconnectRate) {
            return CompletableFuture.allOf(
                sessions.values().stream().map(IMQTTSession::disconnect).toArray(CompletableFuture[]::new));
        }

        @Override
        public void shutdown() {

        }

        @Override
        public void add(String sessionId, IMQTTSession session) {
            sessions.put(sessionId, session);
        }

        @Override
        public boolean remove(String sessionId, IMQTTSession session) {
            return sessions.remove(sessionId, session);
        }

        @Override
        public List<IMQTTSession> removeAll() {
            List<IMQTTSession> ret = new ArrayList<>(sessions.values());
            sessions.clear();
            return ret;
        }
    };
    protected EmbeddedChannel channel;
    protected String tenantId = "testTenantA";
    protected String userId = "testDeviceKey";
    protected String clientId = "testClientId";
    protected String delivererKey = "testGroupKey";
    protected String remoteIp = "127.0.0.1";
    protected int remotePort = 8888;
    protected PublishSubject<Quit> kickSubject = PublishSubject.create();
    protected long disconnectDelay = 5000;
    protected Consumer<Fetched> inboxFetchConsumer;
    protected List<Integer> fetchHints = new ArrayList<>();

    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        testTicker = new TestTicker();
        sessionContext = MQTTSessionContext.builder()
            .authProvider(authProvider)
            .eventCollector(eventCollector)
            .settingProvider(settingProvider)
            .distClient(distClient)
            .inboxClient(inboxClient)
            .retainClient(retainClient)
            .sessionDictClient(sessionDictClient)
            .brokerServer(sessionBrokerServer)
            .maxResendTimes(2)
            .resendDelayMillis(100)
            .defaultKeepAliveTimeSeconds(300)
            .qos2ConfirmWindowSeconds(300)
            .ticker(testTicker)
            .build();
        channel = new EmbeddedChannel(true, true, channelInitializer());
        channel.freezeTime();
        // common mocks
        mockSettings();
    }

    @AfterMethod
    public void clean() throws Exception {
        fetchHints.clear();
        channel.close();
        closeable.close();
    }

    protected ChannelInitializer<EmbeddedChannel> channelInitializer() {
        return new ChannelInitializer<>() {
            @Override
            protected void initChannel(EmbeddedChannel embeddedChannel) {
                embeddedChannel.attr(ChannelAttrs.MQTT_SESSION_CTX).set(sessionContext);
                embeddedChannel.attr(ChannelAttrs.PEER_ADDR).set(new InetSocketAddress(remoteIp, remotePort));
                ChannelPipeline pipeline = embeddedChannel.pipeline();
                pipeline.addLast("trafficShaper", new ChannelTrafficShapingHandler(512 * 1024, 512 * 1024));
                pipeline.addLast("decoder", new MqttDecoder(256 * 1024)); //256kb
                pipeline.addLast(MQTTMessageDebounceHandler.NAME, new MQTTMessageDebounceHandler());
                pipeline.addLast(MQTTConnectHandler.NAME, new MQTTConnectHandler(2));
            }
        };
    }

    protected void mockSettings() {
        Mockito.lenient().when(settingProvider.provide(eq(InBoundBandWidth), anyString())).thenReturn(51200 * 1024L);
        Mockito.lenient().when(settingProvider.provide(eq(OutBoundBandWidth), anyString())).thenReturn(51200 * 1024L);
        Mockito.lenient().when(settingProvider.provide(eq(ForceTransient), anyString())).thenReturn(false);
        Mockito.lenient().when(settingProvider.provide(eq(MaxUserPayloadBytes), anyString())).thenReturn(256 * 1024);
        Mockito.lenient().when(settingProvider.provide(eq(MaxTopicLevelLength), anyString())).thenReturn(40);
        Mockito.lenient().when(settingProvider.provide(eq(MaxTopicLevels), anyString())).thenReturn(16);
        Mockito.lenient().when(settingProvider.provide(eq(MaxTopicLength), anyString())).thenReturn(255);
        Mockito.lenient().when(settingProvider.provide(eq(ByPassPermCheckError), anyString())).thenReturn(true);
        Mockito.lenient().when(settingProvider.provide(eq(MsgPubPerSec), anyString())).thenReturn(200);
        Mockito.lenient().when(settingProvider.provide(eq(DebugModeEnabled), anyString())).thenReturn(true);
        Mockito.lenient().when(settingProvider.provide(eq(RetainEnabled), anyString())).thenReturn(true);
        Mockito.lenient().when(settingProvider.provide(eq(RetainMessageMatchLimit), anyString())).thenReturn(10);
        Mockito.lenient().when(settingProvider.provide(eq(MaxTopicFiltersPerSub), anyString())).thenReturn(10);
    }

    protected void mockAuthPass() {
        when(authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder()
                    .setTenantId(tenantId)
                    .setUserId(userId)
                    .build())
                .build()));
    }

    protected void mockAuthReject(Reject.Code code, String reason) {
        when(authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setReject(Reject.newBuilder()
                    .setCode(code)
                    .setReason(reason)
                    .build())
                .build()));
    }

    protected void mockAuthCheck(boolean allow) {
        when(authProvider.check(any(ClientInfo.class), any()))
            .thenReturn(CompletableFuture.completedFuture(allow));
    }

    protected void mockCheckError(String message) {
        when(authProvider.check(any(ClientInfo.class), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException(message)));
    }

    protected void mockInboxHas(boolean success) {
        when(inboxClient.has(anyLong(), anyString(), any(ClientInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(success));
    }

    protected void mockInboxCreate(boolean success) {
        when(inboxClient.create(anyLong(), anyString(), any(ClientInfo.class)))
            .thenReturn(
                CompletableFuture.completedFuture(CreateInboxReply.newBuilder()
                    .setResult(success ? CreateInboxReply.Result.OK : CreateInboxReply.Result.ERROR)
                    .build())
            );
    }

    protected void mockInboxDelete(boolean success) {
        when(inboxClient.delete(anyLong(), anyString(), any(ClientInfo.class)))
            .thenReturn(
                CompletableFuture.completedFuture(DeleteInboxReply.newBuilder()
                    .setResult(success ? DeleteInboxReply.Result.OK : DeleteInboxReply.Result.ERROR)
                    .build())
            );
    }

    protected void mockInboxCommit(QoS qoS) {
        when(inboxReader.commit(anyLong(), eq(qoS), anyLong()))
            .thenReturn(
                CompletableFuture.completedFuture(
                    CommitReply.newBuilder().setResult(CommitReply.Result.OK).build()
                )
            );
    }

    protected void mockDistClear(boolean success) {
        when(inboxClient.getDelivererKey(anyString(), any(ClientInfo.class))).thenReturn(delivererKey);
        when(distClient.clear(anyLong(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(
                success ? CompletableFuture.completedFuture(null) :
                    CompletableFuture.failedFuture(new RuntimeException("Mock error"))
            );
    }

    protected void mockDistSub(QoS qos, boolean success) {
        int subResult;
        if (success) {
            subResult = qos.getNumber();
        } else {
            subResult = 0x80;
        }
        when(distClient.sub(anyLong(), anyString(), anyString(), eq(qos), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(subResult));
    }

    protected void mockDistUnSub(boolean... success) {
        CompletableFuture<Boolean>[] unsubResults = new CompletableFuture[success.length];
        for (int i = 0; i < success.length; i++) {
            unsubResults[i] = success[i] ? CompletableFuture.completedFuture(true)
                : CompletableFuture.failedFuture(new RuntimeException("InternalError"));
        }
        OngoingStubbing<CompletableFuture<Boolean>> ongoingStubbing =
            when(distClient.unsub(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()));
        for (CompletableFuture<Boolean> result : unsubResults) {
            ongoingStubbing = ongoingStubbing.thenReturn(result);
        }
    }

    protected void mockDistDist(boolean success) {
        when(distClient.pub(anyLong(), anyString(), any(QoS.class), any(ByteBuffer.class), anyInt(),
            any(ClientInfo.class)))
            .thenReturn(success ? CompletableFuture.completedFuture(null) :
                CompletableFuture.failedFuture(new RuntimeException("Mock error")));
    }

    protected void mockDistDrop() {
        when(distClient.pub(anyLong(), anyString(), any(QoS.class), any(ByteBuffer.class), anyInt(),
            any(ClientInfo.class)))
            .thenReturn(CompletableFuture.failedFuture(DropException.EXCEED_LIMIT));
    }

    protected void mockSessionReg() {
        when(sessionDictClient.reg(any(ClientInfo.class))).thenReturn(kickStream);
        when(kickStream.msg()).thenReturn(kickSubject);
    }

    protected void mockInboxReader() {
        when(inboxClient.getDelivererKey(anyString(), any(ClientInfo.class))).thenReturn(delivererKey);
        when(inboxClient.openInboxReader(anyString(), anyString(), any(ClientInfo.class))).thenReturn(inboxReader);
        doAnswer(invocationOnMock -> {
            inboxFetchConsumer = invocationOnMock.getArgument(0);
            return null;
        }).when(inboxReader).fetch(any(Consumer.class));
        lenient().doAnswer(invocationOnMock -> {
            fetchHints.add(invocationOnMock.getArgument(0));
            return null;
        }).when(inboxReader).hint(anyInt());
    }

    protected void mockRetainMatch() {
        when(retainClient.match(anyLong(), anyString(), anyString(), anyInt(), any(ClientInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(
                MatchReply.newBuilder().setResult(MatchReply.Result.OK).build()
            ));
    }

    protected void mockRetainPipeline(RetainReply.Result result) {
        when(retainClient.open(any(ClientInfo.class))).thenReturn(retainPipeline);
        when(retainPipeline.retain(anyLong(), anyString(), any(QoS.class), any(ByteBuffer.class), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(RetainReply.newBuilder().setResult(result).build()));
    }

    protected void verifyEvent(int count, EventType... types) {
        ArgumentCaptor<Event> eventArgumentCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventCollector, times(count)).report(eventArgumentCaptor.capture());
        assertArrayEquals(types, eventArgumentCaptor.getAllValues().stream().map(Event::type).toArray());
    }

    protected MqttConnAckMessage connectAndVerify(boolean cleanSession) {
        return connectAndVerify(cleanSession, false, 0);
    }

    protected MqttConnAckMessage connectAndVerify(boolean cleanSession, boolean hasInbox) {
        return connectAndVerify(cleanSession, hasInbox, 0);
    }

    protected MqttConnAckMessage connectAndVerify(boolean cleanSession,
                                                  boolean hasInbox,
                                                  int keepAliveInSec) {
        return connectAndVerify(cleanSession, hasInbox, keepAliveInSec, false);
    }

    protected MqttConnAckMessage connectAndVerify(boolean cleanSession,
                                                  boolean hasInbox,
                                                  int keepAliveInSec,
                                                  boolean willMessage) {
        return connectAndVerify(cleanSession, hasInbox, keepAliveInSec, willMessage, false);
    }

    protected MqttConnAckMessage connectAndVerify(boolean cleanSession,
                                                  boolean hasInbox,
                                                  int keepAliveInSec,
                                                  boolean willMessage,
                                                  boolean willRetain) {
        mockAuthPass();
        mockSessionReg();
        mockInboxHas(hasInbox);
        if (cleanSession && hasInbox) {
            mockInboxDelete(true);
            mockDistClear(true);
        }
        if (!cleanSession) {
            mockInboxReader();
            if (!hasInbox) {
                mockInboxCreate(true);
            }
        }
        MqttConnectMessage connectMessage;
        if (!willMessage) {
            connectMessage = MQTTMessageUtils.mqttConnectMessage(cleanSession, clientId, keepAliveInSec);
        } else {
            if (!willRetain) {
                connectMessage = MQTTMessageUtils.qoSWillMqttConnectMessage(1, cleanSession);
            } else {
                connectMessage = MQTTMessageUtils.willRetainMqttConnectMessage(1, cleanSession);
            }
        }
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);
        if (!cleanSession && hasInbox) {
            assertTrue(ackMessage.variableHeader().isSessionPresent());
        }
        return ackMessage;
    }

}
