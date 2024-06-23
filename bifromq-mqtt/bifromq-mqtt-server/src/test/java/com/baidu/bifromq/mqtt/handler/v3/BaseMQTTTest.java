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

package com.baidu.bifromq.mqtt.handler.v3;

import com.baidu.bifromq.dist.client.DistResult;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.client.MatchResult;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.rpc.proto.AttachReply;
import com.baidu.bifromq.inbox.rpc.proto.CommitReply;
import com.baidu.bifromq.inbox.rpc.proto.CreateReply;
import com.baidu.bifromq.inbox.rpc.proto.DetachReply;
import com.baidu.bifromq.inbox.rpc.proto.ExpireReply;
import com.baidu.bifromq.inbox.rpc.proto.GetReply;
import com.baidu.bifromq.inbox.rpc.proto.SubReply;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.InboxVersion;
import com.baidu.bifromq.mqtt.handler.ChannelAttrs;
import com.baidu.bifromq.mqtt.handler.ConditionalRejectHandler;
import com.baidu.bifromq.mqtt.handler.MQTTMessageDebounceHandler;
import com.baidu.bifromq.mqtt.handler.MQTTPreludeHandler;
import com.baidu.bifromq.mqtt.handler.condition.HeapMemPressureCondition;
import com.baidu.bifromq.mqtt.service.ILocalDistService;
import com.baidu.bifromq.mqtt.service.ILocalSessionRegistry;
import com.baidu.bifromq.mqtt.service.LocalDistService;
import com.baidu.bifromq.mqtt.service.LocalSessionRegistry;
import com.baidu.bifromq.mqtt.session.MQTTSessionContext;
import com.baidu.bifromq.mqtt.utils.MQTTMessageUtils;
import com.baidu.bifromq.mqtt.utils.TestTicker;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.authprovider.type.CheckResult;
import com.baidu.bifromq.plugin.authprovider.type.Denied;
import com.baidu.bifromq.plugin.authprovider.type.Granted;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import com.baidu.bifromq.plugin.authprovider.type.Ok;
import com.baidu.bifromq.plugin.authprovider.type.Reject;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.EventType;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.retain.rpc.proto.MatchReply;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import com.baidu.bifromq.sessiondict.client.ISessionRegister;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import com.bifromq.plugin.resourcethrottler.IResourceThrottler;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Tags;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

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

public abstract class BaseMQTTTest {

    @Mock
    protected IAuthProvider authProvider;
    @Mock
    protected IEventCollector eventCollector;
    @Mock
    protected IResourceThrottler resourceThrottler;
    @Mock
    protected ISettingProvider settingProvider;
    @Mock
    protected IDistClient distClient;
    @Mock
    protected IInboxClient inboxClient;
    @Mock
    protected IRetainClient retainClient;
    @Mock
    protected ISessionDictClient sessionDictClient;
    @Mock
    protected ISessionRegister sessionRegister;
    @Mock
    protected IInboxClient.IInboxReader inboxReader;

    protected AtomicReference<Consumer<ClientInfo>> onKick = new AtomicReference<>();
    protected TestTicker testTicker;
    protected MQTTSessionContext sessionContext;
    protected EmbeddedChannel channel;
    protected String serverId = "testServerId";
    protected String tenantId = "testTenantA";
    protected String userId = "testDeviceKey";
    protected String clientId = "testClientId";
    protected String delivererKey = "testGroupKey";
    protected ILocalSessionRegistry sessionRegistry;
    protected ILocalDistService distService;
    protected String remoteIp = "127.0.0.1";
    protected int remotePort = 8888;
    protected long disconnectDelay = 5000;
    protected Consumer<Fetched> inboxFetchConsumer;
    protected List<Integer> fetchHints = new ArrayList<>();

    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        testTicker = new TestTicker();
        sessionRegistry = new LocalSessionRegistry();
        distService = new LocalDistService(serverId, distClient, resourceThrottler, eventCollector);
        sessionContext = MQTTSessionContext.builder()
                .authProvider(authProvider)
                .eventCollector(eventCollector)
                .resourceThrottler(resourceThrottler)
                .settingProvider(settingProvider)
                .distClient(distClient)
                .inboxClient(inboxClient)
                .retainClient(retainClient)
                .sessionDictClient(sessionDictClient)
                .localSessionRegistry(sessionRegistry)
                .localDistService(distService)
                .defaultKeepAliveTimeSeconds(300)
                .ticker(testTicker)
                .serverId(serverId)
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
                pipeline.addLast(MqttDecoder.class.getName(), new MqttDecoder(256 * 1024)); //256kb
                pipeline.addLast(MQTTMessageDebounceHandler.NAME, new MQTTMessageDebounceHandler());
                pipeline.addLast(ConditionalRejectHandler.NAME,
                        new ConditionalRejectHandler(Sets.newHashSet(HeapMemPressureCondition.INSTANCE),
                                eventCollector));
                pipeline.addLast(MQTTPreludeHandler.NAME, new MQTTPreludeHandler(2));
            }
        };
    }

    protected void mockSettings() {
        Mockito.lenient().when(resourceThrottler.hasResource(anyString(), any())).thenReturn(true);
        Mockito.lenient().when(settingProvider.provide(any(Setting.class), anyString())).thenAnswer(
                invocation -> ((Setting) invocation.getArgument(0)).current(invocation.getArgument(1)));
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

    protected void mockAuthPass(String... attrsKeyValues) {
        Map<String, String> attrsMap = new HashMap<>();
        Tags.of(attrsKeyValues).iterator().forEachRemaining(tag -> attrsMap.put(tag.getKey(), tag.getValue()));
        when(authProvider.auth(any(MQTT3AuthData.class)))
                .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                        .setOk(Ok.newBuilder()
                                .setTenantId(tenantId)
                                .setUserId(userId)
                                .putAllAttrs(attrsMap)
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
        when(authProvider.checkPermission(any(ClientInfo.class), any()))
                .thenReturn(CompletableFuture.completedFuture(allow ?
                        CheckResult.newBuilder()
                                .setGranted(Granted.getDefaultInstance())
                                .build() :
                        CheckResult.newBuilder()
                                .setDenied(Denied.getDefaultInstance())
                                .build()));
    }

    protected void mockInboxGet(boolean success) {
        when(inboxClient.get(any()))
                .thenReturn(CompletableFuture.completedFuture(success ?
                        GetReply.newBuilder().setCode(GetReply.Code.EXIST).build() :
                        GetReply.newBuilder().setCode(GetReply.Code.NO_INBOX).build()));
    }

    protected void mockInboxGet(InboxVersion... inboxVersions) {
        when(inboxClient.get(any()))
                .thenReturn(CompletableFuture.completedFuture(GetReply.newBuilder()
                        .setCode(inboxVersions.length > 0 ? GetReply.Code.EXIST : GetReply.Code.NO_INBOX)
                        .addAllInbox(List.of(inboxVersions))
                        .build()));
    }

    protected void mockInboxGetError() {
        when(inboxClient.get(any()))
                .thenReturn(CompletableFuture.completedFuture(GetReply.newBuilder()
                        .setCode(GetReply.Code.ERROR)
                        .build()));
    }

    protected void mockAttach(AttachReply.Code code) {
        when(inboxClient.attach(any()))
                .thenReturn(CompletableFuture.completedFuture(AttachReply.newBuilder().setCode(code).build()));
    }

    protected void mockDetach(DetachReply.Code code) {
        when(inboxClient.detach(any()))
                .thenReturn(CompletableFuture.completedFuture(DetachReply.newBuilder().setCode(code).build()));
    }

    protected void mockInboxCreate(boolean success) {
        when(inboxClient.create(any()))
                .thenReturn(CompletableFuture.completedFuture(CreateReply.newBuilder()
                        .setCode(success ? CreateReply.Code.OK : CreateReply.Code.ERROR)
                        .build())
                );
    }

    protected void mockInboxCreate(CreateReply.Code code) {
        when(inboxClient.create(any()))
                .thenReturn(CompletableFuture.completedFuture(CreateReply.newBuilder()
                        .setCode(code)
                        .build())
                );
    }

    protected void mockInboxExpire(boolean success) {
        when(inboxClient.expire(any()))
                .thenReturn(CompletableFuture.completedFuture(ExpireReply.newBuilder()
                        .setCode(success ? ExpireReply.Code.OK : ExpireReply.Code.ERROR)
                        .build()));
    }

    protected void mockInboxExpire(ExpireReply.Code code) {
        when(inboxClient.expire(any()))
                .thenReturn(CompletableFuture.completedFuture(ExpireReply.newBuilder()
                        .setCode(code)
                        .build()));
    }

    protected void mockInboxCommit(CommitReply.Code code) {
        when(inboxClient.commit(any()))
                .thenReturn(
                        CompletableFuture.completedFuture(CommitReply.newBuilder().setCode(code).build()));
    }

    protected void mockDistMatch(QoS qos, boolean success) {
        when(distClient.match(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(success ? MatchResult.OK : MatchResult.ERROR));
    }

    protected void mockDistMatch(String topic, boolean success) {
        when(distClient.match(anyLong(), anyString(), eq(topic), anyString(), anyString(), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(success ? MatchResult.OK : MatchResult.ERROR));
    }


    protected void mockInboxSub(QoS qos, boolean success) {
        when(inboxClient.sub(any())).thenReturn(CompletableFuture.completedFuture(
                SubReply.newBuilder()
                        .setCode(success ? SubReply.Code.OK : SubReply.Code.ERROR)
                        .build()));
    }

    protected void mockDistUnmatch(boolean... success) {
        CompletableFuture<UnmatchResult>[] unsubResults = new CompletableFuture[success.length];
        for (int i = 0; i < success.length; i++) {
            unsubResults[i] = success[i] ? CompletableFuture.completedFuture(UnmatchResult.OK)
                    : CompletableFuture.failedFuture(new RuntimeException("InternalError"));
        }
        OngoingStubbing<CompletableFuture<UnmatchResult>> ongoingStubbing =
                when(distClient.unmatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()));
        for (CompletableFuture<UnmatchResult> result : unsubResults) {
            ongoingStubbing = ongoingStubbing.thenReturn(result);
        }
    }

    protected void mockDistDist(boolean success) {
        when(distClient.pub(anyLong(), anyString(), any(), any(ClientInfo.class)))
                .thenReturn(CompletableFuture.completedFuture(success ? DistResult.OK : DistResult.ERROR));
    }

    protected void mockDistBackPressure() {
        when(distClient.pub(anyLong(), anyString(), any(), any(ClientInfo.class)))
                .thenReturn(CompletableFuture.completedFuture(DistResult.BACK_PRESSURE_REJECTED));
    }

    protected void mockSessionReg() {
        when(sessionDictClient.reg(any(), any())).thenAnswer(
                (Answer<ISessionRegister>) invocation -> {
                    onKick.set(invocation.getArgument(1));
                    return sessionRegister;
                });
    }

    protected void mockInboxReader() {
        when(inboxClient.openInboxReader(anyString(), anyString(), anyLong())).thenReturn(inboxReader);
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
        when(retainClient.match(any()))
                .thenReturn(CompletableFuture.completedFuture(
                        MatchReply.newBuilder().setResult(MatchReply.Result.OK).build()
                ));
    }

    protected void mockRetainPipeline(RetainReply.Result result) {
        when(retainClient.retain(anyLong(), anyString(), any(QoS.class), any(ByteString.class), anyInt(),
                any(ClientInfo.class)))
                .thenReturn(CompletableFuture.completedFuture(RetainReply.newBuilder().setResult(result).build()));
    }

    protected void verifyEvent(EventType... types) {
        ArgumentCaptor<Event> eventArgumentCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventCollector, times(types.length)).report(eventArgumentCaptor.capture());
        assertArrayEquals(types, eventArgumentCaptor.getAllValues().stream().map(Event::type).toArray());
    }

    protected void setupTransientSession() {
        mockAuthPass();
        mockSessionReg();
        mockInboxExpire(ExpireReply.Code.NOT_FOUND);
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(true);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);
        verifyEvent(EventType.MQTT_SESSION_START, EventType.CLIENT_CONNECTED);
    }

    protected void setupPersistentSession() {
        mockAuthPass();
        mockSessionReg();
        mockInboxGet();
        mockInboxCreate(CreateReply.Code.OK);
        mockInboxReader();
        MqttConnectMessage connectMessage = MQTTMessageUtils.mqttConnectMessage(false);
        channel.writeInbound(connectMessage);
        channel.runPendingTasks();
        MqttConnAckMessage ackMessage = channel.readOutbound();
        assertEquals(ackMessage.variableHeader().connectReturnCode(), CONNECTION_ACCEPTED);
        verifyEvent(EventType.CLIENT_CONNECTED);
    }

    protected MqttConnAckMessage connectAndVerify(boolean cleanSession,
                                                  boolean hasInbox,
                                                  int keepAliveInSec,
                                                  boolean willMessage,
                                                  boolean willRetain) {
        mockAuthPass();
        mockSessionReg();
        mockInboxGet(hasInbox);
        if (cleanSession && hasInbox) {
            mockInboxExpire(true);
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
