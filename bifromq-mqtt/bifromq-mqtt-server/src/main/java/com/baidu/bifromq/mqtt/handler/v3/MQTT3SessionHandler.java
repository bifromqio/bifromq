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

package com.baidu.bifromq.mqtt.handler.v3;

import static com.baidu.bifromq.metrics.TrafficMetric.MqttChannelLatency;
import static com.baidu.bifromq.metrics.TrafficMetric.MqttConnectCount;
import static com.baidu.bifromq.metrics.TrafficMetric.MqttDisconnectCount;
import static com.baidu.bifromq.metrics.TrafficMetric.MqttQoS0DistBytes;
import static com.baidu.bifromq.metrics.TrafficMetric.MqttQoS0EgressBytes;
import static com.baidu.bifromq.metrics.TrafficMetric.MqttQoS0IngressBytes;
import static com.baidu.bifromq.metrics.TrafficMetric.MqttQoS0InternalLatency;
import static com.baidu.bifromq.metrics.TrafficMetric.MqttQoS1DeliverBytes;
import static com.baidu.bifromq.metrics.TrafficMetric.MqttQoS1DistBytes;
import static com.baidu.bifromq.metrics.TrafficMetric.MqttQoS1EgressBytes;
import static com.baidu.bifromq.metrics.TrafficMetric.MqttQoS1ExternalLatency;
import static com.baidu.bifromq.metrics.TrafficMetric.MqttQoS1IngressBytes;
import static com.baidu.bifromq.metrics.TrafficMetric.MqttQoS1InternalLatency;
import static com.baidu.bifromq.metrics.TrafficMetric.MqttQoS2DeliverBytes;
import static com.baidu.bifromq.metrics.TrafficMetric.MqttQoS2DistBytes;
import static com.baidu.bifromq.metrics.TrafficMetric.MqttQoS2EgressBytes;
import static com.baidu.bifromq.metrics.TrafficMetric.MqttQoS2ExternalLatency;
import static com.baidu.bifromq.metrics.TrafficMetric.MqttQoS2IngressBytes;
import static com.baidu.bifromq.metrics.TrafficMetric.MqttQoS2InternalLatency;
import static com.baidu.bifromq.mqtt.handler.v3.MQTTSessionIdUtil.userSessionId;
import static com.baidu.bifromq.mqtt.utils.AuthUtil.buildPubAction;
import static com.baidu.bifromq.mqtt.utils.AuthUtil.buildSubAction;
import static com.baidu.bifromq.mqtt.utils.AuthUtil.buildUnsubAction;
import static com.baidu.bifromq.mqtt.utils.TopicUtil.parseTopicFilter;
import static com.baidu.bifromq.plugin.eventcollector.EventType.ACCESS_CONTROL_ERROR;
import static com.baidu.bifromq.plugin.eventcollector.EventType.BAD_PACKET;
import static com.baidu.bifromq.plugin.eventcollector.EventType.BY_CLIENT;
import static com.baidu.bifromq.plugin.eventcollector.EventType.BY_SERVER;
import static com.baidu.bifromq.plugin.eventcollector.EventType.CLIENT_CHANNEL_ERROR;
import static com.baidu.bifromq.plugin.eventcollector.EventType.CLIENT_CONNECTED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.DISCARD;
import static com.baidu.bifromq.plugin.eventcollector.EventType.IDLE;
import static com.baidu.bifromq.plugin.eventcollector.EventType.INVALID_TOPIC;
import static com.baidu.bifromq.plugin.eventcollector.EventType.INVALID_TOPIC_FILTER;
import static com.baidu.bifromq.plugin.eventcollector.EventType.KICKED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.MATCH_RETAIN_ERROR;
import static com.baidu.bifromq.plugin.eventcollector.EventType.MSG_RETAINED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.MSG_RETAINED_ERROR;
import static com.baidu.bifromq.plugin.eventcollector.EventType.NO_PUB_PERMISSION;
import static com.baidu.bifromq.plugin.eventcollector.EventType.PING_REQ;
import static com.baidu.bifromq.plugin.eventcollector.EventType.PROTOCOL_VIOLATION;
import static com.baidu.bifromq.plugin.eventcollector.EventType.PUB_ACKED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.PUB_ACK_DROPPED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.PUB_ACTION_DISALLOW;
import static com.baidu.bifromq.plugin.eventcollector.EventType.PUB_PERMISSION_CHECK_ERROR;
import static com.baidu.bifromq.plugin.eventcollector.EventType.PUB_RECED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.PUB_REC_DROPPED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS0_DIST_ERROR;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS0_DROPPED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS0_PUSHED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS1_CONFIRMED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS1_DIST_ERROR;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS1_DROPPED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS1_PUSHED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS2_CONFIRMED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS2_DIST_ERROR;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS2_DROPPED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS2_PUSHED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS2_RECEIVED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.RETAIN_MSG_CLEARED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.SUB_ACKED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.SUB_ACTION_DISALLOW;
import static com.baidu.bifromq.plugin.eventcollector.EventType.TOO_LARGE_SUBSCRIPTION;
import static com.baidu.bifromq.plugin.eventcollector.EventType.TOO_LARGE_UNSUBSCRIPTION;
import static com.baidu.bifromq.plugin.eventcollector.EventType.UNSUB_ACKED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.UNSUB_ACTION_DISALLOW;
import static com.baidu.bifromq.plugin.eventcollector.EventType.WILL_DISTED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.WILL_DIST_ERROR;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.plugin.settingprovider.Setting.ByPassPermCheckError;
import static com.baidu.bifromq.plugin.settingprovider.Setting.DebugModeEnabled;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicFiltersPerSub;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicLength;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicLevelNameLength;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicLevels;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MsgPubPerSec;
import static com.baidu.bifromq.plugin.settingprovider.Setting.RetainEnabled;
import static com.baidu.bifromq.plugin.settingprovider.Setting.RetainMessageMatchLimit;
import static com.baidu.bifromq.type.QoS.AT_LEAST_ONCE;
import static com.baidu.bifromq.type.QoS.AT_MOST_ONCE;
import static com.baidu.bifromq.type.QoS.EXACTLY_ONCE;
import static java.util.concurrent.CompletableFuture.allOf;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.dist.client.UnsubResult;
import com.baidu.bifromq.metrics.TrafficMeter;
import com.baidu.bifromq.mqtt.handler.MPSThrottler;
import com.baidu.bifromq.mqtt.handler.MQTTMessageHandler;
import com.baidu.bifromq.mqtt.handler.SendBufferCapacityHinter;
import com.baidu.bifromq.mqtt.handler.event.ConnectionWillClose;
import com.baidu.bifromq.mqtt.session.v3.IMQTT3Session;
import com.baidu.bifromq.mqtt.utils.MQTTMessageSizer;
import com.baidu.bifromq.mqtt.utils.TopicUtil;
import com.baidu.bifromq.plugin.authprovider.CheckResult;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.PingReq;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.accessctrl.AccessControlError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.accessctrl.PubActionDisallow;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.accessctrl.SubActionDisallow;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.accessctrl.UnsubActionDisallow;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientconnected.ClientConnected;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.BadPacket;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ByClient;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ByServer;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ClientChannelError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.Idle;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.InvalidTopic;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.InvalidTopicFilter;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.Kicked;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.NoPubPermission;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ProtocolViolation;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.PubPermissionCheckError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.TooLargeSubscription;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.TooLargeUnsubscription;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.Discard;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS0DistError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS1DistError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS1PubAckDropped;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS1PubAcked;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS2DistError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS2PubRecDropped;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS2PubReced;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.WillDistError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.WillDisted;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.DropReason;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS0Dropped;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS0Pushed;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS1Confirmed;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS1Dropped;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS1Pushed;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS2Confirmed;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS2Dropped;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS2Pushed;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS2Received;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.retainhandling.MatchRetainError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.retainhandling.MsgRetained;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.retainhandling.MsgRetainedError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.retainhandling.RetainMsgCleared;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.subhandling.SubAcked;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.subhandling.UnsubAcked;
import com.baidu.bifromq.retain.client.IRetainServiceClient;
import com.baidu.bifromq.sessiondict.rpc.proto.Ping;
import com.baidu.bifromq.sessiondict.rpc.proto.Quit;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessage;
import io.micrometer.core.instrument.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapIterator;
import org.apache.commons.collections4.map.LinkedMap;

@Slf4j
abstract class MQTT3SessionHandler extends MQTTMessageHandler implements IMQTT3Session {
    public static final String NAME = "MQTT3SessionHandler";
    private static final CompletableFuture<Void> DONE = CompletableFuture.completedFuture(null);
    private final ClientInfo clientInfo;
    private final int keepAliveTimeSeconds;
    private final long idleTimeoutNanos;
    private final boolean cleanSession;
    private final boolean sessionPresent;
    private final WillMessage willMessage;
    private final LinkedMap<Integer, UnconfirmedQoS1Message> unconfirmedQoS1Messages = new LinkedMap<>();
    // key: id used in qos2 protocol interaction, value: message's original messageId
    private final LinkedMap<Integer, QoS2MessageKey> unconfirmedQoS2Indexes = new LinkedMap<>();
    // key: message's original clientInfo and messageId and des topicFilter(for retain messages)
    private final LinkedMap<QoS2MessageKey, UnconfirmedQoS2Message> unconfirmedQoS2Messages = new LinkedMap<>();
    private final TrafficMeter trafficMeter;
    protected boolean debugMode;
    protected SendBufferCapacityHinter bufferCapacityHinter;
    private ScheduledFuture<?> idleTimeoutTask;
    private ScheduledFuture<?> resendUnconfirmedTask;
    private MPSThrottler throttler;
    private IRPCClient.IMessageStream<Quit, Ping> sessionDictEntry;
    private Disposable sessionKickDisposable;
    private int publishPacketId = 0;
    private long lastActiveAtNanos;

    protected MQTT3SessionHandler(ClientInfo clientInfo, int keepAliveTimeSeconds, boolean cleanSession,
                                  boolean sessionPresent, WillMessage willMessage) {
        this.clientInfo = clientInfo;
        this.keepAliveTimeSeconds = keepAliveTimeSeconds;
        this.idleTimeoutNanos = Duration.ofMillis(keepAliveTimeSeconds * 1500L).toNanos(); // x1.5
        this.cleanSession = cleanSession;
        this.willMessage = willMessage;
        this.sessionPresent = sessionPresent;
        this.trafficMeter = TrafficMeter.get(clientInfo.getTrafficId());
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        super.handlerAdded(ctx);
        sessionCtx.localSessionRegistry.add(channelId(), this);
        bufferCapacityHinter = new SendBufferCapacityHinter(ctx.channel(),
            () -> unconfirmedQoS1Messages.size() + unconfirmedQoS2Messages.size(), 0.2f);
        int mps = settingProvider.provide(MsgPubPerSec, clientInfo);
        debugMode = settingProvider.provide(DebugModeEnabled, clientInfo);
        throttler = new MPSThrottler(Math.max(mps, 1));
        sessionDictEntry = sessionCtx.sessionDictClient.reg(clientInfo);
        sessionKickDisposable = sessionDictEntry.msg()
            .observeOn(Schedulers.from(ctx.channel().eventLoop()))
            .subscribe(quit -> {
                log.trace("Received quit request:reqId={},killer={}", quit.getReqId(), quit.getKiller());
                closeConnectionNow(getLocal(KICKED, Kicked.class)
                    .kicker(quit.getKiller()).clientInfo(clientInfo));
            });
        trafficMeter.recordCount(MqttConnectCount);
        lastActiveAtNanos = sessionCtx.nanoTime();
        idleTimeoutTask = ctx.channel().eventLoop()
            .scheduleAtFixedRate(this::checkIdle, idleTimeoutNanos, idleTimeoutNanos, TimeUnit.NANOSECONDS);

        // report client connected event
        eventCollector.report(getLocal(CLIENT_CONNECTED, ClientConnected.class).clientInfo(clientInfo)
            .serverId(sessionCtx.serverId)
            .userSessionId(userSessionId(clientInfo))
            .keepAliveTimeSeconds(keepAliveTimeSeconds)
            .cleanSession(cleanSession)
            .sessionPresent(sessionPresent)
            .lastWill(willMessage != null ? new ClientConnected.WillInfo().topic(willMessage.topic)
                .isRetain(willMessage.retain)
                .qos(willMessage.qos)
                .payload(willMessage.payload.nioBuffer()) : null));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        super.channelInactive(ctx);
        sessionCtx.closeClientRetainPipeline(clientInfo);
        sessionCtx.localSessionRegistry.remove(channelId(), this);
        sessionKickDisposable.dispose();
        sessionDictEntry.close();
        trafficMeter.recordCount(MqttDisconnectCount);
    }

    @Override
    public final void channelRead(ChannelHandlerContext ctx, Object msg) {
        assert msg instanceof MqttMessage;
        MqttMessage mqttMessage = (MqttMessage) msg;
        if (mqttMessage.decoderResult().isSuccess()) {
            lastActiveAtNanos = sessionCtx.nanoTime();
            log.trace("Received mqtt message:{}", mqttMessage);
            switch (mqttMessage.fixedHeader().messageType()) {
                case CONNECT: {
                    // according to [MQTT-3.1.0-2]
                    closeConnectionWithSomeDelay(
                        getLocal(PROTOCOL_VIOLATION, ProtocolViolation.class)
                            .statement("MQTT-3.1.0-2")
                            .clientInfo(clientInfo));
                    break;
                }
                case DISCONNECT: {
                    closeConnectionNow(getLocal(BY_CLIENT, ByClient.class).clientInfo(clientInfo));
                    break;
                }
                case PINGREQ: {
                    writeAndFlush(MqttMessage.PINGRESP);
                    if (debugMode) {
                        eventCollector.report(getLocal(PING_REQ, PingReq.class).pong(true).clientInfo(clientInfo));
                    }
                    break;
                }
                case SUBSCRIBE:
                    handleSubMsg((MqttSubscribeMessage) mqttMessage);
                    break;
                case UNSUBSCRIBE:
                    handleUnsubMsg((MqttUnsubscribeMessage) mqttMessage);
                    break;
                case PUBLISH:
                    handlePubMsg((MqttPublishMessage) mqttMessage);
                    break;
                case PUBACK:
                    handlePubAckMsg((MqttPubAckMessage) mqttMessage);
                    break;
                case PUBREC:
                    handlePubRecMsg(mqttMessage);
                    break;
                case PUBREL:
                    handlePubRelMsg(mqttMessage);
                    break;
                case PUBCOMP:
                    handlePubCompMsg(mqttMessage);
                    break;
                default:
                    ctx.fireChannelRead(msg);
            }
        } else {
            closeConnectionNow(getLocal(BAD_PACKET, BadPacket.class)
                .cause(mqttMessage.decoderResult().cause())
                .clientInfo(clientInfo));
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        bufferCapacityHinter.onWritabilityChanged();
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("ctx: {}, cause:", ctx, cause);
        // if disconnection is caused purely by channel error
        closeConnectionNow(getLocal(CLIENT_CHANNEL_ERROR, ClientChannelError.class)
            .clientInfo(clientInfo).cause(cause));
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof ConnectionWillClose) {
            if (idleTimeoutTask != null) {
                idleTimeoutTask.cancel(true);
            }
            if (resendUnconfirmedTask != null) {
                resendUnconfirmedTask.cancel(true);
            }
            log.debug("Session closed: lwt={}, reason={}", willMessage, ((ConnectionWillClose) evt).reason);
            // don't send last will if disconnect by client, MQTT Spec 3.1.2.5 or kicked
            if (willMessage != null && !(((ConnectionWillClose) evt).reason instanceof ByClient)
                && (!(((ConnectionWillClose) evt).reason instanceof Kicked)
                || !isSelfKick((Kicked) ((ConnectionWillClose) evt).reason))) {
                submitBgTask(() -> distWillMessage(willMessage));
            }
        }
        ctx.fireUserEventTriggered(evt);
    }

    @Override
    public String channelId() {
        return ctx.channel().id().asLongText();
    }

    @Override
    public ClientInfo clientInfo() {
        return clientInfo;
    }

    public boolean sessionPresent() {
        return sessionPresent;
    }

    private void handlePubMsg(MqttPublishMessage mqttMessage) {
        long reqId = mqttMessage.variableHeader().packetId() > 0 ? mqttMessage.variableHeader()
            .packetId() : System.nanoTime();
        String topic = mqttMessage.variableHeader().topicName();
        ByteBuf payload = mqttMessage.payload();
        boolean isRetain = mqttMessage.fixedHeader().isRetain();
        if (!throttler.pass()) {
            eventCollector.report(getLocal(DISCARD, Discard.class).reqId(reqId)
                .topic(mqttMessage.variableHeader().topicName())
                .size(payload.readableBytes())
                .rateLimit(throttler.rateLimit())
                .qos(QoS.forNumber(mqttMessage.fixedHeader().qosLevel().value()))
                .clientInfo(clientInfo));
            mqttMessage.release();
            return;
        }
        if (!TopicUtil.isValidTopic(topic,
            settingProvider.provide(MaxTopicLevelNameLength, clientInfo),
            settingProvider.provide(MaxTopicLevels, clientInfo),
            settingProvider.provide(MaxTopicLength, clientInfo))) {
            closeConnectionWithSomeDelay(getLocal(INVALID_TOPIC, InvalidTopic.class)
                .topic(topic)
                .clientInfo(clientInfo));
            mqttMessage.release();
            return;
        }
        switch (mqttMessage.fixedHeader().qosLevel()) {
            case AT_MOST_ONCE:
                if (mqttMessage.fixedHeader().isDup()) {
                    // ignore the QoS = 0 Dup = 1 messages according to [MQTT-3.3.1-2]
                    closeConnectionWithSomeDelay(getLocal(PROTOCOL_VIOLATION, ProtocolViolation.class)
                        .statement("MQTT-3.3.1-2")
                        .clientInfo(clientInfo));
                    mqttMessage.release();
                    return;
                }
                log.trace("Checking authorization of pub qos0 action: reqId={}, sessionId={}, topic={}", reqId,
                    userSessionId(clientInfo), topic);
                authProvider.check(clientInfo(), buildPubAction(topic, QoS.AT_MOST_ONCE, isRetain))
                    .thenComposeAsync(checkResult -> {
                        log.trace("Checked authorization of pub qos0 action: reqId={}, sessionId={}, topic={}\n{}",
                            reqId,
                            userSessionId(clientInfo), topic, checkResult);
                        switch (checkResult.type()) {
                            case ERROR:
                                eventCollector.report(getLocal(ACCESS_CONTROL_ERROR, AccessControlError.class)
                                    .clientInfo(clientInfo)
                                    .cause(((CheckResult.Error) checkResult).cause));
                                boolean byPass = settingProvider.provide(ByPassPermCheckError, clientInfo);
                                if (!byPass) {
                                    closeConnectionWithSomeDelay(getLocal(PUB_PERMISSION_CHECK_ERROR,
                                        PubPermissionCheckError.class)
                                        .cause(((CheckResult.Error) checkResult).cause)
                                        .topic(topic)
                                        .qos(QoS.AT_MOST_ONCE)
                                        .retain(isRetain)
                                        .clientInfo(clientInfo));
                                    return DONE;
                                }
                                // fallthrough
                                // treat error as allowed
                            case ALLOW:
                                return distQos0Message(reqId, topic, payload, isRetain);
                            case DISALLOW:
                            default: {
                                log.trace("Unauthorized qos0 topic: reqId={}, sessionId={}, topic={}", reqId,
                                    userSessionId(clientInfo), topic);
                                eventCollector.report(getLocal(PUB_ACTION_DISALLOW, PubActionDisallow.class)
                                    .isLastWill(false)
                                    .topic(topic)
                                    .qos(AT_MOST_ONCE)
                                    .isRetain(isRetain)
                                    .clientInfo(clientInfo));
                                // either make a positive acknowledgement, according to the normal QoS rules,
                                // or close the Network Connection[MQTT-3.3.5-2]
                                // TODO: we choose close connection for now,
                                // or introduce a setting to control the behavior?
                                closeConnectionWithSomeDelay(getLocal(NO_PUB_PERMISSION, NoPubPermission.class)
                                    .topic(topic)
                                    .qos(QoS.AT_MOST_ONCE)
                                    .retain(isRetain)
                                    .clientInfo(clientInfo));
                                return DONE;
                            }
                        }
                    }, ctx.channel().eventLoop()).whenComplete((v, e) -> mqttMessage.release());
                return;
            case AT_LEAST_ONCE:
                log.trace("Checking authorization of pub qos1 action: reqId={}, sessionId={}, topic={}", reqId,
                    userSessionId(clientInfo), topic);
                cancelOnInactive(authProvider.check(clientInfo(),
                    buildPubAction(topic, AT_LEAST_ONCE, isRetain))).thenComposeAsync(checkResult -> {
                    switch (checkResult.type()) {
                        case ERROR:
                            eventCollector.report(getLocal(ACCESS_CONTROL_ERROR, AccessControlError.class)
                                .clientInfo(clientInfo)
                                .cause(((CheckResult.Error) checkResult).cause));
                            boolean byPass = settingProvider.provide(ByPassPermCheckError, clientInfo);
                            if (!byPass) {
                                closeConnectionWithSomeDelay(
                                    getLocal(PUB_PERMISSION_CHECK_ERROR, PubPermissionCheckError.class)
                                        .cause(
                                            ((CheckResult.Error) checkResult).cause)
                                        .topic(topic)
                                        .qos(AT_LEAST_ONCE)
                                        .retain(isRetain)
                                        .clientInfo(clientInfo));
                                return DONE;
                            }
                            // fallthrough
                        case ALLOW: {
                            return distQos1Message(reqId, topic, payload, isRetain,
                                mqttMessage.fixedHeader().isDup()).thenAcceptAsync(ok -> {
                                if (!ok) {
                                    return;
                                }
                                log.trace("Disted qos1 msg: reqId={}, sessionId={}, topic={}", reqId,
                                    userSessionId(clientInfo), topic);
                                boolean sendAck = ctx.channel().isActive() && ctx.channel().isWritable();
                                if (sendAck) {
                                    writeAndFlush(MqttMessageFactory.newMessage(
                                        new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE,
                                            false, 2),
                                        MqttMessageIdVariableHeader.from(mqttMessage.variableHeader().packetId()),
                                        null));
                                    if (debugMode) {
                                        eventCollector.report(getLocal(PUB_ACKED, QoS1PubAcked.class)
                                            .reqId(reqId)
                                            .isDup(mqttMessage.fixedHeader().isDup())
                                            .topic(mqttMessage.variableHeader().topicName())
                                            .size(payload.readableBytes())
                                            .clientInfo(clientInfo));
                                    }
                                } else {
                                    eventCollector.report(getLocal(PUB_ACK_DROPPED, QoS1PubAckDropped.class)
                                        .reqId(reqId)
                                        .isDup(mqttMessage.fixedHeader().isDup())
                                        .topic(mqttMessage.variableHeader().topicName())
                                        .size(payload.readableBytes())
                                        .clientInfo(clientInfo));
                                }
                            }, ctx.channel().eventLoop());
                        }
                        case DISALLOW:
                        default: {
                            log.trace("Unauthorized qos1 topic: reqId={}, sessionId={}, topic={}", reqId,
                                userSessionId(clientInfo), topic);
                            eventCollector.report(getLocal(PUB_ACTION_DISALLOW, PubActionDisallow.class)
                                .isLastWill(false)
                                .topic(topic)
                                .qos(AT_LEAST_ONCE)
                                .isRetain(isRetain)
                                .clientInfo(clientInfo));
                            // TODO: we choose close connection for now,
                            // or introduce a setting to control the behavior?
                            closeConnectionWithSomeDelay(getLocal(NO_PUB_PERMISSION, NoPubPermission.class)
                                .qos(AT_LEAST_ONCE)
                                .topic(topic)
                                .retain(isRetain)
                                .clientInfo(clientInfo));
                            return DONE;
                        }
                    }
                }, ctx.channel().eventLoop()).whenComplete((v, e) -> mqttMessage.release());
                return;
            case EXACTLY_ONCE:
            default:
                if (sessionCtx.isConfirming(clientInfo.getTrafficId(), channelId(),
                    mqttMessage.variableHeader().packetId())) {
                    // duplicated qos2 pub just reply pubrec
                    writeAndFlush(MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 2),
                        MqttMessageIdVariableHeader.from(mqttMessage.variableHeader().packetId()), null));
                    mqttMessage.release();
                    return;
                }
                cancelOnInactive(authProvider.check(clientInfo(),
                    buildPubAction(topic, QoS.EXACTLY_ONCE, isRetain))).thenComposeAsync(checkResult -> {
                    switch (checkResult.type()) {
                        case ERROR:
                            eventCollector.report(getLocal(ACCESS_CONTROL_ERROR, AccessControlError.class)
                                .clientInfo(clientInfo)
                                .cause(((CheckResult.Error) checkResult).cause));
                            boolean byPass = settingProvider.provide(ByPassPermCheckError, clientInfo);
                            if (!byPass) {
                                closeConnectionWithSomeDelay(
                                    getLocal(PUB_PERMISSION_CHECK_ERROR, PubPermissionCheckError.class)
                                        .cause(
                                            ((CheckResult.Error) checkResult).cause)
                                        .topic(topic)
                                        .qos(EXACTLY_ONCE)
                                        .retain(isRetain)
                                        .clientInfo(clientInfo));
                                return DONE;
                            }
                            // fallthrough
                        case ALLOW:
                            return distQoS2Message(reqId, topic, payload, isRetain,
                                mqttMessage.fixedHeader().isDup()).thenAcceptAsync(ok -> {
                                if (!ok) {
                                    return;
                                }
                                log.trace("Disted qos2 msg: reqId={}, sessionId={}, topic={}", reqId,
                                    userSessionId(clientInfo), topic);
                                int packetId = mqttMessage.variableHeader().packetId();
                                sessionCtx.addForConfirming(clientInfo.getTrafficId(), channelId(), packetId);
                                boolean sendPubRec = ctx.channel().isActive() && ctx.channel().isWritable();
                                if (sendPubRec) {
                                    writeAndFlush(MqttMessageFactory.newMessage(
                                        new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE,
                                            false, 2), MqttMessageIdVariableHeader.from(packetId), null));
                                    if (debugMode) {
                                        eventCollector.report(getLocal(PUB_RECED, QoS2PubReced.class)
                                            .reqId(reqId)
                                            .isDup(mqttMessage.fixedHeader().isDup())
                                            .topic(mqttMessage.variableHeader().topicName())
                                            .size(payload.readableBytes())
                                            .clientInfo(clientInfo));
                                    }
                                } else {
                                    eventCollector.report(getLocal(PUB_REC_DROPPED, QoS2PubRecDropped.class)
                                        .reqId(reqId)
                                        .isDup(mqttMessage.fixedHeader().isDup())
                                        .topic(mqttMessage.variableHeader().topicName())
                                        .size(payload.readableBytes())
                                        .clientInfo(clientInfo));
                                }
                            }, ctx.channel().eventLoop());
                        case DISALLOW:
                        default: {
                            log.trace("Unauthorized qos2 topic: reqId={}, sessionId={}, topic={}", reqId,
                                userSessionId(clientInfo), topic);
                            eventCollector.report(getLocal(PUB_ACTION_DISALLOW, PubActionDisallow.class)
                                .isLastWill(false)
                                .topic(topic)
                                .qos(EXACTLY_ONCE)
                                .isRetain(isRetain)
                                .clientInfo(clientInfo));
                            // either make a positive acknowledgement, according to the normal QoS rules,
                            // or close the Network Connection[MQTT-3.3.5-2]
                            // TODO: we choose close connection for now,
                            // or introduce a setting to control the behavior?
                            closeConnectionWithSomeDelay(getLocal(NO_PUB_PERMISSION, NoPubPermission.class)
                                .topic(topic)
                                .qos(QoS.EXACTLY_ONCE)
                                .retain(isRetain)
                                .clientInfo(clientInfo));
                            return DONE;
                        }
                    }
                }, ctx.channel().eventLoop()).whenComplete((v, e) -> mqttMessage.release());
        }
    }

    private void handleSubMsg(MqttSubscribeMessage mqttMessage) {
        log.trace("Received subscribe from client: sessionId={}", userSessionId(clientInfo));
        int messageId = mqttMessage.variableHeader().messageId();
        List<MqttTopicSubscription> topicSubscriptions = mqttMessage.payload().topicSubscriptions();
        if (topicSubscriptions.isEmpty()) {
            // Ignore instead of disconnect [MQTT-3.8.3-3]
            closeConnectionWithSomeDelay(getLocal(PROTOCOL_VIOLATION, ProtocolViolation.class)
                .statement("MQTT-3.8.3-3")
                .clientInfo(clientInfo));
            return;
        }
        int maxSubs = sessionCtx.settingsProvider.provide(MaxTopicFiltersPerSub, clientInfo);
        if (topicSubscriptions.size() > maxSubs) {
            closeConnectionWithSomeDelay(getLocal(TOO_LARGE_SUBSCRIPTION, TooLargeSubscription.class)
                .actual(topicSubscriptions.size())
                .max(maxSubs)
                .clientInfo(clientInfo));
            return;
        }
        Optional<MqttTopicSubscription> invalidTopicSub = topicSubscriptions.stream()
            .filter(s -> !TopicUtil.isValidTopicFilter(s.topicName(),
                settingProvider.provide(MaxTopicLevelNameLength, clientInfo),
                settingProvider.provide(MaxTopicLevels, clientInfo),
                settingProvider.provide(MaxTopicLength, clientInfo)))
            .findFirst();
        if (invalidTopicSub.isPresent()) {
            closeConnectionWithSomeDelay(getLocal(INVALID_TOPIC_FILTER, InvalidTopicFilter.class)
                .topicFilter(invalidTopicSub.get().topicName())
                .clientInfo(clientInfo));
            return;
        }
        List<CompletableFuture<MqttQoS>> subTasks = doSubscribe(messageId, topicSubscriptions);
        allOf(subTasks.toArray(new CompletableFuture[0]))
            .thenApply(v -> subTasks.stream()
                .map(CompletableFuture::join)
                .map(MqttQoS::value)
                .collect(Collectors.toList()))
            .thenAcceptAsync(grantedQoSList -> {
                if (ctx.channel().isActive()) {
                    writeAndFlush(MQTT3MessageUtils.toMqttSubAckMessage(messageId, grantedQoSList));
                }
                eventCollector.report(getLocal(SUB_ACKED, SubAcked.class)
                    .messageId(messageId)
                    .granted(grantedQoSList)
                    .topicFilter(topicSubscriptions.stream()
                        .map(MqttTopicSubscription::topicName)
                        .collect(Collectors.toList()))
                    .clientInfo(clientInfo));
            }, ctx.channel().eventLoop());

    }

    private void handleUnsubMsg(MqttUnsubscribeMessage mqttMessage) {
        int messageId = mqttMessage.variableHeader().messageId();
        List<String> topicFilters = mqttMessage.payload().topics();
        if (topicFilters.isEmpty()) {
            // Ignore instead of disconnect [3.10.3-2]
            closeConnectionWithSomeDelay(
                getLocal(PROTOCOL_VIOLATION, ProtocolViolation.class)
                    .statement("MQTT-3.10.3-2")
                    .clientInfo(clientInfo));
            return;
        }
        int maxUnsubs = sessionCtx.settingsProvider.provide(MaxTopicFiltersPerSub, clientInfo);
        if (topicFilters.size() > maxUnsubs) {
            closeConnectionWithSomeDelay(getLocal(TOO_LARGE_UNSUBSCRIPTION, TooLargeUnsubscription.class)
                .max(maxUnsubs)
                .actual(topicFilters.size())
                .clientInfo(clientInfo));
            return;
        }
        Optional<String> invalidTopicFilter = topicFilters.stream()
            .filter(t -> !TopicUtil.isValidTopicFilter(t,
                settingProvider.provide(MaxTopicLevelNameLength, clientInfo),
                settingProvider.provide(MaxTopicLevels, clientInfo),
                settingProvider.provide(MaxTopicLength, clientInfo)))
            .findFirst();
        if (invalidTopicFilter.isPresent()) {
            closeConnectionWithSomeDelay(getLocal(INVALID_TOPIC_FILTER, InvalidTopicFilter.class)
                .topicFilter(invalidTopicFilter.get())
                .clientInfo(clientInfo));
            return;
        }
        // using subMessageId for reqId to thread every calls
        List<CompletableFuture<UnsubResult>> unsubTasks = doUnsubscribe(messageId, topicFilters);
        allOf(unsubTasks.toArray(new CompletableFuture[0])).thenApply(
                v -> unsubTasks.stream().map(CompletableFuture::join).collect(Collectors.toList()))
            .thenAcceptAsync(unsubReplies -> {
                if (unsubReplies.stream().anyMatch(r -> r.type() != UnsubResult.Type.OK)) {
                    // if any unsub not ok, don't reply, since MQTT3 Unsubscribe response takes no payload
                    return;
                }
                if (ctx.channel().isActive()) {
                    writeAndFlush(MQTT3MessageUtils.toMqttUnsubAckMessage(messageId));
                }
                eventCollector.report(getLocal(UNSUB_ACKED, UnsubAcked.class)
                    .messageId(messageId)
                    .topicFilter(topicFilters)
                    .clientInfo(clientInfo));
            }, ctx.channel().eventLoop());
    }

    private void handlePubAckMsg(MqttPubAckMessage mqttMessage) {
        Integer messageId = mqttMessage.variableHeader().messageId();
        // boxed integer is needed here to force map remove
        UnconfirmedQoS1Message confirmed = unconfirmedQoS1Messages.remove(messageId);
        if (confirmed != null) {
            trafficMeter.recordSummary(MqttQoS1DeliverBytes,
                MQTTMessageSizer.sizePublishMsg(confirmed.topic, confirmed.message));
            if (!confirmed.isRetain) {
                trafficMeter.timer(MqttQoS1ExternalLatency)
                    .record(HLC.INST.getPhysical() - confirmed.message.getTimestamp(), TimeUnit.MILLISECONDS);
                onDistQoS1MessageConfirmed(messageId, confirmed.seq, confirmed.topic, confirmed.message, true);
            }
            if (debugMode) {
                eventCollector.report(getLocal(QOS1_CONFIRMED, QoS1Confirmed.class)
                    .reqId(confirmed.message.getMessageId())
                    .messageId(messageId)
                    .isRetain(confirmed.isRetain)
                    .sender(confirmed.sender)
                    .delivered(true)
                    .topic(confirmed.topic)
                    .matchedFilter(confirmed.topicFilter)
                    .size(confirmed.message.getPayload().size())
                    .clientInfo(clientInfo));
            }
        } else {
            log.trace("No msg to confirm: sessionId={}, packetId={}", userSessionId(clientInfo),
                mqttMessage.variableHeader().messageId());
        }
    }

    private void handlePubRecMsg(MqttMessage mqttMessage) {
        Integer messageId = ((MqttMessageIdVariableHeader) mqttMessage.variableHeader()).messageId();
        // boxed integer is needed here to force map remove
        QoS2MessageKey messageKey = unconfirmedQoS2Indexes.get(messageId);
        if (messageKey == null) {
            log.trace("No msg to confirm: sessionId={}, packetId={}", userSessionId(clientInfo), messageId);
            return;
        }
        UnconfirmedQoS2Message received = unconfirmedQoS2Messages.get(messageKey);
        if (received != null && received.mqttMessage.fixedHeader().messageType() == MqttMessageType.PUBLISH) {
            // the qos2 message has been received but not confirmed semantically
            if (debugMode) {
                eventCollector.report(getLocal(QOS2_RECEIVED, QoS2Received.class)
                    .reqId(received.message.getMessageId())
                    .messageId(messageId)
                    .isRetain(received.isRetain)
                    .sender(received.sender)
                    .topic(received.topic)
                    .matchedFilter(received.topicFilter)
                    .size(received.message.getPayload().size())
                    .clientInfo(clientInfo));
            }
            writeAndFlush(received.received());
        } else {
            log.trace("No msg to confirm: sessionId={}, packetId={}", userSessionId(clientInfo), messageId);
        }
    }

    private void handlePubRelMsg(MqttMessage mqttMessage) {
        int messageId = ((MqttMessageIdVariableHeader) mqttMessage.variableHeader()).messageId();
        sessionCtx.confirm(clientInfo.getTrafficId(), channelId(), messageId);
        writeAndFlush(MqttMessageFactory.newMessage(
            new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 2),
            MqttMessageIdVariableHeader.from(messageId), null));
    }

    private void handlePubCompMsg(MqttMessage mqttMessage) {
        Integer messageId = ((MqttMessageIdVariableHeader) mqttMessage.variableHeader()).messageId();
        // boxed integer is needed here to force map remove
        QoS2MessageKey messageKey = unconfirmedQoS2Indexes.remove(messageId);
        if (messageKey == null) {
            log.trace("No msg to confirm: sessionId={}, packetId={}", userSessionId(clientInfo), messageId);
            return;
        }
        UnconfirmedQoS2Message confirmed = unconfirmedQoS2Messages.remove(messageKey);
        if (confirmed != null && confirmed.mqttMessage.fixedHeader().messageType() == MqttMessageType.PUBREL) {
            trafficMeter.recordSummary(MqttQoS2DeliverBytes,
                MQTTMessageSizer.sizePublishMsg(confirmed.topic, confirmed.message));
            if (!confirmed.isRetain) {
                trafficMeter.timer(MqttQoS2ExternalLatency)
                    .record(HLC.INST.getPhysical() - confirmed.message.getTimestamp(), TimeUnit.MILLISECONDS);
                onDistQoS2MessageConfirmed(messageId, confirmed.seq, confirmed.topic, confirmed.message, true);
            }
            if (debugMode) {
                eventCollector.report(getLocal(QOS2_CONFIRMED, QoS2Confirmed.class)
                    .reqId(confirmed.message.getMessageId())
                    .messageId(messageId)
                    .isRetain(confirmed.mqttMessage.fixedHeader().isRetain())
                    .sender(confirmed.sender)
                    .delivered(true)
                    .topic(confirmed.topic)
                    .matchedFilter(confirmed.topicFilter)
                    .size(confirmed.message.getPayload().size())
                    .clientInfo(clientInfo));
            }
        } else {
            log.trace("No msg to confirm: sessionId={}, packetId={}", userSessionId(clientInfo), messageId);
        }
    }

    private List<CompletableFuture<MqttQoS>> doSubscribe(long reqId, List<MqttTopicSubscription> subs) {
        return subs.stream()
            .map(topicSubscription -> cancelOnInactive(authProvider.check(clientInfo(),
                buildSubAction(topicSubscription.topicName(),
                    QoS.forNumber(topicSubscription.qualityOfService().value())))).thenComposeAsync(
                checkResult -> {
                    switch (checkResult.type()) {
                        case ERROR:
                            eventCollector.report(getLocal(ACCESS_CONTROL_ERROR, AccessControlError.class)
                                .clientInfo(clientInfo)
                                .cause(((CheckResult.Error) checkResult).cause));
                            boolean byPass = settingProvider.provide(ByPassPermCheckError, clientInfo);
                            if (!byPass) {
                                return CompletableFuture.completedFuture(MqttQoS.FAILURE);
                            }
                            // fallthrough
                        case ALLOW:
                            return cancelOnInactive(doSubscribe(reqId, topicSubscription).thenCompose(qos -> {
                                String topicFilter = parseTopicFilter(topicSubscription.topicName());
                                switch (qos) {
                                    case FAILURE:
                                        return CompletableFuture.completedFuture(MqttQoS.FAILURE);
                                    default:
                                        return matchRetainMessages(reqId, topicFilter,
                                            QoS.values()[qos.ordinal()]).thenApply(
                                            ok -> ok ? qos : MqttQoS.FAILURE);
                                }
                            }));
                        case DISALLOW:
                        default: {
                            eventCollector.report(getLocal(SUB_ACTION_DISALLOW, SubActionDisallow.class)
                                .topicFilter(topicSubscription.topicName())
                                .qos(QoS.forNumber(topicSubscription.qualityOfService().value()))
                                .clientInfo(clientInfo));
                            return CompletableFuture.completedFuture(MqttQoS.FAILURE);
                        }
                    }
                }, ctx.channel().eventLoop()))
            .collect(Collectors.toList());
    }

    protected abstract CompletableFuture<MqttQoS> doSubscribe(long reqId, MqttTopicSubscription topicSub);

    private List<CompletableFuture<UnsubResult>> doUnsubscribe(long reqId, List<String> unsubs) {
        return unsubs.stream()
            .map(unsub -> cancelOnInactive(
                authProvider.check(clientInfo(), buildUnsubAction(unsub))).thenComposeAsync(checkResult -> {
                switch (checkResult.type()) {
                    case ERROR:
                        log.warn("Check unsub permission error:{}",
                            ((CheckResult.Error) checkResult).cause.getMessage());
                        eventCollector.report(getLocal(ACCESS_CONTROL_ERROR, AccessControlError.class)
                            .clientInfo(clientInfo)
                            .cause(((CheckResult.Error) checkResult).cause));
                        boolean byPass = settingProvider.provide(ByPassPermCheckError, clientInfo);
                        if (!byPass) {
                            return CompletableFuture.completedFuture(UnsubResult.InternalError);
                        }
                        // fallthrough
                    case ALLOW:
                        return cancelOnInactive(doUnsubscribe(reqId, unsub));
                    case DISALLOW:
                    default: {
                        eventCollector.report(getLocal(UNSUB_ACTION_DISALLOW, UnsubActionDisallow.class)
                            .clientInfo(clientInfo)
                            .topicFilter(unsub));
                        return CompletableFuture.completedFuture(UnsubResult.InternalError);
                    }
                }
            }, ctx.channel().eventLoop()))
            .collect(Collectors.toList());
    }

    protected abstract CompletableFuture<UnsubResult> doUnsubscribe(long reqId, String topicFilter);

    protected abstract void onDistQoS1MessageConfirmed(int messageId, long seq, String topic, Message message,
                                                       boolean delivered);

    protected abstract void onDistQoS2MessageConfirmed(int messageId, long seq, String topic, Message message,
                                                       boolean delivered);

    protected CompletableFuture<Boolean> matchRetainMessages(long reqId, String topicFilter, QoS grantedQoS) {
        log.trace("Matching retain message: reqId={}, topicFilter={}, qos={}", reqId, topicFilter, grantedQoS);
        boolean retainEnabled = settingProvider.provide(RetainEnabled, clientInfo);
        if (!retainEnabled) {
            return CompletableFuture.completedFuture(true);
        }
        return cancelOnInactive(sessionCtx.retainClient.match(reqId, clientInfo.getTrafficId(), topicFilter,
            settingProvider.provide(RetainMessageMatchLimit, clientInfo), clientInfo)).thenApplyAsync(
            matchReply -> {
                log.trace("Finish matching retain message: reqId={}, topicFilter={}, qos={}\n{}", reqId,
                    topicFilter, grantedQoS, matchReply);
                switch (matchReply.getResult()) {
                    case OK:
                        long timestamp = HLC.INST.getPhysical();
                        for (TopicMessage topicMsg : matchReply.getMessagesList()) {
                            String topic = topicMsg.getTopic();
                            Message retained = topicMsg.getMessage();
                            QoS finalQoS = QoS.forNumber(
                                Math.min(retained.getPubQoS().getNumber(), grantedQoS.getNumber()));
                            assert finalQoS != null;
                            switch (finalQoS) {
                                case AT_MOST_ONCE:
                                    if (bufferCapacityHinter.hasCapacity() &&
                                        sendQoS0TopicMessage(topic, retained, true, true, timestamp)) {
                                        if (debugMode) {
                                            eventCollector.report(getLocal(QOS0_PUSHED, QoS0Pushed.class)
                                                .reqId(reqId)
                                                .isRetain(true)
                                                .sender(topicMsg.getSender())
                                                .topic(topic)
                                                .matchedFilter(topicFilter)
                                                .size(retained.getPayload().size())
                                                .clientInfo(clientInfo));
                                        }
                                    } else {
                                        eventCollector.report(
                                            getLocal(QOS0_DROPPED, QoS0Dropped.class)
                                                .reason(DropReason.Overflow)
                                                .reqId(reqId)
                                                .isRetain(true)
                                                .sender(topicMsg.getSender())
                                                .topic(topic)
                                                .matchedFilter(topicFilter)
                                                .size(retained.getPayload().size())
                                                .clientInfo(clientInfo));
                                    }
                                    break;
                                case AT_LEAST_ONCE:
                                    if (bufferCapacityHinter.hasCapacity()) {
                                        // retain message always seq 0
                                        int messageId = sendQoS1TopicMessage(0, topicFilter, topic, retained,
                                            topicMsg.getSender(), true, true, timestamp);
                                        if (messageId < 0) {
                                            log.error("Message id exhausted");
                                        }
                                    } else {
                                        eventCollector.report(
                                            getLocal(QOS1_DROPPED, QoS1Dropped.class)
                                                .reason(DropReason.Overflow)
                                                .reqId(reqId)
                                                .isRetain(true)
                                                .sender(topicMsg.getSender())
                                                .topic(topic)
                                                .matchedFilter(topicFilter)
                                                .size(retained.getPayload().size())
                                                .clientInfo(clientInfo));
                                    }
                                    break;
                                case EXACTLY_ONCE:
                                default:
                                    if (bufferCapacityHinter.hasCapacity()) {
                                        // retain message always seq 0
                                        int messageId =
                                            sendQoS2TopicMessage(0, topicFilter, topic, retained,
                                                topicMsg.getSender(), true, true, timestamp);
                                        if (messageId < 0) {
                                            log.error("Message id exhausted");
                                        }
                                    } else {
                                        eventCollector.report(
                                            getLocal(QOS2_DROPPED, QoS2Dropped.class)
                                                .reason(DropReason.Overflow)
                                                .reqId(reqId)
                                                .isRetain(true)
                                                .sender(topicMsg.getSender())
                                                .topic(topic)
                                                .matchedFilter(topicFilter)
                                                .size(retained.getPayload().size())
                                                .clientInfo(clientInfo));
                                    }
                            }
                        }
                        return true;
                    case ERROR:
                        eventCollector.report(getLocal(MATCH_RETAIN_ERROR, MatchRetainError.class)
                            .reqId(reqId)
                            .topicFilter(topicFilter)
                            .clientInfo(clientInfo));
                        // fallthrough
                    default:
                        return false;
                }
            }, ctx.channel().eventLoop());
    }

    protected boolean sendQoS0TopicMessage(String topic,
                                           Message message,
                                           boolean isRetain,
                                           boolean flush,
                                           long timestamp) {
        trafficMeter.timer(MqttQoS0InternalLatency).record(timestamp - message.getTimestamp(), TimeUnit.MILLISECONDS);
        if (ctx.channel().isActive()) {
            Timer.Sample start = Timer.start();
            ctx.write(new MqttPublishMessage(
                    new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, isRetain, 0),
                    new MqttPublishVariableHeader(topic, 0),
                    Unpooled.wrappedBuffer(message.getPayload().asReadOnlyByteBuffer())))
                .addListener(f -> {
                    if (f.isSuccess()) {
                        start.stop(trafficMeter.timer(MqttChannelLatency));
                    }
                });
            if (flush) {
                flush(true);
            }
            int msgSize = MQTTMessageSizer.sizePublishMsg(topic, message);
            trafficMeter.recordSummary(MqttQoS0EgressBytes, msgSize);
            // FixedHeaderSize = 2, VariableHeaderSize = 4 + topicBytes
            bufferCapacityHinter.onOneMessageBuffered(msgSize);
            return true;
        }
        return false;
    }

    protected int sendQoS1TopicMessage(long seq,
                                       String topicFilter,
                                       String topic,
                                       Message message,
                                       ClientInfo sender,
                                       boolean isRetain,
                                       boolean flush,
                                       long timestamp) {
        int messageId = nextMessageId();
        if (messageId < 0) {
            if (!isRetain) {
                onDistQoS1MessageConfirmed(messageId, seq, topic, message, false);
            }
            if (debugMode) {
                eventCollector.report(getLocal(QOS1_CONFIRMED, QoS1Confirmed.class)
                    .reqId(message.getMessageId())
                    .messageId(messageId)
                    .isRetain(isRetain)
                    .sender(sender)
                    .delivered(false)
                    .topic(topic)
                    .matchedFilter(topicFilter)
                    .size(message.getPayload().size())
                    .clientInfo(clientInfo));
            }
            if (flush) {
                flush(true);
            }
            return messageId;
        }
        UnconfirmedQoS1Message msg = new UnconfirmedQoS1Message(seq, messageId, topicFilter,
            topic, message, isRetain, sender, sessionCtx.nanoTime());
        unconfirmedQoS1Messages.put(messageId, msg);
        trafficMeter.timer(MqttQoS1InternalLatency)
            .record(timestamp - message.getTimestamp(), TimeUnit.MILLISECONDS);
        if (ctx.channel().isActive()) {
            Timer.Sample start = Timer.start();
            ctx.write(msg.buildMQTTMessage(sessionCtx.nanoTime()))
                .addListener(f -> {
                    if (f.isSuccess()) {
                        start.stop(trafficMeter.timer(MqttChannelLatency));
                    }
                });
            if (flush) {
                flush(true);
            }
            if (debugMode) {
                eventCollector.report(getLocal(QOS1_PUSHED, QoS1Pushed.class)
                    .reqId(message.getMessageId())
                    .messageId(messageId)
                    .dup(false)
                    .isRetain(false)
                    .sender(sender)
                    .matchedFilter(topicFilter)
                    .topic(topic)
                    .size(message.getPayload().size())
                    .clientInfo(clientInfo));
            }
            // FixedHeaderSize = 2, VariableHeaderSize = 4 + topicBytes
            int msgSize = MQTTMessageSizer.sizePublishMsg(topic, message);
            trafficMeter.recordSummary(MqttQoS1EgressBytes, msgSize);
            bufferCapacityHinter.onOneMessageBuffered(msgSize);
            if (resendUnconfirmedTask == null) {
                scheduleResend(sessionCtx.resendDelayMillis, TimeUnit.MILLISECONDS);
            }
        }
        return messageId;
    }

    protected int sendQoS2TopicMessage(long seq,
                                       String topicFilter,
                                       String topic,
                                       Message message,
                                       ClientInfo sender,
                                       boolean isRetain,
                                       boolean flush,
                                       long timestamp) {
        QoS2MessageKey messageKey = QoS2MessageKey.builder()
            .messageId(message.getMessageId())
            .sourceClientInfo(sender)
            .topicFilter(topicFilter)
            .build();
        if (unconfirmedQoS2Messages.containsKey(messageKey)) {
            if (flush) {
                flush(true);
            }
            return unconfirmedQoS2Messages.get(messageKey).messageId;
        }
        int messageId = nextMessageId();
        if (messageId < 0) {
            if (!isRetain) {
                onDistQoS2MessageConfirmed(messageId, seq, topic, message, false);
            }
            if (debugMode) {
                eventCollector.report(getLocal(QOS2_CONFIRMED, QoS2Confirmed.class)
                    .reqId(message.getMessageId())
                    .messageId(messageId)
                    .isRetain(isRetain)
                    .sender(sender)
                    .delivered(false)
                    .topic(topic)
                    .matchedFilter(topicFilter)
                    .size(message.getPayload().size())
                    .clientInfo(clientInfo));
            }
            if (flush) {
                flush(true);
            }
            return messageId;
        }
        UnconfirmedQoS2Message msg = new UnconfirmedQoS2Message(seq, messageId, topicFilter, topic,
            message, isRetain, sender, sessionCtx.nanoTime());
        unconfirmedQoS2Indexes.put(messageId, messageKey);
        unconfirmedQoS2Messages.put(messageKey, msg);
        trafficMeter.timer(MqttQoS2InternalLatency)
            .record(timestamp - message.getTimestamp(), TimeUnit.MILLISECONDS);
        if (ctx.channel().isActive()) {
            Timer.Sample start = Timer.start();
            ctx.write(msg.buildMQTTMessage(sessionCtx.nanoTime()))
                .addListener(f -> {
                    if (f.isSuccess()) {
                        start.stop(trafficMeter.timer(MqttChannelLatency));
                    }
                });
            if (flush) {
                flush(true);
            }
            if (debugMode) {
                eventCollector.report(getLocal(QOS2_PUSHED, QoS2Pushed.class)
                    .reqId(message.getMessageId())
                    .messageId(messageId)
                    .dup(false)
                    .isRetain(isRetain)
                    .sender(sender)
                    .topic(topic)
                    .matchedFilter(topicFilter)
                    .size(message.getPayload().size())
                    .clientInfo(clientInfo));
            }
            if (resendUnconfirmedTask == null) {
                scheduleResend(sessionCtx.resendDelayMillis, TimeUnit.MILLISECONDS);
            }
            int msgSize = MQTTMessageSizer.sizePublishMsg(topic, message);
            // FixedHeaderSize = 2, VariableHeaderSize = 4 + topicBytes
            trafficMeter.recordSummary(MqttQoS2EgressBytes, msgSize);
            bufferCapacityHinter.onOneMessageBuffered(msgSize);
        }
        return messageId;
    }

    private void resendUnconfirmed() {
        if (!ctx.channel().isActive()) {
            return;
        }

        long now = sessionCtx.nanoTime();
        long delay = TimeUnit.NANOSECONDS.convert(sessionCtx.resendDelayMillis, TimeUnit.MILLISECONDS);
        // resend qos1
        MapIterator<Integer, UnconfirmedQoS1Message> qos1MsgItr = unconfirmedQoS1Messages.mapIterator();
        while (qos1MsgItr.hasNext()) {
            int messageId = qos1MsgItr.next();
            UnconfirmedQoS1Message unconfirmed = qos1MsgItr.getValue();
            if (unconfirmed.resendTimes > sessionCtx.maxResendTimes) {
                qos1MsgItr.remove();
                if (!unconfirmed.isRetain) {
                    onDistQoS1MessageConfirmed(messageId, unconfirmed.seq, unconfirmed.topic, unconfirmed.message,
                        false);
                }
                if (debugMode) {
                    eventCollector.report(getLocal(QOS1_CONFIRMED, QoS1Confirmed.class)
                        .reqId(unconfirmed.message.getMessageId())
                        .messageId(messageId)
                        .isRetain(unconfirmed.isRetain)
                        .sender(unconfirmed.sender)
                        .delivered(false)
                        .topic(unconfirmed.topic)
                        .matchedFilter(unconfirmed.topicFilter)
                        .size(unconfirmed.message.getPayload().size())
                        .clientInfo(clientInfo));
                }
            } else if (now - unconfirmed.lastPubNanos >= delay) {
                if (ctx.channel().isActive()) {
                    writeAndFlush(unconfirmed.buildMQTTMessage(now));
                    if (debugMode) {
                        eventCollector.report(getLocal(QOS1_PUSHED, QoS1Pushed.class)
                            .reqId(unconfirmed.message.getMessageId())
                            .messageId(messageId)
                            .dup(true)
                            .isRetain(unconfirmed.isRetain)
                            .sender(unconfirmed.sender)
                            .topic(unconfirmed.topic)
                            .matchedFilter(unconfirmed.topicFilter)
                            .size(unconfirmed.message.getPayload().size())
                            .clientInfo(clientInfo));
                    }
                }
            } else {
                delay = Math.min(now - unconfirmed.lastPubNanos, delay);
            }
        }

        // resend qos2
        MapIterator<QoS2MessageKey, UnconfirmedQoS2Message> qos2MsgItr = unconfirmedQoS2Messages.mapIterator();
        while (qos2MsgItr.hasNext()) {
            qos2MsgItr.next();
            UnconfirmedQoS2Message unconfirmed = qos2MsgItr.getValue();
            int messageId = unconfirmed.messageId;
            MqttFixedHeader header = unconfirmed.mqttMessage.fixedHeader();
            if (unconfirmed.resendTimes > sessionCtx.maxResendTimes) {
                qos2MsgItr.remove();
                if (header.messageType() == MqttMessageType.PUBLISH) {
                    if (!unconfirmed.isRetain) {
                        // the qos2 message has been confirmed but not delivered
                        onDistQoS2MessageConfirmed(messageId, unconfirmed.seq, unconfirmed.topic, unconfirmed.message,
                            false);
                    }
                    if (debugMode) {
                        eventCollector.report(getLocal(QOS2_CONFIRMED, QoS2Confirmed.class)
                            .reqId(unconfirmed.message.getMessageId())
                            .messageId(messageId)
                            .isRetain(unconfirmed.isRetain)
                            .sender(unconfirmed.sender)
                            .delivered(false)
                            .topic(unconfirmed.topic)
                            .matchedFilter(unconfirmed.topicFilter)
                            .size(unconfirmed.message.getPayload().size())
                            .clientInfo(clientInfo));
                    }
                }
            } else if (now - unconfirmed.lastSendNanos >= delay) {
                if (ctx.channel().isActive()) {
                    if (header.messageType() == MqttMessageType.PUBLISH) {
                        writeAndFlush(unconfirmed.buildMQTTMessage(now));
                        if (debugMode) {
                            eventCollector.report(getLocal(QOS2_PUSHED, QoS2Pushed.class)
                                .reqId(unconfirmed.message.getMessageId())
                                .messageId(messageId)
                                .dup(true)
                                .isRetain(unconfirmed.isRetain)
                                .sender(unconfirmed.sender)
                                .topic(unconfirmed.topic)
                                .matchedFilter(unconfirmed.topicFilter)
                                .size(unconfirmed.message.getPayload().size())
                                .clientInfo(clientInfo));
                        }
                    } else {
                        writeAndFlush(unconfirmed.mqttMessage);
                    }
                }
            } else {
                delay = Math.min(now - unconfirmed.lastSendNanos, delay);
            }
        }

        if (!unconfirmedQoS1Messages.isEmpty() || !unconfirmedQoS2Messages.isEmpty()) {
            scheduleResend(delay, TimeUnit.NANOSECONDS);
        } else {
            resendUnconfirmedTask = null;
        }
    }

    private void scheduleResend(long delay, TimeUnit unit) {
        resendUnconfirmedTask = ctx.channel().eventLoop().schedule(this::resendUnconfirmed, delay, unit);
    }

    private CompletableFuture<Void> distQos0Message(long reqId, String topic, ByteBuf payload, boolean isRetain) {
        log.trace("Disting qos0 msg: req={}, topic={}, size={}", reqId, topic, payload.readableBytes());
        CompletableFuture<Boolean> retainTask = isRetain ? cancelOnInactive(
            retainMessage(reqId, topic, QoS.AT_MOST_ONCE, payload.duplicate())) : CompletableFuture.completedFuture(
            true);
        trafficMeter.recordSummary(MqttQoS0IngressBytes,
            MQTTMessageSizer.sizePublishMsg(topic, payload.readableBytes()));
        CompletableFuture<Void> distTask = cancelOnInactive(
            sessionCtx.distClient.dist(reqId, topic, AT_MOST_ONCE, payload.duplicate().nioBuffer(),
                    Integer.MAX_VALUE, clientInfo)
                .handleAsync((v, e) -> {
                    if (e != null) {
                        eventCollector.report(getLocal(QOS0_DIST_ERROR, QoS0DistError.class)
                            .reqId(reqId)
                            .topic(topic)
                            .size(payload.readableBytes())
                            .clientInfo(clientInfo));
                    } else {
                        switch (v.type()) {
                            case SUCCEED:
                                log.trace("Qos0 msg published: reqId={}, sessionId={}, topic={}, size={}",
                                    reqId,
                                    userSessionId(clientInfo), topic, payload.readableBytes());
                                trafficMeter.recordSummary(MqttQoS0DistBytes,
                                    MQTTMessageSizer.sizePublishMsg(topic, payload.readableBytes()));
                                break;
                            case ERROR:
                            default:
                                eventCollector.report(getLocal(QOS0_DIST_ERROR, QoS0DistError.class)
                                    .reqId(reqId)
                                    .topic(topic)
                                    .size(payload.readableBytes())
                                    .clientInfo(clientInfo));
                                break;
                        }
                    }
                    return null;
                }, ctx.channel().eventLoop()));
        return allOf(retainTask, distTask);
    }

    private CompletableFuture<Boolean> distQos1Message(long reqId, String topic, ByteBuf payload, boolean isRetain,
                                                       boolean isDup) {
        CompletableFuture<Boolean> retainTask = isRetain ? cancelOnInactive(
            retainMessage(reqId, topic, AT_LEAST_ONCE, payload.duplicate())) : CompletableFuture.completedFuture(
            true);

        trafficMeter.recordSummary(MqttQoS1IngressBytes,
            MQTTMessageSizer.sizePublishMsg(topic, payload.readableBytes()));
        CompletableFuture<Boolean> distTask = cancelOnInactive(
            sessionCtx.distClient.dist(reqId, topic, AT_LEAST_ONCE, payload.duplicate().nioBuffer(),
                    Integer.MAX_VALUE, clientInfo)
                .handleAsync((v, e) -> {
                    if (e != null) {
                        eventCollector.report(getLocal(QOS1_DIST_ERROR, QoS1DistError.class)
                            .reqId(reqId)
                            .isDup(isDup)
                            .topic(topic)
                            .size(payload.readableBytes())
                            .clientInfo(clientInfo));
                        return false;
                    } else {
                        switch (v.type()) {
                            case SUCCEED:
                                log.trace("Qos1 msg published: reqId={}, sessionId={}, topic={}, size={}",
                                    reqId,
                                    userSessionId(clientInfo), topic, payload.readableBytes());
                                trafficMeter.recordSummary(MqttQoS1DistBytes,
                                    MQTTMessageSizer.sizePublishMsg(topic, payload.readableBytes()));

                                return true;
                            case ERROR:
                            default:
                                eventCollector.report(getLocal(QOS1_DIST_ERROR, QoS1DistError.class)
                                    .reqId(reqId)
                                    .isDup(isDup)
                                    .topic(topic)
                                    .size(payload.readableBytes())
                                    .clientInfo(clientInfo));
                                return false;
                        }
                    }
                }, ctx.channel().eventLoop()));
        return allOf(distTask, retainTask).thenApply(v -> distTask.join() && retainTask.join());
    }

    private CompletableFuture<Boolean> distQoS2Message(long reqId, String topic, ByteBuf payload, boolean isRetain,
                                                       boolean isDup) {
        CompletableFuture<Boolean> retainTask = isRetain ? cancelOnInactive(
            retainMessage(reqId, topic, EXACTLY_ONCE, payload.duplicate())) : CompletableFuture.completedFuture(
            true);
        trafficMeter.recordSummary(MqttQoS2IngressBytes,
            MQTTMessageSizer.sizePublishMsg(topic, payload.readableBytes()));
        CompletableFuture<Boolean> distTask = cancelOnInactive(
            sessionCtx.distClient.dist(reqId, topic, EXACTLY_ONCE, payload.duplicate().nioBuffer(),
                    Integer.MAX_VALUE, clientInfo)
                .handleAsync((v, e) -> {
                    if (e != null) {
                        eventCollector.report(getLocal(QOS2_DIST_ERROR, QoS2DistError.class)
                            .reqId(reqId)
                            .isDup(isDup)
                            .topic(topic)
                            .size(payload.readableBytes())
                            .clientInfo(clientInfo));
                        return false;
                    } else {
                        switch (v.type()) {
                            case SUCCEED:
                                log.trace("Qos2 msg published: reqId={}, sessionId={}, topic={}, size={}",
                                    reqId,
                                    userSessionId(clientInfo), topic, payload.readableBytes());
                                trafficMeter.recordSummary(MqttQoS2DistBytes,
                                    MQTTMessageSizer.sizePublishMsg(topic, payload.readableBytes()));
                                return true;
                            case ERROR:
                            default:
                                eventCollector.report(getLocal(QOS2_DIST_ERROR, QoS2DistError.class)
                                    .reqId(reqId)
                                    .isDup(isDup)
                                    .topic(topic)
                                    .size(payload.readableBytes())
                                    .clientInfo(clientInfo));
                                return false;
                        }
                    }
                }, ctx.channel().eventLoop()));
        return allOf(distTask, retainTask).thenApply(v -> distTask.join() && retainTask.join());
    }

    private CompletableFuture<Void> distWillMessage(WillMessage willMessage) {
        return authProvider.check(clientInfo(), buildPubAction(willMessage.topic, willMessage.qos, willMessage.retain))
            .thenComposeAsync(checkResult -> {
                switch (checkResult.type()) {
                    case ERROR:
                        eventCollector.report(getLocal(ACCESS_CONTROL_ERROR, AccessControlError.class)
                            .clientInfo(clientInfo)
                            .cause(((CheckResult.Error) checkResult).cause));
                        boolean byPass = settingProvider.provide(ByPassPermCheckError, clientInfo);
                        if (!byPass) {
                            return DONE;
                        }
                        // fallthrough
                    case ALLOW:
                        long reqId = System.nanoTime();
                        return allOf(distWillMessage(reqId, willMessage), retainWillMessage(reqId, willMessage));
                    case DISALLOW:
                    default:
                        eventCollector.report(getLocal(PUB_ACTION_DISALLOW, PubActionDisallow.class)
                            .isLastWill(true)
                            .topic(willMessage.topic)
                            .qos(willMessage.qos)
                            .isRetain(willMessage.retain)
                            .clientInfo(clientInfo));
                        return DONE;
                }
            }, ctx.channel().eventLoop());
    }

    private CompletableFuture<Void> distWillMessage(long reqId, WillMessage willMessage) {
        return sessionCtx.distClient.dist(reqId, willMessage.topic, willMessage.qos,
                willMessage.payload.duplicate().nioBuffer(), Integer.MAX_VALUE, clientInfo)
            .handleAsync((v, e) -> {
                if (e != null) {
                    eventCollector.report(getLocal(WILL_DIST_ERROR, WillDistError.class)
                        .clientInfo(clientInfo)
                        .reqId(reqId)
                        .topic(willMessage.topic)
                        .qos(willMessage.qos)
                        .size(willMessage.payload.readableBytes()));
                } else {
                    switch (v.type()) {
                        case SUCCEED:
                            eventCollector.report(getLocal(WILL_DISTED, WillDisted.class)
                                .clientInfo(clientInfo)
                                .reqId(reqId)
                                .topic(willMessage.topic)
                                .qos(willMessage.qos)
                                .size(willMessage.payload.readableBytes()));
                            break;
                        case ERROR:
                        default:
                            eventCollector.report(getLocal(WILL_DIST_ERROR, WillDistError.class)
                                .clientInfo(clientInfo)
                                .reqId(reqId)
                                .topic(willMessage.topic)
                                .qos(willMessage.qos)
                                .size(willMessage.payload.readableBytes()));
                            break;
                    }
                }
                return null;
            }, ctx.channel().eventLoop());
    }

    private CompletableFuture<Boolean> retainMessage(long reqId, String topic, QoS qos, ByteBuf payload) {
        boolean retainEnabled = settingProvider.provide(RetainEnabled, clientInfo);
        if (!retainEnabled) {
            return CompletableFuture.completedFuture(true);
        }
        log.trace("Retaining message: reqId={}, qos={}, topic={}, size={}", reqId, qos, topic, payload.readableBytes());
        IRetainServiceClient.IClientPipeline pipeline = sessionCtx.getClientRetainPipeline(clientInfo);
        return cancelOnInactive(pipeline.retain(reqId, topic, qos, payload.duplicate().nioBuffer(), Integer.MAX_VALUE)
            .thenApplyAsync(v -> {
                log.trace("Message retained: reqId={}, result={}", v.getReqId(), v.getResult());
                switch (v.getResult()) {
                    case RETAINED:
                        eventCollector.report(getLocal(MSG_RETAINED, MsgRetained.class)
                            .reqId(v.getReqId())
                            .topic(topic)
                            .isLastWill(false)
                            .qos(qos)
                            .size(payload.readableBytes())
                            .clientInfo(clientInfo));
                        return true;
                    case CLEARED:
                        eventCollector.report(getLocal(RETAIN_MSG_CLEARED, RetainMsgCleared.class)
                            .reqId(v.getReqId())
                            .isLastWill(false)
                            .clientInfo(clientInfo)
                            .topic(topic));
                        return true;
                    case ERROR:
                    default:
                        eventCollector.report(getLocal(MSG_RETAINED_ERROR, MsgRetainedError.class)
                            .reqId(v.getReqId())
                            .clientInfo(clientInfo)
                            .topic(topic)
                            .isLastWill(false)
                            .qos(qos)
                            .payload(payload.duplicate().nioBuffer())
                            .size(payload.readableBytes()));
                        return false;
                }
            }, ctx.channel().eventLoop()));
    }

    private CompletableFuture<Void> retainWillMessage(long reqId, WillMessage willMessage) {
        boolean retainEnabled = settingProvider.provide(RetainEnabled, clientInfo);
        if (!retainEnabled) {
            return DONE;
        }
        if (!willMessage.retain) {
            return DONE;
        }
        String topic = willMessage.topic;
        QoS qos = willMessage.qos;
        ByteBuf payload = willMessage.payload;

        IRetainServiceClient.IClientPipeline pipeline = sessionCtx.retainClient.open(clientInfo);
        return pipeline.retain(reqId, topic, willMessage.qos, payload.duplicate().nioBuffer(), Integer.MAX_VALUE)
            .handleAsync((v, e) -> {
                switch (v.getResult()) {
                    case RETAINED:
                        eventCollector.report(getLocal(MSG_RETAINED, MsgRetained.class)
                            .reqId(v.getReqId())
                            .topic(topic)
                            .isLastWill(true)
                            .qos(qos)
                            .size(payload.readableBytes())
                            .clientInfo(clientInfo));
                        break;
                    case CLEARED:
                        eventCollector.report(getLocal(RETAIN_MSG_CLEARED, RetainMsgCleared.class)
                            .reqId(v.getReqId())
                            .isLastWill(true)
                            .clientInfo(clientInfo)
                            .topic(topic));
                        break;
                    case ERROR:
                    default:
                        eventCollector.report(getLocal(MSG_RETAINED_ERROR, MsgRetainedError.class)
                            .reqId(v.getReqId())
                            .clientInfo(clientInfo)
                            .topic(topic)
                            .isLastWill(true)
                            .qos(qos)
                            .payload(payload.duplicate().nioBuffer())
                            .size(payload.readableBytes()));
                        break;
                }
                pipeline.close();
                return null;
            }, ctx.channel().eventLoop());
    }

    private int nextMessageId() {
        publishPacketId = ++publishPacketId % 65536;
        int tryCount = 0;
        while (publishPacketId == 0
            || unconfirmedQoS1Messages.containsKey(publishPacketId)
            || unconfirmedQoS2Indexes.containsKey(publishPacketId)) {
            publishPacketId = ++publishPacketId % 65536;
            tryCount++;
            if (tryCount >= 9) {
                return -1;
            }
        }
        return publishPacketId;
    }

    private void checkIdle() {
        if (sessionCtx.nanoTime() - lastActiveAtNanos > idleTimeoutNanos) {
            idleTimeoutTask.cancel(true);
            closeConnectionNow(getLocal(IDLE, Idle.class)
                .keepAliveTimeSeconds(keepAliveTimeSeconds)
                .clientInfo(clientInfo));
        }
    }

    private boolean isSelfKick(Kicked kicked) {
        ClientInfo kicker = kicked.kicker();
        if (!kicker.hasMqtt3ClientInfo()) {
            return false;
        }
        return kicker.getTrafficId().equals(clientInfo.getTrafficId()) &&
            kicker.getUserId().equals(clientInfo.getUserId()) &&
            kicker.getMqtt3ClientInfo().getClientId().equals(clientInfo.getMqtt3ClientInfo().getClientId());
    }

    @Override
    public CompletableFuture<Void> disconnect() {
        if (ctx.channel().isActive()) {
            ctx.channel()
                .eventLoop()
                .execute(() -> closeConnectionNow(getLocal(BY_SERVER, ByServer.class)
                    .clientInfo(clientInfo)));
            CompletableFuture<Void> onTeardown = new CompletableFuture<>();
            addTearDownHook(() -> onTeardown.complete(null));
//            ctx.channel().closeFuture().addListener(f -> onTeardown.complete(null));
            return onTeardown;
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private static class UnconfirmedQoS1Message {
        final long seq;
        final int messageId;
        final String topic;
        final String topicFilter;
        final ByteBuffer payload;
        final boolean isRetain;
        final Message message;
        final ClientInfo sender;
        long lastPubNanos;
        MqttPublishMessage pubMsg;
        int resendTimes;

        UnconfirmedQoS1Message(long seq, int messageId,
                               String topicFilter,
                               String topic,
                               Message message,
                               boolean isRetain,
                               ClientInfo sender,
                               long nanoTime) {
            this.seq = seq;
            this.messageId = messageId;
            this.topic = topic;
            this.topicFilter = topicFilter;
            this.message = message;
            this.payload = message.getPayload().asReadOnlyByteBuffer();
            this.isRetain = isRetain;
            this.sender = sender;
            this.lastPubNanos = nanoTime;
            resendTimes = 0;
        }

        MqttPublishMessage buildMQTTMessage(long nanoTime) {
            pubMsg = new MqttPublishMessage(
                new MqttFixedHeader(MqttMessageType.PUBLISH, resendTimes > 0, MqttQoS.AT_LEAST_ONCE, isRetain, 0),
                new MqttPublishVariableHeader(topic, messageId),
                Unpooled.wrappedBuffer(payload));
            resendTimes++;
            lastPubNanos = nanoTime;
            return pubMsg;
        }
    }

    private static class UnconfirmedQoS2Message {
        final long seq;
        final int messageId;
        final String topicFilter;
        final String topic;
        final Message message;
        final ClientInfo sender;
        final ByteBuffer payload;
        final boolean isRetain;
        MqttMessage mqttMessage;
        long lastSendNanos;
        int resendTimes;

        UnconfirmedQoS2Message(long seq, int messageId, String topicFilter,
                               String topic, Message message, boolean isRetain,
                               ClientInfo sender, long nanoTime) {
            this.seq = seq;
            this.messageId = messageId;
            this.topicFilter = topicFilter;
            this.topic = topic;
            this.message = message;
            this.payload = message.getPayload().asReadOnlyByteBuffer();
            this.isRetain = isRetain;
            this.sender = sender;
            this.lastSendNanos = nanoTime;
            mqttMessage = new MqttPublishMessage(
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.EXACTLY_ONCE, isRetain, 0),
                new MqttPublishVariableHeader(topic, messageId), Unpooled.wrappedBuffer(payload));
        }

        MqttMessage buildMQTTMessage(long nanoTime) {
            MqttMessage pubMsg;
            if (resendTimes == 0 || mqttMessage.fixedHeader().messageType() != MqttMessageType.PUBLISH) {
                pubMsg = mqttMessage;
            } else {
                pubMsg = new MqttPublishMessage(
                    new MqttFixedHeader(MqttMessageType.PUBLISH,
                        true,
                        MqttQoS.EXACTLY_ONCE,
                        isRetain,
                        0),
                    new MqttPublishVariableHeader(topic, messageId),
                    Unpooled.wrappedBuffer(payload));
            }
            resendTimes++;
            lastSendNanos = nanoTime;
            return pubMsg;
        }

        MqttMessage received() {
            if (mqttMessage.fixedHeader().messageType() == MqttMessageType.PUBREL) {
                return mqttMessage;
            }
            lastSendNanos = System.nanoTime();
            resendTimes = 0;
            mqttMessage = MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE,
                    // according to [MQTT-3.6.1-1]
                    false, 2), MqttMessageIdVariableHeader.from(
                    ((MqttPublishVariableHeader) mqttMessage.variableHeader()).packetId()), null);
            return mqttMessage;
        }
    }

    @Builder
    public static final class WillMessage {
        public final String topic;
        public final QoS qos;
        public final boolean retain;
        public final ByteBuf payload;
    }

    @Builder
    @EqualsAndHashCode
    private static final class QoS2MessageKey {
        private final ClientInfo sourceClientInfo;
        private final Long messageId;
        private final String topicFilter;
    }
}
