/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

import static com.baidu.bifromq.metrics.TenantMetric.MqttChannelLatency;
import static com.baidu.bifromq.metrics.TenantMetric.MqttConnectCount;
import static com.baidu.bifromq.metrics.TenantMetric.MqttDisconnectCount;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS0DistBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS0EgressBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS0IngressBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS1DeliverBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS1DistBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS1EgressBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS1ExternalLatency;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS1IngressBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS2DeliverBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS2DistBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS2EgressBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS2ExternalLatency;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS2IngressBytes;
import static com.baidu.bifromq.mqtt.handler.MQTTSessionIdUtil.packetId;
import static com.baidu.bifromq.mqtt.handler.MQTTSessionIdUtil.userSessionId;
import static com.baidu.bifromq.mqtt.utils.AuthUtil.buildPubAction;
import static com.baidu.bifromq.mqtt.utils.AuthUtil.buildSubAction;
import static com.baidu.bifromq.mqtt.utils.AuthUtil.buildUnsubAction;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.type.QoS.AT_LEAST_ONCE;
import static com.baidu.bifromq.type.QoS.AT_MOST_ONCE;
import static com.baidu.bifromq.type.QoS.EXACTLY_ONCE;
import static com.baidu.bifromq.util.TopicUtil.isValidTopicFilter;
import static java.util.concurrent.CompletableFuture.allOf;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.baserpc.utils.FutureTracker;
import com.baidu.bifromq.dist.client.DistResult;
import com.baidu.bifromq.inbox.storage.proto.LWT;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterOption;
import com.baidu.bifromq.metrics.TenantMeter;
import com.baidu.bifromq.mqtt.handler.record.GoAway;
import com.baidu.bifromq.mqtt.handler.record.ResponseOrGoAway;
import com.baidu.bifromq.mqtt.session.IMQTTSession;
import com.baidu.bifromq.mqtt.session.MQTTSessionContext;
import com.baidu.bifromq.mqtt.utils.MQTTMessageSizer;
import com.baidu.bifromq.mqtt.utils.MQTTUtf8Util;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.PingReq;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.accessctrl.PubActionDisallow;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.accessctrl.SubActionDisallow;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.accessctrl.UnsubActionDisallow;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ClientChannelError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.InvalidTopicFilter;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.MalformedTopicFilter;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS0DistError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS1DistError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS1PubAckDropped;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS1PubAcked;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS2DistError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS2PubRecDropped;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS2PubReced;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.WillDistError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.WillDisted;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS0Pushed;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS1Confirmed;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS1Pushed;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS2Confirmed;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS2Pushed;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS2Received;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.retainhandling.MsgRetained;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.retainhandling.MsgRetainedError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.retainhandling.RetainMsgCleared;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.subhandling.SubAcked;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.subhandling.UnsubAcked;
import com.baidu.bifromq.sessiondict.client.ISessionRegister;
import com.baidu.bifromq.sysprops.BifroMQSysProp;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.MQTTClientInfoConstants;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.UserProperties;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MQTTSessionHandler extends MQTTMessageHandler implements IMQTTSession {
    protected static final boolean SANITY_CHECK = BifroMQSysProp.MQTT_UTF8_SANITY_CHECK.get();

    public record SubMessage(String topic,
                             Message message,
                             ClientInfo publisher,
                             String topicFilter,
                             TopicFilterOption option) {
        public boolean isRetain() {
            return message.getIsRetained() ||
                option.getRetainAsPublished() && message.getIsRetain();
        }

        public QoS qos() {
            return QoS.forNumber(Math.min(message.getPubQoS().getNumber(), option.getQos().getNumber()));
        }

    }

    private static class AckState {
        final long seq;
        final long timestamp;
        final SubMessage message;
        boolean acked = false;

        private AckState(long seq, SubMessage message, long timestamp) {
            this.seq = seq;
            this.message = message;
            this.timestamp = timestamp;
        }

        void setAcked() {
            acked = true;
        }
    }

    protected final TenantSettings settings;
    protected final String userSessionId;
    protected final int keepAliveTimeSeconds;
    protected final ClientInfo clientInfo;
    protected final TenantMeter tenantMeter;
    private final long idleTimeoutNanos;
    private final MPSThrottler throttler;
    private final FutureTracker fgTasks = new FutureTracker();
    private final FutureTracker bgTasks = new FutureTracker();
    private final Set<Integer> inUsePacketIds = new HashSet<>();

    protected ChannelHandlerContext ctx;
    protected MQTTSessionContext sessionCtx;
    protected IAuthProvider authProvider;
    protected IEventCollector eventCollector;
    private LWT willMessage;
    private boolean isGoAway;
    private ScheduledFuture<?> idleTimeoutTask;
    private ISessionRegister sessionRegister;
    private long lastActiveAtNanos;
    private final LinkedHashMap<Integer, AckState> unconfirmedPacketIds = new LinkedHashMap<>();
    private int receivingCount = 0;

    protected MQTTSessionHandler(TenantSettings settings,
                                 String userSessionId,
                                 int keepAliveTimeSeconds,
                                 ClientInfo clientInfo,
                                 @Nullable LWT willMessage) {
        this.settings = settings;
        this.userSessionId = userSessionId;
        this.keepAliveTimeSeconds = keepAliveTimeSeconds;
        this.clientInfo = clientInfo;
        this.willMessage = willMessage;
        this.tenantMeter = TenantMeter.get(clientInfo.getTenantId());
        this.throttler = new MPSThrottler(settings.maxMsgPerSec);
        this.idleTimeoutNanos = Duration.ofMillis(keepAliveTimeSeconds * 1500L).toNanos(); // x1.5
    }

    protected abstract IMQTTProtocolHelper helper();

    @Override
    public final String channelId() {
        return clientInfo.getMetadataOrDefault(MQTTClientInfoConstants.MQTT_CHANNEL_ID_KEY, "");
    }

    @Override
    public final ClientInfo clientInfo() {
        return clientInfo;
    }

    @Override
    public final CompletableFuture<Void> disconnect() {
        ctx.channel().eventLoop().execute(() -> handleGoAway(helper().onDisconnect()));
        return bgTasks.whenComplete((v, e) -> log.trace("All bg tasks finished: client={}", clientInfo));
    }

    protected final LWT willMessage() {
        return willMessage;
    }

    protected final <T> CompletableFuture<T> addFgTask(CompletableFuture<T> task) {
        return fgTasks.track(task);
    }

    protected final <T> CompletableFuture<T> addBgTask(CompletableFuture<T> task) {
        return bgTasks.track(task);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        super.handlerAdded(ctx);
        this.ctx = ctx;
        ChannelAttrs.trafficShaper(ctx).setReadLimit(settings.inboundBandwidth);
        ChannelAttrs.trafficShaper(ctx).setWriteLimit(settings.outboundBandwidth);
        ChannelAttrs.setMaxPayload(settings.maxPacketSize, ctx);
        sessionCtx = ChannelAttrs.mqttSessionContext(ctx);
        authProvider = sessionCtx.authProvider(ctx);
        eventCollector = sessionCtx.eventCollector;
        sessionCtx.localSessionRegistry.add(channelId(), this);
        sessionRegister = ChannelAttrs.mqttSessionContext(ctx).sessionDictClient
            .reg(clientInfo, kicker -> ctx.channel().eventLoop().execute(() -> handleGoAway(helper().onKick(kicker))));
        lastActiveAtNanos = sessionCtx.nanoTime();
        idleTimeoutTask = ctx.channel().eventLoop()
            .scheduleAtFixedRate(this::checkIdle, idleTimeoutNanos, idleTimeoutNanos, TimeUnit.NANOSECONDS);
        tenantMeter.recordCount(MqttConnectCount);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (idleTimeoutTask != null) {
            idleTimeoutTask.cancel(true);
        }
        if (willMessage != null) {
            addBgTask(distWillMessage(willMessage));
        }
        fgTasks.stop();
        sessionCtx.localSessionRegistry.remove(channelId(), this);
        sessionRegister.stop();
        tenantMeter.recordCount(MqttDisconnectCount);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        super.exceptionCaught(ctx, cause);
        log.debug("ctx: {}, cause:", ctx, cause);
        // if disconnection is caused purely by channel error
        handleGoAway(GoAway.now(getLocal(ClientChannelError.class).clientInfo(clientInfo).cause(cause)));
    }

    @Override
    public final void channelRead(ChannelHandlerContext ctx, Object msg) {
        assert msg instanceof MqttMessage;
        MqttMessage mqttMessage = (MqttMessage) msg;
        log.trace("Received {}", mqttMessage);
        if (mqttMessage.decoderResult().isSuccess()) {
            lastActiveAtNanos = sessionCtx.nanoTime();
            if (log.isTraceEnabled()) {
                log.trace("Received mqtt message:{}", mqttMessage);
            }
            switch (mqttMessage.fixedHeader().messageType()) {
                case CONNECT -> handleGoAway(helper().respondDuplicateConnect((MqttConnectMessage) mqttMessage));
                case DISCONNECT -> handleGoAway(handleDisconnect(mqttMessage));
                case PINGREQ -> {
                    writeAndFlush(MqttMessage.PINGRESP);
                    if (settings.debugMode) {
                        eventCollector.report(getLocal(PingReq.class).pong(true).clientInfo(clientInfo));
                    }
                }
                case PUBLISH -> handlePubMsg((MqttPublishMessage) mqttMessage);
                case PUBREL -> handlePubRelMsg(mqttMessage);
                case PUBACK -> handlePubAckMsg((MqttPubAckMessage) mqttMessage);
                case PUBREC -> handlePubRecMsg(mqttMessage);
                case PUBCOMP -> handlePubCompMsg(mqttMessage);
                case SUBSCRIBE -> handleSubMsg((MqttSubscribeMessage) mqttMessage);
                case UNSUBSCRIBE -> handleUnsubMsg((MqttUnsubscribeMessage) mqttMessage);
                default -> handleOther(mqttMessage);
            }
        } else {
            handleGoAway(helper().respondDecodeError(mqttMessage));
        }
    }

    protected void handleOther(MqttMessage message) {

    }

    protected abstract GoAway handleDisconnect(MqttMessage message);

    private void handlePubMsg(MqttPublishMessage mqttMessage) {
        if (isExceedReceivingMaximum()) {
            handleGoAway(helper().respondReceivingMaximumExceeded());
            mqttMessage.release();
        }
        if (!throttler.pass()) {
            handleGoAway(helper().respondPubRateExceeded());
            mqttMessage.release();
            return;
        }
        GoAway goAwayOnInvalid = helper().validatePubMessage(mqttMessage);
        if (goAwayOnInvalid != null) {
            handleGoAway(goAwayOnInvalid);
            mqttMessage.release();
            return;
        }
        int packetId = mqttMessage.variableHeader().packetId();
        long reqId = packetId > 0 ? packetId : System.nanoTime();
        String topic = helper().getTopic(mqttMessage);
        switch (mqttMessage.fixedHeader().qosLevel()) {
            case AT_MOST_ONCE -> handleQoS0Pub(reqId, topic, mqttMessage);
            case AT_LEAST_ONCE -> handleQoS1Pub(reqId, topic, mqttMessage);
            case EXACTLY_ONCE -> handleQoS2Pub(reqId, topic, mqttMessage);
        }
    }

    private void handleSubMsg(MqttSubscribeMessage message) {
        GoAway goAwayOnInvalid = helper().validateSubMessage(message);
        if (goAwayOnInvalid != null) {
            handleGoAway(goAwayOnInvalid);
            return;
        }
        int packetId = message.variableHeader().messageId();
        if (helper().checkPacketIdUsage() && inUsePacketIds.contains(packetId)) {
            writeAndFlush(helper().respondPacketIdInUse(message));
            return;
        }
        inUsePacketIds.add(packetId);
        doSubscribe(packetId, message)
            .thenAcceptAsync(subAckMsg -> {
                writeAndFlush(subAckMsg);
                inUsePacketIds.remove(packetId);
                eventCollector.report(getLocal(SubAcked.class)
                    .messageId(packetId)
                    .granted(subAckMsg.payload().grantedQoSLevels())
                    .topicFilter(message.payload().topicSubscriptions().stream()
                        .map(MqttTopicSubscription::topicName)
                        .collect(Collectors.toList()))
                    .clientInfo(clientInfo));
            }, ctx.channel().eventLoop());
    }

    private CompletableFuture<MqttSubAckMessage> doSubscribe(long reqId, MqttSubscribeMessage message) {
        List<CompletableFuture<IMQTTProtocolHelper.SubResult>> resultFutures = helper().getSubTask(message).stream()
            .map(subTask -> checkAndSubscribe(reqId, subTask.topicFilter(), subTask.option(), subTask.userProperties()))
            .toList();
        return CompletableFuture.allOf(resultFutures.toArray(CompletableFuture[]::new))
            .thenApply(v -> resultFutures.stream().map(CompletableFuture::join).toList())
            .thenApply(subResults -> helper().buildSubAckMessage(message, subResults));
    }

    protected final CompletableFuture<IMQTTProtocolHelper.SubResult> checkAndSubscribe(long reqId,
                                                                                       String topicFilter,
                                                                                       TopicFilterOption option,
                                                                                       UserProperties userProps) {
        if (!MQTTUtf8Util.isWellFormed(topicFilter, SANITY_CHECK)) {
            eventCollector.report(getLocal(MalformedTopicFilter.class)
                .topicFilter(topicFilter)
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(IMQTTProtocolHelper.SubResult.TOPIC_FILTER_INVALID);
        }
        if (!isValidTopicFilter(topicFilter,
            settings.maxTopicLevelLength,
            settings.maxTopicLevels,
            settings.maxTopicLength)) {
            eventCollector.report(getLocal(InvalidTopicFilter.class)
                .topicFilter(topicFilter)
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(IMQTTProtocolHelper.SubResult.TOPIC_FILTER_INVALID);
        }

        return addFgTask(authProvider.check(clientInfo, buildSubAction(topicFilter, option.getQos(), userProps))
            .thenComposeAsync(allow -> {
                if (allow) {
                    return addFgTask(subTopicFilter(reqId, topicFilter, option));
                } else {
                    eventCollector.report(getLocal(SubActionDisallow.class)
                        .topicFilter(topicFilter)
                        .qos(option.getQos())
                        .clientInfo(clientInfo));
                    return CompletableFuture.completedFuture(IMQTTProtocolHelper.SubResult.NOT_AUTHORIZED);
                }
            }, ctx.channel().eventLoop()));
    }

    protected abstract CompletableFuture<IMQTTProtocolHelper.SubResult> subTopicFilter(long reqId,
                                                                                       String topicFilter,
                                                                                       TopicFilterOption option);

    private void handleUnsubMsg(MqttUnsubscribeMessage message) {
        GoAway goAwayOnInvalid = helper().validateUnsubMessage(message);
        if (goAwayOnInvalid != null) {
            handleGoAway(goAwayOnInvalid);
            return;
        }
        int packetId = message.variableHeader().messageId();
        if (helper().checkPacketIdUsage() && inUsePacketIds.contains(packetId)) {
            writeAndFlush(helper().respondPacketIdInUse(message));
            return;
        }
        inUsePacketIds.add(packetId);
        doUnsubscribe(packetId, message)
            .thenAcceptAsync(unsubAckMsg -> {
                inUsePacketIds.remove(packetId);
                writeAndFlush(unsubAckMsg);
                eventCollector.report(getLocal(UnsubAcked.class)
                    .messageId(packetId)
                    .topicFilter(message.payload().topics())
                    .clientInfo(clientInfo));
            }, ctx.channel().eventLoop());
    }

    private CompletableFuture<MqttUnsubAckMessage> doUnsubscribe(long reqId, MqttUnsubscribeMessage message) {
        UserProperties userProps = helper().getUserProps(message);
        List<CompletableFuture<IMQTTProtocolHelper.UnsubResult>> resultFutures = message.payload().topics().stream()
            .map(topicFilter -> checkAndUnsubscribe(reqId, topicFilter, userProps))
            .toList();
        return CompletableFuture.allOf(resultFutures.toArray(CompletableFuture[]::new))
            .thenApply(v -> resultFutures.stream().map(CompletableFuture::join).toList())
            .thenApply(subResults -> helper().buildUnsubAckMessage(message, subResults));
    }


    protected final CompletableFuture<IMQTTProtocolHelper.UnsubResult> checkAndUnsubscribe(long reqId,
                                                                                           String topicFilter,
                                                                                           UserProperties userProps) {
        if (!isValidTopicFilter(topicFilter,
            settings.maxTopicLevelLength,
            settings.maxTopicLevels,
            settings.maxTopicLength)) {
            eventCollector.report(getLocal(InvalidTopicFilter.class)
                .topicFilter(topicFilter)
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(IMQTTProtocolHelper.UnsubResult.TOPIC_FILTER_INVALID);
        }
        return addFgTask(authProvider.check(clientInfo, buildUnsubAction(topicFilter, userProps)))
            .thenComposeAsync(allow -> {
                if (allow) {
                    return addFgTask(unsubTopicFilter(reqId, topicFilter));
                } else {
                    eventCollector.report(getLocal(UnsubActionDisallow.class)
                        .clientInfo(clientInfo)
                        .topicFilter(topicFilter));
                    // always reply unsub ack
                    return CompletableFuture.completedFuture(IMQTTProtocolHelper.UnsubResult.NOT_AUTHORIZED);
                }
            }, ctx.channel().eventLoop());
    }

    protected abstract CompletableFuture<IMQTTProtocolHelper.UnsubResult> unsubTopicFilter(long reqId,
                                                                                           String topicFilter);

    private void handlePubRelMsg(MqttMessage mqttMessage) {
        int packetId = ((MqttMessageIdVariableHeader) mqttMessage.variableHeader()).messageId();
        if (!inUsePacketIds.contains(packetId)) {
            writeAndFlush(helper().onPubRelReceived(mqttMessage, false));
            return;
        }
        decReceivingCount();
        inUsePacketIds.remove(packetId);
        writeAndFlush(helper().onPubRelReceived(mqttMessage, true));
    }

    private void handlePubAckMsg(MqttPubAckMessage mqttMessage) {
        int packetId = mqttMessage.variableHeader().messageId();
        if (isConfirming(packetId)) {
            SubMessage confirmed = confirm(packetId);
            tenantMeter.recordSummary(MqttQoS1DeliverBytes,
                MQTTMessageSizer.sizePublishMsg(confirmed.topic(), confirmed.message().getPayload().size()));
        } else {
            log.trace("No packetId to confirm released: sessionId={}, packetId={}",
                userSessionId(clientInfo), packetId);
        }
    }

    private void handlePubRecMsg(MqttMessage message) {
        int packetId = ((MqttMessageIdVariableHeader) message.variableHeader()).messageId();
        if (isConfirming(packetId)) {
            if (helper().isQoS2Received(message)) {
                handleResponseOrGoAway(helper().respondPubRecMsg(message, false));
                if (settings.debugMode) {
                    SubMessage received = getConfirming(packetId);
                    eventCollector.report(getLocal(QoS2Received.class)
                        .reqId(packetId)
                        .messageId(packetId)
                        .isRetain(received.isRetain())
                        .sender(received.publisher())
                        .topic(received.topic())
                        .matchedFilter(received.topicFilter())
                        .size(received.message().getPayload().size())
                        .clientInfo(clientInfo));
                }
            } else {
                confirm(packetId);
            }
        } else {
            handleResponseOrGoAway(helper().respondPubRecMsg(message, true));
        }
    }

    private void handlePubCompMsg(MqttMessage message) {
        MqttMessageIdVariableHeader variableHeader = (MqttMessageIdVariableHeader) message.variableHeader();
        int packetId = variableHeader.messageId();
        if (isConfirming(packetId)) {
            SubMessage confirmed = confirm(packetId);
            if (settings.debugMode) {
                eventCollector.report(getLocal(QoS2Confirmed.class)
                    .reqId(confirmed.message().getMessageId())
                    .messageId(packetId)
                    .isRetain(confirmed.isRetain())
                    .sender(confirmed.publisher())
                    .delivered(false)
                    .topic(confirmed.topic())
                    .matchedFilter(confirmed.topicFilter())
                    .size(confirmed.message().getPayload().size())
                    .clientInfo(clientInfo));
            }
            tenantMeter.recordSummary(MqttQoS2DeliverBytes,
                MQTTMessageSizer.sizePublishMsg(confirmed.topic(), confirmed.message().getPayload().size()));
        } else {
            log.trace("No packetId to confirm released: sessionId={}, packetId={}",
                userSessionId(clientInfo), packetId);
        }
    }

    protected int clientReceiveMaximum() {
        return helper().clientReceiveMaximum();
    }

    protected final boolean isConfirming(int packetId) {
        return unconfirmedPacketIds.containsKey(packetId);
    }

    private SubMessage getConfirming(int packetId) {
        return unconfirmedPacketIds.get(packetId).message;
    }

    protected final int clientReceiveQuota() {
        return clientReceiveMaximum() - unconfirmedPacketIds.size();
    }

    private SubMessage confirm(int packetId) {
        AckState ackState = unconfirmedPacketIds.get(packetId);
        SubMessage msg = null;
        if (ackState != null) {
            long now = System.nanoTime();
            msg = ackState.message;
            ackState.setAcked();
            Iterator<Integer> packetIdItr = unconfirmedPacketIds.keySet().iterator();
            while (packetIdItr.hasNext()) {
                packetId = packetIdItr.next();
                ackState = unconfirmedPacketIds.get(packetId);
                if (ackState.acked) {
                    packetIdItr.remove();
                    SubMessage confirmed = ackState.message;
                    onConfirm(ackState.seq);
                    switch (confirmed.qos()) {
                        case AT_LEAST_ONCE -> {
                            tenantMeter.timer(MqttQoS1ExternalLatency)
                                .record(now - ackState.timestamp, TimeUnit.NANOSECONDS);
                            if (settings.debugMode) {
                                eventCollector.report(getLocal(QoS1Confirmed.class)
                                    .reqId(confirmed.message().getMessageId())
                                    .messageId(packetId)
                                    .isRetain(confirmed.isRetain())
                                    .sender(confirmed.publisher())
                                    .delivered(true)
                                    .topic(confirmed.topic())
                                    .matchedFilter(confirmed.topicFilter())
                                    .size(confirmed.message().getPayload().size())
                                    .clientInfo(clientInfo));
                            }
                        }
                        case EXACTLY_ONCE -> tenantMeter.timer(MqttQoS2ExternalLatency)
                            .record(now - ackState.timestamp, TimeUnit.NANOSECONDS);
                    }
                }
            }
        } else {
            log.trace("No msg to confirm: sessionId={}, packetId={}", userSessionId, packetId);
        }
        return msg;
    }

    protected abstract void onConfirm(long seq);

    protected final void sendQoS0SubMessage(SubMessage msg) {
        assert msg.qos() == AT_MOST_ONCE;
        sendMqttPubMessage(-1, msg, msg.topicFilter(), msg.publisher());
    }

    protected final void sendSubMessage(long seq, SubMessage msg) {
        assert seq > -1;
        AckState prev = unconfirmedPacketIds.putIfAbsent(packetId(seq), new AckState(seq, msg, System.nanoTime()));
        if (prev == null) {
            sendMqttPubMessage(seq, msg, msg.topicFilter(), msg.publisher());
        } else {
            log.warn("Bad state: sequence duplicate seq={}", seq);
        }
    }

    private void sendMqttPubMessage(long seq, SubMessage msg, String topicFilter, ClientInfo publisher) {
        MqttPublishMessage pubMsg = helper().buildMqttPubMessage(packetId(seq), msg);
        Timer.Sample start = Timer.start();
        int msgSize =
            MQTTMessageSizer.sizePublishMsg(pubMsg.variableHeader().topicName(), pubMsg.payload().readableBytes());
        ctx.write(pubMsg).addListener(f -> {
            if (f.isSuccess()) {
                switch (pubMsg.fixedHeader().qosLevel()) {
                    case AT_MOST_ONCE -> {
                        if (settings.debugMode) {
                            eventCollector.report(getLocal(QoS0Pushed.class)
                                .reqId(pubMsg.variableHeader().packetId())
                                .isRetain(false)
                                .sender(publisher)
                                .matchedFilter(topicFilter)
                                .topic(pubMsg.variableHeader().topicName())
                                .size(msgSize)
                                .clientInfo(clientInfo));
                        }
                        tenantMeter.recordSummary(MqttQoS0EgressBytes, msgSize);
                    }
                    case AT_LEAST_ONCE -> {
                        eventCollector.report(getLocal(QoS1Pushed.class)
                            .reqId(pubMsg.variableHeader().packetId())
                            .messageId(pubMsg.variableHeader().packetId())
                            .dup(false)
                            .isRetain(pubMsg.fixedHeader().isRetain())
                            .sender(publisher)
                            .matchedFilter(topicFilter)
                            .topic(pubMsg.variableHeader().topicName())
                            .size(msgSize)
                            .clientInfo(clientInfo));
                        tenantMeter.recordSummary(MqttQoS1EgressBytes, msgSize);
                    }
                    case EXACTLY_ONCE -> {
                        eventCollector.report(getLocal(QoS2Pushed.class)
                            .reqId(pubMsg.variableHeader().packetId())
                            .messageId(pubMsg.variableHeader().packetId())
                            .dup(false)
                            .isRetain(pubMsg.fixedHeader().isRetain())
                            .sender(publisher)
                            .matchedFilter(topicFilter)
                            .topic(pubMsg.variableHeader().topicName())
                            .size(msgSize)
                            .clientInfo(clientInfo));
                        tenantMeter.recordSummary(MqttQoS2EgressBytes, msgSize);
                    }
                }
                start.stop(tenantMeter.timer(MqttChannelLatency));
            }
        });
    }

    private boolean isExceedReceivingMaximum() {
        return receivingCount > settings.receiveMaximum;
    }

    private void incReceivingCount() {
        receivingCount = Math.min(receivingCount + 1, settings.receiveMaximum);
    }

    private void decReceivingCount() {
        receivingCount = Math.max(receivingCount - 1, 0);
    }

    private CompletableFuture<Boolean> checkPubPermission(String topic, Message distMessage, UserProperties userProps) {
        return authProvider.check(clientInfo(),
            buildPubAction(topic, distMessage.getPubQoS(), distMessage.getIsRetain(), userProps));
    }

    private void handleQoS0Pub(long reqId, String topic, MqttPublishMessage message) {
        if (log.isTraceEnabled()) {
            log.trace("Checking authorization of pub qos0 action: reqId={}, sessionId={}, topic={}", reqId,
                userSessionId(clientInfo), topic);
        }
        Message distMessage = helper().buildDistMessage(message);
        UserProperties userProps = helper().getUserProps(message);
        fgTasks.track(checkPubPermission(topic, distMessage, userProps))
            .thenComposeAsync(allow -> {
                if (log.isTraceEnabled()) {
                    log.trace("Checked authorization of pub qos0 action: reqId={}, sessionId={}, topic={}:{}",
                        reqId, userSessionId(clientInfo), topic, allow);
                }
                if (allow) {
                    return distMessage(reqId, topic, distMessage, false);
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("Unauthorized qos0 topic: reqId={}, sessionId={}, topic={}",
                            reqId, userSessionId(clientInfo), topic);
                    }
                    eventCollector.report(getLocal(PubActionDisallow.class)
                        .isLastWill(false)
                        .topic(topic)
                        .qos(AT_MOST_ONCE)
                        .isRetain(distMessage.getIsRetain())
                        .clientInfo(clientInfo));
                    // either make a positive acknowledgement, according to the normal QoS rules,
                    // or close the Network Connection[MQTT-3.3.5-2]
                    // TODO: we choose close connection for now,
                    // or introduce a setting to control the behavior?
                    handleGoAway(helper().onQoS0DistDenied(topic, distMessage));
                    return CompletableFuture.completedFuture(DistResult.ERROR);
                }
            }, ctx.channel().eventLoop())
            .whenComplete((v, e) -> message.release());
    }

    private void handleQoS1Pub(long reqId, String topic, MqttPublishMessage message) {
        int packetId = message.variableHeader().packetId();
        if (inUsePacketIds.contains(packetId)) {
            message.release();
            return;
        }
        inUsePacketIds.add(packetId);
        incReceivingCount();
        if (log.isTraceEnabled()) {
            log.trace("Checking authorization of pub qos1 action: reqId={}, sessionId={}, topic={}",
                reqId, userSessionId(clientInfo), topic);
        }
        Message distMessage = helper().buildDistMessage(message);
        UserProperties userProps = helper().getUserProps(message);
        fgTasks.track(checkPubPermission(topic, distMessage, userProps))
            .thenComposeAsync(allow -> {
                if (allow) {
                    return distMessage(reqId, topic, distMessage, message.fixedHeader().isDup())
                        .thenAcceptAsync(distResult -> {
                            if (log.isTraceEnabled()) {
                                log.trace("Disted qos1 msg: reqId={}, sessionId={}, topic={}",
                                    reqId, userSessionId(clientInfo), topic);
                            }
                            decReceivingCount();
                            inUsePacketIds.remove(packetId);
                            if (ctx.channel().isActive() && ctx.channel().isWritable()) {
                                writeAndFlush(helper().onQoS1Disted(distResult, message));
                                if (settings.debugMode) {
                                    eventCollector.report(getLocal(QoS1PubAcked.class)
                                        .reqId(reqId)
                                        .isDup(message.fixedHeader().isDup())
                                        .topic(topic)
                                        .size(message.payload().readableBytes())
                                        .clientInfo(clientInfo));
                                }
                            } else {
                                eventCollector.report(getLocal(QoS1PubAckDropped.class)
                                    .reqId(reqId)
                                    .isDup(message.fixedHeader().isDup())
                                    .topic(topic)
                                    .size(message.payload().readableBytes())
                                    .clientInfo(clientInfo));
                            }
                        }, ctx.channel().eventLoop());
                } else {
                    decReceivingCount();
                    inUsePacketIds.remove(packetId);
                    if (log.isTraceEnabled()) {
                        log.trace("Unauthorized qos1 topic: reqId={}, sessionId={}, topic={}",
                            reqId, userSessionId(clientInfo), topic);
                    }
                    eventCollector.report(getLocal(PubActionDisallow.class)
                        .isLastWill(false)
                        .topic(topic)
                        .qos(AT_LEAST_ONCE)
                        .isRetain(distMessage.getIsRetain())
                        .clientInfo(clientInfo));
                    handleResponseOrGoAway(helper().onQoS1DistDenied(topic, packetId, distMessage));
                    return CompletableFuture.completedFuture(null);
                }
            }, ctx.channel().eventLoop())
            .whenComplete((v, e) -> message.release());

    }

    private void handleQoS2Pub(long reqId, String topic, MqttPublishMessage message) {
        int packetId = message.variableHeader().packetId();
        if (inUsePacketIds.contains(packetId)) {
            handleResponseOrGoAway(helper().respondQoS2PacketInUse(message));
            message.release();
            return;
        }
        incReceivingCount();
        inUsePacketIds.add(packetId);
        Message distMessage = helper().buildDistMessage(message);
        UserProperties userProps = helper().getUserProps(message);
        fgTasks.track(checkPubPermission(topic, distMessage, userProps))
            .thenComposeAsync(allow -> {
                if (allow) {
                    return distMessage(reqId, topic, distMessage, message.fixedHeader().isDup())
                        .thenAcceptAsync(distResult -> {
                            if (log.isTraceEnabled()) {
                                log.trace("Disted qos2 msg: reqId={}, sessionId={}, topic={}",
                                    reqId, userSessionId(clientInfo), topic);
                            }
                            if (ctx.channel().isActive()) {
                                if (ctx.channel().isWritable()) {
                                    if (distResult == DistResult.EXCEED_LIMIT || distResult == DistResult.ERROR) {
                                        decReceivingCount();
                                        inUsePacketIds.remove(packetId);
                                    }
                                    writeAndFlush(helper().onQoS2Disted(distResult, message));
                                    if (settings.debugMode) {
                                        eventCollector.report(getLocal(QoS2PubReced.class)
                                            .reqId(reqId)
                                            .isDup(message.fixedHeader().isDup())
                                            .topic(topic)
                                            .size(message.payload().readableBytes())
                                            .clientInfo(clientInfo));
                                    }
                                } else {
                                    decReceivingCount();
                                    inUsePacketIds.remove(packetId);
                                    eventCollector.report(getLocal(QoS2PubRecDropped.class)
                                        .reqId(reqId)
                                        .isDup(message.fixedHeader().isDup())
                                        .topic(topic)
                                        .size(message.payload().readableBytes())
                                        .clientInfo(clientInfo));
                                }
                            }
                        }, ctx.channel().eventLoop());
                } else {
                    decReceivingCount();
                    inUsePacketIds.remove(packetId);
                    if (log.isTraceEnabled()) {
                        log.trace("Unauthorized qos2 topic: reqId={}, sessionId={}, topic={}",
                            reqId, userSessionId(clientInfo), topic);
                    }
                    eventCollector.report(getLocal(PubActionDisallow.class)
                        .isLastWill(false)
                        .topic(topic)
                        .qos(EXACTLY_ONCE)
                        .isRetain(distMessage.getIsRetain())
                        .clientInfo(clientInfo));
                    handleResponseOrGoAway(helper().onQoS2DistDenied(topic, packetId, distMessage));
                    return CompletableFuture.completedFuture(null);
                }
            }, ctx.channel().eventLoop()).whenComplete((v, e) -> message.release());
    }

    private void checkIdle() {
        if (sessionCtx.nanoTime() - lastActiveAtNanos > idleTimeoutNanos) {
            idleTimeoutTask.cancel(true);
            handleGoAway(helper().onIdleTimeout(keepAliveTimeSeconds));
        }
    }

    protected final void discardLWT() {
        willMessage = null;
    }

    protected final void resumeChannelRead() {
        if (isGoAway) {
            return;
        }
        // resume reading
        ctx.channel().config().setAutoRead(true);
        ctx.read();
    }

    protected void handleResponseOrGoAway(ResponseOrGoAway responseOrGoAway) {
        if (responseOrGoAway.response() != null) {
            writeAndFlush(responseOrGoAway.response());
        } else {
            assert responseOrGoAway.goAway() != null;
            handleGoAway(responseOrGoAway.goAway());
        }
    }

    protected final void handleGoAway(GoAway goAway) {
        assert ctx.channel().eventLoop().inEventLoop();
        if (isGoAway) {
            return;
        }
        isGoAway = true;
        for (Event<?> reason : goAway.reasons()) {
            sessionCtx.eventCollector.report(reason);
        }
        if (!ctx.channel().isActive()) {
            return;
        }
        // disable auto read
        ctx.channel().config().setAutoRead(false);
        if (goAway.rightNow()) {
            if (goAway.farewell() != null) {
                writeAndFlush(goAway.farewell()).addListener(ChannelFutureListener.CLOSE);
            } else {
                ctx.channel().close();
            }
        } else {
            ctx.channel().eventLoop().schedule(() -> ctx.channel().close(),
                ThreadLocalRandom.current().nextInt(100, 5000), TimeUnit.MILLISECONDS);
        }
    }

    private CompletableFuture<DistResult> distMessage(long reqId,
                                                      String topic,
                                                      Message message,
                                                      boolean isDup) {
        if (log.isTraceEnabled()) {
            log.trace("Disting msg: req={}, topic={}, qos={}, size={}",
                reqId, topic, message.getPubQoS(), message.getPayload().size());
        }
        CompletableFuture<Boolean> retainTask = message.getIsRetain() ?
            fgTasks.track(retainMessage(reqId, topic, message)) : CompletableFuture.completedFuture(true);
        switch (message.getPubQoS()) {
            case AT_MOST_ONCE -> tenantMeter.recordSummary(MqttQoS0IngressBytes,
                MQTTMessageSizer.sizePublishMsg(topic, message.getPayload().size()));
            case AT_LEAST_ONCE -> tenantMeter.recordSummary(MqttQoS1IngressBytes,
                MQTTMessageSizer.sizePublishMsg(topic, message.getPayload().size()));
            case EXACTLY_ONCE -> tenantMeter.recordSummary(MqttQoS2IngressBytes,
                MQTTMessageSizer.sizePublishMsg(topic, message.getPayload().size()));
        }
        CompletableFuture<DistResult> distTask = fgTasks.track(
            sessionCtx.distClient.pub(reqId, topic, message, clientInfo)
                .thenApplyAsync(v -> {
                    switch (v) {
                        case OK, NO_MATCH -> {
                            if (log.isTraceEnabled()) {
                                log.trace("Msg published: reqId={}, sessionId={}, topic={}, qos={}, size={}",
                                    reqId, userSessionId, topic, message.getPubQoS(), message.getPayload().size());
                            }
                            switch (message.getPubQoS()) {
                                case AT_MOST_ONCE -> tenantMeter.recordSummary(MqttQoS0DistBytes,
                                    MQTTMessageSizer.sizePublishMsg(topic, message.getPayload().size()));
                                case AT_LEAST_ONCE -> tenantMeter.recordSummary(MqttQoS1DistBytes,
                                    MQTTMessageSizer.sizePublishMsg(topic, message.getPayload().size()));
                                case EXACTLY_ONCE -> tenantMeter.recordSummary(MqttQoS2DistBytes,
                                    MQTTMessageSizer.sizePublishMsg(topic, message.getPayload().size()));
                            }
                        }
                        default -> {
                            switch (message.getPubQoS()) {
                                case AT_MOST_ONCE -> eventCollector.report(getLocal(QoS0DistError.class)
                                    .reqId(reqId)
                                    .topic(topic)
                                    .size(message.getPayload().size())
                                    .clientInfo(clientInfo));
                                case AT_LEAST_ONCE -> eventCollector.report(getLocal(QoS1DistError.class)
                                    .reqId(reqId)
                                    .topic(topic)
                                    .isDup(isDup)
                                    .size(message.getPayload().size())
                                    .clientInfo(clientInfo));
                                case EXACTLY_ONCE -> eventCollector.report(getLocal(QoS2DistError.class)
                                    .reqId(reqId)
                                    .topic(topic)
                                    .isDup(isDup)
                                    .size(message.getPayload().size())
                                    .clientInfo(clientInfo));
                            }
                        }
                    }
                    return v;
                }, ctx.channel().eventLoop()));
        return allOf(retainTask, distTask).thenApply(v -> distTask.join());
    }

    private CompletableFuture<Boolean> retainMessage(long reqId, String topic, Message message) {
        if (!settings.retainEnabled) {
            return CompletableFuture.completedFuture(true);
        }
        if (log.isTraceEnabled()) {
            log.trace("Retaining message: reqId={}, qos={}, topic={}, size={}",
                reqId, message.getPubQoS(), topic, message.getPayload().size());
        }
        return fgTasks.track(
            // TODO: refactoring needed
            sessionCtx.retainClient.retain(
                    reqId,
                    topic,
                    message.getPubQoS(),
                    message.getPayload(),
                    message.getExpiryInterval(),
                    clientInfo)
                .thenApplyAsync(v -> {
                    if (log.isTraceEnabled()) {
                        log.trace("Message retained: reqId={}, result={}", v.getReqId(), v.getResult());
                    }
                    return switch (v.getResult()) {
                        case RETAINED -> {
                            eventCollector.report(getLocal(MsgRetained.class)
                                .reqId(v.getReqId())
                                .topic(topic)
                                .isLastWill(false)
                                .qos(message.getPubQoS())
                                .size(message.getPayload().size())
                                .clientInfo(clientInfo));
                            yield true;
                        }
                        case CLEARED -> {
                            eventCollector.report(getLocal(RetainMsgCleared.class)
                                .reqId(v.getReqId())
                                .isLastWill(false)
                                .clientInfo(clientInfo)
                                .topic(topic));
                            yield true;
                        }
                        default -> {
                            eventCollector.report(getLocal(MsgRetainedError.class)
                                .reqId(v.getReqId())
                                .clientInfo(clientInfo)
                                .topic(topic)
                                .isLastWill(false)
                                .qos(message.getPubQoS())
                                .payload(message.getPayload().asReadOnlyByteBuffer())
                                .size(message.getPayload().size()));
                            yield false;
                        }
                    };
                }, ctx.channel().eventLoop()));
    }

    private CompletableFuture<Void> distWillMessage(LWT willMessage) {
        return sessionCtx.authProvider(ctx).check(clientInfo(), buildPubAction(willMessage.getTopic(),
                willMessage.getMessage()
                    .getPubQoS(),
                willMessage.getRetain()))
            .thenComposeAsync(allow -> {
                if (allow) {
                    long reqId = System.nanoTime();
                    return allOf(distWillMessage(reqId, willMessage), retainWillMessage(reqId, willMessage));
                } else {
                    sessionCtx.eventCollector.report(getLocal(PubActionDisallow.class)
                        .isLastWill(true)
                        .topic(willMessage.getTopic())
                        .qos(willMessage.getMessage().getPubQoS())
                        .isRetain(willMessage.getRetain())
                        .clientInfo(clientInfo));
                    return CompletableFuture.completedFuture(null);
                }
            }, ctx.channel().eventLoop());
    }

    private CompletableFuture<Void> distWillMessage(long reqId, LWT willMessage) {
        return sessionCtx.distClient.pub(reqId,
                willMessage.getTopic(),
                willMessage.getMessage().toBuilder().setTimestamp(HLC.INST.getPhysical()).build(),
                clientInfo)
            .handleAsync((v, e) -> {
                if (e != null || v == DistResult.EXCEED_LIMIT || v == DistResult.ERROR) {
                    eventCollector.report(getLocal(WillDistError.class)
                        .clientInfo(clientInfo)
                        .reqId(reqId)
                        .topic(willMessage.getTopic())
                        .qos(willMessage.getMessage().getPubQoS())
                        .size(willMessage.getMessage().getPayload().size()));
                } else {
                    eventCollector.report(getLocal(WillDisted.class)
                        .clientInfo(clientInfo)
                        .reqId(reqId)
                        .topic(willMessage.getTopic())
                        .qos(willMessage.getMessage().getPubQoS())
                        .size(willMessage.getMessage().getPayload().size()));
                }
                return null;
            }, ctx.channel().eventLoop());
    }

    private CompletableFuture<Void> retainWillMessage(long reqId, LWT willMessage) {
        if (!settings.retainEnabled) {
            return CompletableFuture.completedFuture(null);
        }
        if (!willMessage.getRetain()) {
            return CompletableFuture.completedFuture(null);
        }
        String topic = willMessage.getTopic();
        QoS qos = willMessage.getMessage().getPubQoS();
        ByteString payload = willMessage.getMessage().getPayload();

        return sessionCtx.retainClient.retain(
                reqId,
                topic,
                qos,
                payload,
                willMessage.getMessage().getExpiryInterval(),
                clientInfo)
            .handleAsync((v, e) -> {
                switch (v.getResult()) {
                    case RETAINED -> eventCollector.report(getLocal(MsgRetained.class)
                        .reqId(v.getReqId())
                        .topic(topic)
                        .isLastWill(true)
                        .qos(qos)
                        .size(payload.size())
                        .clientInfo(clientInfo));
                    case CLEARED -> eventCollector.report(getLocal(RetainMsgCleared.class)
                        .reqId(v.getReqId())
                        .isLastWill(true)
                        .clientInfo(clientInfo)
                        .topic(topic));
                    default -> eventCollector.report(getLocal(MsgRetainedError.class)
                        .reqId(v.getReqId())
                        .clientInfo(clientInfo)
                        .topic(topic)
                        .isLastWill(true)
                        .qos(qos)
                        .payload(payload.asReadOnlyByteBuffer())
                        .size(payload.size()));
                }
                return null;
            }, ctx.channel().eventLoop());
    }

}
