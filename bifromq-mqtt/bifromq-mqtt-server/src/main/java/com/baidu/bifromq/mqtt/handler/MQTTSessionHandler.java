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

import static com.baidu.bifromq.inbox.storage.proto.RetainHandling.SEND_AT_SUBSCRIBE;
import static com.baidu.bifromq.inbox.storage.proto.RetainHandling.SEND_AT_SUBSCRIBE_IF_NOT_YET_EXISTS;
import static com.baidu.bifromq.metrics.TenantMetric.MqttConnectCount;
import static com.baidu.bifromq.metrics.TenantMetric.MqttDisconnectCount;
import static com.baidu.bifromq.metrics.TenantMetric.MqttIngressBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS0DistBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS0IngressBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS1DeliverBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS1DistBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS1ExternalLatency;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS1IngressBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS2DeliverBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS2DistBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS2ExternalLatency;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS2IngressBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttTransientSubLatency;
import static com.baidu.bifromq.mqtt.handler.IMQTTProtocolHelper.SubResult.EXCEED_LIMIT;
import static com.baidu.bifromq.mqtt.handler.MQTTSessionIdUtil.packetId;
import static com.baidu.bifromq.mqtt.handler.MQTTSessionIdUtil.userSessionId;
import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.messageExpiryInterval;
import static com.baidu.bifromq.mqtt.utils.AuthUtil.buildPubAction;
import static com.baidu.bifromq.mqtt.utils.AuthUtil.buildSubAction;
import static com.baidu.bifromq.mqtt.utils.AuthUtil.buildUnsubAction;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_5_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_KEY;
import static com.baidu.bifromq.type.QoS.AT_LEAST_ONCE;
import static com.baidu.bifromq.type.QoS.AT_MOST_ONCE;
import static com.baidu.bifromq.type.QoS.EXACTLY_ONCE;
import static com.baidu.bifromq.util.TopicUtil.isSharedSubscription;
import static com.baidu.bifromq.util.TopicUtil.isValidTopicFilter;
import static com.baidu.bifromq.util.TopicUtil.isWildcardTopicFilter;
import static com.bifromq.plugin.resourcethrottler.TenantResourceType.TotalRetainMatchBytesPerSecond;
import static com.bifromq.plugin.resourcethrottler.TenantResourceType.TotalRetainMatchPerSeconds;
import static com.bifromq.plugin.resourcethrottler.TenantResourceType.TotalRetainMessageSpaceBytes;
import static com.bifromq.plugin.resourcethrottler.TenantResourceType.TotalRetainTopics;
import static com.bifromq.plugin.resourcethrottler.TenantResourceType.TotalRetainedBytesPerSecond;
import static com.bifromq.plugin.resourcethrottler.TenantResourceType.TotalRetainedMessagesPerSeconds;
import static com.bifromq.plugin.resourcethrottler.TenantResourceType.TotalSharedSubscriptions;
import static java.util.concurrent.CompletableFuture.allOf;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.baserpc.utils.FutureTracker;
import com.baidu.bifromq.dist.client.DistResult;
import com.baidu.bifromq.inbox.storage.proto.LWT;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterOption;
import com.baidu.bifromq.metrics.ITenantMeter;
import com.baidu.bifromq.mqtt.handler.record.ProtocolResponse;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.SubReply;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.UnsubReply;
import com.baidu.bifromq.mqtt.session.IMQTTSession;
import com.baidu.bifromq.mqtt.session.MQTTSessionContext;
import com.baidu.bifromq.mqtt.utils.IMQTTMessageSizer;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.authprovider.type.CheckResult;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.PingReq;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.accessctrl.PubActionDisallow;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.accessctrl.SubActionDisallow;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.accessctrl.UnsubActionDisallow;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ClientChannelError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.InvalidTopicFilter;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.MalformedTopicFilter;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ResourceThrottled;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS0DistError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS1DistError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS1PubAckDropped;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS2DistError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.QoS2PubRecDropped;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.WillDistError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.WillDisted;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.DropReason;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS0Dropped;
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
import com.baidu.bifromq.retain.rpc.proto.MatchReply;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import com.baidu.bifromq.sessiondict.client.ISessionRegister;
import com.baidu.bifromq.sysprops.BifroMQSysProp;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.MQTTClientInfoConstants;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.UserProperties;
import com.baidu.bifromq.util.UTF8Util;
import com.bifromq.plugin.resourcethrottler.IResourceThrottler;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MQTTSessionHandler extends MQTTMessageHandler implements IMQTTSession {
    protected static final boolean SANITY_CHECK = BifroMQSysProp.MQTT_UTF8_SANITY_CHECK.get();

    @Accessors(fluent = true)
    @Getter
    public static class SubMessage {
        private final String topic;
        private final Message message;
        private final ClientInfo publisher;
        private final String topicFilter;
        private final TopicFilterOption option;
        private final int bytesSize;

        public SubMessage(String topic, Message message, ClientInfo publisher, String topicFilter,
                          TopicFilterOption option) {
            this.topic = topic;
            this.message = message;
            this.publisher = publisher;
            this.topicFilter = topicFilter;
            this.option = option;
            this.bytesSize = topic.length() + topicFilter.length() + message.getPayload().size();
        }

        public boolean isRetain() {
            return message.getIsRetained() ||
                option.getRetainAsPublished() && message.getIsRetain();
        }

        public QoS qos() {
            return QoS.forNumber(Math.min(message.getPubQoS().getNumber(), option.getQos().getNumber()));
        }

        public int estBytes() {
            return bytesSize;
        }
    }

    private static class ConfirmingMessage {
        final long seq;
        final SubMessage message;
        final long timestamp; // timestamp of first sent
        int sentCount = 1;
        boolean acked = false;

        private ConfirmingMessage(long seq, SubMessage message) {
            this.seq = seq;
            this.message = message;
            this.timestamp = System.nanoTime();
        }

        int packetId() {
            return MQTTSessionIdUtil.packetId(seq);
        }

        void setAcked() {
            acked = true;
        }
    }

    protected final TenantSettings settings;
    protected final String userSessionId;
    protected final int keepAliveTimeSeconds;
    protected final ClientInfo clientInfo;
    protected final AtomicLong memUsage;
    protected final ITenantMeter tenantMeter;
    private final long idleTimeoutNanos;
    private final MPSThrottler throttler;
    private final Set<CompletableFuture<?>> fgTasks = ConcurrentHashMap.newKeySet();
    private final FutureTracker bgTasks = new FutureTracker();
    private final Set<Integer> inUsePacketIds = new HashSet<>();

    protected final ChannelHandlerContext ctx;
    protected final MQTTSessionContext sessionCtx;
    protected final IAuthProvider authProvider;
    protected final IEventCollector eventCollector;
    protected final IResourceThrottler resourceThrottler;
    private final IMQTTMessageSizer sizer;
    private LWT willMessage;
    private boolean isGoAway;
    private ScheduledFuture<?> idleTimeoutTask;
    private ISessionRegister sessionRegister;
    private long lastActiveAtNanos;
    private final LinkedHashMap<Integer, ConfirmingMessage> unconfirmedPacketIds = new LinkedHashMap<>();
    private final TreeSet<ConfirmingMessage> resendQueue;
    private final CompletableFuture<Void> onInitialized = new CompletableFuture<>();
    private ScheduledFuture<?> resendTask;
    private int receivingCount = 0;

    protected MQTTSessionHandler(TenantSettings settings,
                                 String userSessionId,
                                 int keepAliveTimeSeconds,
                                 ClientInfo clientInfo,
                                 @Nullable LWT willMessage,
                                 ChannelHandlerContext ctx) {
        this.sizer = clientInfo.getMetadataOrDefault(MQTT_PROTOCOL_VER_KEY, "").equals(MQTT_PROTOCOL_VER_5_VALUE) ?
            IMQTTMessageSizer.mqtt5() : IMQTTMessageSizer.mqtt3();
        this.ctx = ctx;
        this.settings = settings;
        this.userSessionId = userSessionId;
        this.keepAliveTimeSeconds = keepAliveTimeSeconds;
        this.clientInfo = clientInfo;
        this.willMessage = willMessage;
        this.tenantMeter = ITenantMeter.get(clientInfo.getTenantId());
        this.throttler = new MPSThrottler(settings.maxMsgPerSec);
        this.idleTimeoutNanos = Duration.ofMillis(keepAliveTimeSeconds * 1500L).toNanos(); // x1.5
        resendQueue = new TreeSet<>(Comparator.comparingLong(this::ackTimeoutNanos));
        sessionCtx = ChannelAttrs.mqttSessionContext(ctx);
        // strong reference to avoid gc
        memUsage = sessionCtx.getSessionMemGauge(clientInfo.getTenantId());
        authProvider = sessionCtx.authProvider(ctx);
        eventCollector = sessionCtx.eventCollector;
        resourceThrottler = sessionCtx.resourceThrottler;
    }

    private int estMemSize() {
        int s = 144; // base size from JOL
        s += userSessionId.length();
        s += clientInfo.getSerializedSize();
        if (willMessage != null) {
            s += willMessage.getSerializedSize();
        }
        return s;
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
        ctx.executor().execute(() -> handleProtocolResponse(helper().onDisconnect()));
        return bgTasks.whenComplete((v, e) -> log.trace("All bg tasks finished: client={}", clientInfo));
    }

    @Override
    public final CompletableFuture<SubReply.Result> subscribe(long reqId, String topicFilter, QoS qos) {
        return CompletableFuture.completedFuture(true)
            .thenComposeAsync(v -> checkAndSubscribe(reqId, topicFilter, TopicFilterOption.newBuilder()
                .setQos(qos)
                .build(), UserProperties.getDefaultInstance())
                .thenApply(subResult -> SubReply.Result.forNumber(subResult.ordinal())), ctx.executor());
    }

    @Override
    public final CompletableFuture<UnsubReply.Result> unsubscribe(long reqId, String topicFilter) {
        return CompletableFuture.completedFuture(true)
            .thenComposeAsync(v -> checkAndUnsubscribe(reqId, topicFilter, UserProperties.getDefaultInstance())
                .thenApply(unsubResult -> UnsubReply.Result.forNumber(unsubResult.ordinal())), ctx.executor());
    }

    public final CompletableFuture<Void> awaitInitialized() {
        return onInitialized;
    }

    protected final void onInitialized() {
        onInitialized.complete(null);
    }

    protected final LWT willMessage() {
        return willMessage;
    }

    protected final <T> CompletableFuture<T> addFgTask(CompletableFuture<T> taskFuture) {
        assert ctx.executor().inEventLoop();
        if (!taskFuture.isDone()) {
            fgTasks.add(taskFuture);
            taskFuture.whenComplete((v, e) -> {
                if (ctx.executor().inEventLoop()) {
                    fgTasks.remove(taskFuture);
                } else {
                    ctx.executor().execute(() -> fgTasks.remove(taskFuture));
                }
            });
        }
        return taskFuture;
    }

    protected final <T> CompletableFuture<T> trackTask(CompletableFuture<T> task, boolean background) {
        if (background) {
            return addBgTask(task);
        }
        return addFgTask(task);
    }

    protected final <T> CompletableFuture<T> addBgTask(CompletableFuture<T> task) {
        return bgTasks.track(sessionCtx.trackBgTask(task));
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        super.handlerAdded(ctx);
        ChannelAttrs.trafficShaper(ctx).setReadLimit(settings.inboundBandwidth);
        ChannelAttrs.trafficShaper(ctx).setWriteLimit(settings.outboundBandwidth);
        ChannelAttrs.setMaxPayload(settings.maxPacketSize, ctx);
        sessionCtx.localSessionRegistry.add(channelId(), this);
        sessionRegister = ChannelAttrs.mqttSessionContext(ctx).sessionDictClient
            .reg(clientInfo, kicker -> ctx.executor().execute(() -> handleProtocolResponse(helper().onKick(kicker))));
        lastActiveAtNanos = sessionCtx.nanoTime();
        idleTimeoutTask = ctx.executor()
            .scheduleAtFixedRate(this::checkIdle, idleTimeoutNanos, idleTimeoutNanos, TimeUnit.NANOSECONDS);
        onInitialized.whenComplete((v, e) -> tenantMeter.recordCount(MqttConnectCount));
        memUsage.addAndGet(estMemSize());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (idleTimeoutTask != null) {
            idleTimeoutTask.cancel(true);
        }
        if (resendTask != null) {
            resendTask.cancel(true);
        }
        if (willMessage != null) {
            addBgTask(pubWillMessage(willMessage));
        }
        fgTasks.forEach(t -> t.cancel(true));
        sessionCtx.localSessionRegistry.remove(channelId(), this);
        sessionRegister.stop();
        tenantMeter.recordCount(MqttDisconnectCount);
        memUsage.addAndGet(-estMemSize());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        super.exceptionCaught(ctx, cause);
        log.debug("ctx: {}, cause:", ctx, cause);
        // if disconnection is caused purely by channel error
        handleProtocolResponse(
            ProtocolResponse.goAwayNow(getLocal(ClientChannelError.class).clientInfo(clientInfo).cause(cause)));
    }

    @Override
    public final void channelRead(ChannelHandlerContext ctx, Object msg) {
        assert msg instanceof MqttMessage;
        MqttMessage mqttMessage = (MqttMessage) msg;
        log.trace("Received {}", mqttMessage);
        if (mqttMessage.decoderResult().isSuccess()) {
            tenantMeter.recordSummary(MqttIngressBytes, mqttMessage.fixedHeader().remainingLength() + 1);
            lastActiveAtNanos = sessionCtx.nanoTime();
            if (log.isTraceEnabled()) {
                log.trace("Received mqtt message:{}", mqttMessage);
            }
            switch (mqttMessage.fixedHeader().messageType()) {
                case CONNECT ->
                    handleProtocolResponse(helper().respondDuplicateConnect((MqttConnectMessage) mqttMessage));
                case DISCONNECT -> handleProtocolResponse(handleDisconnect(mqttMessage));
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
            handleProtocolResponse(helper().respondDecodeError(mqttMessage));
        }
    }

    protected void handleOther(MqttMessage message) {

    }

    protected abstract ProtocolResponse handleDisconnect(MqttMessage message);

    private void handlePubMsg(MqttPublishMessage mqttMessage) {

        if (isExceedReceivingMaximum()) {
            handleProtocolResponse(helper().respondReceivingMaximumExceeded(mqttMessage));
            mqttMessage.release();
        }
        if (!throttler.pass()) {
            handleProtocolResponse(helper().respondPubRateExceeded(mqttMessage));
            mqttMessage.release();
            return;
        }
        ProtocolResponse isInvalid = helper().validatePubMessage(mqttMessage);
        if (isInvalid != null) {
            handleProtocolResponse(isInvalid);
            mqttMessage.release();
            return;
        }
        int packetId = mqttMessage.variableHeader().packetId();
        long reqId = packetId > 0 ? packetId : System.nanoTime();
        String topic = helper().getTopic(mqttMessage);
        int ingressMsgBytes = mqttMessage.fixedHeader().remainingLength() + 1;
        (switch (mqttMessage.fixedHeader().qosLevel()) {
            case AT_MOST_ONCE -> handleQoS0Pub(reqId, topic, mqttMessage, ingressMsgBytes);
            case AT_LEAST_ONCE -> handleQoS1Pub(reqId, topic, mqttMessage, ingressMsgBytes);
            case EXACTLY_ONCE -> handleQoS2Pub(reqId, topic, mqttMessage, ingressMsgBytes);
            case FAILURE -> CompletableFuture.completedFuture(DistResult.ERROR);
        }).whenComplete((v, e) -> mqttMessage.release());
    }

    private void handleSubMsg(MqttSubscribeMessage message) {
        ProtocolResponse isInvalid = helper().validateSubMessage(message);
        if (isInvalid != null) {
            handleProtocolResponse(isInvalid);
            return;
        }
        int packetId = message.variableHeader().messageId();
        if (helper().checkPacketIdUsage() && inUsePacketIds.contains(packetId)) {
            writeAndFlush(helper().respondPacketIdInUse(message));
            return;
        }
        inUsePacketIds.add(packetId);
        doSubscribe(packetId, message)
            .thenAcceptAsync(response -> {
                handleProtocolResponse(response);
                if (response.action() == ProtocolResponse.Action.Response) {
                    inUsePacketIds.remove(packetId);
                    eventCollector.report(getLocal(SubAcked.class)
                        .messageId(packetId)
                        .granted(((MqttSubAckMessage) (response.message())).payload().grantedQoSLevels())
                        .topicFilter(message.payload().topicSubscriptions().stream()
                            .map(MqttTopicSubscription::topicName)
                            .collect(Collectors.toList()))
                        .clientInfo(clientInfo));
                }
            }, ctx.executor());
    }

    private CompletableFuture<ProtocolResponse> doSubscribe(long reqId, MqttSubscribeMessage message) {
        List<CompletableFuture<IMQTTProtocolHelper.SubResult>> resultFutures = helper().getSubTask(message).stream()
            .map(subTask -> checkAndSubscribe(reqId, subTask.topicFilter(), subTask.option(), subTask.userProperties()))
            .toList();
        return CompletableFuture.allOf(resultFutures.toArray(CompletableFuture[]::new))
            .thenApplyAsync(v -> {
                List<IMQTTProtocolHelper.SubResult> subResults =
                    resultFutures.stream().map(CompletableFuture::join).toList();
                if (subResults.stream().anyMatch(r -> r == IMQTTProtocolHelper.SubResult.BACK_PRESSURE_REJECTED)) {
                    return helper().onSubBackPressured(message);
                }
                return helper().buildSubAckMessage(message, subResults);
            }, ctx.executor());
    }

    protected final CompletableFuture<IMQTTProtocolHelper.SubResult> checkAndSubscribe(long reqId,
                                                                                       String topicFilter,
                                                                                       TopicFilterOption option,
                                                                                       UserProperties userProps) {
        if (!UTF8Util.isWellFormed(topicFilter, SANITY_CHECK)) {
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
        if (isWildcardTopicFilter(topicFilter) && !settings.wildcardSubscriptionEnabled) {
            return CompletableFuture.completedFuture(IMQTTProtocolHelper.SubResult.WILDCARD_NOT_SUPPORTED);
        }

        if (isSharedSubscription(topicFilter) && !settings.subscriptionIdentifierEnabled) {
            return CompletableFuture.completedFuture(IMQTTProtocolHelper.SubResult.SHARED_SUBSCRIPTION_NOT_SUPPORTED);
        }

        return addFgTask(
            authProvider.checkPermission(clientInfo, buildSubAction(topicFilter, option.getQos(), userProps))
                .thenComposeAsync(checkResult -> {
                    if (checkResult.hasGranted()) {
                        if (isSharedSubscription(topicFilter)
                            && !resourceThrottler.hasResource(clientInfo.getTenantId(), TotalSharedSubscriptions)) {
                            eventCollector.report(getLocal(ResourceThrottled.class)
                                .reason(TotalSharedSubscriptions.name())
                                .clientInfo(clientInfo));
                            return CompletableFuture.completedFuture(IMQTTProtocolHelper.SubResult.EXCEED_LIMIT);
                        }
                        Timer.Sample start = Timer.start();
                        return addFgTask(subTopicFilter(reqId, topicFilter, option))
                            .thenComposeAsync(subResult -> {
                                switch (subResult) {
                                    case OK, EXISTS -> {
                                        start.stop(tenantMeter.timer(MqttTransientSubLatency));
                                        if (!isSharedSubscription(topicFilter) && settings.retainEnabled &&
                                            (option.getRetainHandling() == SEND_AT_SUBSCRIBE ||
                                                (subResult == IMQTTProtocolHelper.SubResult.OK
                                                    && option.getRetainHandling() ==
                                                    SEND_AT_SUBSCRIBE_IF_NOT_YET_EXISTS))) {
                                            if (!resourceThrottler.hasResource(clientInfo.getTenantId(),
                                                TotalRetainMatchPerSeconds)) {
                                                eventCollector.report(getLocal(ResourceThrottled.class)
                                                    .reason(TotalRetainMatchPerSeconds.name())
                                                    .clientInfo(clientInfo));
                                                return CompletableFuture.completedFuture(EXCEED_LIMIT);
                                            }
                                            if (!resourceThrottler.hasResource(clientInfo.getTenantId(),
                                                TotalRetainMatchBytesPerSecond)) {
                                                eventCollector.report(getLocal(ResourceThrottled.class)
                                                    .reason(TotalRetainMatchBytesPerSecond.name())
                                                    .clientInfo(clientInfo));
                                                return CompletableFuture.completedFuture(EXCEED_LIMIT);
                                            }
                                            return addFgTask(matchRetainedMessage(reqId, topicFilter))
                                                .handle((v, e) -> {
                                                    if (e != null || v.getResult() == MatchReply.Result.ERROR) {
                                                        return IMQTTProtocolHelper.SubResult.ERROR;
                                                    } else {
                                                        return subResult;
                                                    }
                                                });
                                        }
                                        return CompletableFuture.completedFuture(subResult);
                                    }
                                    case EXCEED_LIMIT -> {
                                        return CompletableFuture.completedFuture(
                                            IMQTTProtocolHelper.SubResult.EXCEED_LIMIT);
                                    }
                                    default -> {
                                        return CompletableFuture.completedFuture(IMQTTProtocolHelper.SubResult.ERROR);
                                    }
                                }
                            }, ctx.executor());
                    } else {
                        eventCollector.report(getLocal(SubActionDisallow.class)
                            .topicFilter(topicFilter)
                            .qos(option.getQos())
                            .clientInfo(clientInfo));
                        return CompletableFuture.completedFuture(IMQTTProtocolHelper.SubResult.NOT_AUTHORIZED);
                    }
                }, ctx.executor()));
    }

    protected abstract CompletableFuture<IMQTTProtocolHelper.SubResult> subTopicFilter(long reqId,
                                                                                       String topicFilter,
                                                                                       TopicFilterOption option);

    protected abstract CompletableFuture<MatchReply> matchRetainedMessage(long reqId, String topicFilter);

    private void handleUnsubMsg(MqttUnsubscribeMessage message) {
        ProtocolResponse goAwayOnInvalid = helper().validateUnsubMessage(message);
        if (goAwayOnInvalid != null) {
            handleProtocolResponse(goAwayOnInvalid);
            return;
        }
        int packetId = message.variableHeader().messageId();
        if (helper().checkPacketIdUsage() && inUsePacketIds.contains(packetId)) {
            writeAndFlush(helper().respondPacketIdInUse(message));
            return;
        }
        inUsePacketIds.add(packetId);
        doUnsubscribe(packetId, message)
            .thenAcceptAsync(response -> {
                inUsePacketIds.remove(packetId);
                handleProtocolResponse(response);
                if (response.action() == ProtocolResponse.Action.Response) {
                    eventCollector.report(getLocal(UnsubAcked.class)
                        .messageId(packetId)
                        .topicFilter(message.payload().topics())
                        .clientInfo(clientInfo));
                }
            }, ctx.executor());
    }

    private CompletableFuture<ProtocolResponse> doUnsubscribe(long reqId, MqttUnsubscribeMessage message) {
        UserProperties userProps = helper().getUserProps(message);
        List<CompletableFuture<IMQTTProtocolHelper.UnsubResult>> resultFutures = message.payload().topics().stream()
            .map(topicFilter -> checkAndUnsubscribe(reqId, topicFilter, userProps))
            .toList();
        return CompletableFuture.allOf(resultFutures.toArray(CompletableFuture[]::new))
            .thenApply(v -> resultFutures.stream().map(CompletableFuture::join).toList())
            .thenApply(subResults -> {
                if (subResults.stream().anyMatch(r -> r == IMQTTProtocolHelper.UnsubResult.BACK_PRESSURE_REJECTED)) {
                    return helper().onUnsubBackPressured(message);
                }
                return helper().buildUnsubAckMessage(message, subResults);
            });
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
        return addFgTask(authProvider.checkPermission(clientInfo, buildUnsubAction(topicFilter, userProps)))
            .thenComposeAsync(checkResult -> {
                if (checkResult.hasGranted()) {
                    return addFgTask(unsubTopicFilter(reqId, topicFilter));
                } else {
                    eventCollector.report(getLocal(UnsubActionDisallow.class)
                        .clientInfo(clientInfo)
                        .topicFilter(topicFilter));
                    // always reply unsub ack
                    return CompletableFuture.completedFuture(IMQTTProtocolHelper.UnsubResult.NOT_AUTHORIZED);
                }
            }, ctx.executor());
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
            tenantMeter.recordSummary(MqttQoS1DeliverBytes, confirmed.message().getPayload().size());
        } else {
            log.trace("No packetId to confirm released: sessionId={}, packetId={}",
                userSessionId(clientInfo), packetId);
        }
    }

    private void handlePubRecMsg(MqttMessage message) {
        int packetId = ((MqttMessageIdVariableHeader) message.variableHeader()).messageId();
        if (isConfirming(packetId)) {
            if (helper().isQoS2Received(message)) {
                handleProtocolResponse(helper().respondPubRecMsg(message, false));
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
            handleProtocolResponse(helper().respondPubRecMsg(message, true));
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
            tenantMeter.recordSummary(MqttQoS2DeliverBytes, confirmed.message.getPayload().size());
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
        ConfirmingMessage confirmingMsg = unconfirmedPacketIds.get(packetId);
        SubMessage msg = null;
        if (confirmingMsg != null) {
            long now = System.nanoTime();
            msg = confirmingMsg.message;
            confirmingMsg.setAcked();
            resendQueue.remove(confirmingMsg);
            Iterator<Integer> packetIdItr = unconfirmedPacketIds.keySet().iterator();
            while (packetIdItr.hasNext()) {
                packetId = packetIdItr.next();
                confirmingMsg = unconfirmedPacketIds.get(packetId);
                if (confirmingMsg.acked) {
                    packetIdItr.remove();
                    SubMessage confirmed = confirmingMsg.message;
                    onConfirm(confirmingMsg.seq);
                    switch (confirmed.qos()) {
                        case AT_LEAST_ONCE -> {
                            tenantMeter.timer(MqttQoS1ExternalLatency)
                                .record(now - confirmingMsg.timestamp, TimeUnit.NANOSECONDS);
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
                            .record(now - confirmingMsg.timestamp, TimeUnit.NANOSECONDS);
                    }
                }
            }
            if (resendTask != null && !resendTask.isDone()) {
                resendTask.cancel(true);
            }
            if (!resendQueue.isEmpty()) {
                scheduleResend();
            }
        } else {
            log.trace("No msg to confirm: sessionId={}, packetId={}", userSessionId, packetId);
        }
        return msg;
    }

    protected abstract void onConfirm(long seq);

    protected final void sendQoS0SubMessage(SubMessage msg) {
        assert msg.qos() == AT_MOST_ONCE;
        sendMqttPubMessage(-1, msg, msg.topicFilter(), msg.publisher(), false);
    }

    protected final void sendConfirmableMessage(long seq, SubMessage msg) {
        assert seq > -1;
        ConfirmingMessage confirmingMessage = new ConfirmingMessage(seq, msg);
        ConfirmingMessage prev = unconfirmedPacketIds.putIfAbsent(confirmingMessage.packetId(), confirmingMessage);
        if (prev == null) {
            resendQueue.add(confirmingMessage);
            if (resendTask == null || resendTask.isDone()) {
                scheduleResend();
            }
            sendMqttPubMessage(seq, msg, msg.topicFilter(), msg.publisher(), false);
        } else {
            log.warn("Bad state: sequence duplicate seq={}", seq);
        }
    }

    private long ackTimeoutNanos(ConfirmingMessage msg) {
        return msg.timestamp + Duration.ofSeconds(settings.resendTimeoutSeconds).multipliedBy(msg.sentCount).toNanos();
    }

    private void scheduleResend() {
        resendTask =
            ctx.executor().schedule(this::resend, ackTimeoutNanos(resendQueue.first()), TimeUnit.NANOSECONDS);
    }

    private void resend() {
        long now = System.nanoTime() + Duration.ofMillis(100).toNanos();
        while (!resendQueue.isEmpty()) {
            ConfirmingMessage confirmingMessage = resendQueue.first();
            if (ackTimeoutNanos(confirmingMessage) > now) {
                scheduleResend();
                break;
            }
            if (confirmingMessage.sentCount < settings.maxResendTimes + 1) {
                resendQueue.remove(confirmingMessage);
                confirmingMessage.sentCount++;
                sendMqttPubMessage(confirmingMessage.seq, confirmingMessage.message,
                    confirmingMessage.message.topicFilter(), confirmingMessage.message.publisher(), true);
                resendQueue.add(confirmingMessage);
            } else {
                confirm(confirmingMessage.packetId());
            }
        }
    }

    private void sendMqttPubMessage(long seq, SubMessage msg, String topicFilter, ClientInfo publisher, boolean isDup) {
        MqttPublishMessage pubMsg = helper().buildMqttPubMessage(packetId(seq), msg, isDup);
        if (messageExpiryInterval(pubMsg.variableHeader().properties()).orElse(Integer.MAX_VALUE) < 0) {
            //  If the Message Expiry Interval has passed and the Server has not managed to start onward delivery to a matching subscriber, then it MUST delete the copy of the message for that subscriber [MQTT-3.3.2-5]
            return;
        }
        int msgSize = sizer.sizeOf(pubMsg).encodedBytes();
        if (!ctx.channel().isWritable()) {
            if (pubMsg.fixedHeader().qosLevel() == MqttQoS.AT_MOST_ONCE) {
                eventCollector.report(getLocal(QoS0Dropped.class)
                    .reason(DropReason.Overflow)
                    .isRetain(false)
                    .sender(publisher)
                    .topic(msg.topic)
                    .matchedFilter(topicFilter)
                    .size(msgSize)
                    .clientInfo(clientInfo()));
            }
            // qos1 and qos2 will be resent by the server
            return;
        }
        memUsage.addAndGet(msgSize);
        ctx.write(pubMsg).addListener(f -> {
            memUsage.addAndGet(-msgSize);
            if (f.isSuccess()) {
                if (settings.debugMode) {
                    switch (pubMsg.fixedHeader().qosLevel()) {
                        case AT_MOST_ONCE -> eventCollector.report(getLocal(QoS0Pushed.class)
                            .reqId(pubMsg.variableHeader().packetId())
                            .isRetain(msg.isRetain())
                            .sender(publisher)
                            .matchedFilter(topicFilter)
                            .topic(msg.topic)
                            .size(msgSize)
                            .clientInfo(clientInfo));
                        case AT_LEAST_ONCE -> eventCollector.report(getLocal(QoS1Pushed.class)
                            .reqId(pubMsg.variableHeader().packetId())
                            .messageId(pubMsg.variableHeader().packetId())
                            .dup(false)
                            .isRetain(pubMsg.fixedHeader().isRetain())
                            .sender(publisher)
                            .matchedFilter(topicFilter)
                            .topic(pubMsg.variableHeader().topicName())
                            .size(msgSize)
                            .clientInfo(clientInfo));
                        case EXACTLY_ONCE -> eventCollector.report(getLocal(QoS2Pushed.class)
                            .reqId(pubMsg.variableHeader().packetId())
                            .messageId(pubMsg.variableHeader().packetId())
                            .dup(false)
                            .isRetain(pubMsg.fixedHeader().isRetain())
                            .sender(publisher)
                            .matchedFilter(topicFilter)
                            .topic(pubMsg.variableHeader().topicName())
                            .size(msgSize)
                            .clientInfo(clientInfo));
                    }
                }
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

    private CompletableFuture<CheckResult> checkPubPermission(String topic, Message distMessage,
                                                              UserProperties userProps) {
        return authProvider.checkPermission(clientInfo(),
            buildPubAction(topic, distMessage.getPubQoS(), distMessage.getIsRetain(), userProps));
    }

    private CompletableFuture<Void> handleQoS0Pub(long reqId,
                                                  String topic,
                                                  MqttPublishMessage message,
                                                  int ingressMsgBytes) {
        if (log.isTraceEnabled()) {
            log.trace("Checking authorization of pub qos0 action: reqId={}, sessionId={}, topic={}", reqId,
                userSessionId(clientInfo), topic);
        }
        Message distMessage = helper().buildDistMessage(message);
        UserProperties userProps = helper().getUserProps(message);
        return addFgTask(checkPubPermission(topic, distMessage, userProps))
            .thenComposeAsync(checkResult -> {
                if (log.isTraceEnabled()) {
                    log.trace("Checked authorization of pub qos0 action: reqId={}, sessionId={}, topic={}:{}",
                        reqId, userSessionId(clientInfo), topic, checkResult.getTypeCase());
                }
                if (checkResult.getTypeCase() == CheckResult.TypeCase.GRANTED) {
                    tenantMeter.recordSummary(MqttQoS0IngressBytes, ingressMsgBytes);
                    return doPub(reqId, topic, distMessage, false, ingressMsgBytes)
                        .thenAcceptAsync(pubResult -> {
                            if (log.isTraceEnabled()) {
                                log.trace("Disted qos0 msg: reqId={}, sessionId={}, topic={}",
                                    reqId, userSessionId(clientInfo), topic);
                            }
                            handleProtocolResponse(helper().onQoS0PubHandled(pubResult, message,
                                checkResult.getGranted().getUserProps()));
                        }, ctx.executor());
                }
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
                handleProtocolResponse(helper().onQoS0DistDenied(topic, distMessage, checkResult));
                return CompletableFuture.completedFuture(null);
            }, ctx.executor());
    }

    private CompletableFuture<Void> handleQoS1Pub(long reqId,
                                                  String topic,
                                                  MqttPublishMessage message,
                                                  int ingressMsgBytes) {
        int packetId = message.variableHeader().packetId();
        if (inUsePacketIds.contains(packetId)) {
            return CompletableFuture.completedFuture(null);
        }
        inUsePacketIds.add(packetId);
        incReceivingCount();
        if (log.isTraceEnabled()) {
            log.trace("Checking authorization of pub qos1 action: reqId={}, sessionId={}, topic={}",
                reqId, userSessionId(clientInfo), topic);
        }
        Message distMessage = helper().buildDistMessage(message);
        UserProperties userProps = helper().getUserProps(message);
        return addFgTask(checkPubPermission(topic, distMessage, userProps))
            .thenComposeAsync(checkResult -> {
                if (checkResult.getTypeCase() == CheckResult.TypeCase.GRANTED) {
                    tenantMeter.recordSummary(MqttQoS1IngressBytes, ingressMsgBytes);
                    return doPub(reqId, topic, distMessage, message.fixedHeader().isDup(), ingressMsgBytes)
                        .thenAcceptAsync(pubResult -> {
                            if (log.isTraceEnabled()) {
                                log.trace("Disted qos1 msg: reqId={}, sessionId={}, topic={}",
                                    reqId, userSessionId(clientInfo), topic);
                            }
                            decReceivingCount();
                            inUsePacketIds.remove(packetId);
                            if (ctx.channel().isActive() && ctx.channel().isWritable()) {
                                handleProtocolResponse(helper()
                                    .onQoS1PubHandled(pubResult, message, checkResult.getGranted().getUserProps()));
                            } else {
                                eventCollector.report(getLocal(QoS1PubAckDropped.class)
                                    .reqId(reqId)
                                    .isDup(message.fixedHeader().isDup())
                                    .topic(topic)
                                    .size(message.payload().readableBytes())
                                    .clientInfo(clientInfo));
                            }
                        }, ctx.executor());
                }
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
                handleProtocolResponse(
                    helper().onQoS1DistDenied(topic, packetId, distMessage, checkResult));
                return CompletableFuture.completedFuture(null);
            }, ctx.executor());
    }

    private CompletableFuture<Void> handleQoS2Pub(long reqId,
                                                  String topic,
                                                  MqttPublishMessage message,
                                                  int ingressMsgBytes) {
        int packetId = message.variableHeader().packetId();
        if (inUsePacketIds.contains(packetId)) {
            handleProtocolResponse(helper().respondQoS2PacketInUse(message));
            return CompletableFuture.completedFuture(null);
        }

        incReceivingCount();
        inUsePacketIds.add(packetId);
        Message distMessage = helper().buildDistMessage(message);
        UserProperties userProps = helper().getUserProps(message);
        return addFgTask(checkPubPermission(topic, distMessage, userProps))
            .thenComposeAsync(checkResult -> {
                if (checkResult.getTypeCase() == CheckResult.TypeCase.GRANTED) {
                    tenantMeter.recordSummary(MqttQoS2IngressBytes, ingressMsgBytes);
                    return doPub(reqId, topic, distMessage, message.fixedHeader().isDup(), ingressMsgBytes)
                        .thenAcceptAsync(pubResult -> {
                            if (log.isTraceEnabled()) {
                                log.trace("Published qos2 msg: reqId={}, sessionId={}, topic={}",
                                    reqId, userSessionId(clientInfo), topic);
                            }
                            if (ctx.channel().isActive()) {
                                if (ctx.channel().isWritable()) {
                                    if (pubResult.distResult() == DistResult.BACK_PRESSURE_REJECTED
                                        || pubResult.distResult() == DistResult.ERROR) {
                                        decReceivingCount();
                                        inUsePacketIds.remove(packetId);
                                    }
                                    handleProtocolResponse(helper()
                                        .onQoS2PubHandled(pubResult, message,
                                            checkResult.getGranted().getUserProps()));
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
                        }, ctx.executor());
                }
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
                handleProtocolResponse(
                    helper().onQoS2DistDenied(topic, packetId, distMessage, checkResult));
                return CompletableFuture.completedFuture(null);
            }, ctx.executor());
    }

    private CompletableFuture<Void> pubWillMessage(LWT willMessage) {
        return sessionCtx.authProvider(ctx).checkPermission(clientInfo(), buildPubAction(willMessage.getTopic(),
                willMessage.getMessage()
                    .getPubQoS(),
                willMessage.getMessage().getIsRetain()))
            .thenComposeAsync(checkResult -> {
                if (checkResult.hasGranted()) {
                    return doPubLastWill(willMessage);
                } else {
                    sessionCtx.eventCollector.report(getLocal(PubActionDisallow.class)
                        .isLastWill(true)
                        .topic(willMessage.getTopic())
                        .qos(willMessage.getMessage().getPubQoS())
                        .isRetain(willMessage.getMessage().getIsRetain())
                        .clientInfo(clientInfo));
                    return CompletableFuture.completedFuture(null);
                }
            }, ctx.executor());
    }

    private void checkIdle() {
        if (sessionCtx.nanoTime() - lastActiveAtNanos > idleTimeoutNanos) {
            idleTimeoutTask.cancel(true);
            handleProtocolResponse(helper().onIdleTimeout(keepAliveTimeSeconds));
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

    protected void handleProtocolResponse(ProtocolResponse response) {
        assert ctx.executor().inEventLoop();
        if (isGoAway) {
            return;
        }
        for (Event<?> reason : response.reasons()) {
            sessionCtx.eventCollector.report(reason);
        }
        switch (response.action()) {
            case NoResponse -> {
                assert response.message() == null;
            }
            case Response -> writeAndFlush(response.message());
            case GoAway, GoAwayNow -> {
                isGoAway = true;
                ctx.channel().config().setAutoRead(false);
                if (response.action() == ProtocolResponse.Action.GoAwayNow) {
                    ctx.close();
                } else {
                    ctx.executor().schedule(() -> ctx.close(),
                        ThreadLocalRandom.current().nextInt(100, 3000), TimeUnit.MILLISECONDS);
                }
            }
            case ResponseAndGoAway, ResponseAndGoAwayNow -> {
                isGoAway = true;
                // disable auto read
                ctx.channel().config().setAutoRead(false);
                Runnable farewell = () -> {
                    if (response.message() != null) {
                        writeAndFlush(response.message()).addListener(ChannelFutureListener.CLOSE);
                    } else {
                        ctx.close();
                    }
                };
                if (response.action() == ProtocolResponse.Action.ResponseAndGoAwayNow) {
                    farewell.run();
                } else {
                    ctx.executor()
                        .schedule(farewell, ThreadLocalRandom.current().nextInt(100, 3000), TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    private CompletableFuture<IMQTTProtocolHelper.PubResult> doPub(long reqId,
                                                                   String topic,
                                                                   Message message,
                                                                   boolean isDup,
                                                                   int ingressMsgSize) {
        return doPub(reqId, topic, message, false, false)
            .thenApplyAsync(v -> {
                switch (v.distResult()) {
                    case OK, NO_MATCH -> {
                        if (log.isTraceEnabled()) {
                            log.trace("Msg published: reqId={}, sessionId={}, topic={}, qos={}, size={}",
                                reqId, userSessionId, topic, message.getPubQoS(), message.getPayload().size());
                        }
                        switch (message.getPubQoS()) {
                            case AT_MOST_ONCE -> tenantMeter.recordSummary(MqttQoS0DistBytes, ingressMsgSize);
                            case AT_LEAST_ONCE -> tenantMeter.recordSummary(MqttQoS1DistBytes, ingressMsgSize);
                            case EXACTLY_ONCE -> tenantMeter.recordSummary(MqttQoS2DistBytes, ingressMsgSize);
                        }
                    }
                    default -> {
                        switch (message.getPubQoS()) {
                            case AT_MOST_ONCE -> eventCollector.report(getLocal(QoS0DistError.class)
                                .reqId(reqId)
                                .topic(topic)
                                .size(ingressMsgSize)
                                .clientInfo(clientInfo));
                            case AT_LEAST_ONCE -> eventCollector.report(getLocal(QoS1DistError.class)
                                .reqId(reqId)
                                .topic(topic)
                                .isDup(isDup)
                                .size(ingressMsgSize)
                                .clientInfo(clientInfo));
                            case EXACTLY_ONCE -> eventCollector.report(getLocal(QoS2DistError.class)
                                .reqId(reqId)
                                .topic(topic)
                                .isDup(isDup)
                                .size(ingressMsgSize)
                                .clientInfo(clientInfo));
                        }
                    }
                }
                return v;
            }, ctx.executor());
    }

    private CompletableFuture<Void> doPubLastWill(LWT willMessage) {
        Message message = willMessage.getMessage().toBuilder()
            .setTimestamp(HLC.INST.getPhysical())
            .build();
        long reqId = System.nanoTime();
        int size = message.getPayload().size() + willMessage.getTopic().length();
        return doPub(reqId, willMessage.getTopic(), message, true, true)
            .handleAsync((v, e) -> {
                if (e != null) {
                    eventCollector.report(getLocal(WillDistError.class)
                        .clientInfo(clientInfo)
                        .reqId(reqId)
                        .topic(willMessage.getTopic())
                        .qos(willMessage.getMessage().getPubQoS())
                        .size(willMessage.getMessage().getPayload().size()));
                } else {
                    switch (v.distResult()) {
                        case OK, NO_MATCH -> {
                            switch (message.getPubQoS()) {
                                case AT_MOST_ONCE -> tenantMeter.recordSummary(MqttQoS0DistBytes, size);
                                case AT_LEAST_ONCE -> tenantMeter.recordSummary(MqttQoS1DistBytes, size);
                                case EXACTLY_ONCE -> tenantMeter.recordSummary(MqttQoS2DistBytes, size);
                            }
                            eventCollector.report(getLocal(WillDisted.class)
                                .clientInfo(clientInfo)
                                .reqId(reqId)
                                .topic(willMessage.getTopic())
                                .qos(willMessage.getMessage().getPubQoS())
                                .size(willMessage.getMessage().getPayload().size()));
                        }
                        default -> eventCollector.report(getLocal(WillDistError.class)
                            .clientInfo(clientInfo)
                            .reqId(reqId)
                            .topic(willMessage.getTopic())
                            .qos(willMessage.getMessage().getPubQoS())
                            .size(willMessage.getMessage().getPayload().size()));
                    }
                }
                return null;
            }, ctx.executor());
    }

    private CompletableFuture<IMQTTProtocolHelper.PubResult> doPub(long reqId,
                                                                   String topic,
                                                                   Message message,
                                                                   boolean isLWT,
                                                                   boolean background) {
        if (log.isTraceEnabled()) {
            log.trace("Disting msg: req={}, topic={}, qos={}, size={}",
                reqId, topic, message.getPubQoS(), message.getPayload().size());
        }

        CompletableFuture<DistResult> distTask =
            trackTask(sessionCtx.distClient.pub(reqId, topic, message, clientInfo), background);
        if (!message.getIsRetain()) {
            return distTask.thenApply(v -> new IMQTTProtocolHelper.PubResult(v, RetainReply.Result.RETAINED));
        } else {
            CompletableFuture<RetainReply.Result> retainTask =
                trackTask(retainMessage(reqId, topic, message, isLWT), background);
            return allOf(retainTask, distTask).thenApply(
                v -> new IMQTTProtocolHelper.PubResult(distTask.join(), retainTask.join()));
        }
    }

    private CompletableFuture<RetainReply.Result> retainMessage(long reqId, String topic, Message message,
                                                                boolean isLWT) {
        if (!settings.retainEnabled) {
            eventCollector.report(getLocal(MsgRetainedError.class)
                .reqId(reqId)
                .topic(topic)
                .qos(message.getPubQoS())
                .payload(message.getPayload().asReadOnlyByteBuffer())
                .size(message.getPayload().size())
                .reason("Retain Disabled")
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(RetainReply.Result.RETAINED);
        }
        if (log.isTraceEnabled()) {
            log.trace("Retaining message: reqId={}, qos={}, topic={}, size={}",
                reqId, message.getPubQoS(), topic, message.getPayload().size());
        }
        if (!resourceThrottler.hasResource(clientInfo.getTenantId(), TotalRetainMessageSpaceBytes)) {
            eventCollector.report(getLocal(ResourceThrottled.class)
                .reason(TotalRetainMessageSpaceBytes.name())
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(RetainReply.Result.EXCEED_LIMIT);
        }
        if (!resourceThrottler.hasResource(clientInfo.getTenantId(), TotalRetainTopics)) {
            eventCollector.report(getLocal(ResourceThrottled.class)
                .reason(TotalRetainTopics.name())
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(RetainReply.Result.EXCEED_LIMIT);
        }
        if (!resourceThrottler.hasResource(clientInfo.getTenantId(), TotalRetainedMessagesPerSeconds)) {
            eventCollector.report(getLocal(ResourceThrottled.class)
                .reason(TotalRetainedMessagesPerSeconds.name())
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(RetainReply.Result.EXCEED_LIMIT);
        }
        if (!resourceThrottler.hasResource(clientInfo.getTenantId(), TotalRetainedBytesPerSecond)) {
            eventCollector.report(getLocal(ResourceThrottled.class)
                .reason(TotalRetainedBytesPerSecond.name())
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(RetainReply.Result.EXCEED_LIMIT);
        }
        return sessionCtx.retainClient.retain(
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
                switch (v.getResult()) {
                    case RETAINED -> eventCollector.report(getLocal(MsgRetained.class)
                        .reqId(v.getReqId())
                        .topic(topic)
                        .isLastWill(isLWT)
                        .qos(message.getPubQoS())
                        .size(message.getPayload().size())
                        .clientInfo(clientInfo));
                    case CLEARED -> eventCollector.report(getLocal(RetainMsgCleared.class)
                        .reqId(v.getReqId())
                        .isLastWill(false)
                        .clientInfo(clientInfo)
                        .topic(topic));
                    case EXCEED_LIMIT -> eventCollector.report(getLocal(MsgRetainedError.class)
                        .reqId(v.getReqId())
                        .clientInfo(clientInfo)
                        .topic(topic)
                        .isLastWill(isLWT)
                        .qos(message.getPubQoS())
                        .payload(message.getPayload().asReadOnlyByteBuffer())
                        .size(message.getPayload().size())
                        .reason("Exceed Limit"));
                    case BACK_PRESSURE_REJECTED -> eventCollector.report(getLocal(MsgRetainedError.class)
                        .reqId(v.getReqId())
                        .clientInfo(clientInfo)
                        .topic(topic)
                        .isLastWill(isLWT)
                        .qos(message.getPubQoS())
                        .payload(message.getPayload().asReadOnlyByteBuffer())
                        .size(message.getPayload().size())
                        .reason("Server Busy"));
                    default -> eventCollector.report(getLocal(MsgRetainedError.class)
                        .reqId(v.getReqId())
                        .clientInfo(clientInfo)
                        .topic(topic)
                        .isLastWill(isLWT)
                        .qos(message.getPubQoS())
                        .payload(message.getPayload().asReadOnlyByteBuffer())
                        .size(message.getPayload().size())
                        .reason("Internal Error"));
                }
                return v.getResult();
            }, ctx.executor());
    }
}
