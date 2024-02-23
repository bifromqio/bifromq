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

import static com.baidu.bifromq.metrics.TenantMetric.MqttPersistentSubCount;
import static com.baidu.bifromq.metrics.TenantMetric.MqttPersistentSubLatency;
import static com.baidu.bifromq.metrics.TenantMetric.MqttPersistentUnsubCount;
import static com.baidu.bifromq.metrics.TenantMetric.MqttPersistentUnsubLatency;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS0InternalLatency;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS1InternalLatency;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS2InternalLatency;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static com.baidu.bifromq.type.QoS.AT_LEAST_ONCE;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.rpc.proto.AttachReply;
import com.baidu.bifromq.inbox.rpc.proto.AttachRequest;
import com.baidu.bifromq.inbox.rpc.proto.CommitRequest;
import com.baidu.bifromq.inbox.rpc.proto.CreateReply;
import com.baidu.bifromq.inbox.rpc.proto.CreateRequest;
import com.baidu.bifromq.inbox.rpc.proto.DetachRequest;
import com.baidu.bifromq.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.inbox.rpc.proto.TouchRequest;
import com.baidu.bifromq.inbox.rpc.proto.UnsubRequest;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.InboxMessage;
import com.baidu.bifromq.inbox.storage.proto.LWT;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterOption;
import com.baidu.bifromq.mqtt.handler.record.ProtocolResponse;
import com.baidu.bifromq.mqtt.session.IMQTTPersistentSession;
import com.baidu.bifromq.mqtt.utils.AuthUtil;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ByClient;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.DropReason;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS0Dropped;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS1Dropped;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS2Dropped;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessage;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MQTTPersistentSessionHandler extends MQTTSessionHandler implements IMQTTPersistentSession {
    private final int sessionExpirySeconds;
    private final boolean sessionPresent;
    private final long incarnation;
    private final NavigableMap<Long, SubMessage> stagingBuffer = new TreeMap<>();
    private final IInboxClient inboxClient;
    private long version = 0;
    private boolean qos0Confirming = false;
    private boolean inboxConfirming = false;
    private long nextSendSeq = 0;
    private long qos0ConfirmUpToSeq;
    private long inboxConfirmedUpToSeq;
    private IInboxClient.IInboxReader inboxReader;
    private long touchIdleTimeMS;
    private ScheduledFuture<?> touchTimeout;

    protected MQTTPersistentSessionHandler(TenantSettings settings,
                                           String userSessionId,
                                           int keepAliveTimeSeconds,
                                           int sessionExpirySeconds,
                                           ClientInfo clientInfo,
                                           @Nullable MQTTConnectHandler.ExistingSession existingSession,
                                           @Nullable LWT willMessage,
                                           ChannelHandlerContext ctx) {
        super(settings, userSessionId, keepAliveTimeSeconds, clientInfo, willMessage, ctx);
        this.sessionPresent = existingSession != null;
        this.inboxClient = sessionCtx.inboxClient;
        if (sessionPresent) {
            incarnation = existingSession.incarnation();
            version = existingSession.version();
        } else {
            incarnation = HLC.INST.get();
        }
        this.sessionExpirySeconds = sessionExpirySeconds;
    }

    private int estBaseMemSize() {
        return 72; // base size from JOL
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        super.handlerAdded(ctx);
        touchIdleTimeMS = Duration.ofSeconds(keepAliveTimeSeconds).dividedBy(2).toMillis();
        if (sessionPresent) {
            AttachRequest.Builder reqBuilder = AttachRequest.newBuilder()
                .setReqId(System.nanoTime())
                .setInboxId(userSessionId)
                .setIncarnation(incarnation)
                .setVersion(version)
                .setKeepAliveSeconds(keepAliveTimeSeconds)
                .setExpirySeconds(sessionExpirySeconds)
                .setClient(clientInfo())
                .setNow(HLC.INST.getPhysical());
            if (willMessage() != null && willMessage().getDelaySeconds() > 0) {
                reqBuilder.setLwt(willMessage());
                discardLWT(); // lwt will be triggered by inbox service
            }
            addFgTask(inboxClient.attach(reqBuilder.build())
                .thenAcceptAsync(reply -> {
                    if (reply.getCode() == AttachReply.Code.OK) {
                        version++;
                        setupInboxReader();
                    } else {
                        handleProtocolResponse(helper().onInboxTransientError());
                    }
                }, ctx.executor()));
        } else {
            CreateRequest.Builder reqBuilder = CreateRequest.newBuilder()
                .setReqId(System.nanoTime())
                .setInboxId(userSessionId)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(keepAliveTimeSeconds)
                .setExpirySeconds(sessionExpirySeconds)
                .setLimit(settings.inboxQueueLength)
                .setDropOldest(settings.inboxDropOldest)
                .setClient(clientInfo)
                .setNow(HLC.INST.getPhysical());
            if (willMessage() != null && willMessage().getDelaySeconds() > 0) {
                reqBuilder.setLwt(willMessage());
                discardLWT();// lwt will be triggered by inbox service
            }
            addFgTask(inboxClient.create(reqBuilder.build())
                .thenAcceptAsync(reply -> {
                    if (reply.getCode() == CreateReply.Code.OK) {
                        setupInboxReader();
                    } else {
                        handleProtocolResponse(helper().onInboxTransientError());
                    }
                }, ctx.executor()));
        }
        sessionCtx.logSessionUsedSpace(clientInfo.getTenantId(), estBaseMemSize());
    }

    @Override
    public final void channelInactive(ChannelHandlerContext ctx) {
        super.channelInactive(ctx);
        touchTimeout.cancel(true);
        if (inboxReader != null) {
            inboxReader.close();
        }
        sessionCtx.logSessionUsedSpace(clientInfo.getTenantId(), -estBaseMemSize());
        ctx.fireChannelInactive();
    }

    @Override
    protected final ProtocolResponse handleDisconnect(MqttMessage message) {
        Optional<Integer> requestSEI = helper().sessionExpiryIntervalOnDisconnect(message);
        int finalSEI =
            settings.forceTransient ? 0 : Math.min(requestSEI.orElse(sessionExpirySeconds), settings.maxSEI);
        if (helper().isNormalDisconnect(message)) {
            discardLWT();
            if (finalSEI == 0) {
                // expire without triggering Will Message if any
                inboxClient.detach(DetachRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setInboxId(userSessionId)
                    .setIncarnation(incarnation)
                    .setVersion(version)
                    .setExpirySeconds(0)
                    .setDiscardLWT(true)
                    .setClient(clientInfo)
                    .setNow(HLC.INST.getPhysical())
                    .build());
            } else {
                // update inbox with requested SEI and discard will message
                inboxClient.detach(DetachRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setInboxId(userSessionId)
                    .setIncarnation(incarnation)
                    .setVersion(version)
                    .setExpirySeconds(finalSEI)
                    .setDiscardLWT(true)
                    .setClient(clientInfo)
                    .setNow(HLC.INST.getPhysical())
                    .build());
            }
        } else if (helper().isDisconnectWithLWT(message)) {
            inboxClient.detach(DetachRequest.newBuilder()
                .setReqId(System.nanoTime())
                .setInboxId(userSessionId)
                .setIncarnation(incarnation)
                .setVersion(version)
                .setExpirySeconds(finalSEI)
                .setDiscardLWT(false)
                .setClient(clientInfo)
                .setNow(HLC.INST.getPhysical())
                .build());
        }
        return ProtocolResponse.goAwayNow(getLocal(ByClient.class).clientInfo(clientInfo));
    }

    @Override
    protected final CompletableFuture<IMQTTProtocolHelper.SubResult> subTopicFilter(long reqId, String topicFilter,
                                                                                    TopicFilterOption option) {
        tenantMeter.recordCount(MqttPersistentSubCount);
        rescheduleTouch();
        Timer.Sample start = Timer.start();
        return inboxClient.sub(SubRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(clientInfo.getTenantId())
                .setInboxId(userSessionId)
                .setIncarnation(incarnation)
                .setVersion(version)
                .setTopicFilter(topicFilter)
                .setOption(option)
                .setNow(HLC.INST.getPhysical()).build())
            .thenApplyAsync(v -> {
                switch (v.getCode()) {
                    case OK -> {
                        start.stop(tenantMeter.timer(MqttPersistentSubLatency));
                        return IMQTTProtocolHelper.SubResult.OK;
                    }
                    case EXISTS -> {
                        start.stop(tenantMeter.timer(MqttPersistentSubLatency));
                        return IMQTTProtocolHelper.SubResult.EXISTS;
                    }
                    case EXCEED_LIMIT -> {
                        return IMQTTProtocolHelper.SubResult.EXCEED_LIMIT;
                    }
                    case ERROR -> {
                        return IMQTTProtocolHelper.SubResult.ERROR;
                    }
                    case NO_INBOX, CONFLICT -> handleProtocolResponse(helper().onInboxTransientError());
                }
                return IMQTTProtocolHelper.SubResult.ERROR;
            }, ctx.executor());
    }

    @Override
    protected final CompletableFuture<IMQTTProtocolHelper.UnsubResult> unsubTopicFilter(long reqId,
                                                                                        String topicFilter) {
        tenantMeter.recordCount(MqttPersistentUnsubCount);
        Timer.Sample start = Timer.start();
        rescheduleTouch();
        return inboxClient.unsub(UnsubRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(clientInfo.getTenantId())
                .setInboxId(userSessionId)
                .setIncarnation(incarnation)
                .setVersion(version)
                .setTopicFilter(topicFilter)
                .setNow(HLC.INST.getPhysical())
                .build())
            .thenApplyAsync(v -> {
                switch (v.getCode()) {
                    case OK -> {
                        start.stop(tenantMeter.timer(MqttPersistentUnsubLatency));
                        return IMQTTProtocolHelper.UnsubResult.OK;
                    }
                    case NO_SUB -> {
                        start.stop(tenantMeter.timer(MqttPersistentUnsubLatency));
                        return IMQTTProtocolHelper.UnsubResult.NO_SUB;
                    }
                    case NO_INBOX, CONFLICT -> {
                        handleProtocolResponse(helper().onInboxTransientError());
                        return IMQTTProtocolHelper.UnsubResult.ERROR;
                    }
                    default -> {
                        return IMQTTProtocolHelper.UnsubResult.ERROR;
                    }
                }
            }, ctx.executor());
    }

    private void setupInboxReader() {
        if (!ctx.channel().isActive()) {
            return;
        }
        inboxReader = inboxClient.openInboxReader(clientInfo().getTenantId(), userSessionId, incarnation);
        inboxReader.fetch(this::consume);
        inboxReader.hint(clientReceiveMaximum());
        // resume channel read after inbox being setup
        onInitialized();
        resumeChannelRead();
        rescheduleTouch();
    }

    private void confirmQoS0() {
        if (qos0Confirming) {
            return;
        }
        qos0Confirming = true;
        long upToSeq = qos0ConfirmUpToSeq;
        addBgTask(inboxClient.commit(CommitRequest.newBuilder()
            .setReqId(HLC.INST.get())
            .setTenantId(clientInfo.getTenantId())
            .setInboxId(userSessionId)
            .setIncarnation(incarnation)
            .setVersion(version)
            .setQos0UpToSeq(upToSeq)
            .setNow(HLC.INST.getPhysical())
            .build()))
            .thenAcceptAsync(v -> {
                switch (v.getCode()) {
                    case OK -> {
                        qos0Confirming = false;
                        if (upToSeq < qos0ConfirmUpToSeq) {
                            confirmQoS0();
                        }
                    }
                    case NO_INBOX, CONFLICT -> handleProtocolResponse(helper().onInboxTransientError());
                    case ERROR -> {
                        // try again with same version
                        qos0Confirming = false;
                        if (upToSeq < qos0ConfirmUpToSeq) {
                            confirmQoS0();
                        }
                    }
                }
            }, ctx.executor());
    }

    @Override
    protected final void onConfirm(long seq) {
        inboxConfirmedUpToSeq = seq;
        stagingBuffer.remove(seq);
        confirmSendBuffer();
        ctx.executor().execute(this::drainStaging);
    }

    private void confirmSendBuffer() {
        if (inboxConfirming) {
            return;
        }
        inboxConfirming = true;
        long upToSeq = inboxConfirmedUpToSeq;
        addBgTask(inboxClient.commit(CommitRequest.newBuilder()
            .setReqId(HLC.INST.get())
            .setTenantId(clientInfo.getTenantId())
            .setInboxId(userSessionId)
            .setIncarnation(incarnation)
            .setVersion(version)
            .setSendBufferUpToSeq(upToSeq)
            .setNow(HLC.INST.getPhysical())
            .build()))
            .thenAcceptAsync(v -> {
                switch (v.getCode()) {
                    case OK -> {
                        inboxConfirming = false;
                        if (upToSeq < inboxConfirmedUpToSeq) {
                            confirmSendBuffer();
                        } else {
                            inboxReader.hint(clientReceiveQuota());
                        }
                    }
                    case NO_INBOX, CONFLICT -> handleProtocolResponse(helper().onInboxTransientError());
                    case ERROR -> {
                        // try again with same version
                        inboxConfirming = false;
                        if (upToSeq < inboxConfirmedUpToSeq) {
                            confirmSendBuffer();
                        } else {
                            inboxReader.hint(clientReceiveQuota());
                        }
                    }
                }
            }, ctx.executor());
    }

    private void consume(Fetched fetched) {
        log.trace("Got fetched : tenantId={}, inboxId={}, qos0={}, sendBuffer={}", clientInfo().getTenantId(),
            clientInfo().getMetadataOrThrow(MQTT_CLIENT_ID_KEY), fetched.getQos0MsgCount(),
            fetched.getSendBufferMsgCount());
        ctx.executor().execute(() -> {
            switch (fetched.getResult()) {
                case OK -> {
                    // deal with qos0
                    if (fetched.getQos0MsgCount() > 0) {
                        CompletableFuture.allOf(fetched.getQos0MsgList()
                                .stream()
                                .map(this::pubQoS0Message)
                                .toArray(CompletableFuture[]::new))
                            .whenCompleteAsync((v, e) -> flush(true), ctx.executor());
                        // commit immediately
                        qos0ConfirmUpToSeq = fetched.getQos0Msg(fetched.getQos0MsgCount() - 1).getSeq();
                        confirmQoS0();
                    }
                    // deal with buffered message
                    if (fetched.getSendBufferMsgCount() > 0) {
                        CompletableFuture.allOf(fetched.getSendBufferMsgList()
                                .stream()
                                .map(this::pubBufferedMessage)
                                .toArray(CompletableFuture[]::new))
                            .whenCompleteAsync((v, e) -> drainStaging(), ctx.executor());
                    }
                    rescheduleTouch();
                }
                case ERROR -> inboxReader.hint(clientReceiveQuota());
                case NO_INBOX -> handleProtocolResponse(helper().onInboxTransientError());
            }
        });
    }

    private CompletableFuture<Void> pubQoS0Message(InboxMessage inboxMsg) {
        String topicFilter = inboxMsg.getTopicFilter();
        TopicFilterOption option = inboxMsg.getOption();
        TopicMessage topicMsg = inboxMsg.getMsg();
        String topic = topicMsg.getTopic();
        Message message = topicMsg.getMessage();
        ClientInfo publihser = topicMsg.getPublisher();
        return addFgTask(
            authProvider.checkPermission(clientInfo(), AuthUtil.buildSubAction(topicFilter, QoS.AT_MOST_ONCE)))
            .thenAcceptAsync(checkResult -> {
                if (checkResult.hasGranted()) {
                    tenantMeter.timer(MqttQoS0InternalLatency)
                        .record(HLC.INST.getPhysical() - message.getTimestamp(), TimeUnit.MILLISECONDS);
                    if (option.getNoLocal() && clientInfo.equals(publihser)) {
                        // skip local sub
                        if (settings.debugMode) {
                            eventCollector.report(getLocal(QoS0Dropped.class)
                                .reason(DropReason.NoLocal)
                                .reqId(message.getMessageId())
                                .isRetain(false)
                                .sender(topicMsg.getPublisher())
                                .topic(topic)
                                .matchedFilter(topicFilter)
                                .size(message.getPayload().size())
                                .clientInfo(clientInfo()));
                        }
                        return;
                    }
                    sendQoS0SubMessage(new SubMessage(topic, message, publihser, topicFilter, option));
                } else {
                    eventCollector.report(getLocal(QoS0Dropped.class)
                        .reason(DropReason.NoSubPermission)
                        .reqId(message.getMessageId())
                        .isRetain(false)
                        .sender(topicMsg.getPublisher())
                        .topic(topic)
                        .matchedFilter(topicFilter)
                        .size(message.getPayload().size())
                        .clientInfo(clientInfo()));
                    addBgTask(unsubTopicFilter(message.getMessageId(), topicFilter));
                }
            }, ctx.executor());
    }

    private CompletableFuture<Void> pubBufferedMessage(InboxMessage inboxMsg) {
        String topicFilter = inboxMsg.getTopicFilter();
        TopicFilterOption option = inboxMsg.getOption();
        TopicMessage topicMsg = inboxMsg.getMsg();
        String topic = topicMsg.getTopic();
        Message message = topicMsg.getMessage();
        ClientInfo publisher = topicMsg.getPublisher();
        return addFgTask(
            authProvider.checkPermission(clientInfo(), AuthUtil.buildSubAction(topicFilter, AT_LEAST_ONCE)))
            .thenAcceptAsync(checkResult -> {
                SubMessage msg = new SubMessage(topic, message, publisher, topicFilter, option);
                if (checkResult.hasGranted()) {
                    tenantMeter.timer(msg.qos() == AT_LEAST_ONCE ? MqttQoS1InternalLatency : MqttQoS2InternalLatency)
                        .record(HLC.INST.getPhysical() - message.getTimestamp(), TimeUnit.MILLISECONDS);
                    if (option.getNoLocal() && clientInfo.equals(topicMsg.getPublisher())) {
                        // skip local sub
                        if (settings.debugMode) {
                            switch (msg.qos()) {
                                case AT_LEAST_ONCE -> eventCollector.report(getLocal(QoS1Dropped.class)
                                    .reason(DropReason.NoLocal)
                                    .reqId(message.getMessageId())
                                    .isRetain(false)
                                    .sender(topicMsg.getPublisher())
                                    .topic(topic)
                                    .matchedFilter(topicFilter)
                                    .size(message.getPayload().size())
                                    .clientInfo(clientInfo()));
                                case EXACTLY_ONCE -> eventCollector.report(getLocal(QoS2Dropped.class)
                                    .reason(DropReason.NoLocal)
                                    .reqId(message.getMessageId())
                                    .isRetain(msg.isRetain())
                                    .sender(topicMsg.getPublisher())
                                    .topic(topic)
                                    .matchedFilter(topicFilter)
                                    .size(message.getPayload().size())
                                    .clientInfo(clientInfo()));
                            }
                        }
                        return;
                    }
                    SubMessage prev = stagingBuffer.put(inboxMsg.getSeq(), msg);
                    assert prev == null;
                } else {
                    switch (msg.qos()) {
                        case AT_LEAST_ONCE -> eventCollector.report(getLocal(QoS1Dropped.class)
                            .reason(DropReason.NoSubPermission)
                            .reqId(message.getMessageId())
                            .isRetain(false)
                            .sender(topicMsg.getPublisher())
                            .topic(topic)
                            .matchedFilter(topicFilter)
                            .size(message.getPayload().size())
                            .clientInfo(clientInfo()));
                        case EXACTLY_ONCE -> eventCollector.report(getLocal(QoS2Dropped.class)
                            .reason(DropReason.NoSubPermission)
                            .reqId(message.getMessageId())
                            .isRetain(false)
                            .sender(topicMsg.getPublisher())
                            .topic(topic)
                            .matchedFilter(topicFilter)
                            .size(message.getPayload().size())
                            .clientInfo(clientInfo()));
                    }
                    addBgTask(unsubTopicFilter(message.getMessageId(), topicFilter));
                }
            }, ctx.executor());
    }

    private void drainStaging() {
        SortedMap<Long, SubMessage> toBeSent = stagingBuffer.tailMap(nextSendSeq);
        if (toBeSent.isEmpty()) {
            return;
        }
        Iterator<Map.Entry<Long, SubMessage>> itr = toBeSent.entrySet().iterator();
        while (clientReceiveQuota() > 0 && itr.hasNext()) {
            Map.Entry<Long, SubMessage> entry = itr.next();
            long seq = entry.getKey();
            sendConfirmableMessage(seq, entry.getValue());
            nextSendSeq = seq + 1;
        }
        flush(true);
    }

    private void rescheduleTouch() {
        if (touchTimeout != null) {
            touchTimeout.cancel(true);
        }
        touchTimeout = ctx.executor().schedule(() -> {
            inboxClient.touch(TouchRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setTenantId(clientInfo.getTenantId())
                    .setInboxId(userSessionId)
                    .setIncarnation(incarnation)
                    .setVersion(version)
                    .setNow(HLC.INST.getPhysical())
                    .build())
                .thenAcceptAsync(v -> {
                    switch (v.getCode()) {
                        case OK, ERROR -> rescheduleTouch();
                        case CONFLICT -> handleProtocolResponse(helper().onInboxTransientError());
                    }
                }, ctx.executor());
        }, touchIdleTimeMS, TimeUnit.MILLISECONDS);
    }
}