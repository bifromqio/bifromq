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

import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static com.baidu.bifromq.type.QoS.AT_LEAST_ONCE;
import static com.baidu.bifromq.type.QoS.AT_MOST_ONCE;
import static com.baidu.bifromq.type.QoS.EXACTLY_ONCE;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.rpc.proto.AttachReply;
import com.baidu.bifromq.inbox.rpc.proto.AttachRequest;
import com.baidu.bifromq.inbox.rpc.proto.CommitRequest;
import com.baidu.bifromq.inbox.rpc.proto.CreateReply;
import com.baidu.bifromq.inbox.rpc.proto.CreateRequest;
import com.baidu.bifromq.inbox.rpc.proto.DetachReply;
import com.baidu.bifromq.inbox.rpc.proto.DetachRequest;
import com.baidu.bifromq.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.inbox.rpc.proto.TouchRequest;
import com.baidu.bifromq.inbox.rpc.proto.UnsubRequest;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.InboxMessage;
import com.baidu.bifromq.mqtt.handler.TenantSettings;
import com.baidu.bifromq.mqtt.session.v3.IMQTT3PersistentSession;
import com.baidu.bifromq.mqtt.utils.AuthUtil;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ByClient;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.InboxTransientError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.SessionCreateError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.DropReason;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS0Dropped;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS0Pushed;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS1Dropped;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS2Dropped;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import java.time.Duration;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MQTT3PersistentSessionHandler extends MQTT3SessionHandler implements IMQTT3PersistentSession {
    public record ExistingSession(long incarnation, long version) {

    }

    // key: the seq that wait for confirm, value: the seq for triggering sending confirm to inbox
    private final SortedMap<Long, Long> qos1ConfirmSeqMap = new TreeMap<>();
    private final SortedMap<Long, Long> qos2ConfirmSeqMap = new TreeMap<>();
    private final int sessionExpirySeconds;
    private final boolean sessionPresent;
    private final long incarnation;
    private long version;
    private boolean qos0Confirming = false;
    private boolean qos1Confirming = false;
    private boolean qos2Confirming = false;
    private long qos0ConfirmUpToSeq;
    private long qos1ConfirmUpToSeq;
    private long qos2ConfirmUpToSeq;
    private IInboxClient inboxClient;
    private IInboxClient.IInboxReader inboxReader;
    private long touchIdleTimeMS;
    private ScheduledFuture<?> touchTimeout;

    @Builder
    public MQTT3PersistentSessionHandler(TenantSettings settings,
                                         String userSessionId,
                                         ClientInfo clientInfo,
                                         int keepAliveSeconds,
                                         int sessionExpirySeconds,
                                         WillMessage willMessage,
                                         ExistingSession existingSession) {
        super(settings, userSessionId, clientInfo, keepAliveSeconds, willMessage);
        this.sessionExpirySeconds = sessionExpirySeconds;
        this.sessionPresent = existingSession != null;
        if (sessionPresent) {
            incarnation = existingSession.incarnation;
            version = existingSession.version;
        } else {
            incarnation = HLC.INST.get();
        }

    }


    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        super.handlerAdded(ctx);
        this.inboxClient = sessionCtx.inboxClient;
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
            cancelOnInactive(inboxClient.attach(reqBuilder.build())
                .thenAcceptAsync(reply -> {
                    if (reply.getCode() == AttachReply.Code.OK) {
                        version++;
                        setupInboxPipeline();
                    } else {
                        closeConnectionWithSomeDelay(getLocal(SessionCreateError.class).clientInfo(clientInfo()));
                    }
                }, ctx.channel().eventLoop()));
        } else {
            cancelOnInactive(inboxClient.create(CreateRequest.newBuilder()
                .setReqId(System.nanoTime())
                .setInboxId(userSessionId)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(keepAliveTimeSeconds)
                .setExpirySeconds(sessionExpirySeconds)
                .setLimit(tenantSettings.inboxQueueLength)
                .setDropOldest(tenantSettings.inboxDropOldest)
                .setClient(clientInfo())
                .build()))
                .thenAcceptAsync(createReply -> {
                    if (createReply.getCode() == CreateReply.Code.OK) {
                        setupInboxPipeline();
                    } else {
                        closeConnectionWithSomeDelay(getLocal(SessionCreateError.class).clientInfo(clientInfo()));
                    }
                }, ctx.channel().eventLoop());
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        super.channelInactive(ctx);
        touchTimeout.cancel(true);
        if (inboxReader != null) {
            inboxReader.close();
        }
        ctx.fireChannelInactive();
    }

    @Override
    protected void handleDisconnect(MqttMessage mqttMessage) {
        inboxClient.detach(DetachRequest.newBuilder()
                .setReqId(System.nanoTime())
                .setInboxId(userSessionId)
                .setIncarnation(incarnation)
                .setVersion(version)
                .setExpirySeconds(tenantSettings.maxSEI)
                .setDiscardLWT(true)
                .setClient(clientInfo)
                .setNow(HLC.INST.getPhysical())
                .build())
            .thenAccept(v -> {
                if (v.getCode() == DetachReply.Code.OK) {
                    version++;
                }
            });
        closeConnectionNow(getLocal(ByClient.class).clientInfo(clientInfo));
    }

    @Override
    protected void onDistQoS1MessageConfirmed(int messageId, long seq, String topic, Message message,
                                              boolean delivered) {
        // thread the commit using messageId as the reqId
        bufferCapacityHinter.onConfirm();
        assert qos1ConfirmSeqMap.containsKey(seq);
        long triggerSeq = qos1ConfirmSeqMap.remove(seq);
        if (qos1ConfirmSeqMap.isEmpty() || qos1ConfirmSeqMap.firstKey() > triggerSeq) {
            // all seq < triggerSeq has been confirmed
            log.trace("Committing qos1 up to seq: tenantId={}, inboxId={}, seq={}", clientInfo().getTenantId(),
                clientInfo().getMetadataOrThrow(MQTT_CLIENT_ID_KEY), triggerSeq);
            qos1ConfirmUpToSeq = triggerSeq;
            confirmQoS1();
        }
        rescheduleTouch();
    }

    @Override
    protected void onDistQoS2MessageConfirmed(int messageId, long seq, String topic, Message message,
                                              boolean delivered) {
        // thread the commit using messageId as the reqId
        bufferCapacityHinter.onConfirm();
        assert qos2ConfirmSeqMap.containsKey(seq);
        long triggerSeq = qos2ConfirmSeqMap.remove(seq);
        if (qos2ConfirmSeqMap.isEmpty() || qos2ConfirmSeqMap.firstKey() > triggerSeq) {
            // all seq < triggerSeq has been confirmed
            log.trace("Committing qos2 up to seq: tenantId={}, inboxId={}, seq={}", clientInfo().getTenantId(),
                clientInfo().getMetadataOrThrow(MQTT_CLIENT_ID_KEY), triggerSeq);
            qos2ConfirmUpToSeq = triggerSeq;
            confirmQoS2();
        }
        rescheduleTouch();
    }

    @Override
    protected CompletableFuture<MqttQoS> doSubscribe(long reqId, MqttTopicSubscription topicSub) {
        rescheduleTouch();
        QoS qos = QoS.forNumber(topicSub.qualityOfService().value());
        return inboxClient.sub(SubRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(clientInfo.getTenantId())
                .setInboxId(userSessionId)
                .setIncarnation(incarnation)
                .setVersion(version)
                .setTopicFilter(topicSub.topicName())
                .setSubQoS(qos)
                .setNow(HLC.INST.getPhysical())
                .build())
            .thenApplyAsync(v -> {
                switch (v.getCode()) {
                    case OK, EXISTS -> {
                        return topicSub.qualityOfService();
                    }
                    case EXCEED_LIMIT, ERROR -> {
                        return MqttQoS.FAILURE;
                    }
                    case NO_INBOX, CONFLICT ->
                        closeConnectionWithSomeDelay(getLocal(InboxTransientError.class).clientInfo(clientInfo()));
                }
                return MqttQoS.FAILURE;
            }, ctx.channel().eventLoop());
    }

    @Override
    protected CompletableFuture<Boolean> doUnsubscribe(long reqId, String topicFilter) {
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
                    case OK, NO_SUB -> {
                        return true;
                    }
                    case NO_INBOX, CONFLICT ->
                        closeConnectionWithSomeDelay(getLocal(InboxTransientError.class).clientInfo(clientInfo()));
                }
                return false;
            }, ctx.channel().eventLoop());
    }

    private void confirmQoS0() {
        if (qos0Confirming) {
            return;
        }
        qos0Confirming = true;
        long upToSeq = qos0ConfirmUpToSeq;
        tearDownTasks(inboxClient.commit(CommitRequest.newBuilder()
            .setReqId(HLC.INST.get())
            .setTenantId(clientInfo.getTenantId())
            .setInboxId(userSessionId)
            .setIncarnation(incarnation)
            .setVersion(version)
            .setQos(AT_MOST_ONCE)
            .setUpToSeq(upToSeq)
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
                    case NO_INBOX, CONFLICT ->
                        closeConnectionWithSomeDelay(getLocal(InboxTransientError.class).clientInfo(clientInfo()));
                    case ERROR -> {
                        // try again with same version
                        qos0Confirming = false;
                        if (upToSeq < qos0ConfirmUpToSeq) {
                            confirmQoS0();
                        }
                    }
                }
            }, ctx.channel().eventLoop());
    }

    private void confirmQoS1() {
        if (qos1Confirming) {
            return;
        }
        qos1Confirming = true;
        long upToSeq = qos1ConfirmUpToSeq;
        tearDownTasks(inboxClient.commit(CommitRequest.newBuilder()
            .setReqId(HLC.INST.get())
            .setTenantId(clientInfo.getTenantId())
            .setInboxId(userSessionId)
            .setIncarnation(incarnation)
            .setVersion(version)
            .setQos(AT_LEAST_ONCE)
            .setUpToSeq(upToSeq)
            .setNow(HLC.INST.getPhysical())
            .build()))
            .thenAcceptAsync(v -> {
                switch (v.getCode()) {
                    case OK -> {
                        qos1Confirming = false;
                        if (upToSeq < qos1ConfirmUpToSeq) {
                            confirmQoS1();
                        }
                    }
                    case NO_INBOX, CONFLICT ->
                        closeConnectionWithSomeDelay(getLocal(InboxTransientError.class).clientInfo(clientInfo()));
                    case ERROR -> {
                        // try again with same version
                        qos1Confirming = false;
                        if (upToSeq < qos1ConfirmUpToSeq) {
                            confirmQoS1();
                        }
                    }
                }
            }, ctx.channel().eventLoop());
    }

    private void confirmQoS2() {
        if (qos2Confirming) {
            return;
        }
        qos2Confirming = true;

        long upToSeq = qos2ConfirmUpToSeq;
        tearDownTasks(inboxClient.commit(CommitRequest.newBuilder()
            .setReqId(HLC.INST.get())
            .setTenantId(clientInfo.getTenantId())
            .setInboxId(userSessionId)
            .setIncarnation(incarnation)
            .setVersion(version)
            .setQos(EXACTLY_ONCE)
            .setUpToSeq(upToSeq)
            .setNow(HLC.INST.getPhysical())
            .build()))
            .thenAcceptAsync(v -> {
                switch (v.getCode()) {
                    case OK -> {
                        qos2Confirming = false;
                        if (upToSeq < qos2ConfirmUpToSeq) {
                            confirmQoS2();
                        }
                    }
                    case NO_INBOX, CONFLICT ->
                        closeConnectionWithSomeDelay(getLocal(InboxTransientError.class).clientInfo(clientInfo()));
                    case ERROR -> {
                        // try again with same version
                        qos2Confirming = false;
                        if (upToSeq < qos2ConfirmUpToSeq) {
                            confirmQoS2();
                        }
                    }
                }
            }, ctx.channel().eventLoop());
    }

    private void setupInboxPipeline() {
        if (!ctx.channel().isActive()) {
            return;
        }
        inboxReader = inboxClient.openInboxReader(clientInfo().getTenantId(), userSessionId, incarnation);
        inboxReader.fetch(this::consume);
        bufferCapacityHinter.hint(inboxReader::hint);
        // resume channel read after inbox being setup
        resumeChannelRead();
        rescheduleTouch();
    }

    private void rescheduleTouch() {
        if (touchTimeout != null) {
            touchTimeout.cancel(true);
        }
        touchTimeout = ctx.channel().eventLoop().schedule(() -> {
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
                        case OK -> {
                            rescheduleTouch();
                        }
                        case CONFLICT ->
                            closeConnectionWithSomeDelay(getLocal(InboxTransientError.class).clientInfo(clientInfo()));
                        case ERROR -> // try again with same version
                            rescheduleTouch();
                    }
                }, ctx.channel().eventLoop());
        }, touchIdleTimeMS, TimeUnit.MILLISECONDS);
    }

    private void consume(Fetched fetched) {
        log.trace("Got fetched : tenantId={}, inboxId={}, qos0={}, qos1={}, qos2={}", clientInfo().getTenantId(),
            clientInfo().getMetadataOrThrow(MQTT_CLIENT_ID_KEY), fetched.getQos0SeqCount(),
            fetched.getQos1SeqCount(), fetched.getQos2SeqCount());
        ctx.channel().eventLoop().execute(() -> {
            switch (fetched.getResult()) {
                case OK -> {
                    long timestamp = HLC.INST.getPhysical();
                    // deal with qos0
                    assert fetched.getQos0SeqCount() == fetched.getQos0MsgCount();
                    for (int i = 0; i < fetched.getQos0SeqCount(); i++) {
                        InboxMessage msg = fetched.getQos0Msg(i);
                        pubQoS0Message(msg.getTopicFilter(), msg.getMsg(), i + 1 == fetched.getQos0MsgCount(),
                            timestamp);
                    }
                    if (fetched.getQos0SeqCount() > 0) {
                        // commit immediately
                        qos0ConfirmUpToSeq = fetched.getQos0Seq(fetched.getQos0SeqCount() - 1);
                        confirmQoS0();
                    }

                    // deal with qos1
                    assert fetched.getQos1SeqCount() == fetched.getQos1MsgCount();
                    if (fetched.getQos1SeqCount() > 0) {
                        long triggerSeq = fetched.getQos1Seq(fetched.getQos1SeqCount() - 1);
                        for (int i = 0; i < fetched.getQos1SeqCount(); i++) {
                            long seq = fetched.getQos1Seq(i);
                            log.trace("QoS1ConfirmSeqMap: {}-{}", seq, triggerSeq);
                            qos1ConfirmSeqMap.put(seq, triggerSeq);
                            InboxMessage distMsg = fetched.getQos1Msg(i);
                            pubQoS1Message(seq, distMsg.getTopicFilter(), distMsg.getMsg(),
                                i + 1 == fetched.getQos1SeqCount(), timestamp);
                        }
                    }
                    // deal with qos2
                    assert fetched.getQos2SeqCount() == fetched.getQos2MsgCount();
                    if (fetched.getQos2SeqCount() > 0) {
                        long triggerSeq = fetched.getQos2Seq(fetched.getQos2SeqCount() - 1);
                        for (int i = 0; i < fetched.getQos2SeqCount(); i++) {
                            long seq = fetched.getQos2Seq(i);
                            qos2ConfirmSeqMap.put(seq, triggerSeq);
                            InboxMessage distMsg = fetched.getQos2Msg(i);
                            TopicMessage topicMsg = distMsg.getMsg();
                            pubQoS2Message(seq, distMsg.getTopicFilter(), topicMsg,
                                i + 1 == fetched.getQos2SeqCount(), timestamp);
                        }
                    }
                    rescheduleTouch();
                }
                case ERROR -> bufferCapacityHinter.reset();
                case NO_INBOX ->
                    closeConnectionWithSomeDelay(getLocal(InboxTransientError.class).clientInfo(clientInfo()));
            }
        });

    }

    protected void pubQoS0Message(String topicFilter, TopicMessage topicMsg, boolean flush, long timestamp) {
        String topic = topicMsg.getTopic();
        Message message = topicMsg.getMessage();
        cancelOnInactive(authProvider.check(clientInfo(), AuthUtil.buildSubAction(topicFilter, QoS.AT_MOST_ONCE)))
            .thenAcceptAsync(allow -> {
                if (allow) {
                    if (sendQoS0TopicMessage(topic, message, false, flush, timestamp)) {
                        if (debugMode) {
                            eventCollector.report(getLocal(QoS0Pushed.class)
                                .reqId(message.getMessageId())
                                .isRetain(false)
                                .sender(topicMsg.getPublisher())
                                .topic(topic)
                                .matchedFilter(topicFilter)
                                .size(message.getPayload().size())
                                .clientInfo(clientInfo()));
                        }
                    } else {
                        eventCollector.report(getLocal(QoS0Dropped.class)
                            .reason(DropReason.ChannelClosed)
                            .reqId(message.getMessageId())
                            .isRetain(false)
                            .sender(topicMsg.getPublisher())
                            .topic(topic)
                            .matchedFilter(topicFilter)
                            .size(message.getPayload().size())
                            .clientInfo(clientInfo()));
                    }
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
                    submitBgTask(
                        () -> doUnsubscribe(message.getMessageId(), topicFilter).thenAccept(
                            v -> {
                            }));
                }
            }, ctx.channel().eventLoop());
    }

    protected void pubQoS1Message(long seq, String topicFilter, TopicMessage topicMsg, boolean flush, long timestamp) {
        String topic = topicMsg.getTopic();
        Message message = topicMsg.getMessage();
        cancelOnInactive(authProvider.check(clientInfo(), AuthUtil.buildSubAction(topicFilter, AT_LEAST_ONCE)))
            .thenAcceptAsync(allow -> {
                if (allow) {
                    int messageId = sendQoS1TopicMessage(seq, topicFilter, topic, message, topicMsg.getPublisher(),
                        false, flush, timestamp);
                    if (messageId < 0) {
                        log.error("MessageId exhausted");
                    }
                } else {
                    eventCollector.report(getLocal(QoS1Dropped.class)
                        .reason(DropReason.NoSubPermission)
                        .reqId(message.getMessageId())
                        .isRetain(false)
                        .sender(topicMsg.getPublisher())
                        .topic(topic)
                        .matchedFilter(topicFilter)
                        .size(message.getPayload().size())
                        .clientInfo(clientInfo()));
                    submitBgTask(() -> doUnsubscribe(message.getMessageId(), topicFilter)
                        .thenAccept(v -> {
                        }));
                }
            }, ctx.channel().eventLoop());
    }

    protected void pubQoS2Message(long seq, String topicFilter, TopicMessage topicMsg, boolean flush, long timestamp) {
        String topic = topicMsg.getTopic();
        Message message = topicMsg.getMessage();
        cancelOnInactive(authProvider.check(clientInfo(), AuthUtil.buildSubAction(topicFilter, EXACTLY_ONCE)))
            .thenAcceptAsync(allow -> {
                if (allow) {
                    int messageId = sendQoS2TopicMessage(seq, topicFilter, topic, message, topicMsg.getPublisher(),
                        false, flush, timestamp);
                    if (messageId < 0) {
                        log.error("MessageId exhausted");
                    }
                } else {
                    eventCollector.report(getLocal(QoS2Dropped.class)
                        .reason(DropReason.NoSubPermission)
                        .reqId(message.getMessageId())
                        .isRetain(false)
                        .sender(topicMsg.getPublisher())
                        .topic(topic)
                        .matchedFilter(topicFilter)
                        .size(message.getPayload().size())
                        .clientInfo(clientInfo()));
                    submitBgTask(() -> doUnsubscribe(message.getMessageId(), topicFilter).thenAccept(
                        v -> {
                        }));
                }
            }, ctx.channel().eventLoop());
    }
}
