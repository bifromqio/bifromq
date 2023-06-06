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

import static com.baidu.bifromq.mqtt.inbox.util.InboxGroupKeyUtil.toInboxGroupKey;
import static com.baidu.bifromq.mqtt.utils.AuthUtil.buildSubAction;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.plugin.settingprovider.Setting.ByPassPermCheckError;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.dist.client.SubResult;
import com.baidu.bifromq.dist.client.UnsubResult;
import com.baidu.bifromq.mqtt.session.v3.IMQTT3TransientSession;
import com.baidu.bifromq.plugin.authprovider.CheckResult;
import com.baidu.bifromq.plugin.eventcollector.EventType;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.accessctrl.AccessControlError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.SubPermissionCheckError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.DropReason;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.PushEvent;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS0Dropped;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS0Pushed;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS1Dropped;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS2Dropped;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class MQTT3TransientSessionHandler extends MQTT3SessionHandler implements IMQTT3TransientSession {
    private final AtomicLong seqNum = new AtomicLong();
    private boolean clearOnDisconnect = false;

    @Builder
    public MQTT3TransientSessionHandler(ClientInfo clientInfo,
                                        int keepAliveTimeSeconds,
                                        boolean sessionPresent,
                                        WillMessage willMessage) {
        super(clientInfo, keepAliveTimeSeconds, true, sessionPresent, willMessage);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        super.handlerAdded(ctx);
        resumeChannelRead();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        super.channelInactive(ctx);
        if (clearOnDisconnect) {
            submitBgTask(() -> sessionCtx.distClient.clear(System.nanoTime(), channelId(),
                    toInboxGroupKey(channelId(), sessionCtx.serverId), 0, clientInfo())
                .thenAccept(clearResult -> {
                    switch (clearResult.type()) {
                        case OK:
                            log.trace("Subscription cleaned for client:\n{}", clientInfo());
                            break;
                        case ERROR:
                            log.warn("Subscription clean error for client:\n{}", clientInfo());
                        default:
                    }
                }));
        }
        ctx.fireChannelInactive();
    }

    @Override
    protected void onDistQoS1MessageConfirmed(int messageId, long seq, String topic, Message message,
                                              boolean delivered) {
    }

    @Override
    protected void onDistQoS2MessageConfirmed(int messageId, long seq, String topic, Message message,
                                              boolean delivered) {
    }

    @Override
    protected CompletableFuture<MqttQoS> doSubscribe(long reqId, MqttTopicSubscription topicSub) {
        return sessionCtx.distClient.sub(reqId, topicSub.topicName(), QoS.forNumber(topicSub.qualityOfService()
                .value()), channelId(), toInboxGroupKey(channelId(), sessionCtx.serverId), 0, clientInfo())
            .thenApplyAsync(subResult -> {
                if (subResult.type() != SubResult.Type.ERROR) {
                    clearOnDisconnect = true;
                }
                switch (subResult.type()) {
                    case OK_QoS0:
                        return MqttQoS.AT_MOST_ONCE;
                    case OK_QoS1:
                        return MqttQoS.AT_LEAST_ONCE;
                    case OK_QoS2:
                        return MqttQoS.EXACTLY_ONCE;
                    case ERROR:
                    default:
                        return MqttQoS.FAILURE;
                }
            }, ctx.channel().eventLoop());
    }

    @Override
    protected CompletableFuture<UnsubResult> doUnsubscribe(long reqId, String topicFilter) {
        return sessionCtx.distClient
            .unsub(reqId, topicFilter, channelId(), toInboxGroupKey(channelId(), sessionCtx.serverId), 0, clientInfo());
    }

    @Override
    public void publish(SubInfo subInfo, TopicMessagePack topicMsgPack) {
        String topic = topicMsgPack.getTopic();
        List<TopicMessagePack.SenderMessagePack> senderMsgPacks = topicMsgPack.getMessageList();
        String topicFilter = subInfo.getTopicFilter();
        QoS subQoS = subInfo.getSubQoS();
        cancelOnInactive(authProvider.check(clientInfo(), buildSubAction(topicFilter, subQoS)))
            .thenAcceptAsync(checkResult -> {
                switch (checkResult.type()) {
                    case ERROR:
                        eventCollector.report(getLocal(EventType.ACCESS_CONTROL_ERROR, AccessControlError.class)
                            .clientInfo(clientInfo())
                            .cause(((CheckResult.Error) checkResult).cause));
                        boolean byPass = settingProvider.provide(ByPassPermCheckError, clientInfo());
                        if (!byPass) {
                            closeConnectionWithSomeDelay(
                                getLocal(EventType.SUB_PERMISSION_CHECK_ERROR, SubPermissionCheckError.class)
                                    .cause(((CheckResult.Error) checkResult).cause)
                                    .topicFilter(topicFilter)
                                    .subQoS(QoS.AT_MOST_ONCE)
                                    .clientInfo(clientInfo()));
                            break;
                        }
                        // fallthrough
                    case ALLOW:
                        long timestamp = HLC.INST.getPhysical();
                        for (int i = 0; i < senderMsgPacks.size(); i++) {
                            TopicMessagePack.SenderMessagePack senderMsgPack = senderMsgPacks.get(i);
                            ClientInfo sender = senderMsgPack.getSender();
                            List<Message> messages = senderMsgPack.getMessageList();
                            for (int j = 0; j < messages.size(); j++) {
                                Message message = messages.get(j);
                                QoS finalQoS =
                                    QoS.forNumber(Math.min(message.getPubQoS().getNumber(), subQoS.getNumber()));
                                boolean flush = i + 1 == senderMsgPacks.size() && j + 1 == messages.size();
                                switch (finalQoS) {
                                    case AT_MOST_ONCE:
                                        if (bufferCapacityHinter.hasCapacity()) {
                                            if (sendQoS0TopicMessage(topic, message, false, flush, timestamp)) {
                                                if (debugMode) {
                                                    eventCollector.report(
                                                        getLocal(EventType.QOS0_PUSHED, QoS0Pushed.class)
                                                            .reqId(message.getMessageId())
                                                            .isRetain(false)
                                                            .sender(sender)
                                                            .topic(topic)
                                                            .matchedFilter(topicFilter)
                                                            .size(message.getPayload().size())
                                                            .clientInfo(clientInfo()));
                                                }
                                            } else {
                                                eventCollector.report(
                                                    getLocal(EventType.QOS0_DROPPED, QoS0Dropped.class)
                                                        .reason(DropReason.ChannelClosed)
                                                        .reqId(message.getMessageId())
                                                        .isRetain(false)
                                                        .sender(sender)
                                                        .topic(topic)
                                                        .matchedFilter(topicFilter)
                                                        .size(message.getPayload().size())
                                                        .clientInfo(clientInfo()));
                                            }
                                        } else {
                                            flush(true);
                                            eventCollector.report(getLocal(EventType.QOS0_DROPPED, QoS0Dropped.class)
                                                .reason(DropReason.Overflow)
                                                .reqId(message.getMessageId())
                                                .isRetain(false)
                                                .sender(sender)
                                                .topic(topic)
                                                .matchedFilter(topicFilter)
                                                .size(message.getPayload().size())
                                                .clientInfo(clientInfo()));
                                        }
                                        break;
                                    case AT_LEAST_ONCE:
                                        if (bufferCapacityHinter.hasCapacity()) {
                                            int messageId = sendQoS1TopicMessage(seqNum.incrementAndGet(),
                                                topicFilter, topic, message, sender, false, flush, timestamp);
                                            if (messageId < 0) {
                                                log.error("MessageId exhausted");
                                            }
                                        } else {
                                            flush(true);
                                            eventCollector.report(getLocal(EventType.QOS1_DROPPED, QoS1Dropped.class)
                                                .reason(DropReason.Overflow)
                                                .reqId(message.getMessageId())
                                                .isRetain(false)
                                                .sender(sender)
                                                .topic(topic)
                                                .matchedFilter(topicFilter)
                                                .size(message.getPayload().size())
                                                .clientInfo(clientInfo()));
                                        }
                                        break;
                                    case EXACTLY_ONCE:
                                        if (bufferCapacityHinter.hasCapacity()) {
                                            int messageId = sendQoS2TopicMessage(seqNum.incrementAndGet(),
                                                topicFilter, topic, message, sender, false, flush, timestamp);
                                            if (messageId < 0) {
                                                log.error("MessageId exhausted");
                                            }
                                        } else {
                                            flush(true);
                                            eventCollector.report(getLocal(EventType.QOS2_DROPPED, QoS2Dropped.class)
                                                .reason(DropReason.Overflow)
                                                .reqId(message.getMessageId())
                                                .isRetain(false)
                                                .sender(sender)
                                                .topic(topic)
                                                .matchedFilter(topicFilter)
                                                .size(message.getPayload().size())
                                                .clientInfo(clientInfo()));
                                        }
                                        break;
                                }
                            }
                        }
                        break;
                    case DISALLOW:
                    default: {
                        for (TopicMessagePack.SenderMessagePack senderMsgPack : senderMsgPacks) {
                            for (Message message : senderMsgPack.getMessageList()) {
                                PushEvent<?> dropEvent;
                                switch (message.getPubQoS()) {
                                    case AT_LEAST_ONCE ->
                                        dropEvent = getLocal(EventType.QOS1_DROPPED, QoS1Dropped.class)
                                            .reason(DropReason.NoSubPermission);
                                    case EXACTLY_ONCE -> dropEvent = getLocal(EventType.QOS2_DROPPED, QoS2Dropped.class)
                                        .reason(DropReason.NoSubPermission);
                                    default -> dropEvent = getLocal(EventType.QOS0_DROPPED, QoS0Dropped.class)
                                        .reason(DropReason.NoSubPermission);
                                }
                                eventCollector.report(dropEvent
                                    .reqId(message.getMessageId())
                                    .isRetain(false)
                                    .sender(senderMsgPack.getSender())
                                    .topic(topic)
                                    .matchedFilter(topicFilter)
                                    .size(message.getPayload().size())
                                    .clientInfo(clientInfo()));
                            }
                        }
                        // just do unsub once
                        submitBgTask(
                            () -> doUnsubscribe(0, topicFilter)
                                .thenAccept(
                                    v -> {
                                    })
                        );
                        break;
                    }
                }
            }, ctx.channel().eventLoop());
    }
}
