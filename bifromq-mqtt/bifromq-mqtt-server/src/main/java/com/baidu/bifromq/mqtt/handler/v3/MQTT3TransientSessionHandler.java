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

import static com.baidu.bifromq.mqtt.inbox.util.DeliveryGroupKeyUtil.toDelivererKey;
import static com.baidu.bifromq.mqtt.utils.AuthUtil.buildSubAction;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.dist.client.MatchResult;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.mqtt.session.v3.IMQTT3TransientSession;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.DropReason;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.PushEvent;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS0Dropped;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS0Pushed;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS1Dropped;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS2Dropped;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class MQTT3TransientSessionHandler extends MQTT3SessionHandler implements IMQTT3TransientSession {
    private final AtomicLong seqNum = new AtomicLong();
    private final Set<String> topicFilters = new HashSet<>();

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
        if (!topicFilters.isEmpty()) {
            submitBgTask(() -> CompletableFuture.allOf(topicFilters.stream()
                .map(topicFilter -> sessionCtx.distClient.unmatch(System.nanoTime(), clientInfo().getTenantId(),
                        topicFilter, channelId(), toDelivererKey(channelId(), sessionCtx.serverId), 0)
                    .exceptionally(e -> {
                        log.error("Failed to unsub: tenantId={}, topicFilter={}, inboxId={}, delivererKey={}",
                            clientInfo().getTenantId(), topicFilter, channelId(),
                            toDelivererKey(channelId(), sessionCtx.serverId), e);
                        return UnmatchResult.ERROR;
                    }))
                .toArray(CompletableFuture[]::new)));
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
        int maxTopicFiltersPerInbox = settingProvider.provide(Setting.MaxTopicFiltersPerInbox,
            clientInfo().getTenantId());
        if (topicFilters.size() >= maxTopicFiltersPerInbox) {
            return CompletableFuture.completedFuture(MqttQoS.FAILURE);
        }
        return distMatch(reqId, topicSub.topicName(), topicSub.qualityOfService())
            .thenApplyAsync(subResult -> {
                if (subResult == MatchResult.OK) {
                    topicFilters.add(topicSub.topicName());
                    return topicSub.qualityOfService();
                }
                return MqttQoS.FAILURE;
            }, ctx.channel().eventLoop());
    }

    @Override
    protected CompletableFuture<Boolean> doUnsubscribe(long reqId, String topicFilter) {
        if (!topicFilters.remove(topicFilter)) {
            return CompletableFuture.completedFuture(false);
        }
        return distUnMatch(reqId, topicFilter)
            .handleAsync((v, e) -> {
                if (e != null) {
                    return false;
                } else {
                    if (Objects.requireNonNull(v) == UnmatchResult.OK) {
                        return true;
                    }
                    return false;
                }
            }, ctx.channel().eventLoop());
    }

    @Override
    public boolean publish(SubInfo subInfo, TopicMessagePack topicMsgPack) {
        String topicFilter = subInfo.getTopicFilter();
        if (!topicFilters.contains(topicFilter)) {
            return false;
        }
        QoS subQoS = subInfo.getSubQoS();
        String topic = topicMsgPack.getTopic();
        List<TopicMessagePack.PublisherPack> publisherPacks = topicMsgPack.getMessageList();
        cancelOnInactive(authProvider.check(clientInfo(), buildSubAction(topicFilter, subQoS)))
            .thenAcceptAsync(allow -> {
                if (allow) {
                    long timestamp = HLC.INST.getPhysical();
                    for (int i = 0; i < publisherPacks.size(); i++) {
                        TopicMessagePack.PublisherPack senderMsgPack = publisherPacks.get(i);
                        ClientInfo publisher = senderMsgPack.getPublisher();
                        List<Message> messages = senderMsgPack.getMessageList();
                        for (int j = 0; j < messages.size(); j++) {
                            Message message = messages.get(j);
                            QoS finalQoS =
                                QoS.forNumber(Math.min(message.getPubQoS().getNumber(), subQoS.getNumber()));
                            boolean flush = i + 1 == publisherPacks.size() && j + 1 == messages.size();
                            switch (finalQoS) {
                                case AT_MOST_ONCE -> {
                                    if (bufferCapacityHinter.hasCapacity()) {
                                        if (sendQoS0TopicMessage(topic, message, false, flush, timestamp)) {
                                            if (debugMode) {
                                                eventCollector.report(
                                                    getLocal(QoS0Pushed.class)
                                                        .reqId(message.getMessageId())
                                                        .isRetain(false)
                                                        .sender(publisher)
                                                        .topic(topic)
                                                        .matchedFilter(topicFilter)
                                                        .size(message.getPayload().size())
                                                        .clientInfo(clientInfo()));
                                            }
                                        } else {
                                            eventCollector.report(
                                                getLocal(QoS0Dropped.class)
                                                    .reason(DropReason.ChannelClosed)
                                                    .reqId(message.getMessageId())
                                                    .isRetain(false)
                                                    .sender(publisher)
                                                    .topic(topic)
                                                    .matchedFilter(topicFilter)
                                                    .size(message.getPayload().size())
                                                    .clientInfo(clientInfo()));
                                        }
                                    } else {
                                        flush(true);
                                        eventCollector.report(getLocal(QoS0Dropped.class)
                                            .reason(DropReason.Overflow)
                                            .reqId(message.getMessageId())
                                            .isRetain(false)
                                            .sender(publisher)
                                            .topic(topic)
                                            .matchedFilter(topicFilter)
                                            .size(message.getPayload().size())
                                            .clientInfo(clientInfo()));
                                    }
                                }
                                case AT_LEAST_ONCE -> {
                                    if (bufferCapacityHinter.hasCapacity()) {
                                        int messageId = sendQoS1TopicMessage(seqNum.incrementAndGet(),
                                            topicFilter, topic, message, publisher, false, flush, timestamp);
                                        if (messageId < 0) {
                                            log.error("MessageId exhausted");
                                        }
                                    } else {
                                        flush(true);
                                        eventCollector.report(getLocal(QoS1Dropped.class)
                                            .reason(DropReason.Overflow)
                                            .reqId(message.getMessageId())
                                            .isRetain(false)
                                            .sender(publisher)
                                            .topic(topic)
                                            .matchedFilter(topicFilter)
                                            .size(message.getPayload().size())
                                            .clientInfo(clientInfo()));
                                    }
                                }
                                case EXACTLY_ONCE -> {
                                    if (bufferCapacityHinter.hasCapacity()) {
                                        int messageId = sendQoS2TopicMessage(seqNum.incrementAndGet(),
                                            topicFilter, topic, message, publisher, false, flush, timestamp);
                                        if (messageId < 0) {
                                            log.error("MessageId exhausted");
                                        }
                                    } else {
                                        flush(true);
                                        eventCollector.report(getLocal(QoS2Dropped.class)
                                            .reason(DropReason.Overflow)
                                            .reqId(message.getMessageId())
                                            .isRetain(false)
                                            .sender(publisher)
                                            .topic(topic)
                                            .matchedFilter(topicFilter)
                                            .size(message.getPayload().size())
                                            .clientInfo(clientInfo()));
                                    }
                                }
                            }
                        }
                    }
                } else {
                    for (TopicMessagePack.PublisherPack senderMsgPack : publisherPacks) {
                        for (Message message : senderMsgPack.getMessageList()) {
                            PushEvent<?> dropEvent;
                            switch (message.getPubQoS()) {
                                case AT_LEAST_ONCE -> dropEvent = getLocal(QoS1Dropped.class)
                                    .reason(DropReason.NoSubPermission);
                                case EXACTLY_ONCE -> dropEvent = getLocal(QoS2Dropped.class)
                                    .reason(DropReason.NoSubPermission);
                                default -> dropEvent = getLocal(QoS0Dropped.class)
                                    .reason(DropReason.NoSubPermission);
                            }
                            eventCollector.report(dropEvent
                                .reqId(message.getMessageId())
                                .isRetain(false)
                                .sender(senderMsgPack.getPublisher())
                                .topic(topic)
                                .matchedFilter(topicFilter)
                                .size(message.getPayload().size())
                                .clientInfo(clientInfo()));
                        }
                    }
                    // just do unsub once
                    submitBgTask(() -> doUnsubscribe(0, topicFilter).thenAccept(
                        v -> {
                        })
                    );
                }
            }, ctx.channel().eventLoop());
        return true;
    }

    @Override
    public CompletableFuture<MqttQoS> subscribe(long reqId, String topicFilter, MqttQoS qos) {
        return CompletableFuture.supplyAsync(this::checkSubTopicFilters, ctx.channel().eventLoop())
                .thenCompose(r -> {
                    if (!r) {
                        return CompletableFuture.completedFuture(MqttQoS.FAILURE);
                    }
                    return cancelOnInactive(distMatch(reqId, topicFilter, qos)
                            .thenApplyAsync(subResult -> {
                                if (subResult == MatchResult.OK) {
                                    topicFilters.add(topicFilter);
                                    return qos;
                                }
                                return MqttQoS.FAILURE;
                            }, ctx.channel().eventLoop())
                            .thenCompose(subQos -> {
                                if (subQos == MqttQoS.FAILURE) {
                                    return CompletableFuture.completedFuture(MqttQoS.FAILURE);
                                }
                                return matchRetainMessages(reqId, topicFilter,
                                        QoS.values()[qos.ordinal()]).thenApply(
                                        ok -> ok ? qos : MqttQoS.FAILURE);
                            }));
                });
    }

    @Override
    public CompletableFuture<Boolean> unsubscribe(long reqId, String topicFilter) {
        return CompletableFuture.supplyAsync(() -> checkUnsubTopicFilter(topicFilter), ctx.channel().eventLoop())
                .thenCompose(r -> {
                    if (!r) {
                        return CompletableFuture.completedFuture(false);
                    }
                    return cancelOnInactive(distUnMatch(reqId, topicFilter)
                            .handleAsync((v, e) -> {
                                if (e != null) {
                                    return false;
                                } else {
                                    if (Objects.requireNonNull(v) == UnmatchResult.OK) {
                                        return true;
                                    }
                                    return false;
                                }
                            }, ctx.channel().eventLoop()));
                });
    }

    private CompletableFuture<MatchResult> distMatch(long reqId, String topicFilter, MqttQoS qoS) {
        return sessionCtx.distClient.match(reqId, clientInfo().getTenantId(), topicFilter,
                QoS.forNumber(qoS.value()), channelId(),
                toDelivererKey(channelId(), sessionCtx.serverId), 0);
    }

    private CompletableFuture<UnmatchResult> distUnMatch(long reqId, String topicFilter) {
        return sessionCtx.distClient.unmatch(reqId, clientInfo().getTenantId(), topicFilter, channelId(),
                toDelivererKey(channelId(), sessionCtx.serverId), 0);
    }

    private boolean checkSubTopicFilters() {
        int maxTopicFiltersPerInbox = settingProvider.provide(Setting.MaxTopicFiltersPerInbox,
                clientInfo().getTenantId());
        if (topicFilters.size() >= maxTopicFiltersPerInbox) {
            return false;
        }
        return true;
    }

    private boolean checkUnsubTopicFilter(String topicFilter) {
        return topicFilters.remove(topicFilter);
    }
}
