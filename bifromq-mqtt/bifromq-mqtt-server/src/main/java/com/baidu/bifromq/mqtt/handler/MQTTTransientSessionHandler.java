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
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS0InternalLatency;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS1InternalLatency;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS2InternalLatency;
import static com.baidu.bifromq.mqtt.handler.IMQTTProtocolHelper.SubResult.EXCEED_LIMIT;
import static com.baidu.bifromq.mqtt.handler.IMQTTProtocolHelper.SubResult.EXISTS;
import static com.baidu.bifromq.mqtt.handler.IMQTTProtocolHelper.SubResult.OK;
import static com.baidu.bifromq.mqtt.inbox.util.DeliveryGroupKeyUtil.toDelivererKey;
import static com.baidu.bifromq.mqtt.service.ILocalDistService.globalize;
import static com.baidu.bifromq.mqtt.utils.AuthUtil.buildSubAction;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.util.TopicUtil.isSharedSubscription;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.dist.client.MatchResult;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.inbox.storage.proto.LWT;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterOption;
import com.baidu.bifromq.mqtt.handler.record.ProtocolResponse;
import com.baidu.bifromq.mqtt.session.IMQTTTransientSession;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ByClient;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.DropReason;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS0Dropped;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS1Dropped;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS2Dropped;
import com.baidu.bifromq.retain.rpc.proto.MatchRequest;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.MatchInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessagePack;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MQTTTransientSessionHandler extends MQTTSessionHandler implements IMQTTTransientSession {
    // the topicFilters could be accessed concurrently
    private final Map<String, TopicFilterOption> topicFilters = new ConcurrentHashMap<>();
    private final NavigableMap<Long, SubMessage> inbox = new TreeMap<>();
    private long nextSendSeq = 0;
    private long msgSeqNo = 0;

    protected MQTTTransientSessionHandler(TenantSettings settings,
                                          String userSessionId,
                                          int keepAliveTimeSeconds,
                                          ClientInfo clientInfo,
                                          @Nullable LWT willMessage,
                                          ChannelHandlerContext ctx) {
        super(settings, userSessionId, keepAliveTimeSeconds, clientInfo, willMessage, ctx);
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
            topicFilters.forEach((topicFilter, option) ->
                addBgTask(sessionCtx.localDistService.unmatch(System.nanoTime(), topicFilter, this)));
        }
        ctx.fireChannelInactive();
    }

    @Override
    protected ProtocolResponse handleDisconnect(MqttMessage message) {
        Optional<Integer> requestSEI = helper().sessionExpiryIntervalOnDisconnect(message);
        if (requestSEI.isPresent() && requestSEI.get() > 0) {
            return helper().respondDisconnectProtocolError();
        }
        if (helper().isNormalDisconnect(message)) {
            discardLWT();
        }
        return ProtocolResponse.goAwayNow(getLocal(ByClient.class).clientInfo(clientInfo));
    }

    @Override
    protected final void onConfirm(long seq) {
        inbox.remove(seq);
    }

    @Override
    protected final CompletableFuture<IMQTTProtocolHelper.SubResult> subTopicFilter(long reqId, String topicFilter,
                                                                                    TopicFilterOption option) {
        int maxTopicFiltersPerInbox = settings.maxTopicFiltersPerSub;
        if (topicFilters.size() >= maxTopicFiltersPerInbox) {
            return CompletableFuture.completedFuture(EXCEED_LIMIT);
        }
        return addMatchRecord(reqId, topicFilter)
            .thenComposeAsync(subResult -> {
                switch (subResult) {
                    case OK -> {
                        TopicFilterOption prevOption = topicFilters.put(topicFilter, option);
                        return CompletableFuture.completedFuture(
                            prevOption == null ? OK : EXISTS);
                    }
                    case EXCEED_LIMIT -> {
                        return CompletableFuture.completedFuture(EXCEED_LIMIT);
                    }
                    default -> {
                        return CompletableFuture.completedFuture(IMQTTProtocolHelper.SubResult.ERROR);
                    }
                }
            }, ctx.executor())
            .thenComposeAsync(subResult -> {
                switch (subResult) {
                    case OK, EXISTS -> {
                        if (!isSharedSubscription(topicFilter) && settings.retainEnabled &&
                            (option.getRetainHandling() == SEND_AT_SUBSCRIBE ||
                                (subResult == IMQTTProtocolHelper.SubResult.OK &&
                                    option.getRetainHandling() == SEND_AT_SUBSCRIBE_IF_NOT_YET_EXISTS))) {
                            return addFgTask(sessionCtx.retainClient.match(MatchRequest.newBuilder()
                                .setReqId(reqId)
                                .setMatchInfo(MatchInfo.newBuilder()
                                    .setTenantId(clientInfo.getTenantId())
                                    .setTopicFilter(topicFilter)
                                    .setReceiverId(globalize(channelId()))
                                    .build())
                                .setDelivererKey(toDelivererKey(globalize(channelId()), sessionCtx.serverId))
                                .setBrokerId(0)
                                .setLimit(settings.retainMatchLimit)
                                .build()))
                                .handle((v, e) -> {
                                    if (e != null) {
                                        return IMQTTProtocolHelper.SubResult.ERROR;
                                    } else {
                                        return subResult;
                                    }
                                });
                        }
                        return CompletableFuture.completedFuture(subResult);
                    }
                    case EXCEED_LIMIT -> {
                        return CompletableFuture.completedFuture(IMQTTProtocolHelper.SubResult.EXCEED_LIMIT);
                    }
                    default -> {
                        return CompletableFuture.completedFuture(IMQTTProtocolHelper.SubResult.ERROR);
                    }
                }
            }, ctx.executor());
    }

    @Override
    protected CompletableFuture<IMQTTProtocolHelper.UnsubResult> unsubTopicFilter(long reqId, String topicFilter) {
        if (topicFilters.remove(topicFilter) == null) {
            return CompletableFuture.completedFuture(IMQTTProtocolHelper.UnsubResult.NO_SUB);
        }
        return removeMatchRecord(reqId, topicFilter)
            .handleAsync((v, e) -> {
                if (e != null || v == UnmatchResult.ERROR) {
                    return IMQTTProtocolHelper.UnsubResult.ERROR;
                } else {
                    return IMQTTProtocolHelper.UnsubResult.OK;
                }
            }, ctx.executor());
    }

    @Override
    public boolean publish(MatchInfo matchInfo, List<TopicMessagePack> topicMsgPacks) {
        String topicFilter = matchInfo.getTopicFilter();
        TopicFilterOption option = topicFilters.get(topicFilter);
        if (option == null) {
            return false;
        }
        ctx.executor().execute(() -> publish(topicFilter, option, topicMsgPacks));
        return true;
    }

    private void publish(String topicFilter, TopicFilterOption option, List<TopicMessagePack> topicMsgPacks) {
        QoS subQoS = option.getQos();
        addFgTask(authProvider.checkPermission(clientInfo(), buildSubAction(topicFilter, subQoS))
            .thenAcceptAsync(checkResult -> {
                if (checkResult.hasGranted()) {
                    forEach(topicFilter, option, topicMsgPacks, subMsg -> {
                        logInternalLatency(subMsg);
                        if (!ctx.channel().isActive()) {
                            reportDropEvent(subMsg, DropReason.ChannelClosed);
                            return;
                        }
                        if (option.getNoLocal() && clientInfo.equals(subMsg.publisher())) {
                            // skip local sub
                            if (settings.debugMode) {
                                reportDropEvent(subMsg, DropReason.NoLocal);
                            }
                            return;
                        }
                        if (subMsg.qos() == QoS.AT_MOST_ONCE) {
                            sendQoS0SubMessage(subMsg);
                        } else {
                            if (inbox.size() >= settings.inboxQueueLength) {
                                reportDropEvent(subMsg, DropReason.Overflow);
                                return;
                            }
                            inbox.put(msgSeqNo++, subMsg);
                        }
                    });
                    drainInbox();
                    flush(true);
                } else {
                    forEach(topicFilter, option, topicMsgPacks,
                        bufferMsg -> reportDropEvent(bufferMsg, DropReason.NoSubPermission));
                    // treat no permission as no_inbox
                    addBgTask(this.unsubTopicFilter(System.nanoTime(), topicFilter));
                }
            }, ctx.executor()));
    }

    private void drainInbox() {
        SortedMap<Long, SubMessage> toBeSent = inbox.tailMap(nextSendSeq);
        if (toBeSent.isEmpty()) {
            return;
        }
        Iterator<Map.Entry<Long, SubMessage>> itr = toBeSent.entrySet().iterator();
        while (clientReceiveQuota() > 0 && itr.hasNext()) {
            Map.Entry<Long, SubMessage> entry = itr.next();
            long seq = entry.getKey();
            sendSubMessage(seq, entry.getValue());
            nextSendSeq = seq + 1;
        }
    }

    private void logInternalLatency(SubMessage message) {
        (switch (message.qos()) {
            case AT_MOST_ONCE -> tenantMeter.timer(MqttQoS0InternalLatency);
            case AT_LEAST_ONCE -> tenantMeter.timer(MqttQoS1InternalLatency);
            default -> tenantMeter.timer(MqttQoS2InternalLatency);
        }).record(HLC.INST.getPhysical() - message.message().getTimestamp(), TimeUnit.MILLISECONDS);
    }

    private void reportDropEvent(SubMessage subMsg, DropReason reason) {
        switch (subMsg.qos()) {
            case AT_MOST_ONCE -> eventCollector.report(getLocal(QoS0Dropped.class)
                .reason(reason)
                .reqId(subMsg.message().getMessageId())
                .isRetain(subMsg.isRetain())
                .sender(subMsg.publisher())
                .topic(subMsg.topic())
                .matchedFilter(subMsg.topicFilter())
                .size(subMsg.message().getPayload().size())
                .clientInfo(clientInfo()));
            case AT_LEAST_ONCE -> eventCollector.report(getLocal(QoS1Dropped.class)
                .reason(reason)
                .reqId(subMsg.message().getMessageId())
                .isRetain(subMsg.isRetain())
                .sender(subMsg.publisher())
                .topic(subMsg.topic())
                .matchedFilter(subMsg.topicFilter())
                .size(subMsg.message().getPayload().size())
                .clientInfo(clientInfo()));
            case EXACTLY_ONCE -> eventCollector.report(getLocal(QoS2Dropped.class)
                .reason(reason)
                .reqId(subMsg.message().getMessageId())
                .isRetain(subMsg.isRetain())
                .sender(subMsg.publisher())
                .topic(subMsg.topic())
                .matchedFilter(subMsg.topicFilter())
                .size(subMsg.message().getPayload().size())
                .clientInfo(clientInfo()));
        }
    }

    private void forEach(String topicFilter,
                         TopicFilterOption option,
                         List<TopicMessagePack> topicMessagePacks,
                         Consumer<SubMessage> consumer) {
        for (TopicMessagePack topicMsgPack : topicMessagePacks) {
            String topic = topicMsgPack.getTopic();
            List<TopicMessagePack.PublisherPack> publisherPacks = topicMsgPack.getMessageList();
            for (TopicMessagePack.PublisherPack senderMsgPack : publisherPacks) {
                ClientInfo publisher = senderMsgPack.getPublisher();
                List<Message> messages = senderMsgPack.getMessageList();
                for (Message message : messages) {
                    consumer.accept(new SubMessage(topic, message, publisher, topicFilter, option));
                }
            }
        }
    }

    private CompletableFuture<MatchResult> addMatchRecord(long reqId, String topicFilter) {
        return sessionCtx.localDistService.match(reqId, topicFilter, this);
    }

    private CompletableFuture<UnmatchResult> removeMatchRecord(long reqId, String topicFilter) {
        return sessionCtx.localDistService.unmatch(reqId, topicFilter, this);
    }
}
