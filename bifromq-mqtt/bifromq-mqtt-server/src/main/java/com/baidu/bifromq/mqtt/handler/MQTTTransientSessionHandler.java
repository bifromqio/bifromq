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

import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS0InternalLatency;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS1InternalLatency;
import static com.baidu.bifromq.metrics.TenantMetric.MqttQoS2InternalLatency;
import static com.baidu.bifromq.metrics.TenantMetric.MqttTransientSubCount;
import static com.baidu.bifromq.metrics.TenantMetric.MqttTransientUnsubCount;
import static com.baidu.bifromq.metrics.TenantMetric.MqttTransientUnsubLatency;
import static com.baidu.bifromq.mqtt.handler.IMQTTProtocolHelper.SubResult.EXCEED_LIMIT;
import static com.baidu.bifromq.mqtt.handler.IMQTTProtocolHelper.SubResult.EXISTS;
import static com.baidu.bifromq.mqtt.handler.IMQTTProtocolHelper.SubResult.OK;
import static com.baidu.bifromq.mqtt.handler.IMQTTProtocolHelper.UnsubResult.ERROR;
import static com.baidu.bifromq.mqtt.inbox.util.DeliveryGroupKeyUtil.toDelivererKey;
import static com.baidu.bifromq.mqtt.service.ILocalDistService.globalize;
import static com.baidu.bifromq.mqtt.utils.AuthUtil.buildSubAction;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.bifromq.plugin.resourcethrottler.TenantResourceType.TotalTransientSubscribePerSecond;
import static com.bifromq.plugin.resourcethrottler.TenantResourceType.TotalTransientSubscriptions;
import static com.bifromq.plugin.resourcethrottler.TenantResourceType.TotalTransientUnsubscribePerSecond;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.dist.client.MatchResult;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.inbox.storage.proto.LWT;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterOption;
import com.baidu.bifromq.metrics.ITenantMeter;
import com.baidu.bifromq.mqtt.handler.condition.Condition;
import com.baidu.bifromq.mqtt.handler.record.ProtocolResponse;
import com.baidu.bifromq.mqtt.session.IMQTTTransientSession;
import com.baidu.bifromq.plugin.eventcollector.OutOfTenantResource;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ByClient;
import com.baidu.bifromq.plugin.eventcollector.session.MQTTSessionStart;
import com.baidu.bifromq.plugin.eventcollector.session.MQTTSessionStop;
import com.baidu.bifromq.retain.rpc.proto.MatchReply;
import com.baidu.bifromq.retain.rpc.proto.MatchRequest;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.MatchInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessagePack;
import io.micrometer.core.instrument.Timer;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
    private AtomicLong subNumGauge;

    protected MQTTTransientSessionHandler(TenantSettings settings,
                                          ITenantMeter tenantMeter,
                                          Condition oomCondition,
                                          String userSessionId,
                                          int keepAliveTimeSeconds,
                                          ClientInfo clientInfo,
                                          @Nullable LWT willMessage,
                                          ChannelHandlerContext ctx) {
        super(settings, tenantMeter, oomCondition, userSessionId, keepAliveTimeSeconds, clientInfo, willMessage, ctx);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        super.handlerAdded(ctx);
        subNumGauge = sessionCtx.getTransientSubNumGauge(clientInfo.getTenantId());
        onInitialized();
        resumeChannelRead();
        memUsage.addAndGet(estBaseMemSize());
        // Transient session lifetime is bounded by the channel lifetime
        eventCollector.report(getLocal(MQTTSessionStart.class).sessionId(userSessionId).clientInfo(clientInfo));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        super.channelInactive(ctx);
        if (!topicFilters.isEmpty()) {
            topicFilters.forEach((topicFilter, option) -> addBgTask(unsubTopicFilter(System.nanoTime(), topicFilter)));
        }
        int remainInboxSize = inbox.values().stream().reduce(0, (acc, msg) -> acc + msg.estBytes(), Integer::sum);
        memUsage.addAndGet(-estBaseMemSize());
        memUsage.addAndGet(-remainInboxSize);
        // Transient session lifetime is bounded by the channel lifetime
        eventCollector.report(getLocal(MQTTSessionStop.class).sessionId(userSessionId).clientInfo(clientInfo));
        ctx.fireChannelInactive();
    }

    private int estBaseMemSize() {
        // estimate bytes from JOL
        return 28;
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
        SubMessage msg = inbox.remove(seq);
        if (msg != null) {
            memUsage.addAndGet(-msg.estBytes());
        }
    }

    @Override
    protected final CompletableFuture<IMQTTProtocolHelper.SubResult> subTopicFilter(long reqId,
                                                                                    String topicFilter,
                                                                                    TopicFilterOption option) {
        // check resources
        if (!resourceThrottler.hasResource(clientInfo.getTenantId(), TotalTransientSubscriptions)) {
            eventCollector.report(getLocal(OutOfTenantResource.class)
                .reason(TotalTransientSubscriptions.name())
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(EXCEED_LIMIT);
        }
        if (!resourceThrottler.hasResource(clientInfo.getTenantId(), TotalTransientSubscribePerSecond)) {
            eventCollector.report(getLocal(OutOfTenantResource.class)
                .reason(TotalTransientSubscribePerSecond.name())
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(EXCEED_LIMIT);
        }
        tenantMeter.recordCount(MqttTransientSubCount);
        int maxTopicFiltersPerInbox = settings.maxTopicFiltersPerSub;
        if (topicFilters.size() >= maxTopicFiltersPerInbox) {
            return CompletableFuture.completedFuture(EXCEED_LIMIT);
        }
        TopicFilterOption prevOption = topicFilters.put(topicFilter, option);
        if (prevOption == null) {
            subNumGauge.addAndGet(1);
            memUsage.addAndGet(topicFilter.length());
        }
        return addMatchRecord(reqId, topicFilter)
            .thenApplyAsync(matchResult -> {
                switch (matchResult) {
                    case OK -> {
                        if (prevOption == null) {
                            return OK;
                        } else {
                            return EXISTS;
                        }
                    }
                    case EXCEED_LIMIT -> {
                        return IMQTTProtocolHelper.SubResult.EXCEED_LIMIT;
                    }
                    case BACK_PRESSURE_REJECTED -> {
                        return IMQTTProtocolHelper.SubResult.BACK_PRESSURE_REJECTED;
                    }
                    default -> {
                        return IMQTTProtocolHelper.SubResult.ERROR;
                    }
                }
            }, ctx.executor());
    }

    @Override
    protected CompletableFuture<MatchReply> matchRetainedMessage(long reqId, String topicFilter) {
        return sessionCtx.retainClient.match(MatchRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(clientInfo.getTenantId())
            .setMatchInfo(MatchInfo.newBuilder()
                .setTopicFilter(topicFilter)
                .setReceiverId(globalize(channelId()))
                .build())
            .setDelivererKey(toDelivererKey(globalize(channelId()), sessionCtx.serverId))
            .setBrokerId(0)
            .setLimit(settings.retainMatchLimit)
            .build());
    }

    @Override
    protected CompletableFuture<IMQTTProtocolHelper.UnsubResult> unsubTopicFilter(long reqId, String topicFilter) {
        // check unsub rate
        if (!resourceThrottler.hasResource(clientInfo.getTenantId(), TotalTransientUnsubscribePerSecond)) {
            eventCollector.report(getLocal(OutOfTenantResource.class)
                .reason(TotalTransientUnsubscribePerSecond.name())
                .clientInfo(clientInfo));
            return CompletableFuture.completedFuture(ERROR);
        }
        tenantMeter.recordCount(MqttTransientUnsubCount);
        Timer.Sample start = Timer.start();
        if (topicFilters.remove(topicFilter) == null) {
            return CompletableFuture.completedFuture(IMQTTProtocolHelper.UnsubResult.NO_SUB);
        }
        return removeMatchRecord(reqId, topicFilter)
            .handleAsync((result, e) -> {
                subNumGauge.addAndGet(-1);
                memUsage.addAndGet(-topicFilter.length());
                if (e != null) {
                    return ERROR;
                } else {
                    switch (result) {
                        case OK -> {
                            start.stop(tenantMeter.timer(MqttTransientUnsubLatency));
                            return IMQTTProtocolHelper.UnsubResult.OK;
                        }
                        case BACK_PRESSURE_REJECTED -> {
                            return IMQTTProtocolHelper.UnsubResult.BACK_PRESSURE_REJECTED;
                        }
                        default -> {
                            return ERROR;
                        }
                    }
                }
            }, ctx.executor());
    }

    @Override
    public boolean publish(MatchInfo matchInfo, List<TopicMessagePack> topicMsgPacks) {
        String topicFilter = matchInfo.getTopicFilter();
        TopicFilterOption option = topicFilters.get(topicFilter);
        if (option == null || !ctx.channel().isActive()) {
            log.warn("TopicFilter: {} is not in session", topicFilter);
            return false;
        }
        ctx.executor().execute(() -> publish(topicFilter, option, topicMsgPacks));
        return true;
    }

    private void publish(String topicFilter,
                         TopicFilterOption option,
                         List<TopicMessagePack> topicMsgPacks) {
        assert ctx.executor().inEventLoop();
        if (!ctx.channel().isActive()) {
            return;
        }
        addFgTask(authProvider.checkPermission(clientInfo(), buildSubAction(topicFilter, option.getQos())))
            .thenAccept(checkResult -> {
                AtomicInteger totalMsgBytesSize = new AtomicInteger();
                forEach(topicFilter, option, topicMsgPacks, checkResult.hasGranted(), subMsg -> {
                    logInternalLatency(subMsg);
                    if (subMsg.qos() == QoS.AT_MOST_ONCE) {
                        sendQoS0SubMessage(subMsg);
                    } else {
                        inbox.put(msgSeqNo++, subMsg);
                        totalMsgBytesSize.addAndGet(subMsg.estBytes());
                    }
                });
                memUsage.addAndGet(totalMsgBytesSize.get());
                send();
            });
    }

    private void send() {
        SortedMap<Long, SubMessage> toBeSent = inbox.tailMap(nextSendSeq);
        if (toBeSent.isEmpty()) {
            return;
        }
        Iterator<Map.Entry<Long, SubMessage>> itr = toBeSent.entrySet().iterator();
        while (clientReceiveQuota() > 0 && itr.hasNext()) {
            Map.Entry<Long, SubMessage> entry = itr.next();
            long seq = entry.getKey();
            SubMessage msg = entry.getValue();
            sendConfirmableSubMessage(seq, msg);
            nextSendSeq = seq + 1;
        }
    }

    private void logInternalLatency(SubMessage message) {
        Timer timer = switch (message.qos()) {
            case AT_MOST_ONCE -> tenantMeter.timer(MqttQoS0InternalLatency);
            case AT_LEAST_ONCE -> tenantMeter.timer(MqttQoS1InternalLatency);
            default -> tenantMeter.timer(MqttQoS2InternalLatency);
        };
        timer.record(HLC.INST.getPhysical() - message.message().getTimestamp(), TimeUnit.MILLISECONDS);
    }

    private void forEach(String topicFilter,
                         TopicFilterOption option,
                         List<TopicMessagePack> topicMessagePacks,
                         boolean permissionGranted,
                         Consumer<SubMessage> consumer) {
        for (TopicMessagePack topicMsgPack : topicMessagePacks) {
            String topic = topicMsgPack.getTopic();
            List<TopicMessagePack.PublisherPack> publisherPacks = topicMsgPack.getMessageList();
            for (TopicMessagePack.PublisherPack senderMsgPack : publisherPacks) {
                ClientInfo publisher = senderMsgPack.getPublisher();
                List<Message> messages = senderMsgPack.getMessageList();
                for (Message message : messages) {
                    consumer.accept(new SubMessage(topic, message, publisher, topicFilter, option, permissionGranted));
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
