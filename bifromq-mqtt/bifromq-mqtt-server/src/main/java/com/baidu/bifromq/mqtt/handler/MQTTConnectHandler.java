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

import static com.baidu.bifromq.metrics.TenantMetric.MqttIngressBytes;
import static com.baidu.bifromq.mqtt.handler.MQTTSessionIdUtil.userSessionId;
import static com.baidu.bifromq.mqtt.handler.condition.ORCondition.or;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_SESSION_TYPE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_SESSION_TYPE_P_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_SESSION_TYPE_T_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_5_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_KEY;
import static com.bifromq.plugin.resourcethrottler.TenantResourceType.TotalConnectPerSecond;
import static com.bifromq.plugin.resourcethrottler.TenantResourceType.TotalConnections;
import static com.bifromq.plugin.resourcethrottler.TenantResourceType.TotalSessionMemoryBytes;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.baserpc.utils.FutureTracker;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.rpc.proto.ExpireReply;
import com.baidu.bifromq.inbox.rpc.proto.ExpireRequest;
import com.baidu.bifromq.inbox.rpc.proto.GetReply;
import com.baidu.bifromq.inbox.rpc.proto.GetRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxVersion;
import com.baidu.bifromq.inbox.storage.proto.LWT;
import com.baidu.bifromq.metrics.ITenantMeter;
import com.baidu.bifromq.mqtt.handler.condition.DirectMemPressureCondition;
import com.baidu.bifromq.mqtt.handler.condition.HeapMemPressureCondition;
import com.baidu.bifromq.mqtt.handler.condition.InboundResourceCondition;
import com.baidu.bifromq.mqtt.handler.record.GoAway;
import com.baidu.bifromq.mqtt.session.IMQTTPersistentSession;
import com.baidu.bifromq.mqtt.session.IMQTTTransientSession;
import com.baidu.bifromq.mqtt.session.MQTTSessionContext;
import com.baidu.bifromq.mqtt.utils.IMQTTMessageSizer;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.ProtocolError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientconnected.ClientConnected;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.sysprops.props.SanityCheckMqttUtf8String;
import com.baidu.bifromq.type.ClientInfo;
import com.bifromq.plugin.resourcethrottler.IResourceThrottler;
import com.bifromq.plugin.resourcethrottler.TenantResourceType;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MQTTConnectHandler extends ChannelDuplexHandler {
    protected static final boolean SANITY_CHECK = SanityCheckMqttUtf8String.INSTANCE.get();

    public record ExistingSession(long incarnation, long version) {

    }

    public record AuthResult(ClientInfo clientInfo, GoAway goAway) {
        public static AuthResult goAway(MqttMessage farewell, Event<?>... reasons) {
            return new AuthResult(null, new GoAway(farewell, reasons));
        }

        public static AuthResult ok(ClientInfo clientInfo) {
            return new AuthResult(clientInfo, null);
        }
    }

    private static final int MIN_CLIENT_KEEP_ALIVE_DURATION = 5;
    private static final int MAX_CLIENT_KEEP_ALIVE_DURATION = 2 * 60 * 60;
    private final FutureTracker cancellableTasks = new FutureTracker();
    private ChannelHandlerContext ctx;
    private MQTTSessionContext sessionCtx;
    private IInboxClient inboxClient;
    private IEventCollector eventCollector;
    private IResourceThrottler resourceThrottler;
    private ISettingProvider settingProvider;
    private boolean isGoAway;


    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.ctx = ctx;
        sessionCtx = ChannelAttrs.mqttSessionContext(ctx);
        inboxClient = sessionCtx.inboxClient;
        eventCollector = sessionCtx.eventCollector;
        resourceThrottler = sessionCtx.resourceThrottler;
        settingProvider = sessionCtx.settingProvider;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        cancellableTasks.stop();
        ctx.fireChannelInactive();
    }

    @Override
    public final void channelRead(ChannelHandlerContext ctx, Object msg) {
        MqttMessage mqttMessage = (MqttMessage) msg;
        if (mqttMessage.fixedHeader().messageType() == MqttMessageType.CONNECT) {
            MqttConnectMessage connMsg = (MqttConnectMessage) msg;
            GoAway goAway = sanityCheck(connMsg);
            if (goAway != null) {
                handleGoAway(goAway);
                return;
            }
            long reqId = System.nanoTime();
            cancellableTasks.track(authenticate(connMsg))
                .thenComposeAsync(okOrGoAway -> {
                    if (okOrGoAway.goAway != null) {
                        handleGoAway(okOrGoAway.goAway);
                        return CompletableFuture.completedFuture(null);
                    } else {
                        // check tenant resource
                        ClientInfo clientInfo = okOrGoAway.clientInfo;
                        String tenantId = clientInfo.getTenantId();
                        if (!resourceThrottler.hasResource(tenantId, TotalConnections)) {
                            handleGoAway(onNoEnoughResources(connMsg, TotalConnections, okOrGoAway.clientInfo));
                            return CompletableFuture.completedFuture(null);
                        }
                        if (!resourceThrottler.hasResource(tenantId, TotalSessionMemoryBytes)) {
                            handleGoAway(onNoEnoughResources(connMsg, TotalSessionMemoryBytes, okOrGoAway.clientInfo));
                            return CompletableFuture.completedFuture(null);
                        }
                        if (!resourceThrottler.hasResource(tenantId, TotalConnectPerSecond)) {
                            handleGoAway(onNoEnoughResources(connMsg, TotalConnectPerSecond, okOrGoAway.clientInfo));
                            return CompletableFuture.completedFuture(null);
                        }
                        TenantSettings settings = new TenantSettings(tenantId, settingProvider);
                        GoAway isInvalid = validate(connMsg, settings, clientInfo);
                        if (isInvalid != null) {
                            handleGoAway(isInvalid);
                            return CompletableFuture.completedFuture(null);
                        }
                        LWT willMessage = connMsg.variableHeader().isWillFlag() ? getWillMessage(connMsg) : null;
                        int keepAliveSeconds = keepAliveSeconds(connMsg.variableHeader().keepAliveTimeSeconds());
                        String userSessionId = userSessionId(clientInfo);
                        String requestClientId = connMsg.payload().clientIdentifier();
                        if (isCleanSession(connMsg, settings)) {
                            if (requestClientId.isEmpty()) {
                                int sessionExpiryInterval = getSessionExpiryInterval(connMsg, settings);
                                if (sessionExpiryInterval == 0) {
                                    setupTransientSessionHandler(connMsg,
                                        settings,
                                        userSessionId,
                                        keepAliveSeconds,
                                        false,
                                        willMessage,
                                        clientInfo,
                                        ctx);

                                } else {
                                    setupPersistentSessionHandler(connMsg,
                                        settings,
                                        userSessionId,
                                        keepAliveSeconds,
                                        sessionExpiryInterval,
                                        null,
                                        willMessage,
                                        clientInfo,
                                        ctx);
                                }
                                return CompletableFuture.completedFuture(null);
                            } else {
                                return inboxClient.expire(ExpireRequest.newBuilder()
                                        .setReqId(reqId)
                                        .setTenantId(clientInfo.getTenantId())
                                        .setInboxId(userSessionId)
                                        .setNow(HLC.INST.getPhysical())
                                        .build())
                                    .exceptionallyAsync(e -> {
                                        log.error("Failed to expire inbox", e);
                                        return ExpireReply.newBuilder()
                                            .setReqId(reqId)
                                            .setCode(ExpireReply.Code.ERROR)
                                            .build();
                                    }, ctx.executor())
                                    .thenAcceptAsync(reply -> {
                                        if (reply.getCode() == ExpireReply.Code.ERROR) {
                                            handleGoAway(onCleanSessionFailed(clientInfo));
                                        } else {
                                            int sessionExpiryInterval = getSessionExpiryInterval(connMsg, settings);
                                            if (sessionExpiryInterval == 0) {
                                                setupTransientSessionHandler(connMsg,
                                                    settings,
                                                    userSessionId,
                                                    keepAliveSeconds,
                                                    reply.getCode() == ExpireReply.Code.OK,
                                                    willMessage,
                                                    clientInfo,
                                                    ctx);
                                            } else {
                                                setupPersistentSessionHandler(connMsg,
                                                    settings,
                                                    userSessionId,
                                                    keepAliveSeconds,
                                                    sessionExpiryInterval,
                                                    null,
                                                    willMessage,
                                                    clientInfo,
                                                    ctx);
                                            }
                                        }
                                    }, ctx.executor());
                            }
                        } else {
                            return inboxClient.get(GetRequest.newBuilder()
                                    .setReqId(reqId)
                                    .setTenantId(clientInfo.getTenantId())
                                    .setInboxId(userSessionId)
                                    .build())
                                .exceptionallyAsync(e -> {
                                    log.error("Failed to get inbox", e);
                                    return GetReply.newBuilder()
                                        .setReqId(reqId)
                                        .setCode(GetReply.Code.ERROR)
                                        .build();
                                }, ctx.executor())
                                .thenAcceptAsync(reply -> {
                                    if (reply.getCode() == GetReply.Code.ERROR) {
                                        handleGoAway(onGetSessionFailed(clientInfo));
                                    } else if (reply.getCode() == GetReply.Code.EXIST) {
                                        // reuse existing inbox with the highest incarnation, the old ones will be cleaned up eventually
                                        List<InboxVersion> inboxes = reply.getInboxList();
                                        InboxVersion inbox;
                                        if (inboxes.size() == 1) {
                                            inbox = inboxes.get(0);
                                        } else {
                                            inboxes = new ArrayList<>(inboxes);
                                            inboxes.sort(
                                                (o1, o2) -> Long.compare(o2.getIncarnation(), o1.getIncarnation()));
                                            inbox = reply.getInbox(0);
                                        }
                                        int sessionExpiryInterval = getSessionExpiryInterval(connMsg, settings);
                                        setupPersistentSessionHandler(connMsg,
                                            settings,
                                            userSessionId,
                                            keepAliveSeconds,
                                            sessionExpiryInterval,
                                            new ExistingSession(
                                                inbox.getIncarnation(),
                                                inbox.getVersion()),
                                            willMessage,
                                            clientInfo,
                                            ctx);
                                    } else {
                                        int sessionExpiryInterval = getSessionExpiryInterval(connMsg, settings);
                                        if (sessionExpiryInterval == 0) {
                                            setupTransientSessionHandler(connMsg,
                                                settings,
                                                userSessionId,
                                                keepAliveSeconds,
                                                false,
                                                willMessage,
                                                clientInfo,
                                                ctx);
                                        } else {
                                            setupPersistentSessionHandler(connMsg,
                                                settings,
                                                userSessionId,
                                                keepAliveSeconds,
                                                sessionExpiryInterval,
                                                null,
                                                willMessage,
                                                clientInfo,
                                                ctx);
                                        }
                                    }
                                }, ctx.executor());
                        }
                    }
                }, ctx.executor());
        } else {
            if (mqttMessage.decoderResult().isSuccess()) {
                handleMqttMessage(mqttMessage);
            } else {
                handleGoAway(new GoAway(getLocal(ProtocolError.class)
                    .peerAddress(ChannelAttrs.socketAddress(ctx.channel()))
                    .statement(mqttMessage.decoderResult().cause().getMessage())));
            }
        }
    }


    protected abstract GoAway sanityCheck(MqttConnectMessage message);

    protected abstract CompletableFuture<AuthResult> authenticate(MqttConnectMessage message);

    protected abstract void handleMqttMessage(MqttMessage message);

    protected abstract GoAway onNoEnoughResources(MqttConnectMessage message, TenantResourceType resourceType,
                                                  ClientInfo clientInfo);

    protected abstract GoAway validate(MqttConnectMessage message, TenantSettings settings, ClientInfo clientInfo);

    protected abstract LWT getWillMessage(MqttConnectMessage message);

    protected abstract boolean isCleanSession(MqttConnectMessage message, TenantSettings settings);

    protected abstract int getSessionExpiryInterval(MqttConnectMessage message, TenantSettings settings);

    protected abstract GoAway onCleanSessionFailed(ClientInfo clientInfo);

    protected abstract GoAway onGetSessionFailed(ClientInfo clientInfo);

    protected abstract MQTTSessionHandler buildTransientSessionHandler(MqttConnectMessage connMsg,
                                                                       TenantSettings settings,
                                                                       ITenantMeter tenantMeter,
                                                                       String userSessionId,
                                                                       int keepAliveSeconds,
                                                                       @Nullable LWT willMessage,
                                                                       ClientInfo clientInfo,
                                                                       ChannelHandlerContext ctx);

    protected abstract MQTTSessionHandler buildPersistentSessionHandler(MqttConnectMessage connMsg,
                                                                        TenantSettings settings,
                                                                        ITenantMeter tenantMeter,
                                                                        String userSessionId,
                                                                        int keepAliveSeconds,
                                                                        int sessionExpiryInterval,
                                                                        @Nullable ExistingSession existingSession,
                                                                        @Nullable LWT willMessage,
                                                                        ClientInfo clientInfo,
                                                                        ChannelHandlerContext ctx);

    private void setupTransientSessionHandler(MqttConnectMessage connMsg,
                                              TenantSettings settings,
                                              String userSessionId,
                                              int keepAliveSeconds,
                                              boolean sessionExists,
                                              LWT willMessage,
                                              ClientInfo clientInfo,
                                              ChannelHandlerContext ctx) {
        int maxPacketSize = maxPacketSize(connMsg, settings);
        clientInfo = clientInfo.toBuilder()
            .putMetadata(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_T_VALUE)
            .build();
        ctx.pipeline().addBefore(ctx.executor(), MqttDecoder.class.getName(), MQTTPacketFilter.NAME,
            new MQTTPacketFilter(maxPacketSize, settings, clientInfo, eventCollector));
        ctx.pipeline().replace(ctx.pipeline().get(ConditionalRejectHandler.NAME),
            ConditionalSlowDownHandler.NAME, new ConditionalSlowDownHandler(
                or(DirectMemPressureCondition.INSTANCE, HeapMemPressureCondition.INSTANCE,
                    new InboundResourceCondition(resourceThrottler, clientInfo)),
                eventCollector, sessionCtx::nanoTime, clientInfo));
        ITenantMeter tenantMeter = ITenantMeter.get(clientInfo.getTenantId());
        IMQTTMessageSizer sizer = clientInfo.getMetadataOrDefault(MQTT_PROTOCOL_VER_KEY, "")
            .equals(MQTT_PROTOCOL_VER_5_VALUE) ? IMQTTMessageSizer.mqtt5() : IMQTTMessageSizer.mqtt3();
        tenantMeter.recordSummary(MqttIngressBytes, sizer.sizeByHeader(connMsg.fixedHeader()));
        MQTTSessionHandler sessionHandler = buildTransientSessionHandler(
            connMsg,
            settings,
            tenantMeter,
            userSessionId,
            keepAliveSeconds,
            willMessage,
            clientInfo,
            ctx);
        assert sessionHandler instanceof IMQTTTransientSession;
        ctx.pipeline().replace(this, IMQTTTransientSession.NAME, sessionHandler);
        ClientInfo finalClientInfo = clientInfo;
        sessionHandler.awaitInitialized()
            .thenAcceptAsync(v -> {
                ctx.writeAndFlush(onConnected(connMsg, settings,
                    userSessionId, keepAliveSeconds, 0, false, finalClientInfo));
                // report client connected event
                eventCollector.report(getLocal(ClientConnected.class)
                    .serverId(sessionCtx.serverId)
                    .clientInfo(finalClientInfo)
                    .userSessionId(userSessionId)
                    .keepAliveTimeSeconds(keepAliveSeconds)
                    .cleanSession(connMsg.variableHeader().isCleanSession())
                    .sessionPresent(sessionExists)
                    .lastWill(willMessage != null ? new ClientConnected.WillInfo()
                        .topic(willMessage.getTopic())
                        .isRetain(willMessage.getMessage().getIsRetain())
                        .qos(willMessage.getMessage().getPubQoS())
                        .payload(willMessage.getMessage().getPayload().asReadOnlyByteBuffer()) : null));
            }, ctx.executor());
    }

    private void setupPersistentSessionHandler(MqttConnectMessage connMsg,
                                               TenantSettings settings,
                                               String userSessionId,
                                               int keepAliveSeconds,
                                               int sessionExpiryInterval,
                                               @Nullable ExistingSession existingSession,
                                               @Nullable LWT willMessage,
                                               ClientInfo clientInfo,
                                               ChannelHandlerContext ctx) {
        int maxPacketSize = maxPacketSize(connMsg, settings);
        clientInfo = clientInfo.toBuilder()
            .putMetadata(MQTT_CLIENT_SESSION_TYPE, MQTT_CLIENT_SESSION_TYPE_P_VALUE)
            .build();
        ctx.pipeline().addBefore(ctx.executor(), MqttDecoder.class.getName(), MQTTPacketFilter.NAME,
            new MQTTPacketFilter(maxPacketSize, settings, clientInfo, eventCollector));
        ctx.pipeline().replace(ctx.pipeline().get(ConditionalRejectHandler.NAME),
            ConditionalSlowDownHandler.NAME, new ConditionalSlowDownHandler(
                or(DirectMemPressureCondition.INSTANCE, HeapMemPressureCondition.INSTANCE,
                    new InboundResourceCondition(resourceThrottler, clientInfo)),
                eventCollector, sessionCtx::nanoTime, clientInfo));

        ITenantMeter tenantMeter = ITenantMeter.get(clientInfo.getTenantId());
        IMQTTMessageSizer sizer = clientInfo.getMetadataOrDefault(MQTT_PROTOCOL_VER_KEY, "")
            .equals(MQTT_PROTOCOL_VER_5_VALUE) ? IMQTTMessageSizer.mqtt5() : IMQTTMessageSizer.mqtt3();
        tenantMeter.recordSummary(MqttIngressBytes, sizer.sizeByHeader(connMsg.fixedHeader()));
        MQTTSessionHandler sessionHandler = buildPersistentSessionHandler(connMsg,
            settings,
            tenantMeter,
            userSessionId,
            keepAliveSeconds,
            sessionExpiryInterval,
            existingSession,
            willMessage,
            clientInfo,
            ctx);
        assert sessionHandler instanceof IMQTTPersistentSession;
        ctx.pipeline().replace(this, IMQTTPersistentSession.NAME, sessionHandler);
        ClientInfo finalClientInfo = clientInfo;
        sessionHandler.awaitInitialized()
            .thenAcceptAsync(v -> {
                ctx.writeAndFlush(onConnected(connMsg,
                    settings, userSessionId, keepAliveSeconds, sessionExpiryInterval, existingSession != null,
                    finalClientInfo));
                // report client connected event
                eventCollector.report(getLocal(ClientConnected.class)
                    .serverId(sessionCtx.serverId)
                    .clientInfo(finalClientInfo)
                    .userSessionId(userSessionId)
                    .keepAliveTimeSeconds(keepAliveSeconds)
                    .cleanSession(connMsg.variableHeader().isCleanSession())
                    .sessionPresent(existingSession != null)
                    .lastWill(willMessage != null ? new ClientConnected.WillInfo()
                        .topic(willMessage.getTopic())
                        .isRetain(willMessage.getMessage().getIsRetain())
                        .qos(willMessage.getMessage().getPubQoS())
                        .payload(willMessage.getMessage().getPayload().asReadOnlyByteBuffer()) : null));
            }, ctx.executor());
    }


    protected abstract MqttConnAckMessage onConnected(MqttConnectMessage connMsg,
                                                      TenantSettings settings,
                                                      String userSessionId,
                                                      int keepAliveSeconds,
                                                      int sessionExpiryInterval,
                                                      boolean sessionExists,
                                                      ClientInfo clientInfo);

    protected void handleGoAway(GoAway goAway) {
        assert ctx.executor().inEventLoop();
        if (isGoAway) {
            return;
        }
        isGoAway = true;
        if (!ctx.channel().isActive()) {
            return;
        }
        for (Event<?> reason : goAway.reasons()) {
            eventCollector.report(reason);
        }
        Runnable doGoAway = () -> {
            if (goAway.farewell() != null) {
                ctx.writeAndFlush(goAway.farewell()).addListener(ChannelFutureListener.CLOSE);
            } else {
                ctx.channel().close();
            }
        };
        if (goAway.rightNow()) {
            doGoAway.run();
        } else {
            ctx.executor().schedule(doGoAway,
                ThreadLocalRandom.current().nextInt(100, 5000), TimeUnit.MILLISECONDS);
        }
    }

    protected abstract int maxPacketSize(MqttConnectMessage connMsg, TenantSettings settings);

    private int keepAliveSeconds(int requestKeepAliveSeconds) {
        if (requestKeepAliveSeconds == 0) {
            // 20 mins the default keep alive duration
            requestKeepAliveSeconds = sessionCtx.defaultKeepAliveTimeSeconds;
        }
        requestKeepAliveSeconds = Math.max(MIN_CLIENT_KEEP_ALIVE_DURATION, requestKeepAliveSeconds);
        requestKeepAliveSeconds = Math.min(requestKeepAliveSeconds, MAX_CLIENT_KEEP_ALIVE_DURATION);
        return requestKeepAliveSeconds;
    }
}
