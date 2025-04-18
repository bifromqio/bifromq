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
import com.baidu.bifromq.basescheduler.AsyncRetry;
import com.baidu.bifromq.basescheduler.exception.RetryTimeoutException;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.rpc.proto.AttachRequest;
import com.baidu.bifromq.inbox.rpc.proto.DetachReply;
import com.baidu.bifromq.inbox.rpc.proto.DetachRequest;
import com.baidu.bifromq.inbox.rpc.proto.ExistReply;
import com.baidu.bifromq.inbox.rpc.proto.ExistRequest;
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
import com.baidu.bifromq.plugin.authprovider.type.Success;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.ProtocolError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientconnected.ClientConnected;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.sysprops.props.SanityCheckMqttUtf8String;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.UserProperties;
import com.bifromq.plugin.resourcethrottler.IResourceThrottler;
import com.bifromq.plugin.resourcethrottler.TenantResourceType;
import com.google.protobuf.ByteString;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MQTTConnectHandler extends ChannelDuplexHandler {
    protected static final boolean SANITY_CHECK = SanityCheckMqttUtf8String.INSTANCE.get();
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
        log.trace("Received {}", mqttMessage);
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
                        return CompletableFuture.completedFuture(okOrGoAway);
                    }
                    // check conn permission
                    return checkConnectPermission(connMsg, okOrGoAway.successInfo);
                }, ctx.executor())
                .thenComposeAsync(okOrGoAway -> {
                    if (okOrGoAway.goAway != null) {
                        handleGoAway(okOrGoAway.goAway);
                        return CompletableFuture.completedFuture(null);
                    } else {
                        // check tenant resource
                        ClientInfo clientInfo = okOrGoAway.successInfo.clientInfo;
                        String tenantId = clientInfo.getTenantId();
                        if (!resourceThrottler.hasResource(tenantId, TotalConnections)) {
                            handleGoAway(onNoEnoughResources(connMsg, TotalConnections, clientInfo));
                            return CompletableFuture.completedFuture(null);
                        }
                        if (!resourceThrottler.hasResource(tenantId, TotalSessionMemoryBytes)) {
                            handleGoAway(onNoEnoughResources(connMsg, TotalSessionMemoryBytes, clientInfo));
                            return CompletableFuture.completedFuture(null);
                        }
                        if (!resourceThrottler.hasResource(tenantId, TotalConnectPerSecond)) {
                            handleGoAway(onNoEnoughResources(connMsg, TotalConnectPerSecond, clientInfo));
                            return CompletableFuture.completedFuture(null);
                        }
                        TenantSettings settings = new TenantSettings(tenantId, settingProvider);
                        GoAway isInvalid = validate(connMsg, settings, clientInfo);
                        if (isInvalid != null) {
                            handleGoAway(isInvalid);
                            return CompletableFuture.completedFuture(null);
                        }
                        GoAway needRedirect = needRedirect(clientInfo);
                        if (needRedirect != null) {
                            handleGoAway(needRedirect);
                            return CompletableFuture.completedFuture(null);
                        }
                        LWT willMessage = connMsg.variableHeader().isWillFlag() ? getWillMessage(connMsg) : null;
                        int keepAliveSeconds = keepAliveSeconds(connMsg.variableHeader().keepAliveTimeSeconds(),
                            settings);
                        String userSessionId = userSessionId(clientInfo);
                        String requestClientId = connMsg.payload().clientIdentifier();
                        if (isCleanStart(connMsg, settings)) {
                            return expireInbox(reqId, requestClientId, userSessionId, clientInfo)
                                .thenAccept(result -> {
                                    if (result == ExpireResult.ERROR) {
                                        return;
                                    }
                                    int sessionExpiryInterval = getSessionExpiryInterval(connMsg, settings);
                                    if (sessionExpiryInterval == 0) {
                                        setupTransientSessionHandler(connMsg,
                                            settings,
                                            userSessionId,
                                            keepAliveSeconds,
                                            result == ExpireResult.OK,
                                            willMessage,
                                            okOrGoAway.successInfo,
                                            ctx);
                                    } else {
                                        AttachRequest.Builder reqBuilder = AttachRequest.newBuilder()
                                            .setReqId(System.nanoTime())
                                            .setInboxId(userSessionId) // server generated client id
                                            .setExpirySeconds(sessionExpiryInterval)
                                            .setLimit(settings.inboxQueueLength)
                                            .setDropOldest(settings.inboxDropOldest)
                                            .setClient(clientInfo)
                                            .setNow(HLC.INST.getPhysical());
                                        if (willMessage != null && willMessage.getDelaySeconds() > 0) {
                                            reqBuilder.setLwt(willMessage);
                                        }
                                        inboxClient.attach(reqBuilder.build())
                                            .thenAcceptAsync(attachReply -> {
                                                switch (attachReply.getCode()) {
                                                    case OK -> {
                                                        InboxVersion inboxVersion = attachReply.getVersion();
                                                        if (inboxVersion.getMod() > 0) {
                                                            // the server generated client id is already in use
                                                            // disconnected and try again
                                                            handleGoAway(
                                                                onInboxCallRetry(clientInfo, "Client id conflict"));
                                                        } else {
                                                            // setup persistent session and reuse
                                                            // existing inbox but the SEI is reset to 0
                                                            setupPersistentSessionHandler(connMsg,
                                                                settings,
                                                                userSessionId,
                                                                keepAliveSeconds,
                                                                sessionExpiryInterval, // reset SEI to 0
                                                                inboxVersion,
                                                                willMessage,
                                                                okOrGoAway.successInfo,
                                                                ctx);
                                                        }
                                                    }
                                                    case TRY_LATER -> handleGoAway(
                                                        onInboxCallRetry(clientInfo, "Inbox service call needs retry"));
                                                    case BACK_PRESSURE_REJECTED ->
                                                        handleGoAway(onInboxCallBusy(clientInfo, "Inbox service busy"));
                                                    default ->
                                                        handleGoAway(onInboxCallError(clientInfo, "Inbox service error"));
                                                }
                                            }, ctx.executor());
                                    }
                                });
                        } else {
                            int sessionExpiryInterval = getSessionExpiryInterval(connMsg, settings);
                            if (sessionExpiryInterval == 0) {
                                // try to attach to previous session and reset its SEI to 0
                                // or set up a new transient session
                                return AsyncRetry.exec(() -> inboxClient.exist(ExistRequest.newBuilder()
                                            .setReqId(reqId)
                                            .setTenantId(clientInfo.getTenantId())
                                            .setInboxId(userSessionId)
                                            .build()),
                                        (reply, t) -> {
                                            if (t != null) {
                                                return true;
                                            }
                                            return reply.getCode() != ExistReply.Code.TRY_LATER;
                                        }, 1000, 5000)
                                    .exceptionally(e -> {
                                        if (e instanceof RetryTimeoutException
                                            || e.getCause() instanceof RetryTimeoutException) {
                                            return ExistReply.newBuilder()
                                                .setReqId(reqId)
                                                .setCode(ExistReply.Code.TRY_LATER)
                                                .build();
                                        }
                                        log.debug("Failed to get inbox", e);
                                        return ExistReply.newBuilder()
                                            .setReqId(reqId)
                                            .setCode(ExistReply.Code.ERROR)
                                            .build();
                                    })
                                    .thenAcceptAsync(getReply -> {
                                        switch (getReply.getCode()) {
                                            case EXIST -> {
                                                // reuse existing inbox with the highest incarnation,
                                                // the old ones will be cleaned up eventually
                                                AttachRequest.Builder reqBuilder = AttachRequest.newBuilder()
                                                    .setReqId(System.nanoTime())
                                                    .setInboxId(userSessionId)
                                                    .setExpirySeconds(sessionExpiryInterval)
                                                    .setLimit(settings.inboxQueueLength)
                                                    .setDropOldest(settings.inboxDropOldest)
                                                    .setClient(clientInfo)
                                                    .setNow(HLC.INST.getPhysical());
                                                if (willMessage != null && willMessage.getDelaySeconds() > 0) {
                                                    reqBuilder.setLwt(willMessage);
                                                }
                                                inboxClient.attach(reqBuilder.build())
                                                    .thenAcceptAsync(attachReply -> {
                                                        switch (attachReply.getCode()) {
                                                            case OK -> {
                                                                InboxVersion inboxInstance = attachReply.getVersion();
                                                                if (inboxInstance.getMod() > 0) {
                                                                    // setup persistent session and reuse
                                                                    // existing inbox but the SEI is reset to 0
                                                                    setupPersistentSessionHandler(connMsg,
                                                                        settings,
                                                                        userSessionId,
                                                                        keepAliveSeconds,
                                                                        sessionExpiryInterval, // reset SEI to 0
                                                                        inboxInstance,
                                                                        willMessage,
                                                                        okOrGoAway.successInfo,
                                                                        ctx);
                                                                } else {
                                                                    // setup transient session, this may happen
                                                                    // when the inbox is expired between get and attach
                                                                    setupTransientSessionHandler(connMsg,
                                                                        settings,
                                                                        userSessionId,
                                                                        keepAliveSeconds,
                                                                        false,
                                                                        willMessage,
                                                                        okOrGoAway.successInfo,
                                                                        ctx);
                                                                }
                                                            }
                                                            case TRY_LATER -> handleGoAway(
                                                                onInboxCallRetry(clientInfo,
                                                                    "Inbox service call[attach] needs retry"));
                                                            case BACK_PRESSURE_REJECTED -> handleGoAway(
                                                                onInboxCallBusy(clientInfo,
                                                                    "Inbox service call[attach] busy"));
                                                            default -> handleGoAway(
                                                                onInboxCallError(clientInfo,
                                                                    "Inbox service call[attach] error"));
                                                        }
                                                    }, ctx.executor());
                                            }
                                            case NO_INBOX -> setupTransientSessionHandler(connMsg,
                                                settings,
                                                userSessionId,
                                                keepAliveSeconds,
                                                false,
                                                willMessage,
                                                okOrGoAway.successInfo,
                                                ctx);
                                            case BACK_PRESSURE_REJECTED ->
                                                handleGoAway(
                                                    onInboxCallBusy(clientInfo, "Inbox service call[exist] busy"));
                                            case TRY_LATER -> handleGoAway(
                                                onInboxCallRetry(clientInfo, "Inbox service call[exist] needs retry"));
                                            case ERROR -> handleGoAway(
                                                onInboxCallError(clientInfo, "Inbox service call[exist] error"));
                                            default -> {
                                                // do nothing
                                            }
                                        }
                                    }, ctx.executor());
                            } else {
                                // create a new or attach to an existing persistent session
                                AttachRequest.Builder reqBuilder = AttachRequest.newBuilder()
                                    .setReqId(System.nanoTime())
                                    .setInboxId(userSessionId)
                                    .setExpirySeconds(sessionExpiryInterval)
                                    .setLimit(settings.inboxQueueLength)
                                    .setDropOldest(settings.inboxDropOldest)
                                    .setClient(clientInfo)
                                    .setNow(HLC.INST.getPhysical());
                                if (willMessage != null && willMessage.getDelaySeconds() > 0) {
                                    reqBuilder.setLwt(willMessage);
                                }
                                return inboxClient.attach(reqBuilder.build())
                                    .thenAcceptAsync(attachReply -> {
                                        switch (attachReply.getCode()) {
                                            case OK -> setupPersistentSessionHandler(connMsg,
                                                settings,
                                                userSessionId,
                                                keepAliveSeconds,
                                                sessionExpiryInterval,
                                                attachReply.getVersion(),
                                                willMessage,
                                                okOrGoAway.successInfo,
                                                ctx);
                                            case TRY_LATER -> handleGoAway(
                                                onInboxCallRetry(clientInfo, "Inbox service call[attach] needs retry"));
                                            case BACK_PRESSURE_REJECTED -> handleGoAway(
                                                onInboxCallBusy(clientInfo, "Inbox service call[attach] busy"));
                                            default -> handleGoAway(
                                                onInboxCallError(clientInfo, "Inbox service call[attach] error"));
                                        }
                                    }, ctx.executor());
                            }
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

    private CompletableFuture<ExpireResult> expireInbox(long reqId,
                                                        String requestClientId,
                                                        String userSessionId,
                                                        ClientInfo clientInfo) {
        if (requestClientId.isEmpty()) {
            return CompletableFuture.completedFuture(ExpireResult.NOT_FOUND);
        }
        return inboxClient.detach(DetachRequest.newBuilder()
                .setReqId(reqId)
                .setInboxId(userSessionId)
                .setExpirySeconds(0)
                .setDiscardLWT(true)
                .setClient(clientInfo)
                .setNow(HLC.INST.getPhysical())
                .build())
            .exceptionally(e -> {
                log.debug("Failed to expire inbox", e);
                return DetachReply.newBuilder()
                    .setReqId(reqId)
                    .setCode(DetachReply.Code.ERROR)
                    .build();
            })
            .thenApplyAsync(reply -> {
                switch (reply.getCode()) {
                    case OK -> {
                        return ExpireResult.OK;
                    }
                    case NO_INBOX -> {
                        return ExpireResult.NOT_FOUND;
                    }
                    case TRY_LATER -> {
                        handleGoAway(onInboxCallRetry(clientInfo, "Inbox service call[expire] needs retry"));
                        return ExpireResult.ERROR;
                    }
                    case BACK_PRESSURE_REJECTED -> {
                        handleGoAway(onInboxCallBusy(clientInfo, "Inbox service call[expire] busy"));
                        return ExpireResult.ERROR;
                    }
                    default -> {
                        handleGoAway(onInboxCallError(clientInfo, "Inbox service call[expire] error"));
                        return ExpireResult.ERROR;
                    }
                }
            }, ctx.executor());

    }

    protected abstract GoAway sanityCheck(MqttConnectMessage message);

    protected abstract CompletableFuture<AuthResult> authenticate(MqttConnectMessage message);

    protected abstract CompletableFuture<AuthResult> checkConnectPermission(MqttConnectMessage message,
                                                                            SuccessInfo successInfo);

    protected abstract void handleMqttMessage(MqttMessage message);

    protected abstract GoAway onNoEnoughResources(MqttConnectMessage message, TenantResourceType resourceType,
                                                  ClientInfo clientInfo);

    protected abstract GoAway validate(MqttConnectMessage message, TenantSettings settings, ClientInfo clientInfo);

    protected abstract GoAway needRedirect(ClientInfo clientInfo);

    protected abstract LWT getWillMessage(MqttConnectMessage message);

    protected abstract boolean isCleanStart(MqttConnectMessage message, TenantSettings settings);

    protected abstract int getSessionExpiryInterval(MqttConnectMessage message, TenantSettings settings);

    protected abstract GoAway onInboxCallError(ClientInfo clientInfo, String reason);

    protected abstract GoAway onInboxCallRetry(ClientInfo clientInfo, String reason);

    protected abstract GoAway onInboxCallBusy(ClientInfo clientInfo, String reason);

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
                                                                        InboxVersion inboxInstance,
                                                                        @Nullable LWT willMessage,
                                                                        ClientInfo clientInfo,
                                                                        ChannelHandlerContext ctx);

    private void setupTransientSessionHandler(MqttConnectMessage connMsg,
                                              TenantSettings settings,
                                              String userSessionId,
                                              int keepAliveSeconds,
                                              boolean sessionExists,
                                              LWT willMessage,
                                              SuccessInfo successInfo,
                                              ChannelHandlerContext ctx) {
        int maxPacketSize = maxPacketSize(connMsg, settings);
        ClientInfo clientInfo = successInfo.clientInfo;
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
                ctx.writeAndFlush(onConnected(connMsg, settings, userSessionId, keepAliveSeconds,
                    0, false, finalClientInfo,
                    successInfo.responseInfo, successInfo.authData, successInfo.userProperties));
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
                                               InboxVersion inboxVersion,
                                               @Nullable LWT willMessage,
                                               SuccessInfo successInfo,
                                               ChannelHandlerContext ctx) {
        int maxPacketSize = maxPacketSize(connMsg, settings);
        ClientInfo clientInfo = successInfo.clientInfo;
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
            inboxVersion,
            willMessage,
            clientInfo,
            ctx);
        assert sessionHandler instanceof IMQTTPersistentSession;
        ctx.pipeline().replace(this, IMQTTPersistentSession.NAME, sessionHandler);
        ClientInfo finalClientInfo = clientInfo;
        sessionHandler.awaitInitialized()
            .thenAcceptAsync(v -> {
                ctx.writeAndFlush(onConnected(connMsg, settings, userSessionId, keepAliveSeconds, sessionExpiryInterval,
                    inboxVersion.getMod() > 0, finalClientInfo, successInfo.responseInfo, successInfo.authData,
                    successInfo.userProperties));
                // report client connected event
                eventCollector.report(getLocal(ClientConnected.class)
                    .serverId(sessionCtx.serverId)
                    .clientInfo(finalClientInfo)
                    .userSessionId(userSessionId)
                    .keepAliveTimeSeconds(keepAliveSeconds)
                    .cleanSession(connMsg.variableHeader().isCleanSession())
                    .sessionPresent(inboxVersion.getMod() > 0)
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
                                                      ClientInfo clientInfo,
                                                      Optional<String> responseInfo, // mqtt5
                                                      Optional<ByteString> authData, // mqtt5
                                                      UserProperties userProperties); // mqtt5

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

    private int keepAliveSeconds(int requestKeepAliveSeconds, TenantSettings settings) {
        if (requestKeepAliveSeconds > 0) {
            requestKeepAliveSeconds = Math.max(settings.minKeepAliveSeconds, requestKeepAliveSeconds);
        }
        return requestKeepAliveSeconds;
    }

    private enum ExpireResult {
        OK,
        NOT_FOUND,
        ERROR
    }

    public record SuccessInfo(ClientInfo clientInfo,
                              Optional<String> responseInfo, // mqtt5
                              Optional<ByteString> authData, // mqtt5
                              UserProperties userProperties) { //mqtt5
        public static SuccessInfo of(ClientInfo clientInfo) {
            return new SuccessInfo(clientInfo, Optional.empty(), Optional.empty(), UserProperties.getDefaultInstance());
        }
    }

    public record AuthResult(SuccessInfo successInfo, GoAway goAway) {
        public static AuthResult goAway(MqttMessage farewell, Event<?>... reasons) {
            return new AuthResult(null, new GoAway(farewell, reasons));
        }

        public static AuthResult goAwayNow(MqttMessage farewell, Event<?>... reasons) {
            return new AuthResult(null, GoAway.now(farewell, reasons));
        }

        public static AuthResult ok(Success success, ClientInfo clientInfo) {
            Optional<String> respInfo = Optional.ofNullable(success.hasResponseInfo()
                ? success.getResponseInfo() : null);
            Optional<ByteString> authData =
                Optional.ofNullable(success.hasAuthData() ? success.getAuthData() : null);
            SuccessInfo successInfo =
                new SuccessInfo(clientInfo, respInfo, authData, success.getUserProps());
            return new AuthResult(successInfo, null);
        }

        public static AuthResult ok(ClientInfo clientInfo) {
            return new AuthResult(SuccessInfo.of(clientInfo), null);
        }

        public static AuthResult ok(SuccessInfo successInfo) {
            return new AuthResult(successInfo, null);
        }
    }
}
