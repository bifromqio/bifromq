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

package com.baidu.bifromq.mqtt.handler.v5;

import static com.baidu.bifromq.mqtt.handler.v3.MQTTSessionIdUtil.userSessionId;
import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.toWillMessage;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicLength;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicLevelLength;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicLevels;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CHANNEL_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ADDRESS_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_5_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_TYPE_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_BANNED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_MALFORMED_PACKET;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED_5;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_RETAIN_NOT_SUPPORTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_TOPIC_NAME_INVALID;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_UNSPECIFIED_ERROR;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.rpc.proto.ExpireReply;
import com.baidu.bifromq.inbox.rpc.proto.ExpireRequest;
import com.baidu.bifromq.inbox.rpc.proto.GetReply;
import com.baidu.bifromq.inbox.rpc.proto.GetRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxVersion;
import com.baidu.bifromq.inbox.storage.proto.LWT;
import com.baidu.bifromq.mqtt.handler.ChannelAttrs;
import com.baidu.bifromq.mqtt.handler.MQTTMessageHandler;
import com.baidu.bifromq.mqtt.handler.TenantSettings;
import com.baidu.bifromq.mqtt.utils.AuthUtil;
import com.baidu.bifromq.mqtt.utils.MQTTUtf8Util;
import com.baidu.bifromq.mqtt.utils.TopicUtil;
import com.baidu.bifromq.plugin.authprovider.type.Failed;
import com.baidu.bifromq.plugin.authprovider.type.MQTT5AuthData;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.AuthError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.MalformedClientIdentifier;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.MalformedUserName;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.MalformedWillTopic;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.NotAuthorizedClient;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.UnauthenticatedClient;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientconnected.ClientConnected;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.InboxTransientError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.InvalidTopic;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.RetainNotSupported;
import com.baidu.bifromq.sysprops.BifroMQSysProp;
import com.baidu.bifromq.type.ClientInfo;
import com.google.common.base.Strings;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MQTT5ConnectHandler extends MQTTMessageHandler {
    public static final String NAME = "MQTT5ConnectHandler";
    private static final int MIN_CLIENT_KEEP_ALIVE_DURATION = 5;
    private static final int MAX_CLIENT_KEEP_ALIVE_DURATION = 2 * 60 * 60;
    private static final int MAX_CLIENT_ID_LEN = BifroMQSysProp.MAX_MQTT5_CLIENT_ID_LENGTH.get();
    private static final boolean SANITY_CHECK = BifroMQSysProp.MQTT_UTF8_SANITY_CHECK.get();
    private IInboxClient inboxClient;
    private ClientInfo clientInfo;
    private LWT willMessage;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        super.handlerAdded(ctx);
        inboxClient = sessionCtx.inboxClient;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        super.channelInactive(ctx);
        ctx.fireChannelInactive();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        assert msg instanceof MqttConnectMessage;
        MqttConnectMessage connMsg = (MqttConnectMessage) msg;
        log.trace("Handling MQTT5 conn message:\n{}", msg);
        final InetSocketAddress clientAddress = ChannelAttrs.socketAddress(ctx.channel());
        // sanity check
        final String requestClientId = connMsg.payload().clientIdentifier();
        if (!MQTTUtf8Util.isWellFormed(requestClientId, SANITY_CHECK)) {
            closeConnectionWithSomeDelay(MqttMessageBuilders
                    .connAck()
                    .properties(new MqttMessageBuilders.ConnAckPropertiesBuilder()
                        .reasonString("Malformed clientId")
                        .build())
                    .returnCode(CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID) // [MQTT-3.1.3-8]
                    .build(),
                getLocal(MalformedClientIdentifier.class).peerAddress(clientAddress));
            return;
        }
        if (requestClientId.length() > MAX_CLIENT_ID_LEN) {
            closeConnectionWithSomeDelay(MqttMessageBuilders
                    .connAck()
                    .properties(new MqttMessageBuilders.ConnAckPropertiesBuilder()
                        .reasonString("Max " + MAX_CLIENT_ID_LEN + "chars allowed")
                        .build())
                    .returnCode(CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID) // [MQTT-3.1.3-8]
                    .build(),
                getLocal(MalformedClientIdentifier.class).peerAddress(clientAddress));
            return;
        }
        if (connMsg.variableHeader().isCleanSession() && requestClientId.isEmpty()) {
            closeConnectionWithSomeDelay(MqttMessageBuilders
                    .connAck()
                    .properties(new MqttMessageBuilders.ConnAckPropertiesBuilder()
                        .reasonString("ClientId missing when CleanStart is set to true")
                        .build())
                    .returnCode(CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID) // [MQTT-3.1.3-8]
                    .build(),
                getLocal(MalformedClientIdentifier.class).peerAddress(clientAddress));
            return;
        }
        if (connMsg.variableHeader().hasUserName() &&
            !MQTTUtf8Util.isWellFormed(connMsg.payload().userName(), SANITY_CHECK)) {
            closeConnectionWithSomeDelay(MqttMessageBuilders
                    .connAck()
                    .properties(new MqttMessageBuilders.ConnAckPropertiesBuilder()
                        .reasonString("Malformed username")
                        .build())
                    .returnCode(CONNECTION_REFUSED_MALFORMED_PACKET) // [MQTT-4.13.1-1]
                    .build(),
                getLocal(MalformedUserName.class).peerAddress(clientAddress));
            return;
        }
        if (connMsg.variableHeader().isWillFlag() &&
            !MQTTUtf8Util.isWellFormed(connMsg.payload().willTopic(), SANITY_CHECK)) {
            closeConnectionWithSomeDelay(MqttMessageBuilders
                .connAck()
                .properties(new MqttMessageBuilders.ConnAckPropertiesBuilder()
                    .reasonString("Malformed will topic")
                    .build())
                .returnCode(CONNECTION_REFUSED_TOPIC_NAME_INVALID) // [MQTT-4.13.1-1]
                .build(), getLocal(MalformedWillTopic.class).peerAddress(clientAddress));
            return;
        }
        long reqId = System.nanoTime();
        // TODO: support extended auth workflow
        MQTT5AuthData authData = AuthUtil.buildMQTT5AuthData(ctx.channel(), connMsg);
        cancelOnInactive(authProvider.auth(authData))
            .thenComposeAsync(authResult -> {
                if (!ctx.channel().isActive()) {
                    return CompletableFuture.completedFuture(null);
                }
                switch (authResult.getTypeCase()) {
                    case FAILED -> {
                        Failed failed = authResult.getFailed();
                        switch (failed.getCode()) {
                            case NotAuthorized -> {
                                closeConnectionWithSomeDelay(MqttMessageBuilders
                                        .connAck()
                                        .returnCode(CONNECTION_REFUSED_NOT_AUTHORIZED_5)
                                        .build(),
                                    getLocal(NotAuthorizedClient.class).peerAddress(clientAddress));
                                return CompletableFuture.completedFuture(null);
                            }
                            case BadPass -> {
                                closeConnectionWithSomeDelay(MqttMessageBuilders
                                        .connAck()
                                        .returnCode(CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD)
                                        .build(),
                                    getLocal(UnauthenticatedClient.class).peerAddress(clientAddress));
                                return CompletableFuture.completedFuture(null);
                            }
                            case Banned -> {
                                closeConnectionWithSomeDelay(MqttMessageBuilders
                                        .connAck()
                                        .returnCode(CONNECTION_REFUSED_BANNED)
                                        .build(),
                                    getLocal(UnauthenticatedClient.class).peerAddress(clientAddress));
                                return CompletableFuture.completedFuture(null);
                            }
                            // fallthrough
                            default -> {
                                log.error("Unexpected error from auth provider:{}", failed.getReason());
                                closeConnectionWithSomeDelay(MqttMessageBuilders
                                        .connAck()
                                        .returnCode(CONNECTION_REFUSED_UNSPECIFIED_ERROR)
                                        .build(),
                                    getLocal(AuthError.class).cause(failed.getReason()).peerAddress(clientAddress));
                                return CompletableFuture.completedFuture(null);
                            }
                        }
                    }
                    case SUCCESS -> {
                        final TenantSettings settings =
                            new TenantSettings(authResult.getSuccess().getTenantId(), settingProvider);
                        // tenant spec conformation check
                        Optional<InetSocketAddress> clientAddr =
                            Optional.ofNullable(ChannelAttrs.socketAddress(ctx.channel()));
                        clientInfo = ClientInfo.newBuilder()
                            .setTenantId(authResult.getSuccess().getTenantId())
                            .setType(MQTT_TYPE_VALUE)
                            .putAllMetadata(authResult.getSuccess().getAttrsMap()) // custom attrs
                            .putMetadata(MQTT_PROTOCOL_VER_KEY, MQTT_PROTOCOL_VER_5_VALUE)
                            .putMetadata(MQTT_USER_ID_KEY, authResult.getSuccess().getTenantId())
                            .putMetadata(MQTT_CLIENT_ID_KEY, requestClientId)
                            .putMetadata(MQTT_CHANNEL_ID_KEY, ctx.channel().id().asLongText())
                            .putMetadata(MQTT_CLIENT_ADDRESS_KEY,
                                clientAddr.map(InetSocketAddress::toString).orElse(""))
                            .build();
                        if (connMsg.variableHeader().isWillFlag()) {
                            willMessage = toWillMessage(connMsg);
                            // will topic conforms to tenant spec limit
                            if (!TopicUtil.isValidTopic(willMessage.getTopic(),
                                settingProvider.provide(MaxTopicLevelLength, clientInfo.getTenantId()),
                                settingProvider.provide(MaxTopicLevels, clientInfo.getTenantId()),
                                settingProvider.provide(MaxTopicLength, clientInfo.getTenantId()))) {
                                closeConnectionWithSomeDelay(MqttMessageBuilders
                                        .connAck()
                                        .properties(new MqttMessageBuilders.ConnAckPropertiesBuilder()
                                            .reasonString("Will topic exceeds limits")
                                            .build())
                                        .returnCode(CONNECTION_REFUSED_TOPIC_NAME_INVALID)
                                        .build(),
                                    getLocal(InvalidTopic.class)
                                        .topic(willMessage.getTopic())
                                        .clientInfo(clientInfo));
                                return CompletableFuture.completedFuture(null);
                            }
                            // if retain enabled?
                            if (willMessage.getRetain() && !settings.retainEnabled) {
                                closeConnectionWithSomeDelay(MqttMessageBuilders
                                        .connAck()
                                        .returnCode(CONNECTION_REFUSED_RETAIN_NOT_SUPPORTED)
                                        .build(),
                                    getLocal(RetainNotSupported.class).clientInfo(clientInfo));
                                return CompletableFuture.completedFuture(null);
                            }
                        }
                        // handle clean start
                        final boolean cleanStart = settings.forceTransient || connMsg.variableHeader().isCleanSession();
                        Optional<Integer> requestSEI = Optional.ofNullable(
                                (MqttProperties.IntegerProperty) connMsg.variableHeader().properties()
                                    .getProperty(SESSION_EXPIRY_INTERVAL.value()))
                            .map(MqttProperties.MqttProperty::value);
                        int keepAliveSeconds = keepAliveSeconds(connMsg.variableHeader().keepAliveTimeSeconds());
                        String userSessionId = userSessionId(clientInfo);
                        if (cleanStart) {
                            int assignedSEI = settings.forceTransient ? 0 : requestSEI.orElse(0);
                            final int finalSEI = Math.min(assignedSEI, settings.maxSEI);
                            if (requestClientId.isEmpty()) {
                                if (finalSEI == 0) {
                                    return setupTransientSessionHandler(connMsg, settings, userSessionId,
                                        keepAliveSeconds, false);
                                } else {
                                    return setupPersistentSessionHandler(connMsg, settings, userSessionId, null,
                                        keepAliveSeconds, finalSEI);
                                }
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
                                    }, ctx.channel().eventLoop())
                                    .thenAcceptAsync(reply -> {
                                        if (reply.getCode() == ExpireReply.Code.ERROR) {
                                            closeConnectionWithSomeDelay(MqttMessageBuilders
                                                    .connAck()
                                                    .returnCode(CONNECTION_REFUSED_UNSPECIFIED_ERROR)
                                                    .build(),
                                                getLocal(InboxTransientError.class).clientInfo(clientInfo));
                                        } else {
                                            if (finalSEI == 0) {
                                                setupTransientSessionHandler(connMsg, settings, userSessionId,
                                                    keepAliveSeconds, reply.getCode() == ExpireReply.Code.OK);
                                            } else {
                                                setupPersistentSessionHandler(connMsg, settings, userSessionId,
                                                    null, keepAliveSeconds, finalSEI);
                                            }
                                        }
                                    }, ctx.channel().eventLoop());
                            }
                        } else {
                            int assignedSEI = Math.min(requestSEI.orElse(0), settings.maxSEI);

                            // update session if exists by setting SEI to assignedSEI and will message or return false
                            // upon receiving false from update call
                            return inboxClient.get(GetRequest.newBuilder()
                                    .setReqId(reqId)
                                    .setTenantId(clientInfo.getTenantId())
                                    .setInboxId(userSessionId)
                                    .build())
                                .exceptionallyAsync(e -> {
                                    log.error("Failed to update inbox", e);
                                    return GetReply.newBuilder()
                                        .setReqId(reqId)
                                        .setCode(GetReply.Code.ERROR)
                                        .build();
                                }, ctx.channel().eventLoop())
                                .thenAcceptAsync(reply -> {
                                    if (reply.getCode() == GetReply.Code.ERROR) {
                                        closeConnectionWithSomeDelay(MqttMessageBuilders
                                                .connAck()
                                                .returnCode(CONNECTION_REFUSED_UNSPECIFIED_ERROR)
                                                .build(),
                                            getLocal(InboxTransientError.class).clientInfo(clientInfo));
                                    } else if (reply.getCode() == GetReply.Code.EXIST) {
                                        // reuse existing inbox with the highest incarnation, the old ones will be cleaned up eventually
                                        InboxVersion inbox = reply.getInbox(reply.getInboxCount() - 1);
                                        setupPersistentSessionHandler(connMsg,
                                            settings,
                                            userSessionId,
                                            new MQTT5PersistentSessionHandler.ExistingSession(
                                                inbox.getIncarnation(),
                                                inbox.getVersion()),
                                            keepAliveSeconds,
                                            assignedSEI);
                                    } else {
                                        if (assignedSEI == 0) {
                                            setupTransientSessionHandler(connMsg, settings, userSessionId,
                                                keepAliveSeconds, false);
                                        } else {
                                            setupPersistentSessionHandler(connMsg, settings, userSessionId,
                                                null, keepAliveSeconds, assignedSEI);
                                        }
                                    }
                                }, ctx.channel().eventLoop());
                        }
                    }
                    default -> {
                        log.error("Unexpected auth result: {}", authResult.getTypeCase());
                        return CompletableFuture.completedFuture(null);
                    }
                }
            }, ctx.channel().eventLoop());
    }

    private int keepAliveSeconds(int requestKeepAliveSeconds) {
        if (requestKeepAliveSeconds == 0) {
            // 20 mins the default keep alive duration
            requestKeepAliveSeconds = sessionCtx.defaultKeepAliveTimeSeconds;
        }
        requestKeepAliveSeconds = Math.max(MIN_CLIENT_KEEP_ALIVE_DURATION, requestKeepAliveSeconds);
        requestKeepAliveSeconds = Math.min(requestKeepAliveSeconds, MAX_CLIENT_KEEP_ALIVE_DURATION);
        return requestKeepAliveSeconds;
    }

    private CompletableFuture<Void> setupTransientSessionHandler(MqttConnectMessage connMsg,
                                                                 TenantSettings settings,
                                                                 String userSessionId,
                                                                 int keepAliveSeconds,
                                                                 boolean sessionExists) {
        if (!ctx.channel().isActive()) {
            return CompletableFuture.completedFuture(null);
        }
        MQTT5TransientSessionHandler handler = MQTT5TransientSessionHandler.builder()
            .connProps(connMsg.variableHeader().properties())
            .settings(settings)
            .userSessionId(userSessionId)
            .clientInfo(clientInfo)
            .keepAliveTimeSeconds(keepAliveSeconds)
            .willMessage(willMessage)
            .build();
        // setup transient session handler
        ctx.pipeline().replace(this, MQTT5SessionHandler.NAME, handler);

        MqttMessageBuilders.ConnAckPropertiesBuilder connPropsBuilder =
            new MqttMessageBuilders.ConnAckPropertiesBuilder();
        if (Strings.isNullOrEmpty(clientInfo.getMetadataMap().get(MQTT_CLIENT_ID_KEY))) {
            connPropsBuilder.assignedClientId(clientInfo.getMetadataMap().get(MQTT_CHANNEL_ID_KEY));
        }
        if (connMsg.variableHeader().keepAliveTimeSeconds() != keepAliveSeconds) {
            connPropsBuilder.serverKeepAlive(keepAliveSeconds);
        }
        if (!settings.retainEnabled) {
            connPropsBuilder.retainAvailable(false);
        }
        connPropsBuilder.maximumPacketSize(settings.maxPacketSize);
        connPropsBuilder.topicAliasMaximum(settings.maxTopicAlias);
        ctx.writeAndFlush(MqttMessageBuilders
            .connAck()
            .properties(connPropsBuilder.build())
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
            .build());
        // report client connected event
        eventCollector.report(getLocal(ClientConnected.class).clientInfo(clientInfo)
            .serverId(sessionCtx.serverId)
            .userSessionId(userSessionId)
            .keepAliveTimeSeconds(keepAliveSeconds)
            .cleanSession(connMsg.variableHeader().isCleanSession())
            .sessionPresent(sessionExists)
            .lastWill(willMessage != null ? new ClientConnected.WillInfo().topic(willMessage.getTopic())
                .isRetain(willMessage.getRetain())
                .qos(willMessage.getMessage().getPubQoS())
                .payload(willMessage.getMessage().getPayload().asReadOnlyByteBuffer()) : null));
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> setupPersistentSessionHandler(MqttConnectMessage connMsg,
                                                                  TenantSettings settings,
                                                                  String userSessionId,
                                                                  @Nullable
                                                                  MQTT5PersistentSessionHandler.ExistingSession existingSession,
                                                                  int keepAliveSeconds,
                                                                  int finalSEI) {
        if (!ctx.channel().isActive()) {
            return CompletableFuture.completedFuture(null);
        }
        MQTT5PersistentSessionHandler handler = MQTT5PersistentSessionHandler.builder()
            .connProps(connMsg.variableHeader().properties())
            .settings(settings)
            .userSessionId(userSessionId)
            .clientInfo(clientInfo)
            .existingSession(existingSession)
            .keepAliveTimeSeconds(keepAliveSeconds)
            .sessionExpirySeconds(finalSEI)
            .willMessage(willMessage)
            .build();
        // setup transient session handler
        ctx.pipeline().replace(this, MQTT5SessionHandler.NAME, handler);

        MqttMessageBuilders.ConnAckPropertiesBuilder connPropsBuilder =
            new MqttMessageBuilders.ConnAckPropertiesBuilder();
        if (connMsg.variableHeader().keepAliveTimeSeconds() != keepAliveSeconds) {
            connPropsBuilder.serverKeepAlive(keepAliveSeconds);
        }
        MqttProperties.IntegerProperty intProp = (MqttProperties.IntegerProperty) connMsg.variableHeader().properties()
            .getProperty(SESSION_EXPIRY_INTERVAL.value());
        if (intProp == null || intProp.value() != finalSEI) {
            connPropsBuilder.sessionExpiryInterval(finalSEI);
        }
        if (!settings.retainEnabled) {
            connPropsBuilder.retainAvailable(false);
        }
        connPropsBuilder.maximumPacketSize(settings.maxPacketSize);
        connPropsBuilder.topicAliasMaximum(settings.maxTopicAlias);
        ctx.writeAndFlush(MqttMessageBuilders
            .connAck()
            .sessionPresent(existingSession != null)
            .properties(connPropsBuilder.build())
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
            .build());

        // report client connected event
        eventCollector.report(getLocal(ClientConnected.class).clientInfo(clientInfo)
            .serverId(sessionCtx.serverId)
            .userSessionId(userSessionId)
            .keepAliveTimeSeconds(keepAliveSeconds)
            .cleanSession(connMsg.variableHeader().isCleanSession())
            .sessionPresent(existingSession != null)
            .lastWill(willMessage != null ? new ClientConnected.WillInfo().topic(willMessage.getTopic())
                .isRetain(willMessage.getRetain())
                .qos(willMessage.getMessage().getPubQoS())
                .payload(willMessage.getMessage().getPayload().asReadOnlyByteBuffer()) : null));
        return CompletableFuture.completedFuture(null);
    }
}
