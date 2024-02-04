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

package com.baidu.bifromq.mqtt.handler.v5;

import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.authData;
import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.authMethod;
import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.isUTF8Payload;
import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.requestResponseInformation;
import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.toWillMessage;
import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.topicAliasMaximum;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CHANNEL_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ADDRESS_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_5_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_RESPONSE_INFO;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_TYPE_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_BANNED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_MALFORMED_PACKET;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED_5;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_PAYLOAD_FORMAT_INVALID;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_PROTOCOL_ERROR;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_QUOTA_EXCEEDED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_RETAIN_NOT_SUPPORTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_TOPIC_NAME_INVALID;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_UNSPECIFIED_ERROR;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL;

import com.baidu.bifromq.inbox.storage.proto.LWT;
import com.baidu.bifromq.mqtt.handler.ChannelAttrs;
import com.baidu.bifromq.mqtt.handler.MQTTConnectHandler;
import com.baidu.bifromq.mqtt.handler.TenantSettings;
import com.baidu.bifromq.mqtt.handler.record.GoAway;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5AuthReasonCode;
import com.baidu.bifromq.mqtt.utils.AuthUtil;
import com.baidu.bifromq.mqtt.utils.MQTTUtf8Util;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.authprovider.type.Continue;
import com.baidu.bifromq.plugin.authprovider.type.Failed;
import com.baidu.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthData;
import com.baidu.bifromq.plugin.authprovider.type.Success;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.AuthError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.EnhancedAuthAbortByClient;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.MalformedClientIdentifier;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.MalformedUserName;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.MalformedWillTopic;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.NotAuthorizedClient;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.ProtocolError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.UnauthenticatedClient;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.InboxTransientError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.InvalidTopic;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.RetainNotSupported;
import com.baidu.bifromq.sysprops.BifroMQSysProp;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.util.TopicUtil;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MQTT5ConnectHandler extends MQTTConnectHandler {
    public static final String NAME = "MQTT5ConnectHandler";
    private static final int MAX_CLIENT_ID_LEN = BifroMQSysProp.MAX_MQTT5_CLIENT_ID_LENGTH.get();
    private ChannelHandlerContext ctx;
    private IAuthProvider authProvider;
    private boolean isAuthing;
    private MqttConnectMessage connMsg = null;
    private String authMethod = null;
    private CompletableFuture<OkOrGoAway> extendedAuthFuture;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        super.handlerAdded(ctx);
        this.ctx = ctx;
        authProvider = ChannelAttrs.mqttSessionContext(ctx).authProvider(ctx);
    }

    @Override
    protected GoAway sanityCheck(MqttConnectMessage connMsg) {
        final InetSocketAddress clientAddress = ChannelAttrs.socketAddress(ctx.channel());
        // sanity check
        final String requestClientId = connMsg.payload().clientIdentifier();
        if (!MQTTUtf8Util.isWellFormed(requestClientId, SANITY_CHECK)) {
            return new GoAway(MqttMessageBuilders
                .connAck()
                .properties(new MqttMessageBuilders.ConnAckPropertiesBuilder()
                    .reasonString("Malformed clientId")
                    .build())
                .returnCode(CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID) // [MQTT-3.1.3-8]
                .build(),
                getLocal(MalformedClientIdentifier.class).peerAddress(clientAddress));
        }
        if (requestClientId.length() > MAX_CLIENT_ID_LEN) {
            return new GoAway(MqttMessageBuilders
                .connAck()
                .properties(new MqttMessageBuilders.ConnAckPropertiesBuilder()
                    .reasonString("Max " + MAX_CLIENT_ID_LEN + "chars allowed")
                    .build())
                .returnCode(CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID) // [MQTT-3.1.3-8]
                .build(),
                getLocal(MalformedClientIdentifier.class).peerAddress(clientAddress));
        }
        if (connMsg.variableHeader().isCleanSession() && requestClientId.isEmpty()) {
            return new GoAway(MqttMessageBuilders
                .connAck()
                .properties(new MqttMessageBuilders.ConnAckPropertiesBuilder()
                    .reasonString("ClientId missing when CleanStart is set to true")
                    .build())
                .returnCode(CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID) // [MQTT-3.1.3-8]
                .build(),
                getLocal(MalformedClientIdentifier.class).peerAddress(clientAddress));
        }
        if (connMsg.variableHeader().hasUserName() &&
            !MQTTUtf8Util.isWellFormed(connMsg.payload().userName(), SANITY_CHECK)) {
            return new GoAway(MqttMessageBuilders
                .connAck()
                .properties(new MqttMessageBuilders.ConnAckPropertiesBuilder()
                    .reasonString("Malformed username")
                    .build())
                .returnCode(CONNECTION_REFUSED_MALFORMED_PACKET) // [MQTT-4.13.1-1]
                .build(),
                getLocal(MalformedUserName.class).peerAddress(clientAddress));
        }
        if (authMethod(connMsg.variableHeader().properties()).isEmpty() &&
            authData(connMsg.variableHeader().properties()).isPresent()) {
            return new GoAway(MqttMessageBuilders
                .connAck()
                .properties(new MqttMessageBuilders.ConnAckPropertiesBuilder()
                    .reasonString("Missing auth method for authData")
                    .build())
                .returnCode(CONNECTION_REFUSED_PROTOCOL_ERROR) // [MQTT-4.13.1-1]
                .build(),
                getLocal(MalformedUserName.class).peerAddress(clientAddress));
        }
        if (connMsg.variableHeader().isWillFlag()) {
            if (!MQTTUtf8Util.isWellFormed(connMsg.payload().willTopic(), SANITY_CHECK)) {
                return new GoAway(MqttMessageBuilders
                    .connAck()
                    .properties(new MqttMessageBuilders.ConnAckPropertiesBuilder()
                        .reasonString("Malformed will topic")
                        .build())
                    .returnCode(CONNECTION_REFUSED_TOPIC_NAME_INVALID) // [MQTT-4.13.1-1]
                    .build(), getLocal(MalformedWillTopic.class).peerAddress(clientAddress));
            }
            if (connMsg.payload().willMessageInBytes().length == 0) {
                return new GoAway(MqttMessageBuilders
                    .connAck()
                    .properties(new MqttMessageBuilders.ConnAckPropertiesBuilder()
                        .reasonString("Empty Will payload")
                        .build())
                    .returnCode(CONNECTION_REFUSED_MALFORMED_PACKET) // [MQTT-4.13.1-1]
                    .build(), getLocal(MalformedWillTopic.class).peerAddress(clientAddress));
            }
            if (isUTF8Payload(connMsg.payload().willProperties()) &&
                !MQTTUtf8Util.isValidUTF8Payload(connMsg.payload().willMessageInBytes())) {
                return new GoAway(MqttMessageBuilders
                    .connAck()
                    .properties(new MqttMessageBuilders.ConnAckPropertiesBuilder()
                        .reasonString("payload format is not utf8")
                        .build())
                    .returnCode(CONNECTION_REFUSED_PAYLOAD_FORMAT_INVALID) // [MQTT-4.13.1-1]
                    .build(), getLocal(MalformedWillTopic.class).peerAddress(clientAddress));
            }
        }
        return null;
    }

    @Override
    protected CompletableFuture<OkOrGoAway> authenticate(MqttConnectMessage message) {
        this.connMsg = message;
        Optional<String> authMethodOpt = authMethod(message.variableHeader().properties());
        if (authMethodOpt.isEmpty()) {
            return authProvider.auth(AuthUtil.buildMQTT5AuthData(ctx.channel(), message))
                .thenApply(authResult -> {
                    final InetSocketAddress clientAddress = ChannelAttrs.socketAddress(ctx.channel());
                    switch (authResult.getTypeCase()) {
                        case SUCCESS -> {
                            return new OkOrGoAway(buildClientInfo(clientAddress, authResult.getSuccess()));
                        }
                        case FAILED -> {
                            Failed failed = authResult.getFailed();
                            switch (failed.getCode()) {
                                case NotAuthorized -> {
                                    return new OkOrGoAway(new GoAway(MqttMessageBuilders
                                        .connAck()
                                        .returnCode(CONNECTION_REFUSED_NOT_AUTHORIZED_5)
                                        .build(),
                                        getLocal(NotAuthorizedClient.class).peerAddress(clientAddress)));
                                }
                                case BadPass -> {
                                    return new OkOrGoAway(new GoAway(MqttMessageBuilders
                                        .connAck()
                                        .returnCode(CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD)
                                        .build(),
                                        getLocal(UnauthenticatedClient.class).peerAddress(clientAddress)));
                                }
                                case Banned -> {
                                    return new OkOrGoAway(new GoAway(MqttMessageBuilders
                                        .connAck()
                                        .returnCode(CONNECTION_REFUSED_BANNED)
                                        .build(),
                                        getLocal(UnauthenticatedClient.class).peerAddress(clientAddress)));
                                }
                                // fallthrough
                                default -> {
                                    log.error("Unexpected error from auth provider:{}", failed.getReason());
                                    return new OkOrGoAway(new GoAway(MqttMessageBuilders
                                        .connAck()
                                        .returnCode(CONNECTION_REFUSED_UNSPECIFIED_ERROR)
                                        .build(),
                                        getLocal(AuthError.class).cause(failed.getReason())
                                            .peerAddress(clientAddress)));
                                }
                            }
                        }
                        default -> {
                            log.error("Unexpected auth result: {}", authResult.getTypeCase());
                            return new OkOrGoAway(new GoAway(MqttMessageBuilders
                                .connAck()
                                .returnCode(CONNECTION_REFUSED_UNSPECIFIED_ERROR)
                                .build(),
                                getLocal(AuthError.class).peerAddress(clientAddress).cause("Unknown auth result")));
                        }
                    }
                });
        } else {
            // extended auth
            this.authMethod = authMethodOpt.get();
            this.extendedAuthFuture = new CompletableFuture<>();
            // resume read
            ctx.channel().config().setAutoRead(true);
            ctx.read();
            extendedAuth(AuthUtil.buildMQTT5ExtendedAuthData(ctx.channel(), message));
            return extendedAuthFuture;
        }
    }

    private void extendedAuth(MQTT5ExtendedAuthData authData) {
        this.isAuthing = true;
        authProvider.extendedAuth(authData)
            .thenAcceptAsync(authResult -> {
                this.isAuthing = false;
                final InetSocketAddress clientAddress = ChannelAttrs.socketAddress(ctx.channel());
                switch (authResult.getTypeCase()) {
                    case SUCCESS -> extendedAuthFuture.complete(new OkOrGoAway(buildClientInfo(
                        clientAddress, authResult.getSuccess())));
                    case CONTINUE -> {
                        Continue authContinue = authResult.getContinue();
                        MQTT5MessageBuilders.AuthBuilder authBuilder = MQTT5MessageBuilders
                            .auth(authMethod)
                            .reasonCode(MQTT5AuthReasonCode.Continue)
                            .authData(authContinue.getAuthData())
                            .userProperties(authContinue.getUserProps());
                        if (authContinue.hasReason()) {
                            authBuilder.reasonString(authContinue.getReason());
                        }
                        ctx.channel().writeAndFlush(authBuilder.build());
                    }
                    default -> {
                        Failed failed = authResult.getFailed();
                        switch (failed.getCode()) {
                            case NotAuthorized ->
                                extendedAuthFuture.complete(new OkOrGoAway(new GoAway(MqttMessageBuilders
                                    .connAck()
                                    .returnCode(CONNECTION_REFUSED_NOT_AUTHORIZED_5)
                                    .build(),
                                    getLocal(NotAuthorizedClient.class).peerAddress(clientAddress))));
                            case BadPass -> extendedAuthFuture.complete(new OkOrGoAway(new GoAway(MqttMessageBuilders
                                .connAck()
                                .returnCode(CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD)
                                .build(),
                                getLocal(UnauthenticatedClient.class).peerAddress(clientAddress))));
                            case Banned -> extendedAuthFuture.complete(new OkOrGoAway(new GoAway(MqttMessageBuilders
                                .connAck()
                                .returnCode(CONNECTION_REFUSED_BANNED)
                                .build(),
                                getLocal(UnauthenticatedClient.class).peerAddress(clientAddress))));
                            // fallthrough
                            default -> {
                                log.error("Unexpected error from auth provider:{}", failed.getReason());
                                extendedAuthFuture.complete(new OkOrGoAway(new GoAway(MqttMessageBuilders
                                    .connAck()
                                    .returnCode(CONNECTION_REFUSED_UNSPECIFIED_ERROR)
                                    .build(),
                                    getLocal(AuthError.class).cause(failed.getReason())
                                        .peerAddress(clientAddress))));
                            }
                        }
                    }
                }
            }, ctx.channel().eventLoop());
    }

    private ClientInfo buildClientInfo(InetSocketAddress clientAddress, Success success) {
        ClientInfo.Builder clientInfoBuilder = ClientInfo.newBuilder()
            .setTenantId(success.getTenantId())
            .setType(MQTT_TYPE_VALUE)
            .putAllMetadata(success.getAttrsMap()) // custom attrs
            .putMetadata(MQTT_PROTOCOL_VER_KEY, MQTT_PROTOCOL_VER_5_VALUE)
            .putMetadata(MQTT_USER_ID_KEY, success.getTenantId())
            .putMetadata(MQTT_CLIENT_ID_KEY, connMsg.payload().clientIdentifier())
            .putMetadata(MQTT_CHANNEL_ID_KEY, ctx.channel().id().asLongText())
            .putMetadata(MQTT_CLIENT_ADDRESS_KEY,
                Optional.ofNullable(clientAddress).map(InetSocketAddress::toString).orElse(""));
        if (success.hasResponseInfo()) {
            clientInfoBuilder.putMetadata(MQTT_RESPONSE_INFO, success.getResponseInfo());
        }
        return clientInfoBuilder.build();
    }

    @Override
    protected void handleMqttMessage(MqttMessage message) {
        if (isAuthing) {
            switch (message.fixedHeader().messageType()) {
                case AUTH -> handleGoAway(GoAway.now(getLocal(ProtocolError.class)
                    .statement("Enhanced Auth in progress")
                    .peerAddress(ChannelAttrs.socketAddress(ctx.channel()))));
                case DISCONNECT -> handleGoAway(GoAway.now(getLocal(EnhancedAuthAbortByClient.class)));
                default -> handleGoAway(GoAway.now(getLocal(ProtocolError.class)
                    .statement("Unexpected control packet during enhanced auth: " + message.fixedHeader().messageType())
                    .peerAddress(ChannelAttrs.socketAddress(ctx.channel()))));
            }
        } else {
            switch (message.fixedHeader().messageType()) {
                case AUTH -> {
                    MQTT5AuthReasonCode reasonCode = MQTT5AuthReasonCode.valueOf(
                        ((MqttReasonCodeAndPropertiesVariableHeader) message.variableHeader()).reasonCode());
                    if (reasonCode != MQTT5AuthReasonCode.Continue) {
                        handleGoAway(GoAway.now(getLocal(ProtocolError.class)
                            .statement("Invalid auth reason code: " + reasonCode)
                            .peerAddress(ChannelAttrs.socketAddress(ctx.channel()))));
                        return;
                    }
                    MQTT5ExtendedAuthData authData = AuthUtil.buildMQTT5ExtendedAuthData(message, false);
                    if (!authData.getAuth().getAuthMethod().equals(authMethod)) {
                        handleGoAway(GoAway.now(getLocal(ProtocolError.class)
                            .statement("Invalid auth method: " + authData.getAuth().getAuthMethod())
                            .peerAddress(ChannelAttrs.socketAddress(ctx.channel()))));
                        return;
                    }
                    extendedAuth(authData);
                }
                case DISCONNECT -> handleGoAway(GoAway.now(getLocal(EnhancedAuthAbortByClient.class)));
                default -> handleGoAway(GoAway.now(getLocal(ProtocolError.class)
                    .statement("Unexpected control packet during enhanced auth: " + message.fixedHeader().messageType())
                    .peerAddress(ChannelAttrs.socketAddress(ctx.channel()))));
            }
        }
    }

    @Override
    protected GoAway validate(MqttConnectMessage message, TenantSettings settings, ClientInfo clientInfo) {
        Optional<Integer> topicAliasMaximum = topicAliasMaximum(message.variableHeader().properties());
        if (topicAliasMaximum.orElse(0) > settings.maxTopicAlias) {
            return new GoAway(MqttMessageBuilders
                .connAck()
                .returnCode(CONNECTION_REFUSED_QUOTA_EXCEEDED)
                .properties(new MqttMessageBuilders.ConnAckPropertiesBuilder()
                    .reasonString("Too large TopicAliasMaximum: max=" + settings.maxTopicAlias)
                    .build())
                .build(),
                getLocal(InvalidTopic.class)
                    .topic(message.payload().willTopic())
                    .clientInfo(clientInfo));
        }
        if (message.variableHeader().isWillFlag()) {
            // will topic conforms to tenant spec limit
            if (!TopicUtil.isValidTopic(message.payload().willTopic(),
                settings.maxTopicLevelLength,
                settings.maxTopicLevels,
                settings.maxTopicLength)) {
                return new GoAway(MqttMessageBuilders
                    .connAck()
                    .returnCode(CONNECTION_REFUSED_TOPIC_NAME_INVALID)
                    .properties(new MqttMessageBuilders.ConnAckPropertiesBuilder()
                        .reasonString("Will topic exceeds limits")
                        .build())
                    .build(),
                    getLocal(InvalidTopic.class)
                        .topic(message.payload().willTopic())
                        .clientInfo(clientInfo));
            }
            // if retain enabled?
            if (message.variableHeader().isWillRetain() && !settings.retainEnabled) {
                return new GoAway(MqttMessageBuilders
                    .connAck()
                    .returnCode(CONNECTION_REFUSED_RETAIN_NOT_SUPPORTED)
                    .build(),
                    getLocal(RetainNotSupported.class).clientInfo(clientInfo));
            }
        }
        return null;
    }

    @Override
    protected LWT getWillMessage(MqttConnectMessage message) {
        return toWillMessage(message);
    }

    @Override
    protected boolean isCleanSession(MqttConnectMessage message, TenantSettings settings) {
        return settings.forceTransient || message.variableHeader().isCleanSession();
    }

    @Override
    protected int getSessionExpiryInterval(MqttConnectMessage message, TenantSettings settings) {
        Optional<Integer> requestSEI = Optional.ofNullable(
            (MqttProperties.IntegerProperty) message.variableHeader().properties()
                .getProperty(SESSION_EXPIRY_INTERVAL.value())).map(MqttProperties.MqttProperty::value);
        int assignedSEI = settings.forceTransient ? 0 : requestSEI.orElse(0);
        return Math.min(assignedSEI, settings.maxSEI);
    }

    @Override
    protected GoAway onCleanSessionFailed(ClientInfo clientInfo) {
        return new GoAway(MqttMessageBuilders
            .connAck()
            .returnCode(CONNECTION_REFUSED_UNSPECIFIED_ERROR)
            .build(),
            getLocal(InboxTransientError.class).clientInfo(clientInfo));
    }

    @Override
    protected GoAway onGetSessionFailed(ClientInfo clientInfo) {
        return new GoAway(MqttMessageBuilders
            .connAck()
            .returnCode(CONNECTION_REFUSED_UNSPECIFIED_ERROR)
            .build(),
            getLocal(InboxTransientError.class).clientInfo(clientInfo));
    }

    @Override
    protected ChannelHandler buildTransientSessionHandler(MqttConnectMessage connMsg, TenantSettings settings,
                                                          String userSessionId, int keepAliveSeconds,
                                                          @Nullable LWT willMessage, ClientInfo clientInfo) {
        return MQTT5TransientSessionHandler.builder()
            .connMsg(connMsg)
            .settings(settings)
            .userSessionId(userSessionId)
            .clientInfo(clientInfo)
            .keepAliveTimeSeconds(keepAliveSeconds)
            .willMessage(willMessage)
            .build();
    }

    @Override
    protected ChannelHandler buildPersistentSessionHandler(MqttConnectMessage connMsg,
                                                           TenantSettings settings,
                                                           String userSessionId,
                                                           int keepAliveSeconds,
                                                           int sessionExpiryInterval,
                                                           @Nullable ExistingSession existingSession,
                                                           @Nullable LWT willMessage,
                                                           ClientInfo clientInfo) {
        return MQTT5PersistentSessionHandler.builder()
            .connMsg(connMsg)
            .settings(settings)
            .userSessionId(userSessionId)
            .clientInfo(clientInfo)
            .existingSession(existingSession)
            .keepAliveTimeSeconds(keepAliveSeconds)
            .sessionExpirySeconds(sessionExpiryInterval)
            .willMessage(willMessage)
            .build();
    }

    @Override
    protected MqttConnAckMessage onConnected(MqttConnectMessage connMsg,
                                             TenantSettings settings, String userSessionId,
                                             int keepAliveSeconds,
                                             int sessionExpiryInterval,
                                             boolean sessionExists,
                                             ClientInfo clientInfo) {
        MqttMessageBuilders.ConnAckPropertiesBuilder connPropsBuilder =
            new MqttMessageBuilders.ConnAckPropertiesBuilder();
        if (connMsg.variableHeader().keepAliveTimeSeconds() != keepAliveSeconds) {
            connPropsBuilder.serverKeepAlive(keepAliveSeconds);
        }
        MqttProperties.IntegerProperty intProp = (MqttProperties.IntegerProperty) connMsg.variableHeader().properties()
            .getProperty(SESSION_EXPIRY_INTERVAL.value());
        if (intProp == null || intProp.value() != sessionExpiryInterval) {
            connPropsBuilder.sessionExpiryInterval(sessionExpiryInterval);
        }
        if (!settings.retainEnabled) {
            connPropsBuilder.retainAvailable(false);
        }
        connPropsBuilder.maximumPacketSize(settings.maxPacketSize);
        connPropsBuilder.topicAliasMaximum(settings.maxTopicAlias);
        connPropsBuilder.receiveMaximum(settings.receiveMaximum);
        if (requestResponseInformation(connMsg.variableHeader().properties()) &&
            clientInfo.containsMetadata(MQTT_RESPONSE_INFO)) {
            // include response information only when client requested it
            connPropsBuilder.responseInformation(clientInfo.getMetadataOrDefault(MQTT_RESPONSE_INFO, ""));
        }
        return MqttMessageBuilders
            .connAck()
            .sessionPresent(sessionExists)
            .properties(connPropsBuilder.build())
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
            .build();
    }
}
