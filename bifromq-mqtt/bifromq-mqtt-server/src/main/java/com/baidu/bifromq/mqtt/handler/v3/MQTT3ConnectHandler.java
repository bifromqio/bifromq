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

package com.baidu.bifromq.mqtt.handler.v3;

import static com.baidu.bifromq.metrics.TenantMetric.MqttAuthFailureCount;
import static com.baidu.bifromq.mqtt.handler.MQTTConnectHandler.AuthResult.goAway;
import static com.baidu.bifromq.mqtt.handler.MQTTConnectHandler.AuthResult.ok;
import static com.baidu.bifromq.mqtt.handler.condition.ORCondition.or;
import static com.baidu.bifromq.mqtt.utils.AuthUtil.buildConnAction;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CHANNEL_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ADDRESS_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_BROKER_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_3_1_1_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_3_1_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_TYPE_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION;

import com.baidu.bifromq.inbox.storage.proto.LWT;
import com.baidu.bifromq.metrics.ITenantMeter;
import com.baidu.bifromq.mqtt.handler.ChannelAttrs;
import com.baidu.bifromq.mqtt.handler.MQTTConnectHandler;
import com.baidu.bifromq.mqtt.handler.MQTTSessionHandler;
import com.baidu.bifromq.mqtt.handler.TenantSettings;
import com.baidu.bifromq.mqtt.handler.condition.DirectMemPressureCondition;
import com.baidu.bifromq.mqtt.handler.condition.HeapMemPressureCondition;
import com.baidu.bifromq.mqtt.handler.record.GoAway;
import com.baidu.bifromq.mqtt.utils.AuthUtil;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthData;
import com.baidu.bifromq.plugin.authprovider.type.Ok;
import com.baidu.bifromq.plugin.authprovider.type.Reject;
import com.baidu.bifromq.plugin.clientbalancer.IClientBalancer;
import com.baidu.bifromq.plugin.clientbalancer.Redirection;
import com.baidu.bifromq.plugin.eventcollector.OutOfTenantResource;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.AuthError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.IdentifierRejected;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.MalformedClientIdentifier;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.MalformedUserName;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.MalformedWillTopic;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.NotAuthorizedClient;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.UnauthenticatedClient;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.InboxTransientError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.InvalidTopic;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ProtocolViolation;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.Redirect;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ResourceThrottled;
import com.baidu.bifromq.sysprops.props.MaxMqtt3ClientIdLength;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.UserProperties;
import com.baidu.bifromq.util.TopicUtil;
import com.baidu.bifromq.util.UTF8Util;
import com.bifromq.plugin.resourcethrottler.TenantResourceType;
import com.google.common.base.Strings;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MQTT3ConnectHandler extends MQTTConnectHandler {
    public static final String NAME = "MQTT3ConnectHandler";
    private static final int MAX_CLIENT_ID_LEN = MaxMqtt3ClientIdLength.INSTANCE.get();
    private ChannelHandlerContext ctx;
    private IClientBalancer clientBalancer;
    private IAuthProvider authProvider;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        super.handlerAdded(ctx);
        this.ctx = ctx;
        authProvider = ChannelAttrs.mqttSessionContext(ctx).authProvider(ctx);
        clientBalancer = ChannelAttrs.mqttSessionContext(ctx).clientBalancer;
    }

    @Override
    protected GoAway sanityCheck(MqttConnectMessage message) {
        final InetSocketAddress clientAddress = ChannelAttrs.socketAddress(ctx.channel());
        String requestClientId = message.payload().clientIdentifier();
        if (Strings.isNullOrEmpty(requestClientId)) {
            if (!message.variableHeader().isCleanSession()) {
                // If the Client supplies a zero-byte ClientId,
                // the Client MUST also set CleanSession to 1 [MQTT-3.1.3-7]
                return new GoAway(MqttMessageBuilders
                    .connAck()
                    .returnCode(CONNECTION_REFUSED_IDENTIFIER_REJECTED)
                    .build(),
                    getLocal(IdentifierRejected.class).peerAddress(clientAddress));
            }
        }
        if (!UTF8Util.isWellFormed(requestClientId, SANITY_CHECK)) {
            return new GoAway(getLocal(MalformedClientIdentifier.class).peerAddress(clientAddress));
        }
        if (requestClientId.length() > MAX_CLIENT_ID_LEN) {
            return new GoAway(MqttMessageBuilders
                .connAck()
                .returnCode(CONNECTION_REFUSED_IDENTIFIER_REJECTED)
                .build(),
                getLocal(IdentifierRejected.class).peerAddress(clientAddress));
        }
        if (message.variableHeader().hasUserName()
            && !UTF8Util.isWellFormed(message.payload().userName(), SANITY_CHECK)) {
            return new GoAway(getLocal(MalformedUserName.class).peerAddress(clientAddress));
        }
        if (message.variableHeader().isWillFlag()
            && !UTF8Util.isWellFormed(message.payload().willTopic(), SANITY_CHECK)) {
            return new GoAway(getLocal(MalformedWillTopic.class).peerAddress(clientAddress));
        }
        return null;
    }

    @Override
    protected CompletableFuture<AuthResult> authenticate(MqttConnectMessage message) {
        MQTT3AuthData authData = AuthUtil.buildMQTT3AuthData(ctx.channel(), message);
        return authProvider.auth(authData)
            .thenApplyAsync(authResult -> {
                final InetSocketAddress clientAddress = ChannelAttrs.socketAddress(ctx.channel());
                switch (authResult.getTypeCase()) {
                    case OK -> {
                        Ok ok = authResult.getOk();
                        String requestClientId = message.payload().clientIdentifier();
                        if (requestClientId.isEmpty()) {
                            requestClientId = ctx.channel().id().asLongText();
                        }
                        return ok(ClientInfo.newBuilder()
                            .setTenantId(ok.getTenantId())
                            .setType(MQTT_TYPE_VALUE)
                            .putAllMetadata(ok.getAttrsMap()) // custom attrs
                            .putMetadata(MQTT_PROTOCOL_VER_KEY, message.variableHeader().version() == 3
                                ? MQTT_PROTOCOL_VER_3_1_VALUE : MQTT_PROTOCOL_VER_3_1_1_VALUE)
                            .putMetadata(MQTT_USER_ID_KEY, ok.getUserId())
                            .putMetadata(MQTT_CLIENT_ID_KEY, requestClientId)
                            .putMetadata(MQTT_CHANNEL_ID_KEY, ctx.channel().id().asLongText())
                            .putMetadata(MQTT_CLIENT_ADDRESS_KEY,
                                Optional.ofNullable(clientAddress)
                                    .map(InetSocketAddress::toString)
                                    .orElse(""))
                            .putMetadata(MQTT_CLIENT_BROKER_KEY, ChannelAttrs.mqttSessionContext(ctx).serverId)
                            .build());
                    }
                    case REJECT -> {
                        Reject reject = authResult.getReject();
                        if (reject.hasTenantId()) {
                            ITenantMeter.get(reject.getTenantId()).recordCount(MqttAuthFailureCount);
                        }
                        switch (reject.getCode()) {
                            case NotAuthorized -> {
                                return goAway(MqttMessageBuilders
                                        .connAck()
                                        .returnCode(CONNECTION_REFUSED_NOT_AUTHORIZED)
                                        .build(),
                                    getLocal(NotAuthorizedClient.class)
                                        .tenantId(reject.getTenantId())
                                        .userId(reject.getUserId())
                                        .clientId(authData.getClientId())
                                        .peerAddress(clientAddress));
                            }
                            case BadPass -> {
                                return goAway(MqttMessageBuilders
                                        .connAck()
                                        .returnCode(CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD)
                                        .build(),
                                    getLocal(UnauthenticatedClient.class)
                                        .tenantId(reject.getTenantId())
                                        .userId(reject.getUserId())
                                        .clientId(authData.getClientId())
                                        .peerAddress(clientAddress));
                            }
                            // fallthrough
                            default -> {
                                log.error("Unexpected error from auth provider:{}",
                                    authResult.getReject().getReason());
                                return goAway(MqttMessageBuilders
                                        .connAck()
                                        .returnCode(CONNECTION_REFUSED_SERVER_UNAVAILABLE)
                                        .build(),
                                    getLocal(AuthError.class).cause(reject.getReason())
                                        .peerAddress(clientAddress));
                            }
                        }
                    }
                    default -> {
                        return goAway(MqttMessageBuilders
                                .connAck()
                                .returnCode(CONNECTION_REFUSED_SERVER_UNAVAILABLE)
                                .build(),
                            getLocal(AuthError.class).peerAddress(clientAddress).cause("Unknown auth result"));
                    }
                }
            }, ctx.executor());
    }

    @Override
    protected CompletableFuture<AuthResult> checkConnectPermission(MqttConnectMessage message, ClientInfo clientInfo) {
        return authProvider.checkPermission(clientInfo, buildConnAction(UserProperties.getDefaultInstance()))
            .thenApply(checkResult -> {
                switch (checkResult.getTypeCase()) {
                    case GRANTED -> {
                        return AuthResult.ok(clientInfo);
                    }
                    case DENIED -> {
                        return goAway(MqttMessageBuilders
                                .connAck()
                                .returnCode(CONNECTION_REFUSED_NOT_AUTHORIZED)
                                .build(),
                            getLocal(NotAuthorizedClient.class)
                                .tenantId(clientInfo.getTenantId())
                                .userId(clientInfo.getMetadataOrDefault(MQTT_USER_ID_KEY, ""))
                                .clientId(clientInfo.getMetadataOrDefault(MQTT_CLIENT_ID_KEY, ""))
                                .peerAddress(ChannelAttrs.socketAddress(ctx.channel())));
                    }
                    default -> {
                        return goAway(MqttMessageBuilders
                                .connAck()
                                .returnCode(CONNECTION_REFUSED_SERVER_UNAVAILABLE)
                                .build(),
                            getLocal(AuthError.class)
                                .cause("Failed to check connect permission")
                                .peerAddress(ChannelAttrs.socketAddress(ctx.channel())));
                    }
                }
            });
    }

    @Override
    protected void handleMqttMessage(MqttMessage message) {
        // never happen in MQTT3
    }

    @Override
    protected GoAway onNoEnoughResources(MqttConnectMessage message, TenantResourceType resourceType,
                                         ClientInfo clientInfo) {
        return new GoAway(MqttMessageBuilders
            .connAck()
            .returnCode(CONNECTION_REFUSED_SERVER_UNAVAILABLE)
            .build(),
            getLocal(OutOfTenantResource.class)
                .reason(resourceType.name())
                .clientInfo(clientInfo),
            getLocal(ResourceThrottled.class)
                .reason(resourceType.name())
                .clientInfo(clientInfo));
    }

    @Override
    protected GoAway validate(MqttConnectMessage message, TenantSettings settings, ClientInfo clientInfo) {
        if (message.variableHeader().version() == 3 && !settings.mqtt3Enabled) {
            return new GoAway(MqttMessageBuilders.connAck()
                .returnCode(CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION)
                .build(),
                getLocal(ProtocolViolation.class)
                    .statement("MQTT3.1 not enabled")
                    .clientInfo(clientInfo));
        }
        if (message.variableHeader().version() == 4 && !settings.mqtt4Enabled) {
            return new GoAway(MqttMessageBuilders.connAck()
                .returnCode(CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION)
                .build(),
                getLocal(ProtocolViolation.class)
                    .statement("MQTT3.1.1 not enabled")
                    .clientInfo(clientInfo));
        }
        if (message.variableHeader().isWillFlag()) {
            if (!TopicUtil.isValidTopic(message.payload().willTopic(),
                settings.maxTopicLevelLength,
                settings.maxTopicLevels,
                settings.maxTopicLength)) {
                return new GoAway(getLocal(InvalidTopic.class)
                    .topic(message.payload().willTopic())
                    .clientInfo(clientInfo));
            }
            // if retain enabled?
            if (message.variableHeader().isWillRetain() && !settings.retainEnabled) {
                return new GoAway(getLocal(ProtocolViolation.class)
                    .statement("Retain not supported")
                    .clientInfo(clientInfo));
            }
            if (message.variableHeader().willQos() > settings.maxQoS.getNumber()) {
                return new GoAway(getLocal(ProtocolViolation.class)
                    .statement("Will QoS not supported")
                    .clientInfo(clientInfo));
            }
        }
        return null;
    }

    @Override
    protected GoAway needRedirect(ClientInfo clientInfo) {
        Optional<Redirection> redirection = clientBalancer.needRedirect(clientInfo);
        return redirection.map(value -> new GoAway(MqttMessageBuilders
            .connAck()
            .returnCode(CONNECTION_REFUSED_SERVER_UNAVAILABLE)
            .build(),
            getLocal(Redirect.class)
                .isPermanent(value.permanentMove())
                .serverReference(value.serverReference().orElse(null))
                .clientInfo(clientInfo))).orElse(null);
    }

    @Override
    protected LWT getWillMessage(MqttConnectMessage message) {
        return LWT.newBuilder()
            .setTopic(message.payload().willTopic())
            .setMessage(Message.newBuilder()
                .setPubQoS(QoS.forNumber(message.variableHeader().willQos()))
                .setPayload(UnsafeByteOperations.unsafeWrap(message.payload().willMessageInBytes()))
                .setIsRetain(message.variableHeader().isWillRetain())
                .setExpiryInterval(Integer.MAX_VALUE)
                .setIsRetain(message.variableHeader().isWillRetain())
                .build())
            .build();
    }

    @Override
    protected boolean isCleanSession(MqttConnectMessage message, TenantSettings settings) {
        return settings.forceTransient || message.variableHeader().isCleanSession();
    }

    @Override
    protected int getSessionExpiryInterval(MqttConnectMessage message, TenantSettings settings) {
        return isCleanSession(message, settings) ? 0 : settings.maxSEI;
    }

    @Override
    protected GoAway onCleanSessionFailed(ClientInfo clientInfo) {
        return new GoAway(MqttMessageBuilders
            .connAck()
            .returnCode(CONNECTION_REFUSED_SERVER_UNAVAILABLE)
            .build(), getLocal(InboxTransientError.class).clientInfo(clientInfo));
    }

    @Override
    protected GoAway onGetSessionFailed(ClientInfo clientInfo) {
        return new GoAway(MqttMessageBuilders
            .connAck()
            .returnCode(CONNECTION_REFUSED_SERVER_UNAVAILABLE)
            .build(),
            getLocal(InboxTransientError.class).clientInfo(clientInfo));
    }

    @Override
    protected final MQTTSessionHandler buildTransientSessionHandler(MqttConnectMessage connMsg,
                                                                    TenantSettings settings,
                                                                    ITenantMeter tenantMeter,
                                                                    String userSessionId,
                                                                    int keepAliveSeconds,
                                                                    @Nullable LWT willMessage,
                                                                    ClientInfo clientInfo,
                                                                    ChannelHandlerContext ctx) {
        return MQTT3TransientSessionHandler.builder()
            .settings(settings)
            .tenantMeter(tenantMeter)
            .oomCondition(or(DirectMemPressureCondition.INSTANCE, HeapMemPressureCondition.INSTANCE))
            .userSessionId(userSessionId)
            .keepAliveTimeSeconds(keepAliveSeconds)
            .clientInfo(clientInfo)
            .willMessage(willMessage)
            .ctx(ctx)
            .build();
    }

    @Override
    protected final MQTTSessionHandler buildPersistentSessionHandler(MqttConnectMessage connMsg,
                                                                     TenantSettings settings,
                                                                     ITenantMeter tenantMeter,
                                                                     String userSessionId,
                                                                     int keepAliveSeconds,
                                                                     int sessionExpiryInterval,
                                                                     @Nullable ExistingSession existingSession,
                                                                     @Nullable LWT willMessage,
                                                                     ClientInfo clientInfo,
                                                                     ChannelHandlerContext ctx) {
        return MQTT3PersistentSessionHandler.builder()
            .settings(settings)
            .tenantMeter(tenantMeter)
            .oomCondition(or(DirectMemPressureCondition.INSTANCE, HeapMemPressureCondition.INSTANCE))
            .userSessionId(userSessionId)
            .keepAliveTimeSeconds(keepAliveSeconds)
            .sessionExpirySeconds(settings.maxSEI)
            .clientInfo(clientInfo)
            .existingSession(existingSession)
            .willMessage(willMessage)
            .ctx(ctx)
            .build();
    }

    @Override
    protected MqttConnAckMessage onConnected(MqttConnectMessage connMsg,
                                             TenantSettings settings,
                                             String userSessionId,
                                             int keepAliveSeconds,
                                             int sessionExpiryInterval,
                                             boolean sessionExists,
                                             ClientInfo clientInfo) {
        return MqttMessageBuilders
            .connAck()
            .sessionPresent(sessionExists)
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
            .build();
    }

    @Override
    protected int maxPacketSize(MqttConnectMessage connMsg, TenantSettings settings) {
        return settings.maxPacketSize;
    }
}
