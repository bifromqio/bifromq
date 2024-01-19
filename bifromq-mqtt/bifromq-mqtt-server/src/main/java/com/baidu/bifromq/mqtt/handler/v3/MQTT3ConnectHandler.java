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

import static com.baidu.bifromq.mqtt.handler.v3.MQTTSessionIdUtil.userSessionId;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxUserPayloadBytes;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CHANNEL_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ADDRESS_KEY;
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

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.rpc.proto.ExpireReply;
import com.baidu.bifromq.inbox.rpc.proto.ExpireRequest;
import com.baidu.bifromq.inbox.rpc.proto.GetReply;
import com.baidu.bifromq.inbox.rpc.proto.GetRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxVersion;
import com.baidu.bifromq.mqtt.handler.ChannelAttrs;
import com.baidu.bifromq.mqtt.handler.MQTTMessageHandler;
import com.baidu.bifromq.mqtt.handler.TenantSettings;
import com.baidu.bifromq.mqtt.utils.AuthUtil;
import com.baidu.bifromq.mqtt.utils.MQTTUtf8Util;
import com.baidu.bifromq.mqtt.utils.TopicUtil;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthData;
import com.baidu.bifromq.plugin.authprovider.type.Ok;
import com.baidu.bifromq.plugin.authprovider.type.Reject;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.AuthError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.IdentifierRejected;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.MalformedClientIdentifier;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.MalformedUserName;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.MalformedWillTopic;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.NotAuthorizedClient;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.UnauthenticatedClient;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientconnected.ClientConnected;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.InvalidTopic;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.SessionCheckError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.SessionCleanupError;
import com.baidu.bifromq.sysprops.BifroMQSysProp;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import com.google.common.base.Strings;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MQTT3ConnectHandler extends MQTTMessageHandler {
    public static final String NAME = "MQTT3ConnectHandler";
    private static final int MIN_CLIENT_KEEP_ALIVE_DURATION = 5;
    private static final int MAX_CLIENT_KEEP_ALIVE_DURATION = 2 * 60 * 60;
    private static final int MAX_CLIENT_ID_LEN = BifroMQSysProp.MAX_MQTT3_CLIENT_ID_LENGTH.get();
    private static final boolean SANITY_CHECK = BifroMQSysProp.MQTT_UTF8_SANITY_CHECK.get();
    private IInboxClient inboxClient;
    private ClientInfo clientInfo;
    private MQTT3SessionHandler.WillMessage willMessage;

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
        log.trace("Handling MQTT3 conn message:\n{}", msg);
        final InetSocketAddress clientAddress = ChannelAttrs.socketAddress(ctx.channel());
        String clientIdentifier = connMsg.payload().clientIdentifier();
        if (Strings.isNullOrEmpty(clientIdentifier)) {
            if (connMsg.variableHeader().isCleanSession()) {
                clientIdentifier = ctx.channel().id().asLongText();
            } else {
                // If the Client supplies a zero-byte ClientId,
                // the Client MUST also set CleanSession to 1 [MQTT-3.1.3-7]
                closeConnectionWithSomeDelay(MqttMessageBuilders
                        .connAck()
                        .returnCode(CONNECTION_REFUSED_IDENTIFIER_REJECTED)
                        .build(),
                    getLocal(IdentifierRejected.class).peerAddress(clientAddress));
                return;
            }
        }
        if (!MQTTUtf8Util.isWellFormed(clientIdentifier, SANITY_CHECK)) {
            closeConnectionWithSomeDelay(getLocal(MalformedClientIdentifier.class).peerAddress(clientAddress));
            return;
        }
        if (clientIdentifier.length() > MAX_CLIENT_ID_LEN) {
            closeConnectionWithSomeDelay(MqttMessageBuilders
                    .connAck()
                    .returnCode(CONNECTION_REFUSED_IDENTIFIER_REJECTED)
                    .build(),
                getLocal(IdentifierRejected.class).peerAddress(clientAddress));
            return;
        }
        if (connMsg.variableHeader().hasUserName() &&
            !MQTTUtf8Util.isWellFormed(connMsg.payload().userName(), SANITY_CHECK)) {
            closeConnectionWithSomeDelay(getLocal(MalformedUserName.class).peerAddress(clientAddress));
            return;
        }
        if (connMsg.variableHeader().isWillFlag() &&
            !MQTTUtf8Util.isWellFormed(connMsg.payload().willTopic(), SANITY_CHECK)) {
            closeConnectionWithSomeDelay(getLocal(MalformedWillTopic.class).peerAddress(clientAddress));
            return;
        }

        String mqttClientId = clientIdentifier;
        long reqId = System.nanoTime();
        MQTT3AuthData authData = AuthUtil.buildMQTT3AuthData(ctx.channel(), connMsg);
        cancelOnInactive(authProvider.auth(authData))
            // this stage has to be run in eventloop, since there is a race condition between
            // registry.remove in onChannelInActive and registry.replace here
            .thenComposeAsync(authResult -> {
                if (!ctx.channel().isActive()) {
                    return CompletableFuture.completedFuture(null);
                }
                switch (authResult.getTypeCase()) {
                    case REJECT -> {
                        Reject reject = authResult.getReject();
                        switch (reject.getCode()) {
                            case NotAuthorized -> {
                                closeConnectionWithSomeDelay(MqttMessageBuilders
                                        .connAck()
                                        .returnCode(CONNECTION_REFUSED_NOT_AUTHORIZED)
                                        .build(),
                                    getLocal(NotAuthorizedClient.class).peerAddress(clientAddress));
                                return CompletableFuture.completedFuture(null);
                            }
                            case BadPass -> {
                                closeConnectionWithSomeDelay(MqttMessageBuilders
                                        .connAck()
                                        .returnCode(CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD)
                                        .build(),
                                    getLocal(UnauthenticatedClient.class).peerAddress(clientAddress));
                                return CompletableFuture.completedFuture(null);
                            }
                            // fallthrough
                            default -> {
                                log.error("Unexpected error from auth provider:{}", authResult.getReject().getReason());
                                closeConnectionWithSomeDelay(MqttMessageBuilders
                                        .connAck()
                                        .returnCode(CONNECTION_REFUSED_SERVER_UNAVAILABLE)
                                        .build(),
                                    getLocal(AuthError.class).cause(reject.getReason()).peerAddress(clientAddress));
                                return CompletableFuture.completedFuture(null);
                            }
                        }
                    }
                    case OK -> {
                        Ok ok = authResult.getOk();
                        Optional<InetSocketAddress> clientAddr =
                            Optional.ofNullable(ChannelAttrs.socketAddress(ctx.channel()));
                        final TenantSettings settings = new TenantSettings(ok.getTenantId(), settingProvider);
                        clientInfo = ClientInfo.newBuilder()
                            .setTenantId(ok.getTenantId())
                            .setType(MQTT_TYPE_VALUE)
                            .putAllMetadata(ok.getAttrsMap()) // custom attrs
                            .putMetadata(MQTT_PROTOCOL_VER_KEY, connMsg.variableHeader().version() == 3 ?
                                MQTT_PROTOCOL_VER_3_1_VALUE : MQTT_PROTOCOL_VER_3_1_1_VALUE)
                            .putMetadata(MQTT_USER_ID_KEY, ok.getUserId())
                            .putMetadata(MQTT_CLIENT_ID_KEY, mqttClientId)
                            .putMetadata(MQTT_CHANNEL_ID_KEY, ctx.channel().id().asLongText())
                            .putMetadata(MQTT_CLIENT_ADDRESS_KEY,
                                clientAddr.map(InetSocketAddress::toString).orElse(""))
                            .build();
                        String userSessionId = userSessionId(clientInfo);
                        long inBandWidth = Math.max(settings.inboundBandwidth, 0);
                        if (inBandWidth > 0) {
                            ChannelAttrs.trafficShaper(ctx).setReadLimit(inBandWidth);
                        }
                        long outBandWidth = Math.max(settings.outboundBandwidth, 0);
                        if (outBandWidth > 0) {
                            ChannelAttrs.trafficShaper(ctx).setWriteLimit(outBandWidth);
                        }
                        boolean forceTransient = settings.forceTransient;
                        boolean cleanSession = forceTransient || connMsg.variableHeader().isCleanSession();

                        int mupb = settingProvider.provide(MaxUserPayloadBytes, clientInfo.getTenantId());
                        ChannelAttrs.setMaxPayload(mupb, ctx);

                        if (connMsg.variableHeader().isWillFlag()) {
                            if (!TopicUtil.isValidTopic(connMsg.payload().willTopic(),
                                settings.maxTopicLevelLength, settings.maxTopicLevels, settings.maxTopicLength)) {
                                closeConnectionWithSomeDelay(getLocal(InvalidTopic.class)
                                    .topic(connMsg.payload().willTopic())
                                    .clientInfo(clientInfo));
                                return CompletableFuture.completedFuture(null);
                            }
                            // don't do access control check here
                            willMessage = MQTT3SessionHandler.WillMessage
                                .builder()
                                .topic(connMsg.payload().willTopic())
                                .qos(QoS.forNumber(connMsg.variableHeader().willQos()))
                                .retain(connMsg.variableHeader().isWillRetain())
                                .payload(UnsafeByteOperations.unsafeWrap(connMsg.payload().willMessageInBytes()))
                                .build();
                        }
                        int keepAliveSeconds = keepAliveSeconds(connMsg.variableHeader().keepAliveTimeSeconds());

                        if (cleanSession) {
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
                                            .returnCode(CONNECTION_REFUSED_SERVER_UNAVAILABLE)
                                            .build(), getLocal(SessionCleanupError.class).clientInfo(clientInfo));
                                    } else {
                                        establishTransientSession(settings, userSessionId, keepAliveSeconds);
                                    }
                                }, ctx.channel().eventLoop());

                        } else {
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
                                                .returnCode(CONNECTION_REFUSED_SERVER_UNAVAILABLE)
                                                .build(),
                                            getLocal(SessionCheckError.class).clientInfo(clientInfo));
                                    } else if (reply.getCode() == GetReply.Code.EXIST) {
                                        // reuse existing inbox with the highest incarnation, the old ones will be cleaned up eventually
                                        InboxVersion inbox = reply.getInbox(reply.getInboxCount() - 1);
                                        establishPersistentSession(
                                            settings,
                                            userSessionId,
                                            keepAliveSeconds,
                                            new MQTT3PersistentSessionHandler.ExistingSession(
                                                inbox.getIncarnation(),
                                                inbox.getVersion())
                                        );
                                    } else {
                                        establishPersistentSession(settings, userSessionId, keepAliveSeconds, null);
                                    }
                                }, ctx.channel().eventLoop());
                        }
                    }
                    default -> {
                        closeConnectionWithSomeDelay(MqttMessageBuilders
                                .connAck()
                                .returnCode(CONNECTION_REFUSED_SERVER_UNAVAILABLE)
                                .build(),
                            getLocal(AuthError.class).peerAddress(clientAddress).cause("Unknown auth result"));
                        return CompletableFuture.completedFuture(null);
                    }
                }
            }, ctx.channel().eventLoop());
    }


    private void establishTransientSession(TenantSettings settings,
                                           String userSessionId,
                                           int keepAliveSeconds) {
        if (!ctx.channel().isActive()) {
            return;
        }

        // build create session and associated with channel
        MQTT3TransientSessionHandler handler = MQTT3TransientSessionHandler.builder()
            .settings(settings)
            .clientInfo(clientInfo)
            .userSessionId(userSessionId)
            .keepAliveTimeSeconds(keepAliveSeconds)
            .willMessage(willMessage)
            .build();
        // setup transient session handler
        ctx.pipeline().replace(this, MQTT3SessionHandler.NAME, handler);

        ctx.writeAndFlush(MqttMessageBuilders
            .connAck()
            .sessionPresent(false)
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
            .build());

        // report client connected event
        eventCollector.report(getLocal(ClientConnected.class).clientInfo(clientInfo)
            .serverId(sessionCtx.serverId)
            .userSessionId(userSessionId)
            .keepAliveTimeSeconds(keepAliveSeconds)
            .cleanSession(true)
            .sessionPresent(false)
            .lastWill(willMessage != null ? new ClientConnected.WillInfo().topic(willMessage.topic)
                .isRetain(willMessage.retain)
                .qos(willMessage.qos)
                .payload(willMessage.payload.asReadOnlyByteBuffer()) : null));

    }

    private void establishPersistentSession(TenantSettings settings,
                                            String userSessionId,
                                            int keepAliveSeconds,
                                            @Nullable
                                            MQTT3PersistentSessionHandler.ExistingSession existingSession) {
        if (!ctx.channel().isActive()) {
            return;
        }
        MQTT3PersistentSessionHandler handler = MQTT3PersistentSessionHandler.builder()
            .settings(settings)
            .userSessionId(userSessionId)
            .clientInfo(clientInfo)
            .keepAliveSeconds(keepAliveSeconds)
            .sessionExpirySeconds(settings.maxSEI)
            .willMessage(willMessage)
            .existingSession(existingSession)
            .build();
        // setup persistent session handler
        ctx.pipeline().replace(this, MQTT3SessionHandler.NAME, handler);

        ctx.writeAndFlush(MqttMessageBuilders
            .connAck()
            .sessionPresent(existingSession != null)
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
            .build());

        // report client connected event
        eventCollector.report(getLocal(ClientConnected.class).clientInfo(clientInfo)
            .serverId(sessionCtx.serverId)
            .userSessionId(userSessionId)
            .keepAliveTimeSeconds(keepAliveSeconds)
            .cleanSession(false)
            .sessionPresent(existingSession != null)
            .lastWill(willMessage != null ? new ClientConnected.WillInfo().topic(willMessage.topic)
                .isRetain(willMessage.retain)
                .qos(willMessage.qos)
                .payload(willMessage.payload.asReadOnlyByteBuffer()) : null));
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
}
