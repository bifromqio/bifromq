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

import static com.baidu.bifromq.mqtt.handler.v3.MQTTSessionIdUtil.userSessionId;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.plugin.settingprovider.Setting.ForceTransient;
import static com.baidu.bifromq.plugin.settingprovider.Setting.InBoundBandWidth;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicLength;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicLevelLength;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicLevels;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxUserPayloadBytes;
import static com.baidu.bifromq.plugin.settingprovider.Setting.OutBoundBandWidth;
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

import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.client.InboxCheckResult;
import com.baidu.bifromq.inbox.rpc.proto.DeleteInboxReply;
import com.baidu.bifromq.mqtt.handler.ChannelAttrs;
import com.baidu.bifromq.mqtt.handler.MQTTMessageHandler;
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
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.InvalidTopic;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.SessionCheckError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.SessionCleanupError;
import com.baidu.bifromq.sysprops.BifroMQSysProp;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import com.google.common.base.Strings;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MQTT3ConnectHandler extends MQTTMessageHandler {
    public static final String NAME = "MQTT3ConnectHandler";
    private static final CompletableFuture<Void> DONE = CompletableFuture.completedFuture(null);
    private static final int MIN_CLIENT_KEEP_ALIVE_DURATION = 5;
    private static final int MAX_CLIENT_KEEP_ALIVE_DURATION = 2 * 60 * 60;
    private static final int MAX_CLIENT_ID_LEN = BifroMQSysProp.MAX_CLIENT_ID_LENGTH.get();
    private static final boolean SANITY_CHECK = BifroMQSysProp.MQTT_UTF8_SANITY_CHECK.get();
    private IDistClient distClient;
    private IInboxClient inboxClient;
    private ClientInfo clientInfo;
    private boolean isTransient;
    private MQTT3SessionHandler.WillMessage willMessage;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        super.handlerAdded(ctx);
        distClient = sessionCtx.distClient;
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
        log.trace("Handling mqtt conn message:\n{}", msg);
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
                    return DONE;
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
                                return DONE;
                            }
                            case BadPass -> {
                                closeConnectionWithSomeDelay(MqttMessageBuilders
                                        .connAck()
                                        .returnCode(CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD)
                                        .build(),
                                    getLocal(UnauthenticatedClient.class).peerAddress(clientAddress));
                                return DONE;
                            }
                            // fallthrough
                            default -> {
                                log.error("Unexpected error from auth provider:{}", authResult.getReject().getReason());
                                closeConnectionWithSomeDelay(MqttMessageBuilders
                                        .connAck()
                                        .returnCode(CONNECTION_REFUSED_SERVER_UNAVAILABLE)
                                        .build(),
                                    getLocal(AuthError.class).cause(reject.getReason()).peerAddress(clientAddress));
                                return DONE;
                            }
                        }
                    }
                    case OK -> {
                        Ok ok = authResult.getOk();
                        Optional<InetSocketAddress> clientAddr =
                            Optional.ofNullable(ChannelAttrs.socketAddress(ctx.channel()));
                        clientInfo = ClientInfo.newBuilder()
                            .setTenantId(ok.getTenantId())
                            .setType(MQTT_TYPE_VALUE)
                            .putMetadata(MQTT_PROTOCOL_VER_KEY, connMsg.variableHeader().version() == 3 ?
                                MQTT_PROTOCOL_VER_3_1_VALUE : MQTT_PROTOCOL_VER_3_1_1_VALUE)
                            .putMetadata(MQTT_USER_ID_KEY, ok.getUserId())
                            .putMetadata(MQTT_CLIENT_ID_KEY, mqttClientId)
                            .putMetadata(MQTT_CHANNEL_ID_KEY, ctx.channel().id().asLongText())
                            .putMetadata(MQTT_CLIENT_ADDRESS_KEY,
                                clientAddr.map(InetSocketAddress::toString).orElse(""))
                            .build();
                        Long ibbw = settingProvider.provide(InBoundBandWidth, clientInfo.getTenantId());
                        long inBandWidth = Math.max(ibbw, 0);
                        if (inBandWidth > 0) {
                            ChannelAttrs.trafficShaper(ctx).setReadLimit(inBandWidth);
                        }
                        Long obbw = settingProvider.provide(OutBoundBandWidth, clientInfo.getTenantId());

                        long outBandWidth = Math.max(obbw, 0);
                        if (outBandWidth > 0) {
                            ChannelAttrs.trafficShaper(ctx).setWriteLimit(outBandWidth);
                        }
                        boolean forceTransient = settingProvider.provide(ForceTransient, clientInfo.getTenantId());
                        isTransient = forceTransient || connMsg.variableHeader().isCleanSession();

                        int mupb = settingProvider.provide(MaxUserPayloadBytes, clientInfo.getTenantId());
                        ChannelAttrs.setMaxPayload(mupb, ctx);

                        if (connMsg.variableHeader().isWillFlag()) {
                            if (!TopicUtil.isValidTopic(connMsg.payload().willTopic(),
                                settingProvider.provide(MaxTopicLevelLength, clientInfo.getTenantId()),
                                settingProvider.provide(MaxTopicLevels, clientInfo.getTenantId()),
                                settingProvider.provide(MaxTopicLength, clientInfo.getTenantId()))) {
                                closeConnectionWithSomeDelay(getLocal(InvalidTopic.class)
                                    .topic(connMsg.payload().willTopic())
                                    .clientInfo(clientInfo));
                                return DONE;
                            }
                            // don't do access control check here
                            willMessage = MQTT3SessionHandler.WillMessage
                                .builder()
                                .topic(connMsg.payload().willTopic())
                                .qos(QoS.forNumber(connMsg.variableHeader().willQos()))
                                .retain(connMsg.variableHeader().isWillRetain())
                                .payload(Unpooled.wrappedBuffer(connMsg.payload().willMessageInBytes()))
                                .build();
                        }

                        if (isTransient) {
                            return establishTransientSession(reqId, connMsg);
                        } else {
                            return establishPersistentSession(reqId, connMsg);
                        }
                    }
                    default -> {
                        closeConnectionWithSomeDelay(MqttMessageBuilders
                                .connAck()
                                .returnCode(CONNECTION_REFUSED_SERVER_UNAVAILABLE)
                                .build(),
                            getLocal(AuthError.class).peerAddress(clientAddress).cause("Unknown auth result"));
                        return DONE;
                    }
                }
            }, ctx.channel().eventLoop());
    }

    /**
     * if current connection is transient, the existing connection needs to be terminated with all session state
     * (existing subs) purged case 1: existing connection is transient -  on the same host, clean session state
     * synchronously and disconnect -  on the other host, messaging the broker cluster the kicking operation will be
     * done asynchronously case 2: existing connection is non-transient -  on the same host, clean session state
     * synchronously and disconnect -  on the other host, disconnect and clean session state asynchronously via
     * messaging delete probable exist prev offline routing state
     */
    private CompletableFuture<Void> establishTransientSession(long reqId, MqttConnectMessage mqttConnectMessage) {
        String offlineInboxId = userSessionId(clientInfo);
        return cancelOnInactive(inboxClient.has(reqId, offlineInboxId, clientInfo))
            .exceptionallyComposeAsync(e -> {
                closeConnectionWithSomeDelay(MqttMessageBuilders
                        .connAck()
                        .returnCode(CONNECTION_REFUSED_SERVER_UNAVAILABLE)
                        .build(),
                    getLocal(SessionCheckError.class).clientInfo(clientInfo));
                return null;
            }, ctx.channel().eventLoop())
            .thenComposeAsync(checkResult -> {
                if (checkResult == InboxCheckResult.EXIST) {
                    return cancelOnInactive(inboxClient.delete(reqId, offlineInboxId, clientInfo))
                        .handleAsync((v, e) -> {
                            if (e != null || v.getResult() == DeleteInboxReply.Result.ERROR) {
                                closeConnectionWithSomeDelay(MqttMessageBuilders
                                        .connAck()
                                        .returnCode(CONNECTION_REFUSED_SERVER_UNAVAILABLE)
                                        .build(),
                                    getLocal(SessionCleanupError.class).clientInfo(clientInfo));
                            } else {
                                establishSucceed(mqttConnectMessage, false, true);
                            }
                            return null;
                        }, ctx.channel().eventLoop());
                } else {
                    establishSucceed(mqttConnectMessage, false, true);
                    return DONE;
                }
            }, ctx.channel().eventLoop());
    }

    /**
     * if current connection is non-transient, the existing connection needs to be terminated and if its session
     * state(existing subs) will be purged depends: case 1: existing connection is transient -  on the same host, clean
     * session state synchronously and disconnect -  on the other host, disconnect and clean session state
     * asynchronously via messaging case 2: existing connection is non-transient -  on the same host, disconnect without
     * touching session state synchronously -  on the other host, disconnect without touching session state
     * asynchronously via messaging
     */
    private CompletableFuture<Void> establishPersistentSession(long reqId, MqttConnectMessage mqttConnectMessage) {
        return cancelOnInactive(inboxClient.has(reqId, userSessionId(clientInfo), clientInfo))
            .handleAsync((checkResult, e) -> {
                if (e != null) {
                    closeConnectionWithSomeDelay(MqttMessageBuilders
                            .connAck()
                            .returnCode(CONNECTION_REFUSED_SERVER_UNAVAILABLE)
                            .build(),
                        getLocal(SessionCheckError.class).clientInfo(clientInfo));
                    return null;
                }
                if (checkResult == InboxCheckResult.EXIST) {
                    establishSucceed(mqttConnectMessage, true, false);
                } else {
                    establishSucceed(mqttConnectMessage, false, false);
                }
                return null;
            }, ctx.channel().eventLoop());
    }

    // the method must be executed in eventloop
    private void establishSucceed(MqttConnectMessage connMsg, boolean sessionPresent, boolean isTransient) {
        // if channel is still active
        if (!ctx.channel().isActive()) {
            return;
        }

        int keepAliveTimeSeconds = connMsg.variableHeader().keepAliveTimeSeconds();
        if (keepAliveTimeSeconds == 0) {
            // 20 mins the default keep alive duration
            keepAliveTimeSeconds = sessionCtx.defaultKeepAliveTimeSeconds;
        }
        keepAliveTimeSeconds = Math.max(MIN_CLIENT_KEEP_ALIVE_DURATION, keepAliveTimeSeconds);
        keepAliveTimeSeconds = Math.min(keepAliveTimeSeconds, MAX_CLIENT_KEEP_ALIVE_DURATION);

        if (isTransient) {
            // build create session and associated with channel
            MQTT3TransientSessionHandler handler = MQTT3TransientSessionHandler.builder()
                .clientInfo(clientInfo)
                .keepAliveTimeSeconds(keepAliveTimeSeconds)
                .willMessage(willMessage)
                .build();
            // setup transient session handler
            ctx.pipeline().replace(this, MQTT3SessionHandler.NAME, handler);
        } else {
            MQTT3PersistentSessionHandler handler = MQTT3PersistentSessionHandler.builder()
                .clientInfo(clientInfo)
                .keepAliveTimeSeconds(keepAliveTimeSeconds)
                .willMessage(willMessage)
                .sessionPresent(sessionPresent)
                .build();
            // setup persistent session handler
            ctx.pipeline().replace(this, MQTT3SessionHandler.NAME, handler);
        }
        ctx.writeAndFlush(MqttMessageBuilders
            .connAck()
            .sessionPresent(sessionPresent)
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
            .build());
    }
}
