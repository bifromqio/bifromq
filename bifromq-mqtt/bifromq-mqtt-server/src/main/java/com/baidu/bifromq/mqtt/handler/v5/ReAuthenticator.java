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

import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.authMethod;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;

import com.baidu.bifromq.mqtt.handler.record.GoAway;
import com.baidu.bifromq.mqtt.handler.record.ResponseOrGoAway;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5AuthReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5DisconnectReasonCode;
import com.baidu.bifromq.mqtt.utils.AuthUtil;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.authprovider.type.Continue;
import com.baidu.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthData;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ProtocolViolation;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ReAuthFailed;
import com.baidu.bifromq.type.ClientInfo;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import java.util.Optional;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReAuthenticator implements IReAuthenticator {
    private final ClientInfo clientInfo;
    private final IAuthProvider authProvider;
    private final String origAuthMethod;
    private final ChannelHandlerContext ctx;
    private final Consumer<ResponseOrGoAway> responder;
    private boolean isStarting;
    private boolean isAuthing;

    public ReAuthenticator(ClientInfo clientInfo,
                           IAuthProvider authProvider,
                           String origAuthMethod,
                           ChannelHandlerContext ctx,
                           Consumer<ResponseOrGoAway> responder) {
        this.clientInfo = clientInfo;
        this.authProvider = authProvider;
        this.origAuthMethod = origAuthMethod;
        this.ctx = ctx;
        this.responder = responder;
    }

    public void onAuth(MqttMessage authMessage) {
        MqttReasonCodeAndPropertiesVariableHeader variableHeader =
            ((MqttReasonCodeAndPropertiesVariableHeader) authMessage.variableHeader());
        MQTT5AuthReasonCode reasonCode = MQTT5AuthReasonCode.valueOf(variableHeader.reasonCode());
        switch (reasonCode) {
            case Continue -> {
                if (!isStarting) {
                    responder.accept(new ResponseOrGoAway(new GoAway(
                        MqttMessageBuilders.disconnect()
                            .reasonCode(MQTT5DisconnectReasonCode.ProtocolError.value())
                            .build(),
                        getLocal(ProtocolViolation.class)
                            .statement("Re-auth not started")
                            .clientInfo(clientInfo))));
                    return;
                }
                if (isAuthing) {
                    responder.accept(new ResponseOrGoAway(new GoAway(
                        MqttMessageBuilders.disconnect()
                            .reasonCode(MQTT5DisconnectReasonCode.ProtocolError.value())
                            .build(),
                        getLocal(ProtocolViolation.class)
                            .statement("Unexpected auth packet during re-auth")
                            .clientInfo(clientInfo))));
                    return;
                }
                Optional<String> authMethodOpt = authMethod(variableHeader.properties());
                if (authMethodOpt.isEmpty() || !authMethodOpt.get().equals(origAuthMethod)) {
                    responder.accept(new ResponseOrGoAway(new GoAway(
                        MqttMessageBuilders.disconnect()
                            .reasonCode(MQTT5DisconnectReasonCode.ProtocolError.value())
                            .build(),
                        getLocal(ProtocolViolation.class)
                            .statement("Invalid auth method: " + authMethodOpt.orElse(""))
                            .clientInfo(clientInfo))));
                    return;
                }
                authenticate(AuthUtil.buildMQTT5ExtendedAuthData(authMessage, false));
            }
            case ReAuth -> {
                if (isStarting) {
                    responder.accept(new ResponseOrGoAway(new GoAway(
                        MqttMessageBuilders.disconnect()
                            .reasonCode(MQTT5DisconnectReasonCode.ProtocolError.value())
                            .build(),
                        getLocal(ProtocolViolation.class)
                            .statement("Re-auth in-progress")
                            .clientInfo(clientInfo))));
                    return;
                }
                if (isAuthing) {
                    responder.accept(new ResponseOrGoAway(new GoAway(
                        MqttMessageBuilders.disconnect()
                            .reasonCode(MQTT5DisconnectReasonCode.ProtocolError.value())
                            .build(),
                        getLocal(ProtocolViolation.class)
                            .statement("Re-auth in-progress")
                            .clientInfo(clientInfo))));
                    return;
                }
                Optional<String> authMethodOpt = authMethod(variableHeader.properties());
                if (authMethodOpt.isEmpty() || !authMethodOpt.get().equals(origAuthMethod)) {
                    responder.accept(new ResponseOrGoAway(new GoAway(
                        MqttMessageBuilders.disconnect()
                            .reasonCode(MQTT5DisconnectReasonCode.ProtocolError.value())
                            .build(),
                        getLocal(ProtocolViolation.class)
                            .statement("Invalid auth method: " + authMethodOpt.orElse(""))
                            .clientInfo(clientInfo))));
                    return;
                }
                isStarting = true;
                authenticate(AuthUtil.buildMQTT5ExtendedAuthData(authMessage, true));
            }
            default -> responder.accept(new ResponseOrGoAway(new GoAway(
                MqttMessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.ProtocolError.value())
                    .build(),
                getLocal(ProtocolViolation.class)
                    .statement("Invalid reason code: " + reasonCode.value())
                    .clientInfo(clientInfo))));
        }
    }

    private void authenticate(MQTT5ExtendedAuthData authData) {
        isAuthing = true;
        authProvider.extendedAuth(authData)
            .thenAcceptAsync(authResult -> {
                isAuthing = false;
                switch (authResult.getTypeCase()) {
                    case SUCCESS -> {
                        isStarting = false;
                        responder.accept(new ResponseOrGoAway(MQTT5MessageBuilders.auth(origAuthMethod)
                            .reasonCode(MQTT5AuthReasonCode.Success)
                            .build()));
                    }
                    case CONTINUE -> {
                        Continue authContinue = authResult.getContinue();
                        MQTT5MessageBuilders.AuthBuilder authBuilder = MQTT5MessageBuilders
                            .auth(origAuthMethod)
                            .reasonCode(MQTT5AuthReasonCode.Continue)
                            .authData(authContinue.getAuthData())
                            .userProperties(authContinue.getUserProps());
                        if (authContinue.hasReason()) {
                            authBuilder.reasonString(authContinue.getReason());
                        }
                        responder.accept(new ResponseOrGoAway(authBuilder.build()));
                    }
                    default -> responder.accept(new ResponseOrGoAway(GoAway.now(MqttMessageBuilders.disconnect()
                            .reasonCode(MQTT5DisconnectReasonCode.NotAuthorized.value())
                            .build(),
                        getLocal(ReAuthFailed.class)
                            .clientInfo(clientInfo)
                    )));
                }
            }, ctx.channel().eventLoop());
    }
}
