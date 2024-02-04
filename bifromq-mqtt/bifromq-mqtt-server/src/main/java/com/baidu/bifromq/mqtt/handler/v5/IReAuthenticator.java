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
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5DisconnectReasonCode;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ProtocolViolation;
import com.baidu.bifromq.type.ClientInfo;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageType;
import java.util.Optional;
import java.util.function.Consumer;

public interface IReAuthenticator {
    static IReAuthenticator create(MqttConnectMessage connMsg,
                                   IAuthProvider authProvider,
                                   ChannelHandlerContext ctx,
                                   ClientInfo clientInfo,
                                   Consumer<ResponseOrGoAway> responder) {
        Optional<String> authMethodOpt = authMethod(connMsg.variableHeader().properties());
        if (authMethodOpt.isPresent()) {
            return new ReAuthenticator(clientInfo, authProvider, authMethodOpt.get(), ctx, responder);
        }
        return msg -> {
            if (msg.fixedHeader().messageType() == MqttMessageType.AUTH) {
                responder.accept(new ResponseOrGoAway(new GoAway(
                    MqttMessageBuilders.disconnect()
                        .reasonCode(MQTT5DisconnectReasonCode.ProtocolError.value())
                        .build(),
                    getLocal(ProtocolViolation.class)
                        .statement("Re-auth not supported")
                        .clientInfo(clientInfo))));
            }
        };
    }

    void onAuth(MqttMessage authMessage);
}
