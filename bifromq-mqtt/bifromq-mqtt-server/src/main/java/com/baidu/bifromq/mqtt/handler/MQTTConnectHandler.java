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

package com.baidu.bifromq.mqtt.handler;

import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION;

import com.baidu.bifromq.mqtt.handler.v3.MQTT3ConnectHandler;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.ChannelError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.ConnectTimeout;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.IdentifierRejected;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.UnacceptedProtocolVer;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ProtocolViolation;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttIdentifierRejectedException;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttUnacceptableProtocolVersionException;
import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MQTTConnectHandler extends MQTTMessageHandler {
    public static final String NAME = "MqttConnectMessageHandler";
    private final long timeoutInSec;
    private InetSocketAddress remoteAddr;
    private ScheduledFuture<?> timeoutCloseTask;

    public MQTTConnectHandler(int timeoutInSec) {
        this.timeoutInSec = timeoutInSec;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        remoteAddr = ChannelAttrs.socketAddress(ctx.channel());
        timeoutCloseTask = ctx.channel().eventLoop().schedule(() ->
            closeConnectionNow(getLocal(ConnectTimeout.class).peerAddress(remoteAddr)), timeoutInSec, TimeUnit.SECONDS);
        ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        cancelIfUndone(timeoutCloseTask);
        ctx.fireChannelInactive();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        assert msg instanceof MqttMessage;
        // stop reading next message and resume reading once finish processing current one
        ctx.channel().config().setAutoRead(false);
        // cancel the scheduled connect timeout task
        timeoutCloseTask.cancel(true);
        MqttMessage message = (MqttMessage) msg;
        if (!message.decoderResult().isSuccess()) {
            // decoded with known protocol violation
            Throwable cause = message.decoderResult().cause();
            if (cause instanceof MqttIdentifierRejectedException) {
                closeConnectionWithSomeDelay(MqttMessageBuilders.connAck()
                        .returnCode(CONNECTION_REFUSED_IDENTIFIER_REJECTED)
                        .build(),
                    getLocal(IdentifierRejected.class)
                        .peerAddress(remoteAddr));
            } else if (cause instanceof MqttUnacceptableProtocolVersionException) {
                closeConnectionWithSomeDelay(MqttMessageBuilders.connAck()
                        .returnCode(CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION)
                        .build(),
                    getLocal(UnacceptedProtocolVer.class)
                        .peerAddress(remoteAddr));
            } else {
                // according to [MQTT-4.8.0-2]
                closeConnectionWithSomeDelay(
                    getLocal(ProtocolViolation.class).statement("MQTT-4.8.0-2")
                );
            }
            log.warn("Failed to decode mqtt connect message: remote={}",
                remoteAddr, message.decoderResult().cause());
            return;
        } else if (!(message instanceof MqttConnectMessage)) {
            // according to [MQTT-3.1.0-1]
            closeConnectionWithSomeDelay(
                getLocal(ProtocolViolation.class)
                    .statement("MQTT-3.1.0-1")
            );
            log.warn("First packet must be mqtt connect message: remote={}", remoteAddr);
            return;
        }

        MqttConnectMessage connectMessage = (MqttConnectMessage) message;
        switch (connectMessage.variableHeader().version()) {
            case 3:
            case 4:
                ctx.pipeline().addAfter(MQTTConnectHandler.NAME, MQTT3ConnectHandler.NAME, new MQTT3ConnectHandler());
                // delegate to MQTT 3 handler
                ctx.fireChannelRead(connectMessage);
                ctx.pipeline().remove(this);
                break;
            case 5:
                log.warn("MQTT5 not unsupported now, stay tune!");
            default:
                // TODO: MQTT5
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        cancelIfUndone(timeoutCloseTask);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // simple strategy: shutdown the channel directly
        log.warn("ctx: {}, cause:", ctx, cause);
        if (ctx.channel().isActive() && closeNotScheduled()) {
            // if disconnection is caused purely by channel error
            closeConnectionNow(getLocal(ChannelError.class)
                .peerAddress(remoteAddr)
                .cause(cause));
        }
    }
}
