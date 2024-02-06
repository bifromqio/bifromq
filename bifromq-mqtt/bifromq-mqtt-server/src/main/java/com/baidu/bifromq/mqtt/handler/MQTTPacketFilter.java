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

import static com.baidu.bifromq.metrics.TenantMetric.MqttEgressBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttIngressBytes;
import static com.baidu.bifromq.mqtt.utils.MQTTMessageTrimmer.trim;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_5_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_PROTOCOL_VER_KEY;

import com.baidu.bifromq.metrics.TenantMeter;
import com.baidu.bifromq.mqtt.utils.MQTTMessageSizer;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.OversizePacketDropped;
import com.baidu.bifromq.type.ClientInfo;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.mqtt.MqttMessage;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The packet filter is a duplex handler, MUST be configured after MqttEncoder and before MqttDecoder
 */
public class MQTTPacketFilter extends ChannelDuplexHandler {
    public static final String NAME = "MQTT5SizeBasedPacketFilter";
    private final ClientInfo clientInfo;
    private final IEventCollector eventCollector;
    private final TenantMeter tenantMeter;
    private final int maxPacketSize;
    private final boolean enableTrim;

    public MQTTPacketFilter(int maxPacketSize,
                            TenantSettings settings,
                            ClientInfo clientInfo,
                            IEventCollector eventCollector) {
        this.eventCollector = eventCollector;
        tenantMeter = TenantMeter.get(clientInfo.getTenantId());
        this.clientInfo = clientInfo;
        this.maxPacketSize = Math.min(maxPacketSize, settings.maxPacketSize);
        this.enableTrim = clientInfo.getMetadataOrDefault(MQTT_PROTOCOL_VER_KEY, "").equals(MQTT_PROTOCOL_VER_5_VALUE);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            tenantMeter.recordSummary(MqttIngressBytes, ((ByteBuf) msg).readableBytes());
        }
        super.channelRead(ctx, msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        assert msg instanceof MqttMessage;
        MqttMessage mqttMessage = (MqttMessage) msg;
        MQTTMessageSizer.MqttMessageSize messageSize = MQTTMessageSizer.size(mqttMessage);
        AtomicInteger encodedBytes = new AtomicInteger(messageSize.encodedBytes());
        if (encodedBytes.get() <= maxPacketSize) {
            promise.addListener(future -> tenantMeter.recordSummary(MqttEgressBytes, encodedBytes.get()));
            super.write(ctx, msg, promise);
            return;
        }
        if (enableTrim) {
            encodedBytes.set(messageSize.encodedBytes(true, false));
            if (encodedBytes.get() <= maxPacketSize) {
                // trim reason string
                promise.addListener(future -> tenantMeter.recordSummary(MqttEgressBytes, encodedBytes.get()));
                super.write(ctx, trim(mqttMessage, true, false), promise);
                return;
            }
            encodedBytes.set(messageSize.encodedBytes(false, false));
            if (encodedBytes.get() <= maxPacketSize) {
                // trim reason string and user properties
                promise.addListener(future -> tenantMeter.recordSummary(MqttEgressBytes, encodedBytes.get()));
                super.write(ctx, trim(mqttMessage, true, true), promise);
                return;
            }
        }
        eventCollector.report(
            getLocal(OversizePacketDropped.class)
                .mqttPacketType(mqttMessage.fixedHeader().messageType().value())
                .clientInfo(clientInfo));
    }
}
