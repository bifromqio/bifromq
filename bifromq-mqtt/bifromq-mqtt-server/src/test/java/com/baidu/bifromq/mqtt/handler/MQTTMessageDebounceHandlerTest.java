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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttVersion;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class MQTTMessageDebounceHandlerTest extends BaseMQTTTest {

    @Override
    protected ChannelInitializer<EmbeddedChannel> channelInitializer() {
        return new ChannelInitializer<EmbeddedChannel>() {
            @Override
            protected void initChannel(EmbeddedChannel embeddedChannel) {
                ChannelPipeline pipeline = embeddedChannel.pipeline();
                pipeline.addLast("decoder", new MqttDecoder(256 * 1024)); //256kb
                pipeline.addLast("encoder", MqttEncoder.INSTANCE);
                pipeline.addLast(MQTTMessageDebounceHandler.NAME, new MQTTMessageDebounceHandler());
            }
        };
    }

    @Test
    public void testReadManually() {
        channel.config().setAutoRead(false);
        MqttMessage connMsg = MqttMessageBuilders.connect()
            .protocolVersion(MqttVersion.MQTT_3_1_1).keepAlive(30).build();
        MqttMessage pingReqMsg = MqttMessage.PINGREQ;
        MqttMessage connAckMsg = MqttMessageBuilders.connAck()
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED).build();

        channel.writeInbound(connMsg, pingReqMsg, connAckMsg);
        assertNull(channel.readInbound());

        channel.read();
        assertEquals(connMsg, channel.readInbound());
        assertNull(channel.readInbound());

        channel.read();
        assertEquals(pingReqMsg, channel.readInbound());
        assertNull(channel.readInbound());

        channel.read();
        assertEquals(connAckMsg, channel.readInbound());
        assertNull(channel.readInbound());

        channel.read();
        assertNull(channel.readInbound());
    }

    @Test
    public void testAutoRead() {
        channel.config().setAutoRead(true);
        MqttMessage connMsg = MqttMessageBuilders.connect()
            .protocolVersion(MqttVersion.MQTT_3_1_1).keepAlive(30).build();
        MqttMessage connAckMsg = MqttMessageBuilders.connAck()
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED).build();
        channel.writeInbound(connMsg, connAckMsg);
        assertEquals(connMsg, channel.readInbound());
        assertEquals(connAckMsg, channel.readInbound());
    }

    @Test
    public void testAutoReadToManualRead() {
        channel.config().setAutoRead(true);
        MqttMessage connMsg = MqttMessageBuilders.connect()
            .protocolVersion(MqttVersion.MQTT_3_1_1).keepAlive(30).build();
        channel.writeInbound(connMsg);
        assertEquals(connMsg, channel.readInbound());

        channel.config().setAutoRead(false);
        MqttMessage pingReqMsg = MqttMessage.PINGREQ;
        channel.writeInbound(pingReqMsg);
        assertNull(channel.readInbound());

        channel.read();
        assertEquals(pingReqMsg, channel.readInbound());
        assertNull(channel.readInbound());
    }

    @Test
    public void testManualReadToAutoRead() {
        channel.config().setAutoRead(false);
        MqttMessage connMsg = MqttMessageBuilders.connect()
            .protocolVersion(MqttVersion.MQTT_3_1_1).keepAlive(30).build();
        channel.writeInbound(connMsg);
        assertNull(channel.readInbound());
        channel.read();
        assertEquals(connMsg, channel.readInbound());

        channel.config().setAutoRead(true);
        MqttMessage pingReqMsg = MqttMessage.PINGREQ;
        MqttMessage connAckMsg = MqttMessageBuilders.connAck()
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED).build();
        channel.writeInbound(pingReqMsg, connAckMsg);
        assertEquals(pingReqMsg, channel.readInbound());
        assertEquals(connAckMsg, channel.readInbound());
    }
}
