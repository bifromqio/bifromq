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

package com.baidu.bifromq.mqtt.utils;

import static java.nio.ByteBuffer.wrap;

import com.baidu.bifromq.mqtt.handler.ChannelAttrs;
import com.baidu.bifromq.plugin.authprovider.action.PubAction;
import com.baidu.bifromq.plugin.authprovider.action.SubAction;
import com.baidu.bifromq.plugin.authprovider.action.UnsubAction;
import com.baidu.bifromq.plugin.authprovider.authdata.MQTTBasicAuth;
import com.baidu.bifromq.plugin.authprovider.authdata.MQTTVer;
import com.baidu.bifromq.type.QoS;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import java.security.cert.X509Certificate;
import java.util.Base64;
import lombok.SneakyThrows;

public class AuthUtil {
    @SneakyThrows
    public static MQTTBasicAuth buildMQTTBasicAuth(Channel channel, MqttConnectMessage msg) {
        MQTTBasicAuth authData = new MQTTBasicAuth();
        switch (msg.variableHeader().version()) {
            case 3:
                authData.protocol(MQTTVer.MQTT3_1);
                break;
            case 4:
                authData.protocol(MQTTVer.MQTT3_1_1);
                break;
            default:
                authData.protocol(MQTTVer.MQTT5);
                break;
        }
        X509Certificate cert = ChannelAttrs.clientCertificate(channel);
        if (cert != null) {
            authData.cert(wrap(Base64.getEncoder().encode(cert.getEncoded())).asReadOnlyBuffer());
        }

        authData.username(msg.payload().userName())
            .password(msg.variableHeader().hasPassword() ?
                wrap(msg.payload().passwordInBytes()).asReadOnlyBuffer() : null)
            .remoteAddr(ChannelAttrs.socketAddress(channel))
            .channelId(channel.id().asLongText());
        return authData;
    }


    public static PubAction buildPubAction(String topic, QoS qos, boolean retained) {
        return new PubAction().qos(qos).topic(topic).isRetained(retained);
    }

    public static SubAction buildSubAction(String topicFilter, QoS qos) {
        return new SubAction().qos(qos).topicFilter(topicFilter);
    }

    public static UnsubAction buildUnsubAction(String topicFilter) {
        return new UnsubAction().topicFilter(topicFilter);
    }
}
