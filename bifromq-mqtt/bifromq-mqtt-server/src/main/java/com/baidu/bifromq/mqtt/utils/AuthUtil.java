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

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.baidu.bifromq.mqtt.handler.ChannelAttrs;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTTAction;
import com.baidu.bifromq.plugin.authprovider.type.PubAction;
import com.baidu.bifromq.plugin.authprovider.type.SubAction;
import com.baidu.bifromq.plugin.authprovider.type.UnsubAction;
import com.baidu.bifromq.type.QoS;
import com.google.common.base.Strings;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.util.Base64;
import lombok.SneakyThrows;

public class AuthUtil {
    @SneakyThrows
    public static MQTT3AuthData buildMQTT3AuthData(Channel channel, MqttConnectMessage msg) {
        assert msg.variableHeader().version() != 5;
        MQTT3AuthData.Builder authData = MQTT3AuthData.newBuilder();
        if (msg.variableHeader().version() == 3) {
            authData.setIsMQIsdp(true);
        }
        X509Certificate cert = ChannelAttrs.clientCertificate(channel);
        if (cert != null) {
            authData.setCert(unsafeWrap(Base64.getEncoder().encode(cert.getEncoded())));
        }
        if (msg.variableHeader().hasUserName()) {
            authData.setUsername(msg.payload().userName());
        }
        if (msg.variableHeader().hasPassword()) {
            authData.setPassword(unsafeWrap(msg.payload().passwordInBytes()));
        }
        if (!Strings.isNullOrEmpty(msg.payload().clientIdentifier())) {
            authData.setClientId(msg.payload().clientIdentifier());
        }
        InetSocketAddress remoteAddr = ChannelAttrs.socketAddress(channel);
        if (remoteAddr != null) {
            authData.setRemotePort(remoteAddr.getPort())
                .setChannelId(channel.id().asLongText());
            InetAddress ip = remoteAddr.getAddress();
            if (remoteAddr.getAddress() != null) {
                authData.setRemoteAddr(ip.getHostAddress());
            }
        }
        return authData.build();
    }


    public static MQTTAction buildPubAction(String topic, QoS qos, boolean retained) {
        return MQTTAction.newBuilder()
            .setPub(PubAction.newBuilder()
                .setTopic(topic)
                .setQos(qos)
                .setIsRetained(retained)
                .build())
            .build();
    }

    public static MQTTAction buildSubAction(String topicFilter, QoS qos) {
        return MQTTAction.newBuilder()
            .setSub(SubAction.newBuilder()
                .setTopicFilter(topicFilter)
                .setQos(qos)
                .build())
            .build();
    }

    public static MQTTAction buildUnsubAction(String topicFilter) {
        return MQTTAction.newBuilder()
            .setUnsub(UnsubAction.newBuilder()
                .setTopicFilter(topicFilter)
                .build())
            .build();
    }
}
