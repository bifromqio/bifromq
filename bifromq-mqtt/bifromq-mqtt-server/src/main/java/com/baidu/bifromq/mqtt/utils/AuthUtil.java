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

package com.baidu.bifromq.mqtt.utils;

import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.authData;
import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.authMethod;
import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.toUserProperties;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.baidu.bifromq.mqtt.handler.ChannelAttrs;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT5AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTTAction;
import com.baidu.bifromq.plugin.authprovider.type.PubAction;
import com.baidu.bifromq.plugin.authprovider.type.SubAction;
import com.baidu.bifromq.plugin.authprovider.type.UnsubAction;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.UserProperties;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Optional;
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

    @SneakyThrows
    public static MQTT5AuthData buildMQTT5AuthData(Channel channel, MqttConnectMessage msg) {
        assert msg.variableHeader().version() == 5;
        MQTT5AuthData.Builder authData = MQTT5AuthData.newBuilder();
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
        UserProperties userProperties = toUserProperties(msg.variableHeader().properties());
        return authData.setUserProps(userProperties).build();
    }

    @SneakyThrows
    public static MQTT5ExtendedAuthData buildMQTT5ExtendedAuthData(Channel channel, MqttConnectMessage msg) {
        assert msg.variableHeader().version() == 5;
        MQTT5ExtendedAuthData.Initial.Builder initialBuilder = MQTT5ExtendedAuthData.Initial.newBuilder();
        initialBuilder.setBasic(buildMQTT5AuthData(channel, msg))
            .setAuthMethod(authMethod(msg.variableHeader().properties()).get());
        Optional<ByteString> authData = authData(msg.variableHeader().properties());
        authData.ifPresent(initialBuilder::setAuthData);
        return MQTT5ExtendedAuthData.newBuilder()
            .setInitial(initialBuilder.build())
            .build();
    }

    public static MQTT5ExtendedAuthData buildMQTT5ExtendedAuthData(MqttMessage authMsg, boolean isReAuth) {
        MQTT5ExtendedAuthData.Auth.Builder authBuilder = MQTT5ExtendedAuthData.Auth.newBuilder()
            .setIsReAuth(isReAuth);
        MqttProperties authProps = ((MqttReasonCodeAndPropertiesVariableHeader) authMsg.variableHeader()).properties();
        authMethod(authProps).ifPresent(authBuilder::setAuthMethod);
        authData(authProps).ifPresent(authBuilder::setAuthData);
        authBuilder.setUserProps(toUserProperties(authProps));
        return MQTT5ExtendedAuthData.newBuilder()
            .setAuth(authBuilder.build())
            .build();
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

    public static MQTTAction buildPubAction(String topic, QoS qos, boolean retained, UserProperties userProps) {
        return MQTTAction.newBuilder()
            .setPub(PubAction.newBuilder()
                .setTopic(topic)
                .setQos(qos)
                .setIsRetained(retained)
                .setUserProps(userProps)
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

    public static MQTTAction buildSubAction(String topicFilter, QoS qos, UserProperties userProps) {
        return MQTTAction.newBuilder()
            .setSub(SubAction.newBuilder()
                .setTopicFilter(topicFilter)
                .setQos(qos)
                .setUserProps(userProps)
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

    public static MQTTAction buildUnsubAction(String topicFilter, UserProperties userProps) {
        return MQTTAction.newBuilder()
            .setUnsub(UnsubAction.newBuilder()
                .setTopicFilter(topicFilter)
                .setUserProps(userProps)
                .build())
            .build();
    }
}
