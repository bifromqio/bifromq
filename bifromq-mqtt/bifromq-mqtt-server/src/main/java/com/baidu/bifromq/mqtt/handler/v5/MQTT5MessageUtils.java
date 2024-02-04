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

package com.baidu.bifromq.mqtt.handler.v5;

import static com.baidu.bifromq.dist.client.ByteBufUtil.toRetainedByteBuffer;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.AUTHENTICATION_DATA;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.AUTHENTICATION_METHOD;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.CONTENT_TYPE;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.CORRELATION_DATA;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.PAYLOAD_FORMAT_INDICATOR;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.RESPONSE_TOPIC;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.inbox.storage.proto.LWT;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.StringPair;
import com.baidu.bifromq.type.UserProperties;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.util.List;
import java.util.Optional;

public class MQTT5MessageUtils {
    public static MqttProperties.UserProperties toMqttUserProps(UserProperties userProperties) {
        MqttProperties.UserProperties userProps = new MqttProperties.UserProperties();
        for (StringPair stringPair : userProperties.getUserPropertiesList()) {
            userProps.add(stringPair.getKey(), stringPair.getValue());
        }
        return userProps;
    }

    @SuppressWarnings("unchecked")
    public static UserProperties toUserProperties(MqttProperties mqttProperties) {
        UserProperties.Builder userPropsBuilder = UserProperties.newBuilder();
        List<MqttProperties.UserProperty> userPropertyList = (List<MqttProperties.UserProperty>) mqttProperties
            .getProperties(MqttProperties.MqttPropertyType.USER_PROPERTY.value());
        if (!userPropertyList.isEmpty()) {
            userPropertyList.forEach(up -> userPropsBuilder.addUserProperties(
                StringPair.newBuilder().setKey(up.value().key).setValue(up.value().value).build()));
        }
        return userPropsBuilder.build();
    }


    public static boolean isUTF8Payload(MqttProperties mqttProperties) {
        return packetFormatIndicator(mqttProperties).map(i -> i == 1).orElse(false);
    }

    public static Optional<Integer> receiveMaximum(MqttProperties mqttProperties) {
        return integerMqttProperty(mqttProperties, MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM);
    }

    public static Optional<Integer> topicAliasMaximum(MqttProperties mqttProperties) {
        return integerMqttProperty(mqttProperties, MqttProperties.MqttPropertyType.TOPIC_ALIAS_MAXIMUM);
    }

    public static Optional<Integer> topicAlias(MqttProperties mqttProperties) {
        return integerMqttProperty(mqttProperties, MqttProperties.MqttPropertyType.TOPIC_ALIAS);
    }

    static Optional<Integer> packetFormatIndicator(MqttProperties mqttProperties) {
        return integerMqttProperty(mqttProperties, PAYLOAD_FORMAT_INDICATOR);
    }

    public static Optional<Integer> messageExpiryInterval(MqttProperties mqttProperties) {
        return integerMqttProperty(mqttProperties, MqttProperties.MqttPropertyType.PUBLICATION_EXPIRY_INTERVAL);
    }

    public static Optional<String> contentType(MqttProperties mqttProperties) {
        return stringMqttProperty(mqttProperties, CONTENT_TYPE);
    }

    public static Optional<String> responseTopic(MqttProperties mqttProperties) {
        return stringMqttProperty(mqttProperties, RESPONSE_TOPIC);
    }

    public static Optional<String> authMethod(MqttProperties mqttProperties) {
        return stringMqttProperty(mqttProperties, AUTHENTICATION_METHOD);
    }

    public static Optional<ByteString> authData(MqttProperties mqttProperties) {
        return binaryMqttProperty(mqttProperties, AUTHENTICATION_DATA);
    }

    public static boolean requestResponseInformation(MqttProperties mqttProperties) {
        return integerMqttProperty(mqttProperties, MqttProperties.MqttPropertyType.REQUEST_RESPONSE_INFORMATION)
            .orElse(1) == 1;
    }

    static Optional<Integer> integerMqttProperty(MqttProperties mqttProperties, MqttProperties.MqttPropertyType type) {
        return Optional.ofNullable((MqttProperties.IntegerProperty) mqttProperties.getProperty(type.value()))
            .map(MqttProperties.MqttProperty::value);
    }

    static Optional<String> stringMqttProperty(MqttProperties mqttProperties, MqttProperties.MqttPropertyType type) {
        return Optional.ofNullable((MqttProperties.StringProperty) mqttProperties.getProperty(type.value()))
            .map(MqttProperties.MqttProperty::value);
    }

    static Optional<ByteString> binaryMqttProperty(MqttProperties mqttProperties,
                                                   MqttProperties.MqttPropertyType type) {
        return Optional.ofNullable((MqttProperties.BinaryProperty) mqttProperties.getProperty(type.value()))
            .map(MqttProperties.MqttProperty::value)
            .map(UnsafeByteOperations::unsafeWrap);
    }

    static LWT toWillMessage(MqttConnectMessage connMsg) {
        LWT.Builder lwtBuilder = LWT.newBuilder()
            .setTopic(connMsg.payload().willTopic())
            .setRetain(connMsg.variableHeader().isWillRetain())
            .setDelaySeconds(integerMqttProperty(connMsg.payload().willProperties(),
                MqttProperties.MqttPropertyType.WILL_DELAY_INTERVAL).orElse(0));
        Message willMsg = toMessage(0,
            MqttQoS.valueOf(connMsg.variableHeader().willQos()),
            connMsg.variableHeader().isWillRetain(),
            connMsg.payload().willProperties(),
            UnsafeByteOperations.unsafeWrap(connMsg.payload().willMessageInBytes()));
        return lwtBuilder.setMessage(willMsg).build();
    }

    static Message toMessage(MqttPublishMessage pubMsg) {
        return toMessage(pubMsg.variableHeader().packetId(),
            pubMsg.fixedHeader().qosLevel(),
            pubMsg.fixedHeader().isRetain(),
            pubMsg.variableHeader().properties(),
            toRetainedByteBuffer(pubMsg.payload()));
    }

    static Message toMessage(long packetId,
                             MqttQoS pubQoS,
                             boolean isRetain,
                             MqttProperties mqttProperties,
                             ByteString payload) {
        Message.Builder msgBuilder = Message.newBuilder()
            .setMessageId(packetId)
            .setPubQoS(QoS.forNumber(pubQoS.value()))
            .setPayload(payload)
            .setTimestamp(HLC.INST.getPhysical())
            // MessageExpiryInterval
            .setExpiryInterval(messageExpiryInterval(mqttProperties).orElse(Integer.MAX_VALUE))
            .setIsRetain(isRetain);
        // PacketFormatIndicator
        packetFormatIndicator(mqttProperties).ifPresent(integer -> msgBuilder.setIsUTF8String(integer == 1));
        // ContentType
        contentType(mqttProperties).ifPresent(msgBuilder::setContentType);
        // ResponseTopic
        responseTopic(mqttProperties).ifPresent(msgBuilder::setResponseTopic);
        // CorrelationData
        Optional<ByteString> correlationData =
            binaryMqttProperty(mqttProperties, CORRELATION_DATA);
        correlationData.ifPresent(msgBuilder::setCorrelationData);
        // UserProperty
        UserProperties userProperties = toUserProperties(mqttProperties);
        if (userProperties.getUserPropertiesCount() > 0) {
            msgBuilder.setUserProperties(userProperties);
        }
        return msgBuilder.build();
    }
}
