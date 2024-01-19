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

import com.baidu.bifromq.inbox.storage.proto.LWT;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.StringPair;
import com.baidu.bifromq.type.UserProperties;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import java.util.List;
import java.util.Optional;

public class MQTT5MessageUtils {

    static LWT toWillMessage(MqttConnectMessage connMsg) {
        LWT.Builder lwtBuilder = LWT.newBuilder()
            .setTopic(connMsg.payload().willTopic())
            .setRetain(connMsg.variableHeader().isWillRetain())
            .setDelaySeconds(Optional.ofNullable(
                    (MqttProperties.IntegerProperty) connMsg.payload().willProperties()
                        .getProperty(MqttProperties.MqttPropertyType.WILL_DELAY_INTERVAL.value()))
                .map(MqttProperties.MqttProperty::value).orElse(0));
        Message.Builder msgBuilder = Message.newBuilder()
            .setPubQoS(QoS.forNumber(connMsg.variableHeader().willQos()))
            .setPayload(UnsafeByteOperations.unsafeWrap(connMsg.payload().willMessageInBytes()));
        // PacketFormatIndicator
        Optional<Integer> payloadFormatIndicator = Optional.ofNullable(
                (MqttProperties.IntegerProperty) connMsg.payload().willProperties()
                    .getProperty(MqttProperties.MqttPropertyType.PAYLOAD_FORMAT_INDICATOR.value()))
            .map(MqttProperties.MqttProperty::value);
        payloadFormatIndicator.ifPresent(integer -> msgBuilder.setIsUTF8String(integer == 1));
        // MessageExpiryInterval
        Optional<Integer> messageExpiryInterval = Optional.ofNullable(
                (MqttProperties.IntegerProperty) connMsg.payload().willProperties()
                    .getProperty(MqttProperties.MqttPropertyType.PUBLICATION_EXPIRY_INTERVAL.value()))
            .map(MqttProperties.MqttProperty::value);
        messageExpiryInterval.ifPresent(msgBuilder::setExpiryInterval);
        // ContentType
        Optional<String> contentType = Optional.ofNullable(
                (MqttProperties.StringProperty) connMsg.payload().willProperties()
                    .getProperty(MqttProperties.MqttPropertyType.CONTENT_TYPE.value()))
            .map(MqttProperties.MqttProperty::value);
        contentType.ifPresent(msgBuilder::setContentType);
        // ResponseTopic
        Optional<String> responseTopic = Optional.ofNullable(
                (MqttProperties.StringProperty) connMsg.payload().willProperties()
                    .getProperty(MqttProperties.MqttPropertyType.RESPONSE_TOPIC.value()))
            .map(MqttProperties.MqttProperty::value);
        responseTopic.ifPresent(msgBuilder::setResponseTopic);
        // CorrelationData
        Optional<ByteString> correlationData = Optional.ofNullable(
                (MqttProperties.BinaryProperty) connMsg.payload().willProperties()
                    .getProperty(MqttProperties.MqttPropertyType.CORRELATION_DATA.value()))
            .map(MqttProperties.MqttProperty::value)
            .map(UnsafeByteOperations::unsafeWrap);
        correlationData.ifPresent(msgBuilder::setCorrelationData);
        // UserProperty
        List<MqttProperties.UserProperty> userPropertyList =
            (List<MqttProperties.UserProperty>) connMsg.payload().willProperties()
                .getProperties(MqttProperties.MqttPropertyType.USER_PROPERTY.value());
        if (!userPropertyList.isEmpty()) {
            UserProperties.Builder userProperties = UserProperties.newBuilder();
            userPropertyList.forEach(
                up -> StringPair.newBuilder().setKey(up.value().key).setValue(up.value().value).build());
            msgBuilder.setUserProperties(userProperties.build());
        }
        return lwtBuilder.setMessage(msgBuilder).build();
    }

    static MqttSubAckMessage toMqttSubAckMessage(int packetId, List<Integer> ackedQoSs) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK,
            false,
            MqttQoS.AT_MOST_ONCE,
            false,
            0);
        MqttMessageIdVariableHeader mqttMessageIdVariableHeader = MqttMessageIdVariableHeader.from(packetId);
        io.netty.handler.codec.mqtt.MqttSubAckPayload mqttSubAckPayload = new MqttSubAckPayload(ackedQoSs);
        return new MqttSubAckMessage(mqttFixedHeader,
            mqttMessageIdVariableHeader,
            mqttSubAckPayload);
    }

    static MqttUnsubAckMessage toMqttUnsubAckMessage(int messageId) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBACK,
            false,
            MqttQoS.AT_MOST_ONCE,
            false,
            0);
        MqttMessageIdVariableHeader mqttMessageIdVariableHeader = MqttMessageIdVariableHeader.from(messageId);
        return new MqttUnsubAckMessage(mqttFixedHeader, mqttMessageIdVariableHeader);
    }
}
