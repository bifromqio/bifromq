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

import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.toMqttUserProps;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.AUTHENTICATION_DATA;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.AUTHENTICATION_METHOD;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.CONTENT_TYPE;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.CORRELATION_DATA;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.PAYLOAD_FORMAT_INDICATOR;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.RESPONSE_TOPIC;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.TOPIC_ALIAS;

import com.baidu.bifromq.inbox.storage.proto.TopicFilterOption;
import com.baidu.bifromq.mqtt.handler.MQTTSessionHandler;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5AuthReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5PubCompReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5PubRecReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5PubRelReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5SubAckReasonCode;
import com.baidu.bifromq.type.UserProperties;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageIdAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPubReplyMessageVariableHeader;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import java.util.List;

public class MQTT5MessageBuilders {
    public static AuthBuilder auth(String authMethod) {
        return new AuthBuilder(authMethod);
    }

    public static SubAckBuilder subAck() {
        return new SubAckBuilder();
    }

    public static PubBuilder pub() {
        return new PubBuilder();
    }

    public static PubRecBuilder pubRec() {
        return new PubRecBuilder();
    }

    public static PubRelBuilder pubRel() {
        return new PubRelBuilder();
    }

    public static PubCompBuilder pubComp() {
        return new PubCompBuilder();
    }

    public static final class AuthBuilder {
        private final String authMethod;
        private ByteString authData;
        private MQTT5AuthReasonCode reasonCode;
        private String reasonString;
        private UserProperties userProps;

        public AuthBuilder(String authMethod) {
            this.authMethod = authMethod;
        }

        public AuthBuilder authData(ByteString authData) {
            this.authData = authData;
            return this;
        }

        public AuthBuilder reasonCode(MQTT5AuthReasonCode reasonCode) {
            this.reasonCode = reasonCode;
            return this;
        }

        public AuthBuilder reasonString(String reasonString) {
            this.reasonString = reasonString;
            return this;
        }

        public AuthBuilder userProperties(UserProperties userProps) {
            this.userProps = userProps;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.AUTH,
                false,
                MqttQoS.AT_MOST_ONCE,
                false,
                0);
            MqttProperties mqttProperties = new MqttProperties();
            mqttProperties.add(new MqttProperties.StringProperty(AUTHENTICATION_METHOD.value(), authMethod));
            if (authData != null) {
                mqttProperties.add(new MqttProperties.BinaryProperty(
                    AUTHENTICATION_DATA.value(), authData.toByteArray()));
            }
            if (!Strings.isNullOrEmpty(reasonString)) {
                mqttProperties.add(new MqttProperties.StringProperty(
                    MqttProperties.MqttPropertyType.REASON_STRING.value(), reasonString));
            }
            if (userProps != null) {
                mqttProperties.add(toMqttUserProps(userProps));
            }
            MqttReasonCodeAndPropertiesVariableHeader variableHeader =
                new MqttReasonCodeAndPropertiesVariableHeader(reasonCode.value(), mqttProperties);
            return new MqttMessage(fixedHeader, variableHeader);
        }
    }

    public static final class PubBuilder {
        private int packetId;
        private MQTTSessionHandler.SubMessage message;
        private boolean setupAlias;
        private int topicAlias;

        public PubBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public PubBuilder setupAlias(boolean setupAlias) {
            this.setupAlias = setupAlias;
            return this;
        }

        public PubBuilder topicAlias(int alias) {
            this.topicAlias = alias;
            return this;
        }

        public PubBuilder message(MQTTSessionHandler.SubMessage message) {
            this.message = message;
            return this;
        }

        public MqttPublishMessage build() {
            MqttMessageBuilders.PublishBuilder pubBuilder = MqttMessageBuilders.publish();
            MqttProperties mqttProps = new MqttProperties();
            TopicFilterOption option = message.option();
            if (option.hasSubId()) {
                mqttProps.add(new MqttProperties.IntegerProperty(SUBSCRIPTION_IDENTIFIER.value(), option.getSubId()));
            }
            if (topicAlias > 0) {
                if (setupAlias) {
                    pubBuilder.topicName(message.topic());
                } else {
                    pubBuilder.topicName("");
                }
                mqttProps.add(new MqttProperties.IntegerProperty(TOPIC_ALIAS.value(), topicAlias));
            } else {
                pubBuilder.topicName(message.topic());
            }
            if (message.message().getIsUTF8String()) {
                mqttProps.add(new MqttProperties.IntegerProperty(PAYLOAD_FORMAT_INDICATOR.value(), 1));
            }
            if (message.message().hasContentType()) {
                mqttProps.add(
                    new MqttProperties.StringProperty(CONTENT_TYPE.value(), message.message().getContentType()));
            }
            if (message.message().hasCorrelationData()) {
                mqttProps.add(new MqttProperties.BinaryProperty(CORRELATION_DATA.value(),
                    message.message().getCorrelationData().toByteArray()));
            }
            if (message.message().hasResponseTopic()) {
                mqttProps.add(
                    new MqttProperties.StringProperty(RESPONSE_TOPIC.value(), message.message().getResponseTopic()));
            }
            if (message.message().getUserProperties().getUserPropertiesCount() > 0) {
                mqttProps.add(toMqttUserProps(message.message().getUserProperties()));
            }
            return pubBuilder
                .messageId(packetId)
                .qos(MqttQoS.valueOf(message.qos().getNumber()))
                .retained(message.isRetain())
                .payload(Unpooled.wrappedBuffer(message.message().getPayload().asReadOnlyByteBuffer()))
                .properties(mqttProps)
                .build();
        }
    }

    public static final class PubRecBuilder {
        private int packetId;
        private MQTT5PubRecReasonCode reasonCode;
        private MqttProperties properties;

        PubRecBuilder() {
        }

        public PubRecBuilder reasonCode(MQTT5PubRecReasonCode reasonCode) {
            this.reasonCode = reasonCode;
            return this;
        }

        public PubRecBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public PubRecBuilder properties(MqttProperties properties) {
            this.properties = properties;
            return this;
        }

        public MqttMessage build() {
            if (reasonCode == MQTT5PubRecReasonCode.Success) {
                MqttFixedHeader fixedHeader =
                    new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 2);
                MqttMessageIdVariableHeader varHeader = MqttMessageIdVariableHeader.from(packetId);
                return new MqttMessage(fixedHeader, varHeader);
            } else {
                MqttFixedHeader fixedHeader =
                    new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 3);
                MqttPubReplyMessageVariableHeader varHeader =
                    new MqttPubReplyMessageVariableHeader(packetId, reasonCode.value(), null);
                return new MqttMessage(fixedHeader, varHeader);
            }
        }
    }

    public static final class PubRelBuilder {
        private int packetId;
        private MQTT5PubRelReasonCode reasonCode;
        private MqttProperties properties;

        PubRelBuilder() {
        }

        public PubRelBuilder reasonCode(MQTT5PubRelReasonCode reasonCode) {
            this.reasonCode = reasonCode;
            return this;
        }

        public PubRelBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public PubRelBuilder properties(MqttProperties properties) {
            this.properties = properties;
            return this;
        }

        public MqttMessage build() {
            if (reasonCode == MQTT5PubRelReasonCode.Success) {
                MqttFixedHeader fixedHeader =
                    new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_MOST_ONCE, false, 2);
                MqttMessageIdVariableHeader varHeader = MqttMessageIdVariableHeader.from(packetId);
                return new MqttMessage(fixedHeader, varHeader);
            } else {
                MqttFixedHeader fixedHeader =
                    new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_MOST_ONCE, false, 3);
                MqttPubReplyMessageVariableHeader varHeader =
                    new MqttPubReplyMessageVariableHeader(packetId, reasonCode.value(), null);
                return new MqttMessage(fixedHeader, varHeader);
            }
        }
    }

    public static final class PubCompBuilder {
        private int packetId;
        private MQTT5PubCompReasonCode reasonCode;
        private MqttProperties properties;

        PubCompBuilder() {
        }

        public PubCompBuilder reasonCode(MQTT5PubCompReasonCode reasonCode) {
            this.reasonCode = reasonCode;
            return this;
        }

        public PubCompBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public PubCompBuilder properties(MqttProperties properties) {
            this.properties = properties;
            return this;
        }

        public MqttMessage build() {
            if (reasonCode == MQTT5PubCompReasonCode.Success) {
                MqttFixedHeader fixedHeader =
                    new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 2);
                MqttMessageIdVariableHeader varHeader = MqttMessageIdVariableHeader.from(packetId);
                return new MqttMessage(fixedHeader, varHeader);
            } else {
                MqttFixedHeader fixedHeader =
                    new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 3);
                MqttPubReplyMessageVariableHeader varHeader =
                    new MqttPubReplyMessageVariableHeader(packetId, reasonCode.value(), null);
                return new MqttMessage(fixedHeader, varHeader);
            }
        }
    }

    public static final class SubAckBuilder {
        private int packetId;
        private List<MQTT5SubAckReasonCode> reasonCodes;
        private MqttProperties properties;

        SubAckBuilder() {

        }

        public SubAckBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public SubAckBuilder reasonCodes(MQTT5SubAckReasonCode... reasonCodes) {
            this.reasonCodes = List.of(reasonCodes);
            return this;
        }

        public SubAckBuilder properties(MqttProperties properties) {
            this.properties = properties;
            return this;
        }

        public MqttSubAckMessage build() {
            MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK,
                false,
                MqttQoS.AT_MOST_ONCE,
                false,
                0);
            MqttMessageIdAndPropertiesVariableHeader msgIdAndPropsVarHeader =
                new MqttMessageIdAndPropertiesVariableHeader(packetId, properties);
            MqttSubAckPayload mqttSubAckPayload =
                new MqttSubAckPayload(Lists.transform(reasonCodes, MQTT5SubAckReasonCode::value));
            return new MqttSubAckMessage(mqttFixedHeader, msgIdAndPropsVarHeader, mqttSubAckPayload);
        }
    }

}
