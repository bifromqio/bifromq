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
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.REASON_STRING;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.RESPONSE_TOPIC;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SERVER_REFERENCE;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.TOPIC_ALIAS;

import com.baidu.bifromq.inbox.storage.proto.TopicFilterOption;
import com.baidu.bifromq.mqtt.handler.MQTTSessionHandler;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5AuthReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5DisconnectReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5PubAckReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5PubCompReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5PubRecReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5PubRelReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5SubAckReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5UnsubAckReasonCode;
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
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubAckPayload;
import java.util.ArrayList;
import java.util.List;

public class MQTT5MessageBuilders {
    public static AuthBuilder auth(String authMethod) {
        return new AuthBuilder(authMethod);
    }

    public static DisconnectBuilder disconnect() {
        return new DisconnectBuilder();
    }

    public static SubAckBuilder subAck() {
        return new SubAckBuilder();
    }

    public static UnsubAckBuilder unsubAck() {
        return new UnsubAckBuilder();
    }

    public static PubBuilder pub() {
        return new PubBuilder();
    }

    public static PubAckBuilder pubAck() {
        return new PubAckBuilder();
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
            if (mqttProperties.isEmpty() && reasonCode == MQTT5AuthReasonCode.Success) {
                // The Reason Code and Property Length can be omitted if the Reason Code is 0x00 (Success) and there are no Properties. In this case the AUTH has a Remaining Length of 0
                return new MqttMessage(fixedHeader);
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
            MqttProperties mqttProps = new MqttProperties();
            TopicFilterOption option = message.option();
            if (option.hasSubId()) {
                mqttProps.add(new MqttProperties.IntegerProperty(SUBSCRIPTION_IDENTIFIER.value(), option.getSubId()));
            }
            String topicName;
            if (topicAlias > 0) {
                if (setupAlias) {
                    topicName = message.topic();
                } else {
                    topicName = "";
                }
                mqttProps.add(new MqttProperties.IntegerProperty(TOPIC_ALIAS.value(), topicAlias));
            } else {
                topicName = message.topic();
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


            MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.valueOf(message.qos().getNumber()),
                    message.isRetain(), 0);
            MqttPublishVariableHeader mqttVariableHeader =
                new MqttPublishVariableHeader(topicName, packetId, mqttProps);
            return new MqttPublishMessage(mqttFixedHeader, mqttVariableHeader,
                Unpooled.wrappedBuffer(message.message().getPayload().asReadOnlyByteBuffer()));
        }
    }

    public static final class PubAckBuilder {

        private int packetId;
        private MQTT5PubAckReasonCode reasonCode;
        private String reasonString;
        private UserProperties userProps;

        PubAckBuilder() {
        }

        public PubAckBuilder reasonCode(MQTT5PubAckReasonCode reasonCode) {
            this.reasonCode = reasonCode;
            return this;
        }

        public PubAckBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public PubAckBuilder reasonString(String reason) {
            this.reasonString = reason;
            return this;
        }

        public PubAckBuilder userProps(UserProperties userProps) {
            this.userProps = userProps;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader fixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
            MqttMessageIdVariableHeader varHeader;
            if (Strings.isNullOrEmpty(reasonString)
                && userProps == null
                && reasonCode == MQTT5PubAckReasonCode.Success) {
                varHeader = MqttMessageIdVariableHeader.from(packetId);
            } else {
                MqttProperties mqttProperties = new MqttProperties();
                if (!Strings.isNullOrEmpty(reasonString)) {
                    mqttProperties.add(new MqttProperties.StringProperty(REASON_STRING.value(), reasonString));
                }
                if (userProps != null) {
                    mqttProperties.add(toMqttUserProps(userProps));
                }
                varHeader = new MqttPubReplyMessageVariableHeader(packetId, reasonCode.value(), mqttProperties);
            }
            return new MqttMessage(fixedHeader, varHeader);
        }
    }

    public static final class PubRecBuilder {
        private int packetId;
        private MQTT5PubRecReasonCode reasonCode;
        private String reasonString;
        private UserProperties userProps;

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

        public PubRecBuilder reasonString(String reason) {
            this.reasonString = reason;
            return this;
        }

        public PubRecBuilder userProps(UserProperties userProps) {
            this.userProps = userProps;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader fixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 2);
            MqttMessageIdVariableHeader varHeader;
            if (Strings.isNullOrEmpty(reasonString)
                && userProps == null
                && reasonCode == MQTT5PubRecReasonCode.Success) {
                varHeader = MqttMessageIdVariableHeader.from(packetId);
            } else {
                MqttProperties mqttProperties = new MqttProperties();
                if (!Strings.isNullOrEmpty(reasonString)) {
                    mqttProperties.add(new MqttProperties.StringProperty(REASON_STRING.value(), reasonString));
                }
                if (userProps != null) {
                    mqttProperties.add(toMqttUserProps(userProps));
                }
                varHeader = new MqttPubReplyMessageVariableHeader(packetId, reasonCode.value(), mqttProperties);
            }
            return new MqttMessage(fixedHeader, varHeader);
        }
    }

    public static final class PubRelBuilder {
        private int packetId;
        private MQTT5PubRelReasonCode reasonCode;
        private String reasonString;
        private UserProperties userProps;

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

        public PubRelBuilder reasonString(String reason) {
            this.reasonString = reason;
            return this;
        }

        public PubRelBuilder userProps(UserProperties userProps) {
            this.userProps = userProps;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader fixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_MOST_ONCE, false, 2);
            MqttMessageIdVariableHeader varHeader;
            if (Strings.isNullOrEmpty(reasonString)
                && userProps == null
                && reasonCode == MQTT5PubRelReasonCode.Success) {
                varHeader = MqttMessageIdVariableHeader.from(packetId);
            } else {
                MqttProperties mqttProperties = new MqttProperties();
                if (!Strings.isNullOrEmpty(reasonString)) {
                    mqttProperties.add(new MqttProperties.StringProperty(REASON_STRING.value(), reasonString));
                }
                if (userProps != null) {
                    mqttProperties.add(toMqttUserProps(userProps));
                }
                varHeader = new MqttPubReplyMessageVariableHeader(packetId, reasonCode.value(), mqttProperties);
            }
            return new MqttMessage(fixedHeader, varHeader);
        }
    }

    public static final class PubCompBuilder {
        private int packetId;
        private MQTT5PubCompReasonCode reasonCode;
        private String reasonString;
        private UserProperties userProps;

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

        public PubCompBuilder reasonString(String reason) {
            this.reasonString = reason;
            return this;
        }

        public PubCompBuilder userProps(UserProperties userProps) {
            this.userProps = userProps;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader fixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 2);
            MqttMessageIdVariableHeader varHeader;
            if (Strings.isNullOrEmpty(reasonString)
                && userProps == null
                && reasonCode == MQTT5PubCompReasonCode.Success) {
                varHeader = MqttMessageIdVariableHeader.from(packetId);
            } else {
                MqttProperties mqttProperties = new MqttProperties();
                if (!Strings.isNullOrEmpty(reasonString)) {
                    mqttProperties.add(new MqttProperties.StringProperty(REASON_STRING.value(), reasonString));
                }
                if (userProps != null) {
                    mqttProperties.add(toMqttUserProps(userProps));
                }
                varHeader = new MqttPubReplyMessageVariableHeader(packetId, reasonCode.value(), mqttProperties);
            }
            return new MqttMessage(fixedHeader, varHeader);
        }
    }

    public static final class SubAckBuilder {
        private int packetId;
        private List<MQTT5SubAckReasonCode> reasonCodes;
        private String reasonString;
        private UserProperties userProps;

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

        public SubAckBuilder reasonString(String reason) {
            this.reasonString = reason;
            return this;
        }

        public SubAckBuilder userProps(UserProperties userProps) {
            this.userProps = userProps;
            return this;
        }

        public MqttSubAckMessage build() {
            MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK,
                false,
                MqttQoS.AT_MOST_ONCE,
                false,
                0);
            MqttMessageIdVariableHeader variableHeader;
            if (!Strings.isNullOrEmpty(reasonString) || userProps != null) {
                MqttProperties mqttProps = new MqttProperties();
                if (!Strings.isNullOrEmpty(reasonString)) {
                    mqttProps.add(new MqttProperties.StringProperty(REASON_STRING.value(), reasonString));
                }
                if (userProps != null) {
                    mqttProps.add(toMqttUserProps(userProps));
                }
                variableHeader = new MqttMessageIdAndPropertiesVariableHeader(packetId, mqttProps);
            } else {
                variableHeader = MqttMessageIdVariableHeader.from(packetId);
            }
            MqttSubAckPayload mqttSubAckPayload =
                new MqttSubAckPayload(Lists.transform(reasonCodes, MQTT5SubAckReasonCode::value));
            return new MqttSubAckMessage(mqttFixedHeader, variableHeader, mqttSubAckPayload);
        }
    }

    public static final class DisconnectBuilder {
        private MQTT5DisconnectReasonCode reasonCode;
        private String reasonString;
        private UserProperties userProps;
        private String serverReference;

        DisconnectBuilder() {
        }

        public DisconnectBuilder reasonCode(MQTT5DisconnectReasonCode reasonCode) {
            this.reasonCode = reasonCode;
            return this;
        }

        public DisconnectBuilder reasonString(String reason) {
            this.reasonString = reason;
            return this;
        }

        public DisconnectBuilder userProps(UserProperties userProps) {
            this.userProps = userProps;
            return this;
        }

        public DisconnectBuilder serverReference(String serverReference) {
            this.serverReference = serverReference;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader fixedHeader =
                new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
            if (!Strings.isNullOrEmpty(reasonString) || userProps != null || !Strings.isNullOrEmpty(serverReference)) {
                MqttProperties mqttProps = new MqttProperties();
                if (!Strings.isNullOrEmpty(reasonString)) {
                    mqttProps.add(new MqttProperties.StringProperty(REASON_STRING.value(), reasonString));
                }
                if (!Strings.isNullOrEmpty(serverReference)) {
                    mqttProps.add(new MqttProperties.StringProperty(SERVER_REFERENCE.value(), reasonString));
                }
                if (userProps != null) {
                    mqttProps.add(toMqttUserProps(userProps));
                }
                MqttReasonCodeAndPropertiesVariableHeader variableHeader =
                    new MqttReasonCodeAndPropertiesVariableHeader(reasonCode.value(), mqttProps);
                return new MqttMessage(fixedHeader, variableHeader);
            }
            return new MqttMessage(fixedHeader);

        }
    }

    public static final class UnsubAckBuilder {

        private int packetId;
        private final List<MQTT5UnsubAckReasonCode> reasonCodes = new ArrayList<>();
        private String reasonString;
        private UserProperties userProps;

        UnsubAckBuilder() {
        }

        public UnsubAckBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public UnsubAckBuilder addReasonCode(MQTT5UnsubAckReasonCode reasonCode) {
            this.reasonCodes.add(reasonCode);
            return this;
        }

        public UnsubAckBuilder addReasonCodes(MQTT5UnsubAckReasonCode... reasonCodes) {
            this.reasonCodes.addAll(List.of(reasonCodes));
            return this;
        }

        public MqttUnsubAckMessage build() {
            MqttFixedHeader fixedHeader =
                new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
            MqttMessageIdVariableHeader variableHeader;
            if (!Strings.isNullOrEmpty(reasonString) || userProps != null) {
                MqttProperties mqttProps = new MqttProperties();
                if (!Strings.isNullOrEmpty(reasonString)) {
                    mqttProps.add(new MqttProperties.StringProperty(REASON_STRING.value(), reasonString));
                }
                if (userProps != null) {
                    mqttProps.add(toMqttUserProps(userProps));
                }
                variableHeader = new MqttMessageIdAndPropertiesVariableHeader(packetId, mqttProps);
            } else {
                variableHeader = MqttMessageIdVariableHeader.from(packetId);
            }
            MqttUnsubAckPayload unsubAckPayload =
                new MqttUnsubAckPayload(Lists.transform(reasonCodes, MQTT5UnsubAckReasonCode::value));
            return new MqttUnsubAckMessage(fixedHeader, variableHeader, unsubAckPayload);
        }
    }
}
