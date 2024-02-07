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

import static com.baidu.bifromq.mqtt.handler.record.ProtocolResponse.farewell;
import static com.baidu.bifromq.mqtt.handler.record.ProtocolResponse.farewellNow;
import static com.baidu.bifromq.mqtt.handler.record.ProtocolResponse.response;
import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.isUTF8Payload;
import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.receiveMaximum;
import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.requestProblemInformation;
import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.responseTopic;
import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.subscriptionIdentifier;
import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.toUserProperties;
import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.topicAlias;
import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.topicAliasMaximum;
import static com.baidu.bifromq.mqtt.utils.MQTTUtf8Util.isWellFormed;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ADDRESS_KEY;
import static com.baidu.bifromq.util.TopicUtil.isValidTopic;

import com.baidu.bifromq.dist.client.DistResult;
import com.baidu.bifromq.inbox.storage.proto.RetainHandling;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterOption;
import com.baidu.bifromq.mqtt.handler.IMQTTProtocolHelper;
import com.baidu.bifromq.mqtt.handler.MQTTSessionHandler;
import com.baidu.bifromq.mqtt.handler.TenantSettings;
import com.baidu.bifromq.mqtt.handler.record.ProtocolResponse;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5DisconnectReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5PubAckReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5PubCompReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5PubRecReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5PubRelReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5SubAckReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5UnsubAckReasonCode;
import com.baidu.bifromq.mqtt.utils.MQTTUtf8Util;
import com.baidu.bifromq.plugin.authprovider.type.CheckResult;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.BadPacket;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ByServer;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ExceedPubRate;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ExceedReceivingLimit;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.Idle;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.InboxTransientError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.Kicked;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.MalformedTopic;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.MalformedTopicFilter;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.NoPubPermission;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ProtocolViolation;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.TooLargeSubscription;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.TooLargeUnsubscription;
import com.baidu.bifromq.sysprops.BifroMQSysProp;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.UserProperties;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPubReplyMessageVariableHeader;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

public class MQTT5ProtocolHelper implements IMQTTProtocolHelper {
    private static final boolean SANITY_CHECK = BifroMQSysProp.MQTT_UTF8_SANITY_CHECK.get();
    private final TenantSettings settings;
    private final ClientInfo clientInfo;
    private final int clientReceiveMaximum;
    private final boolean requestProblemInfo;
    private final ReceiverTopicAliasManager receiverTopicAliasManager;
    private final SenderTopicAliasManager senderTopicAliasManager;


    public MQTT5ProtocolHelper(MqttConnectMessage connMsg,
                               TenantSettings settings,
                               ClientInfo clientInfo) {
        this.settings = settings;
        this.clientInfo = clientInfo;
        this.receiverTopicAliasManager = new ReceiverTopicAliasManager();
        this.senderTopicAliasManager =
            new SenderTopicAliasManager(topicAliasMaximum(connMsg.variableHeader().properties()).orElse(0),
                Duration.ofSeconds(60));
        this.clientReceiveMaximum = receiveMaximum(connMsg.variableHeader().properties()).orElse(65535);
        this.requestProblemInfo = requestProblemInformation(connMsg.variableHeader().properties());
    }

    @Override
    public UserProperties getUserProps(MqttPublishMessage mqttMessage) {
        return toUserProperties(mqttMessage.variableHeader().properties());
    }

    @Override
    public UserProperties getUserProps(MqttUnsubscribeMessage mqttMessage) {
        return toUserProperties(mqttMessage.idAndPropertiesVariableHeader().properties());
    }

    @Override
    public boolean checkPacketIdUsage() {
        return true;
    }

    @Override
    public ProtocolResponse onInboxTransientError() {
        return farewell(
            MQTT5MessageBuilders.disconnect()
                .reasonCode(MQTT5DisconnectReasonCode.UnspecifiedError)
                .build(),
            getLocal(InboxTransientError.class).clientInfo(clientInfo));
    }

    @Override
    public Optional<Integer> sessionExpiryIntervalOnDisconnect(MqttMessage disconnectMessage) {
        MqttReasonCodeAndPropertiesVariableHeader variableHeader =
            (MqttReasonCodeAndPropertiesVariableHeader) disconnectMessage.variableHeader();
        return Optional.ofNullable(
                (MqttProperties.IntegerProperty) variableHeader.properties()
                    .getProperty(MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value()))
            .map(MqttProperties.MqttProperty::value);
    }

    @Override
    public ProtocolResponse onDisconnect() {
        return farewellNow(
            MQTT5MessageBuilders.disconnect()
                .reasonCode(MQTT5DisconnectReasonCode.ServerShuttingDown)
                .build(),
            getLocal(ByServer.class).clientInfo(clientInfo));
    }

    @Override
    public ProtocolResponse respondDisconnectProtocolError() {
        return farewell(MQTT5MessageBuilders.disconnect()
                .reasonCode(MQTT5DisconnectReasonCode.ProtocolError) // Protocol Error
                .reasonString("MQTT5-3.14.2.2.2")
                .build(),
            getLocal(ProtocolViolation.class)
                .statement("MQTT5-3.14.2.2.2")
                .clientInfo(clientInfo));
    }

    @Override
    public boolean isNormalDisconnect(MqttMessage message) {
        MqttReasonCodeAndPropertiesVariableHeader variableHeader =
            (MqttReasonCodeAndPropertiesVariableHeader) message.variableHeader();
        return variableHeader.reasonCode() == MQTT5DisconnectReasonCode.Normal.value();
    }

    @Override
    public boolean isDisconnectWithLWT(MqttMessage message) {
        MqttReasonCodeAndPropertiesVariableHeader variableHeader =
            (MqttReasonCodeAndPropertiesVariableHeader) message.variableHeader();
        return variableHeader.reasonCode() == MQTT5DisconnectReasonCode.DisconnectWithWillMessage.value();
    }

    @Override
    public ProtocolResponse respondDecodeError(MqttMessage message) {
        if (message.decoderResult().cause() instanceof TooLongFrameException) {
            return farewell(MQTT5MessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.PacketTooLarge)
                    .build(),
                getLocal(BadPacket.class)
                    .cause(message.decoderResult().cause())
                    .clientInfo(clientInfo));
        }
        return farewell(MQTT5MessageBuilders.disconnect()
                .reasonCode(MQTT5DisconnectReasonCode.MalformedPacket)
                .reasonString(message.decoderResult().cause().getMessage())
                .build(),
            getLocal(BadPacket.class)
                .cause(message.decoderResult().cause())
                .clientInfo(clientInfo));
    }

    @Override
    public ProtocolResponse respondDuplicateConnect(MqttConnectMessage message) {
        return farewell(
            MQTT5MessageBuilders.disconnect()
                .reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                .reasonString("MQTT5-3.1.0-2")
                .build(),
            getLocal(ProtocolViolation.class)
                .statement("MQTT5-3.1.0-2")
                .clientInfo(clientInfo));
    }

    @Override
    public ProtocolResponse validateSubMessage(MqttSubscribeMessage message) {
        List<MqttTopicSubscription> topicSubscriptions = message.payload().topicSubscriptions();
        if (topicSubscriptions.isEmpty()) {
            // Ignore instead of disconnect [MQTT-3.8.3-3]
            return farewell(MQTT5MessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                    .reasonString("MQTT5-3.8.3-2")
                    .build(),
                getLocal(ProtocolViolation.class)
                    .statement("MQTT5-3.8.3-2")
                    .clientInfo(clientInfo));
        }
        if (topicSubscriptions.size() > settings.maxTopicFiltersPerSub) {
            return farewell(
                MQTT5MessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.AdministrativeAction)
                    .build(),
                getLocal(TooLargeSubscription.class)
                    .actual(topicSubscriptions.size())
                    .max(settings.maxTopicFiltersPerSub)
                    .clientInfo(clientInfo));
        }
        for (MqttTopicSubscription topicSub : topicSubscriptions) {
            if (!isWellFormed(topicSub.topicName(), SANITY_CHECK)) {
                return farewell(
                    MQTT5MessageBuilders.disconnect()
                        .reasonCode(MQTT5DisconnectReasonCode.MalformedPacket)
                        .build(),
                    getLocal(MalformedTopicFilter.class)
                        .topicFilter(topicSub.topicName())
                        .clientInfo(clientInfo));
            }
        }
        Optional<Integer> subId = Optional.ofNullable(
                (MqttProperties.IntegerProperty) message.idAndPropertiesVariableHeader().properties()
                    .getProperty(MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER.value()))
            .map(MqttProperties.MqttProperty::value);
        if (subId.isPresent()) {
            if (subId.get() == 0) {
                return farewell(
                    MQTT5MessageBuilders.disconnect()
                        .reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                        .reasonString("MQTT5-3.8.2.1.2")
                        .build(),
                    getLocal(ProtocolViolation.class)
                        .statement("MQTT5-3.8.2.1.2")
                        .clientInfo(clientInfo));
            }
            if (!settings.subscriptionIdentifierEnabled) {
                return response(MQTT5MessageBuilders.subAck()
                    .packetId(message.variableHeader().messageId())
                    .reasonCodes(topicSubscriptions.stream()
                        .map(s -> MQTT5SubAckReasonCode.SubscriptionIdentifierNotSupported)
                        .toArray(MQTT5SubAckReasonCode[]::new))
                    .build());
            }
        }
        return null;
    }

    @Override
    public List<SubTask> getSubTask(MqttSubscribeMessage message) {
        Optional<Integer> subId = Optional.ofNullable(
                (MqttProperties.IntegerProperty) message.idAndPropertiesVariableHeader().properties()
                    .getProperty(MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER.value()))
            .map(MqttProperties.MqttProperty::value);
        UserProperties userProps = toUserProperties(message.idAndPropertiesVariableHeader().properties());
        return message.payload().topicSubscriptions().stream()
            .map(sub -> {
                TopicFilterOption.Builder optionBuilder = TopicFilterOption.newBuilder()
                    .setQos(QoS.forNumber(sub.option().qos().value()))
                    .setRetainAsPublished(sub.option().isRetainAsPublished())
                    .setNoLocal(sub.option().isNoLocal())
                    .setRetainHandling(RetainHandling.forNumber(sub.option().retainHandling().value()));
                subId.ifPresent(optionBuilder::setSubId);
                return new SubTask(sub.topicName(), optionBuilder.build(), userProps);
            })
            .toList();
    }

    @Override
    public MqttSubAckMessage buildSubAckMessage(MqttSubscribeMessage subMessage, List<SubResult> results) {
        MQTT5SubAckReasonCode[] reasonCodes = new MQTT5SubAckReasonCode[results.size()];
        assert subMessage.payload().topicSubscriptions().size() == results.size();
        for (int i = 0; i < results.size(); i++) {
            reasonCodes[i] = switch (results.get(i)) {
                case OK, EXISTS -> MQTT5SubAckReasonCode.valueOf(
                    subMessage.payload().topicSubscriptions().get(i).option().qos().value());
                case EXCEED_LIMIT -> MQTT5SubAckReasonCode.QuotaExceeded;
                case TOPIC_FILTER_INVALID -> MQTT5SubAckReasonCode.TopicFilterInvalid;
                case NOT_AUTHORIZED -> MQTT5SubAckReasonCode.NotAuthorized;
                case WILDCARD_NOT_SUPPORTED -> MQTT5SubAckReasonCode.WildcardSubscriptionsNotSupported;
                case SUBSCRIPTION_IDENTIFIER_NOT_SUPPORTED -> MQTT5SubAckReasonCode.SubscriptionIdentifierNotSupported;
                case SHARED_SUBSCRIPTION_NOT_SUPPORTED -> MQTT5SubAckReasonCode.SharedSubscriptionsNotSupported;
                default -> MQTT5SubAckReasonCode.UnspecifiedError;
            };
        }
        return MQTT5MessageBuilders.subAck()
            .packetId(subMessage.variableHeader().messageId())
            .reasonCodes(reasonCodes)
            .build();
    }

    @Override
    public MqttSubAckMessage respondPacketIdInUse(MqttSubscribeMessage message) {
        return MQTT5MessageBuilders.subAck()
            .packetId(message.variableHeader().messageId())
            .reasonCodes(message.payload().topicSubscriptions().stream()
                .map(v -> MQTT5SubAckReasonCode.PacketIdentifierInUse)
                .toArray(MQTT5SubAckReasonCode[]::new))
            .build();
    }

    @Override
    public ProtocolResponse validateUnsubMessage(MqttUnsubscribeMessage message) {
        List<String> topicFilters = message.payload().topics();
        if (topicFilters.isEmpty()) {
            // Ignore instead of disconnect [3.10.3-2]
            return farewell(
                MQTT5MessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                    .reasonString("MQTT5-3.10.3-2")
                    .build(),
                getLocal(ProtocolViolation.class)
                    .statement("MQTT5-3.10.3-2")
                    .clientInfo(clientInfo));
        }
        if (topicFilters.size() > settings.maxTopicFiltersPerSub) {
            return farewell(
                MQTT5MessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.AdministrativeAction)
                    .build(),
                getLocal(TooLargeUnsubscription.class)
                    .max(settings.maxTopicFiltersPerSub)
                    .actual(topicFilters.size())
                    .clientInfo(clientInfo));
        }

        for (String topicFilter : topicFilters) {
            if (!isWellFormed(topicFilter, SANITY_CHECK)) {
                return farewell(
                    MQTT5MessageBuilders.disconnect()
                        .reasonCode(MQTT5DisconnectReasonCode.MalformedPacket)
                        .build(),
                    getLocal(MalformedTopicFilter.class)
                        .topicFilter(topicFilter)
                        .clientInfo(clientInfo),
                    getLocal(ProtocolViolation.class)
                        .statement("MQTT5-3.8.3-2")
                        .clientInfo(clientInfo));
            }
        }
        return null;
    }

    @Override
    public MqttUnsubAckMessage respondPacketIdInUse(MqttUnsubscribeMessage message) {
        return MQTT5MessageBuilders.unsubAck()
            .packetId(message.variableHeader().messageId())
            .addReasonCodes(message.payload().topics().stream()
                .map(v -> MQTT5UnsubAckReasonCode.PacketIdentifierInUse)
                .toArray(MQTT5UnsubAckReasonCode[]::new))
            .build();
    }

    @Override
    public MqttUnsubAckMessage buildUnsubAckMessage(MqttUnsubscribeMessage unsubMessage, List<UnsubResult> results) {
        MQTT5UnsubAckReasonCode[] reasonCodes = results.stream().map(result -> switch (result) {
                case OK -> MQTT5UnsubAckReasonCode.Success;
                case NO_SUB -> MQTT5UnsubAckReasonCode.NoSubscriptionExisted;
                case TOPIC_FILTER_INVALID -> MQTT5UnsubAckReasonCode.TopicFilterInvalid;
                case NOT_AUTHORIZED -> MQTT5UnsubAckReasonCode.NotAuthorized;
                default -> MQTT5UnsubAckReasonCode.UnspecifiedError;
            })
            .toArray(MQTT5UnsubAckReasonCode[]::new);

        return MQTT5MessageBuilders.unsubAck()
            .packetId(unsubMessage.variableHeader().messageId())
            .addReasonCodes(reasonCodes)
            .build();
    }

    @Override
    public MqttMessage onPubRelReceived(MqttMessage message, boolean packetIdFound) {
        if (packetIdFound) {
            return MQTT5MessageBuilders.pubComp()
                .packetId(((MqttMessageIdVariableHeader) message.variableHeader()).messageId())
                .reasonCode(MQTT5PubCompReasonCode.Success)
                .build();
        } else {
            return MQTT5MessageBuilders.pubComp()
                .packetId(((MqttMessageIdVariableHeader) message.variableHeader()).messageId())
                .reasonCode(MQTT5PubCompReasonCode.PacketIdentifierNotFound)
                .build();
        }
    }

    @Override
    public boolean isQoS2Received(MqttMessage message) {
        MqttPubReplyMessageVariableHeader variableHeader =
            (MqttPubReplyMessageVariableHeader) message.variableHeader();
        MQTT5PubRecReasonCode reasonCode = MQTT5PubRecReasonCode.valueOf(variableHeader.reasonCode());
        return reasonCode == MQTT5PubRecReasonCode.Success || reasonCode == MQTT5PubRecReasonCode.NoMatchingSubscribers;
    }

    @Override
    public ProtocolResponse respondPubRecMsg(MqttMessage message, boolean packetIdNotFound) {
        MqttPubReplyMessageVariableHeader variableHeader =
            (MqttPubReplyMessageVariableHeader) message.variableHeader();
        int packetId = variableHeader.messageId();
        if (packetIdNotFound) {
            return farewell(MQTT5MessageBuilders.pubRel()
                    .packetId(packetId)
                    .reasonCode(
                        MQTT5PubRelReasonCode.PacketIdentifierNotFound)
                    .build(),
                getLocal(ProtocolViolation.class).statement("MQTT5-4.3.3-8").clientInfo(clientInfo));
        } else {
            return response(MQTT5MessageBuilders.pubRel()
                .packetId(packetId)
                .reasonCode(MQTT5PubRelReasonCode.Success)
                .build());
        }
    }

    @Override
    public int clientReceiveMaximum() {
        return clientReceiveMaximum;
    }

    @Override
    public ProtocolResponse onKick(ClientInfo kicker) {
        return farewellNow(
            MQTT5MessageBuilders.disconnect()
                .reasonCode(MQTT5DisconnectReasonCode.SessionTakenOver)
                .reasonString(kicker.getMetadataOrDefault(MQTT_CLIENT_ADDRESS_KEY, ""))
                .build(),
            getLocal(Kicked.class).kicker(kicker).clientInfo(clientInfo));
    }

    @Override
    public MqttPublishMessage buildMqttPubMessage(int packetId, MQTTSessionHandler.SubMessage message) {
        Optional<SenderTopicAliasManager.AliasCreationResult> aliasCreationResult =
            senderTopicAliasManager.tryAlias(message.topic());
        if (aliasCreationResult.isPresent()) {
            if (aliasCreationResult.get().isFirstTime()) {
                return MQTT5MessageBuilders.pub().packetId(packetId)
                    .setupAlias(true)
                    .topicAlias(aliasCreationResult.get().alias())
                    .message(message)
                    .build();
            } else {
                return MQTT5MessageBuilders.pub().packetId(packetId)
                    .topicAlias(aliasCreationResult.get().alias())
                    .message(message)
                    .build();
            }
        }
        return MQTT5MessageBuilders.pub().packetId(packetId).message(message).build();
    }

    @Override
    public ProtocolResponse respondReceivingMaximumExceeded() {
        return farewell(
            MQTT5MessageBuilders.disconnect()
                .reasonCode(MQTT5DisconnectReasonCode.ReceiveMaximumExceeded)
                .build(),
            getLocal(ExceedReceivingLimit.class).limit(settings.receiveMaximum).clientInfo(clientInfo));
    }

    @Override
    public ProtocolResponse respondPubRateExceeded() {
        return farewell(
            MQTT5MessageBuilders.disconnect()
                .reasonCode(MQTT5DisconnectReasonCode.MessageRateToHigh)
                .build(),
            getLocal(ExceedPubRate.class).limit(settings.maxMsgPerSec).clientInfo(clientInfo));
    }

    @Override
    public ProtocolResponse validatePubMessage(MqttPublishMessage message) {
        if (message.fixedHeader().isRetain() && !settings.retainEnabled) {
            return farewell(
                MQTT5MessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.RetainNotSupported)
                    .build(),
                getLocal(ProtocolViolation.class)
                    .statement("MQTT5-3.2.2-14")
                    .clientInfo(clientInfo));
        }
        if (message.fixedHeader().qosLevel().value() > settings.maxQoS.getNumber()) {
            return farewell(
                MQTT5MessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.QoSNotSupported)
                    .build(),
                getLocal(ProtocolViolation.class)
                    .statement("MQTT5-3.2.2-11")
                    .clientInfo(clientInfo));
        }
        String topic = message.variableHeader().topicName();
        MqttProperties mqttProperties = message.variableHeader().properties();
        if (subscriptionIdentifier(mqttProperties).isPresent()) {
            return farewell(
                MQTT5MessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                    .reasonString("MQTT5-3.3.4-6")
                    .build(),
                getLocal(ProtocolViolation.class)
                    .statement("MQTT5-3.3.4-6")
                    .clientInfo(clientInfo));
        }
        // disconnect if malformed packet
        if (!isWellFormed(topic, SANITY_CHECK)) {
            return farewell(
                MQTT5MessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.MalformedPacket)
                    .build(),
                getLocal(MalformedTopic.class)
                    .topic(topic)
                    .clientInfo(clientInfo));
        }
        // disconnect if protocol error
        if (message.fixedHeader().qosLevel() == MqttQoS.AT_MOST_ONCE &&
            message.fixedHeader().isDup()) {
            // ignore the QoS = 0 Dup = 1 messages according to [MQTT-3.3.1-2]
            return farewell(
                MQTT5MessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                    .reasonString("MQTT5-3.3.1-2")
                    .build(),
                getLocal(ProtocolViolation.class)
                    .statement("MQTT5-3.3.1-2")
                    .clientInfo(clientInfo));
        }
        if (responseTopic(mqttProperties)
            .map(responseTopic -> !isWellFormed(topic, SANITY_CHECK)).orElse(false)) {
            return farewell(
                MQTT5MessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.MalformedPacket)
                    .build(),
                getLocal(ProtocolViolation.class)
                    .statement("MQTT5-3.2.2-13")
                    .clientInfo(clientInfo));
        }
        if (responseTopic(mqttProperties)
            .map(responseTopic -> !isValidTopic(responseTopic,
                settings.maxTopicLevelLength,
                settings.maxTopicLevels,
                settings.maxTopicLength)).orElse(false)) {
            return farewell(
                MQTT5MessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.TopicNameInvalid)
                    .build(),
                getLocal(ProtocolViolation.class)
                    .statement("MQTT5-3.2.2-14")
                    .clientInfo(clientInfo));
        }
        if (settings.payloadFormatValidationEnabled && isUTF8Payload(mqttProperties) &&
            !MQTTUtf8Util.isValidUTF8Payload(message.payload().nioBuffer())) {
            return switch (message.fixedHeader().qosLevel()) {
                case AT_MOST_ONCE -> farewell(
                    MQTT5MessageBuilders.disconnect()
                        .reasonCode(MQTT5DisconnectReasonCode.PayloadFormatInvalid)
                        .reasonString("MQTT5-3.3.2.3.2")
                        .build(),
                    getLocal(ProtocolViolation.class)
                        .statement("MQTT5-3.3.2.3.2")
                        .clientInfo(clientInfo));
                case AT_LEAST_ONCE -> response(MQTT5MessageBuilders.pubAck()
                    .packetId(message.variableHeader().packetId())
                    .reasonCode(MQTT5PubAckReasonCode.PayloadFormatInvalid)
                    .build());
                case EXACTLY_ONCE -> response(MQTT5MessageBuilders.pubRec()
                    .packetId(message.variableHeader().packetId())
                    .reasonCode(MQTT5PubRecReasonCode.PayloadFormatInvalid)
                    .build());
                default -> farewell(
                    MQTT5MessageBuilders.disconnect()
                        .reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                        .reasonString("Invalid QoS")
                        .build(),
                    getLocal(ProtocolViolation.class)
                        .statement("Invalid QoS")
                        .clientInfo(clientInfo));
            };
        }
        // process topic alias
        Optional<Integer> topicAlias = topicAlias(mqttProperties);
        if (settings.maxTopicAlias == 0 && topicAlias.isPresent()) {
            return farewell(
                MQTT5MessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.TopicAliasInvalid)
                    .build(),
                getLocal(ProtocolViolation.class)
                    .statement("MQTT5-3.2.2-18")
                    .clientInfo(clientInfo));
        }
        if (settings.maxTopicAlias > 0 && topicAlias.orElse(0) > settings.maxTopicAlias) {
            return farewell(
                MQTT5MessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.TopicAliasInvalid)
                    .build(),
                getLocal(ProtocolViolation.class)
                    .statement("MQTT5-3.2.2-17")
                    .clientInfo(clientInfo));
        }
        // create or update alias
        if (topic.isEmpty()) {
            if (topicAlias.isPresent()) {
                Optional<String> aliasedTopic = receiverTopicAliasManager.getTopic(topicAlias.get());
                if (aliasedTopic.isEmpty()) {
                    return farewell(
                        MQTT5MessageBuilders.disconnect()
                            .reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                            .reasonString("MQTT5-3.3.4")
                            .build(),
                        getLocal(ProtocolViolation.class)
                            .statement("MQTT5-3.3.4")
                            .clientInfo(clientInfo));
                }
            } else {
                return farewell(
                    MQTT5MessageBuilders.disconnect()
                        .reasonCode(MQTT5DisconnectReasonCode.ProtocolError)
                        .reasonString("MQTT5-3.3.4")
                        .build(),
                    getLocal(ProtocolViolation.class)
                        .statement("MQTT5-3.3.4")
                        .clientInfo(clientInfo));
            }
        } else {
            topicAlias.ifPresent(integer -> receiverTopicAliasManager.setAlias(topic, integer));
        }
        return null;
    }

    @Override
    public String getTopic(MqttPublishMessage message) {
        String topic = message.variableHeader().topicName();
        if (topic.isEmpty()) {
            MqttProperties pubMsgProperties = message.variableHeader().properties();
            // process topic alias
            Optional<Integer> topicAlias = topicAlias(pubMsgProperties);
            assert topicAlias.isPresent();
            Optional<String> aliasedTopic = receiverTopicAliasManager.getTopic(topicAlias.get());
            assert aliasedTopic.isPresent();
            topic = aliasedTopic.get();
        }
        return topic;
    }

    @Override
    public Message buildDistMessage(MqttPublishMessage message) {
        return MQTT5MessageUtils.toMessage(message);
    }

    @Override
    public ProtocolResponse onQoS0DistDenied(String topic, Message distMessage, CheckResult result) {
        assert !result.hasGranted();
        return switch (result.getTypeCase()) {
            case DENIED -> farewell(
                MQTT5MessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.NotAuthorized)
                    .reasonString(result.getDenied().hasReason() ? result.getDenied().getReason() : null)
                    .userProps(result.getDenied().getUserProps())
                    .build(),
                getLocal(NoPubPermission.class)
                    .topic(topic)
                    .qos(QoS.AT_MOST_ONCE)
                    .retain(distMessage.getIsRetain())
                    .clientInfo(clientInfo));
            case ERROR -> farewell(
                MQTT5MessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.UnspecifiedError)
                    .reasonString(result.getError().hasReason() ? result.getError().getReason() : null)
                    .userProps(result.getError().getUserProps())
                    .build(),
                getLocal(NoPubPermission.class)
                    .topic(topic)
                    .qos(QoS.AT_MOST_ONCE)
                    .retain(distMessage.getIsRetain())
                    .clientInfo(clientInfo));
            default -> farewell(
                MQTT5MessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.UnspecifiedError)
                    .build(),
                getLocal(NoPubPermission.class)
                    .topic(topic)
                    .qos(QoS.AT_MOST_ONCE)
                    .retain(distMessage.getIsRetain())
                    .clientInfo(clientInfo));
        };
    }

    @Override
    public ProtocolResponse onQoS1DistDenied(String topic, int packetId, Message distMessage, CheckResult result) {
        assert !result.hasGranted();
        return switch (result.getTypeCase()) {
            case DENIED -> response(MQTT5MessageBuilders.pubAck()
                .packetId(packetId)
                .reasonCode(MQTT5PubAckReasonCode.NotAuthorized)
                .reasonString(result.getDenied().hasReason() ? result.getDenied().getReason() : null)
                .userProps(result.getDenied().getUserProps())
                .build());
            case ERROR -> response(MQTT5MessageBuilders.pubAck()
                .packetId(packetId)
                .reasonCode(MQTT5PubAckReasonCode.UnspecifiedError)
                .reasonString(result.getError().hasReason() ? result.getError().getReason() : null)
                .userProps(result.getError().getUserProps())
                .build());
            default -> response(MQTT5MessageBuilders.pubAck()
                .packetId(packetId)
                .reasonCode(MQTT5PubAckReasonCode.UnspecifiedError)
                .build());
        };
    }

    @Override
    public MqttMessage onQoS1Disted(DistResult result, MqttPublishMessage message, UserProperties userProps) {
        int packetId = message.variableHeader().packetId();
        return switch (result) {
            case OK -> MQTT5MessageBuilders.pubAck()
                .packetId(packetId)
                .reasonCode(MQTT5PubAckReasonCode.Success)
                .userProps(userProps)
                .build();
            case NO_MATCH -> MQTT5MessageBuilders.pubAck()
                .packetId(packetId)
                .reasonCode(MQTT5PubAckReasonCode.NoMatchingSubscribers)
                .userProps(userProps)
                .build();
            case EXCEED_LIMIT -> MQTT5MessageBuilders.pubAck()
                .packetId(packetId)
                .reasonCode(MQTT5PubAckReasonCode.QuotaExceeded)
                // TODO: specify which quota in reason string
                .userProps(userProps)
                .build();
            case ERROR -> MQTT5MessageBuilders.pubAck()
                .packetId(packetId)
                .reasonCode(MQTT5PubAckReasonCode.UnspecifiedError)
                .userProps(userProps)
                .build();
        };
    }

    @Override
    public ProtocolResponse respondQoS2PacketInUse(MqttPublishMessage message) {
        return response(MQTT5MessageBuilders.pubRec()
            .packetId(message.variableHeader().packetId())
            .reasonCode(MQTT5PubRecReasonCode.PacketIdentifierInUse)
            .build());
    }

    @Override
    public ProtocolResponse onQoS2DistDenied(String topic, int packetId, Message distMessage, CheckResult result) {
        assert !result.hasGranted();
        return switch (result.getTypeCase()) {
            case DENIED -> response(MQTT5MessageBuilders.pubRec()
                .packetId(packetId)
                .reasonCode(MQTT5PubRecReasonCode.NotAuthorized)
                .reasonString(result.getDenied().hasReason() ? result.getDenied().getReason() : null)
                .userProps(result.getDenied().getUserProps())
                .build());
            case ERROR -> response(MQTT5MessageBuilders.pubRec()
                .packetId(packetId)
                .reasonCode(MQTT5PubRecReasonCode.UnspecifiedError)
                .reasonString(result.getError().hasReason() ? result.getError().getReason() : null)
                .userProps(result.getError().getUserProps())
                .build());
            default -> response(MQTT5MessageBuilders.pubRec()
                .packetId(packetId)
                .reasonCode(MQTT5PubRecReasonCode.UnspecifiedError)
                .build());
        };
    }

    @Override
    public MqttMessage onQoS2Disted(DistResult result, MqttPublishMessage message, UserProperties userProps) {
        int packetId = message.variableHeader().packetId();
        return switch (result) {
            case OK -> MQTT5MessageBuilders.pubRec()
                .packetId(packetId)
                .reasonCode(MQTT5PubRecReasonCode.Success)
                .userProps(userProps)
                .build();
            case NO_MATCH -> MQTT5MessageBuilders.pubRec()
                .packetId(packetId)
                .reasonCode(MQTT5PubRecReasonCode.NoMatchingSubscribers)
                .userProps(userProps)
                .build();
            case EXCEED_LIMIT -> MQTT5MessageBuilders.pubRec()
                .packetId(packetId)
                .reasonCode(MQTT5PubRecReasonCode.QuotaExceeded)
                // TODO: specify which quota in reason string
                .userProps(userProps)
                .build();
            case ERROR -> MQTT5MessageBuilders.pubRec()
                .packetId(packetId)
                .reasonCode(MQTT5PubRecReasonCode.UnspecifiedError)
                .userProps(userProps)
                .build();
        };
    }

    @Override
    public ProtocolResponse onIdleTimeout(int keepAliveTimeSeconds) {
        return farewellNow(
            MQTT5MessageBuilders.disconnect()
                .reasonCode(MQTT5DisconnectReasonCode.KeepAliveTimeout)
                .build(),
            getLocal(Idle.class)
                .keepAliveTimeSeconds(keepAliveTimeSeconds)
                .clientInfo(clientInfo));
    }
}
