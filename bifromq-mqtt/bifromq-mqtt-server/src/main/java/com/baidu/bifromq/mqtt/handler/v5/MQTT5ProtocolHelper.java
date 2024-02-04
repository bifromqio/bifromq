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

import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.receiveMaximum;
import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.responseTopic;
import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.toUserProperties;
import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.topicAlias;
import static com.baidu.bifromq.mqtt.handler.v5.MQTT5MessageUtils.topicAliasMaximum;
import static com.baidu.bifromq.mqtt.utils.MQTTUtf8Util.isWellFormed;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.util.TopicUtil.isValidTopic;

import com.baidu.bifromq.dist.client.DistResult;
import com.baidu.bifromq.inbox.storage.proto.RetainHandling;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterOption;
import com.baidu.bifromq.mqtt.handler.IMQTTProtocolHelper;
import com.baidu.bifromq.mqtt.handler.MQTTSessionHandler;
import com.baidu.bifromq.mqtt.handler.TenantSettings;
import com.baidu.bifromq.mqtt.handler.record.GoAway;
import com.baidu.bifromq.mqtt.handler.record.ResponseOrGoAway;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5DisconnectReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5PubAckReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5PubCompReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5PubRecReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5PubRelReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5SubAckReasonCode;
import com.baidu.bifromq.mqtt.handler.v5.reason.MQTT5UnsubAckReasonCode;
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
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
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
    public GoAway onInboxTransientError() {
        return new GoAway(
            MqttMessageBuilders.disconnect()
                .reasonCode(MQTT5DisconnectReasonCode.UnspecifiedError.value())
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
    public GoAway onDisconnect() {
        return GoAway.now(
            MqttMessageBuilders.disconnect()
                .reasonCode(MQTT5DisconnectReasonCode.ServerShuttingDown.value())
                .build(),
            getLocal(ByServer.class).clientInfo(clientInfo));
    }

    @Override
    public GoAway respondDisconnectProtocolError() {
        return new GoAway(MqttMessageBuilders.disconnect()
            .reasonCode(MQTT5DisconnectReasonCode.ProtocolError.value()) // Protocol Error
            .build(),
            getLocal(ProtocolViolation.class).statement("MQTT5-3.14.2.2.2")
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
    public GoAway respondDecodeError(MqttMessage message) {
        if (message.decoderResult().cause() instanceof TooLongFrameException) {
            return new GoAway(MqttMessageBuilders.disconnect()
                .reasonCode(MQTT5DisconnectReasonCode.PacketTooLarge.value())
                .build(),
                getLocal(BadPacket.class)
                    .cause(message.decoderResult().cause())
                    .clientInfo(clientInfo));
        }
        MqttProperties properties = new MqttProperties();
        properties.add(new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
            message.decoderResult().cause().getMessage()));
        return new GoAway(MqttMessageBuilders.disconnect()
            .reasonCode(MQTT5DisconnectReasonCode.MalformedPacket.value())
            .properties(properties)
            .build(),
            getLocal(BadPacket.class)
                .cause(message.decoderResult().cause())
                .clientInfo(clientInfo));
    }

    @Override
    public GoAway respondDuplicateConnect(MqttConnectMessage message) {
        return new GoAway(
            MqttMessageBuilders.disconnect()
                .reasonCode(MQTT5DisconnectReasonCode.ProtocolError.value())
                .build(),
            getLocal(ProtocolViolation.class)
                .statement("MQTT5-3.1.0-2")
                .clientInfo(clientInfo));
    }

    @Override
    public GoAway validateSubMessage(MqttSubscribeMessage message) {
        List<MqttTopicSubscription> topicSubscriptions = message.payload().topicSubscriptions();
        if (topicSubscriptions.isEmpty()) {
            // Ignore instead of disconnect [MQTT-3.8.3-3]
            return new GoAway(MqttMessageBuilders.disconnect()
                .reasonCode(MQTT5DisconnectReasonCode.ProtocolError.value())
                .build(),
                getLocal(ProtocolViolation.class)
                    .statement("MQTT5-3.8.3-2")
                    .clientInfo(clientInfo));
        }
        Optional<Integer> subId = Optional.ofNullable(
                (MqttProperties.IntegerProperty) message.idAndPropertiesVariableHeader().properties()
                    .getProperty(MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER.value()))
            .map(MqttProperties.MqttProperty::value);
        if (subId.isPresent() && subId.get() == 0) {
            return new GoAway(
                MqttMessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.ProtocolError.value())
                    .build(),
                getLocal(ProtocolViolation.class)
                    .statement("MQTT5-3.8.2.1.2")
                    .clientInfo(clientInfo));
        }
        if (topicSubscriptions.size() > settings.maxTopicFiltersPerSub) {
            return new GoAway(
                MqttMessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.AdministrativeAction.value())
                    .build(),
                getLocal(TooLargeSubscription.class)
                    .actual(topicSubscriptions.size())
                    .max(settings.maxTopicFiltersPerSub)
                    .clientInfo(clientInfo));
        }
        for (MqttTopicSubscription topicSub : topicSubscriptions) {
            if (!isWellFormed(topicSub.topicName(), SANITY_CHECK)) {
                return new GoAway(
                    MqttMessageBuilders.disconnect()
                        .reasonCode(MQTT5DisconnectReasonCode.MalformedPacket.value())
                        .build(),
                    getLocal(MalformedTopicFilter.class)
                        .topicFilter(topicSub.topicName())
                        .clientInfo(clientInfo));
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
    public GoAway validateUnsubMessage(MqttUnsubscribeMessage message) {
        int packetId = message.idAndPropertiesVariableHeader().messageId();
        List<String> topicFilters = message.payload().topics();
        if (topicFilters.isEmpty()) {
            // Ignore instead of disconnect [3.10.3-2]
            return new GoAway(
                MqttMessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.ProtocolError.value())
                    .build(),
                getLocal(ProtocolViolation.class)
                    .statement("MQTT-3.10.3-2")
                    .clientInfo(clientInfo));
        }
        if (topicFilters.size() > settings.maxTopicFiltersPerSub) {
            return new GoAway(
                MqttMessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.AdministrativeAction.value())
                    .build(),
                getLocal(TooLargeUnsubscription.class)
                    .max(settings.maxTopicFiltersPerSub)
                    .actual(topicFilters.size())
                    .clientInfo(clientInfo));
        }

        for (String topicFilter : topicFilters) {
            if (!isWellFormed(topicFilter, SANITY_CHECK)) {
                return new GoAway(
                    MqttMessageBuilders.disconnect()
                        .reasonCode(MQTT5DisconnectReasonCode.MalformedPacket.value())
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
        return MqttMessageBuilders.unsubAck()
            .packetId(message.variableHeader().messageId())
            .addReasonCodes(message.payload().topics().stream()
                .map(v -> MQTT5UnsubAckReasonCode.PacketIdentifierInUse.value())
                .toArray(Short[]::new))
            .build();
    }

    @Override
    public MqttUnsubAckMessage buildUnsubAckMessage(MqttUnsubscribeMessage unsubMessage, List<UnsubResult> results) {
        Short[] reasonCodes = results.stream().map(result -> switch (result) {
                case OK -> MQTT5UnsubAckReasonCode.Success;
                case NO_SUB -> MQTT5UnsubAckReasonCode.NoSubscriptionExisted;
                case TOPIC_FILTER_INVALID -> MQTT5UnsubAckReasonCode.TopicFilterInvalid;
                case NOT_AUTHORIZED -> MQTT5UnsubAckReasonCode.NotAuthorized;
                default -> MQTT5UnsubAckReasonCode.UnspecifiedError;
            })
            .map(MQTT5UnsubAckReasonCode::value)
            .toArray(Short[]::new);

        return MqttMessageBuilders.unsubAck()
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
    public ResponseOrGoAway respondPubRecMsg(MqttMessage message, boolean packetIdNotFound) {
        MqttPubReplyMessageVariableHeader variableHeader =
            (MqttPubReplyMessageVariableHeader) message.variableHeader();
        int packetId = variableHeader.messageId();
        if (packetIdNotFound) {
            return new ResponseOrGoAway(new GoAway(MQTT5MessageBuilders.pubRel()
                .packetId(packetId)
                .reasonCode(
                    MQTT5PubRelReasonCode.PacketIdentifierNotFound)
                .build(),
                getLocal(ProtocolViolation.class).statement("MQTT5-4.3.3-8").clientInfo(clientInfo)));
        } else {
            return new ResponseOrGoAway(MQTT5MessageBuilders.pubRel()
                .packetId(packetId)
                .reasonCode(
                    MQTT5PubRelReasonCode.Success)
                .build());
        }
    }

    @Override
    public int clientReceiveMaximum() {
        return clientReceiveMaximum;
    }

    @Override
    public GoAway onKick(ClientInfo kicker) {
        return GoAway.now(
            MqttMessageBuilders.disconnect().reasonCode(MQTT5DisconnectReasonCode.SessionTakenOver.value()).build(),
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
    public GoAway respondReceivingMaximumExceeded() {
        return new GoAway(
            MqttMessageBuilders.disconnect()
                .reasonCode(MQTT5DisconnectReasonCode.ReceiveMaximumExceeded.value())
                .build(),
            getLocal(ExceedReceivingLimit.class).limit(settings.receiveMaximum).clientInfo(clientInfo));
    }

    @Override
    public GoAway respondPubRateExceeded() {
        return new GoAway(
            MqttMessageBuilders.disconnect()
                .reasonCode(MQTT5DisconnectReasonCode.MessageRateToHigh.value())
                .build(),
            getLocal(ExceedPubRate.class).limit(settings.maxMsgPerSec).clientInfo(clientInfo));
    }

    @Override
    public GoAway validatePubMessage(MqttPublishMessage message) {
        String topic = message.variableHeader().topicName();
        MqttProperties mqttProperties = message.variableHeader().properties();
        // disconnect if malformed packet
        if (!isWellFormed(topic, SANITY_CHECK)) {
            return new GoAway(
                MqttMessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.MalformedPacket.value())
                    .build(),
                getLocal(MalformedTopic.class)
                    .topic(topic)
                    .clientInfo(clientInfo));
        }
        // disconnect if protocol error
        if (message.fixedHeader().qosLevel() == MqttQoS.AT_MOST_ONCE &&
            message.fixedHeader().isDup()) {
            // ignore the QoS = 0 Dup = 1 messages according to [MQTT-3.3.1-2]
            return new GoAway(
                MqttMessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.ProtocolError.value())
                    .build(),
                getLocal(ProtocolViolation.class)
                    .statement("MQTT5-3.3.1-2")
                    .clientInfo(clientInfo));
        }
        if (responseTopic(mqttProperties)
            .map(responseTopic -> !isWellFormed(topic, SANITY_CHECK)).orElse(false)) {
            return new GoAway(
                MqttMessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.TopicNameInvalid.value())
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
            return new GoAway(
                MqttMessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.TopicNameInvalid.value())
                    .build(),
                getLocal(ProtocolViolation.class)
                    .statement("MQTT5-3.2.2-14")
                    .clientInfo(clientInfo));
        }
        // process topic alias
        Optional<Integer> topicAlias = topicAlias(mqttProperties);
        if (settings.maxTopicAlias == 0 && topicAlias.isPresent()) {
            return new GoAway(
                MqttMessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.TopicAliasInvalid.value())
                    .build(),
                getLocal(ProtocolViolation.class)
                    .statement("MQTT5-3.2.2-18")
                    .clientInfo(clientInfo));
        }
        if (settings.maxTopicAlias > 0 && topicAlias.orElse(0) > settings.maxTopicAlias) {
            return new GoAway(
                MqttMessageBuilders.disconnect()
                    .reasonCode(MQTT5DisconnectReasonCode.TopicAliasInvalid.value())
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
                    return new GoAway(
                        MqttMessageBuilders.disconnect()
                            .reasonCode(MQTT5DisconnectReasonCode.ProtocolError.value())
                            .build(),
                        getLocal(ProtocolViolation.class)
                            .statement("MQTT5-3.3.4")
                            .clientInfo(clientInfo));
                }
            } else {
                return new GoAway(
                    MqttMessageBuilders.disconnect()
                        .reasonCode(MQTT5DisconnectReasonCode.ProtocolError.value())
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
    public GoAway onQoS0DistDenied(String topic, Message distMessage) {
        return new GoAway(
            MqttMessageBuilders.disconnect()
                .reasonCode(MQTT5DisconnectReasonCode.NotAuthorized.value())
                .build(),
            getLocal(NoPubPermission.class)
                .topic(topic)
                .qos(QoS.AT_MOST_ONCE)
                .retain(distMessage.getIsRetain())
                .clientInfo(clientInfo));
    }

    @Override
    public ResponseOrGoAway onQoS1DistDenied(String topic, int packetId, Message distMessage) {
        return new ResponseOrGoAway(MqttMessageBuilders.pubAck()
            .packetId(packetId)
            .reasonCode(MQTT5PubAckReasonCode.NotAuthorized.value())
            .build());
    }

    @Override
    public MqttMessage onQoS1Disted(DistResult result, MqttPublishMessage message) {
        int packetId = message.variableHeader().packetId();
        return switch (result) {
            case OK -> MqttMessageBuilders.pubAck()
                .packetId(packetId)
                .reasonCode(MQTT5PubAckReasonCode.Success.value())
                .build();
            case NO_MATCH -> MqttMessageBuilders.pubAck()
                .packetId(packetId)
                .reasonCode(MQTT5PubAckReasonCode.NoMatchingSubscribers.value())
                .build();
            case EXCEED_LIMIT -> MqttMessageBuilders.pubAck()
                .packetId(packetId)
                .reasonCode(MQTT5PubAckReasonCode.QuotaExceeded.value())
                .build();
            case ERROR -> MqttMessageBuilders.pubAck()
                .packetId(packetId)
                .reasonCode(MQTT5PubAckReasonCode.UnspecifiedError.value())
                .build();
        };
    }

    @Override
    public ResponseOrGoAway respondQoS2PacketInUse(MqttPublishMessage message) {
        return new ResponseOrGoAway(MQTT5MessageBuilders.pubRec()
            .packetId(message.variableHeader().packetId())
            .reasonCode(MQTT5PubRecReasonCode.PacketIdentifierInUse)
            .build());
    }

    @Override
    public ResponseOrGoAway onQoS2DistDenied(String topic, int packetId, Message distMessage) {
        return new ResponseOrGoAway(MQTT5MessageBuilders.pubRec()
            .packetId(packetId)
            .reasonCode(MQTT5PubRecReasonCode.NotAuthorized)
            .build());
    }

    @Override
    public MqttMessage onQoS2Disted(DistResult result, MqttPublishMessage message) {
        int packetId = message.variableHeader().packetId();
        return switch (result) {
            case OK -> MQTT5MessageBuilders.pubRec()
                .packetId(packetId)
                .reasonCode(MQTT5PubRecReasonCode.Success)
                .build();
            case NO_MATCH -> MQTT5MessageBuilders.pubRec()
                .packetId(packetId)
                .reasonCode(MQTT5PubRecReasonCode.NoMatchingSubscribers)
                .build();
            case EXCEED_LIMIT -> MQTT5MessageBuilders.pubRec()
                .packetId(packetId)
                .reasonCode(MQTT5PubRecReasonCode.QuotaExceeded)
                .build();
            case ERROR -> MQTT5MessageBuilders.pubRec()
                .packetId(packetId)
                .reasonCode(MQTT5PubRecReasonCode.UnspecifiedError)
                .build();
        };
    }

    @Override
    public GoAway onIdleTimeout(int keepAliveTimeSeconds) {
        return GoAway.now(
            MqttMessageBuilders.disconnect()
                .reasonCode(MQTT5DisconnectReasonCode.KeepAliveTimeout.value())
                .build(),
            getLocal(Idle.class)
                .keepAliveTimeSeconds(keepAliveTimeSeconds)
                .clientInfo(clientInfo));
    }
}
