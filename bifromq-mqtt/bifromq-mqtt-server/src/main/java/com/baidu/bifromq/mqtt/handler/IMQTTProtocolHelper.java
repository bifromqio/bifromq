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

package com.baidu.bifromq.mqtt.handler;

import com.baidu.bifromq.dist.client.DistResult;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterOption;
import com.baidu.bifromq.mqtt.handler.record.GoAway;
import com.baidu.bifromq.mqtt.handler.record.ResponseOrGoAway;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.UserProperties;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import java.util.List;
import java.util.Optional;

public interface IMQTTProtocolHelper {
    record SubTask(String topicFilter, TopicFilterOption option, UserProperties userProperties) {
    }

    enum SubResult {
        OK,
        EXISTS,
        NO_INBOX,
        EXCEED_LIMIT,
        NOT_AUTHORIZED,
        TOPIC_FILTER_INVALID,
        ERROR;
    }

    enum UnsubResult {
        OK,
        NO_SUB,
        NO_INBOX,
        NOT_AUTHORIZED,
        TOPIC_FILTER_INVALID,
        ERROR;
    }

    UserProperties getUserProps(MqttPublishMessage mqttMessage);

    UserProperties getUserProps(MqttUnsubscribeMessage mqttMessage);

    boolean checkPacketIdUsage();

    GoAway onInboxTransientError();

    Optional<Integer> sessionExpiryIntervalOnDisconnect(MqttMessage disconnectMessage);

    GoAway onDisconnect();

    GoAway respondDisconnectProtocolError();

    boolean isNormalDisconnect(MqttMessage message);

    boolean isDisconnectWithLWT(MqttMessage message);

    GoAway respondDecodeError(MqttMessage message);

    GoAway respondDuplicateConnect(MqttConnectMessage message);

    GoAway validateSubMessage(MqttSubscribeMessage message);

    List<SubTask> getSubTask(MqttSubscribeMessage message);

    MqttSubAckMessage buildSubAckMessage(MqttSubscribeMessage subMessage, List<SubResult> results);

    MqttSubAckMessage respondPacketIdInUse(MqttSubscribeMessage message);

    GoAway validateUnsubMessage(MqttUnsubscribeMessage message);

    MqttUnsubAckMessage respondPacketIdInUse(MqttUnsubscribeMessage message);

    MqttUnsubAckMessage buildUnsubAckMessage(MqttUnsubscribeMessage unsubMessage, List<UnsubResult> results);

    MqttMessage onPubRelReceived(MqttMessage message, boolean packetIdFound);

    boolean isQoS2Received(MqttMessage message);

    ResponseOrGoAway respondPubRecMsg(MqttMessage message, boolean packetIdNotFound);

    int clientReceiveMaximum();

    GoAway onKick(ClientInfo kicker);

    MqttPublishMessage buildMqttPubMessage(int packetId, MQTTSessionHandler.SubMessage message);

    GoAway respondReceivingMaximumExceeded();

    GoAway respondPubRateExceeded();

    GoAway validatePubMessage(MqttPublishMessage message);

    String getTopic(MqttPublishMessage message);

    Message buildDistMessage(MqttPublishMessage message);

    GoAway onQoS0DistDenied(String topic, Message distMessage);

    ResponseOrGoAway onQoS1DistDenied(String topic, int packetId, Message distMessage);

    MqttMessage onQoS1Disted(DistResult result, MqttPublishMessage message);

    ResponseOrGoAway respondQoS2PacketInUse(MqttPublishMessage message);

    ResponseOrGoAway onQoS2DistDenied(String topic, int packetId, Message distMessage);

    MqttMessage onQoS2Disted(DistResult result, MqttPublishMessage message);

    GoAway onIdleTimeout(int keepAliveTimeSeconds);
}
