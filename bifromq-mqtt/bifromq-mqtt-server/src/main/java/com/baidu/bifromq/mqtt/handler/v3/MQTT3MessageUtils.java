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

package com.baidu.bifromq.mqtt.handler.v3;

import static com.baidu.bifromq.dist.client.ByteBufUtil.toRetainedByteBuffer;

import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.google.protobuf.ByteString;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;

public final class MQTT3MessageUtils {

    static Message toMessage(long msgId, MqttPublishMessage pubMsg, long nowMillis) {
        return toMessage(msgId,
            pubMsg.fixedHeader().qosLevel(),
            pubMsg.fixedHeader().isRetain(),
            toRetainedByteBuffer(pubMsg.payload()),
            nowMillis);
    }

    static Message toMessage(long msgId,
                             MqttQoS pubQoS,
                             boolean isRetain,
                             ByteString payload,
                             long nowMillis) {
        return Message.newBuilder()
            .setMessageId(msgId)
            .setPubQoS(QoS.forNumber(pubQoS.value()))
            .setPayload(payload)
            .setTimestamp(nowMillis)
            // MessageExpiryInterval
            .setExpiryInterval(Integer.MAX_VALUE)
            .setIsRetain(isRetain)
            .build();
    }
}
