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

import com.baidu.bifromq.inbox.storage.proto.TopicFilterOption;
import com.baidu.bifromq.type.Message;
import io.netty.handler.codec.mqtt.MqttPublishMessage;

public class MQTTMessageSizer {

    public static int sizePublishMsg(String topic, Message msg) {
        return sizePublishMsg(topic, msg.getPayload().size());
    }

    public static int sizePublishMsg(String topic, int payloadSize) {
        // MQTTPublishMessage: FixedHeaderSize = 2, VariableHeaderSize = 4 + topicBytes
        return topic.length() + payloadSize + 6;
    }
}
