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

import com.baidu.bifromq.type.QoS;
import com.google.protobuf.ByteString;
import io.netty.handler.codec.mqtt.MqttProperties;
import java.util.Map;
import lombok.Builder;

@Builder
public class MQTT5WillMessage {
    public final String topic;
    public final QoS qos;
    public final boolean retain;
    public final ByteString payload;
    public final MqttProperties userProperties;
}
