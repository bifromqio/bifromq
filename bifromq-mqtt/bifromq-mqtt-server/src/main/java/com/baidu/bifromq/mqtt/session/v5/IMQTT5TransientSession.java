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

package com.baidu.bifromq.mqtt.session.v5;

import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.util.concurrent.CompletableFuture;

public interface IMQTT5TransientSession extends IMQTT5Session {

    boolean publish(SubInfo subInfo, TopicMessagePack messagePack);

    CompletableFuture<MqttQoS> subscribe(long reqId, String topicFilter, MqttQoS qos);

    CompletableFuture<Boolean> unsubscribe(long reqId, String topicFilter);
}
