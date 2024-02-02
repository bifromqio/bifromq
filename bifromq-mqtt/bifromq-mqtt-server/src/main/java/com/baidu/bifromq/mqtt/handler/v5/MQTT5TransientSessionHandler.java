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

import com.baidu.bifromq.inbox.storage.proto.LWT;
import com.baidu.bifromq.mqtt.handler.IMQTTProtocolHelper;
import com.baidu.bifromq.mqtt.handler.MQTTTransientSessionHandler;
import com.baidu.bifromq.mqtt.handler.TenantSettings;
import com.baidu.bifromq.type.ClientInfo;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import javax.annotation.Nullable;
import lombok.Builder;

public class MQTT5TransientSessionHandler extends MQTTTransientSessionHandler {
    private final IMQTTProtocolHelper helper;

    @Builder
    protected MQTT5TransientSessionHandler(MqttConnectMessage connMsg,
                                           TenantSettings settings,
                                           String userSessionId,
                                           int keepAliveTimeSeconds,
                                           ClientInfo clientInfo,
                                           @Nullable LWT willMessage) {
        super(settings, userSessionId, keepAliveTimeSeconds, clientInfo, willMessage);
        this.helper = new MQTT5ProtocolHelper(connMsg, settings, clientInfo);
    }

    @Override
    protected IMQTTProtocolHelper helper() {
        return helper;
    }
}
