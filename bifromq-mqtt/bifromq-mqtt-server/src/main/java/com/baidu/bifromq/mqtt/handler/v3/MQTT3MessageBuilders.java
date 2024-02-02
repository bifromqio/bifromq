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

package com.baidu.bifromq.mqtt.handler.v3;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;

public class MQTT3MessageBuilders {
    public static PubRecBuilder pubRec() {
        return new PubRecBuilder();
    }

    public static PubRelBuilder pubRel() {
        return new PubRelBuilder();
    }

    public static PubCompBuilder pubComp() {
        return new PubCompBuilder();
    }


    public static final class PubRecBuilder {
        private int packetId;

        PubRecBuilder() {
        }

        public PubRecBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader fixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 2);
            MqttMessageIdVariableHeader varHeader = MqttMessageIdVariableHeader.from(packetId);
            return new MqttMessage(fixedHeader, varHeader);
        }
    }

    public static final class PubRelBuilder {
        private int packetId;

        PubRelBuilder() {
        }

        public PubRelBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader fixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_MOST_ONCE, false, 2);
            MqttMessageIdVariableHeader varHeader = MqttMessageIdVariableHeader.from(packetId);
            return new MqttMessage(fixedHeader, varHeader);
        }
    }

    public static final class PubCompBuilder {
        private int packetId;

        PubCompBuilder() {
        }

        public PubCompBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader fixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 2);
            MqttMessageIdVariableHeader varHeader = MqttMessageIdVariableHeader.from(packetId);
            return new MqttMessage(fixedHeader, varHeader);
        }
    }

}
