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

package com.baidu.bifromq.sysprops.props;

import com.baidu.bifromq.sysprops.BifroMQSysProp;
import com.baidu.bifromq.sysprops.parser.BooleanParser;

/**
 * further check if utf8 string contains any control character or non character according to [MQTT-1.5.3]
 */
public class SanityCheckMqttUtf8String extends BifroMQSysProp<Boolean, BooleanParser> {
    public static final SanityCheckMqttUtf8String INSTANCE = new SanityCheckMqttUtf8String();

    private SanityCheckMqttUtf8String() {
        super("mqtt_utf8_sanity_check", false, BooleanParser.INSTANCE);
    }
}
