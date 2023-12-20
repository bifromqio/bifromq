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

package com.baidu.bifromq.mqtt;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class MQTTBrokerOptions {
    private int connectTimeoutSeconds = 20;
    private int connectRateLimit = 1000;
    private int disconnectRate = 1000;
    private int maxResendTimes = 5;
    private int resendDelayMillis = 3000;
    private int defaultKeepAliveSeconds = 5 * 60; // 5 min
    private int qos2ConfirmWindowSeconds = 5;
    private long writeLimit = 512 * 1024;
    private long readLimit = 512 * 1024;
    private int maxBytesInMessage = 256 * 1024;
}
