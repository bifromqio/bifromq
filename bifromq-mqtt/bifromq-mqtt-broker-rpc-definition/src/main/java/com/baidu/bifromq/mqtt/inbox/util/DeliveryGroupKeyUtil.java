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

package com.baidu.bifromq.mqtt.inbox.util;

import static com.baidu.bifromq.sysprops.BifroMQSysProp.MQTT_DELIVERERS_PER_SERVER;

public class DeliveryGroupKeyUtil {
    private static final int INBOX_GROUPS = MQTT_DELIVERERS_PER_SERVER.get();

    public static String toDelivererKey(String inboxId, String serverId) {
        return serverId + ":" + groupIdx(inboxId);
    }

    public static String parseServerId(String delivererKey) {
        return delivererKey.substring(0, delivererKey.lastIndexOf(":"));
    }

    private static int groupIdx(String inboxId) {
        int idx = inboxId.hashCode() % INBOX_GROUPS;
        if (idx < 0) {
            idx = (idx + INBOX_GROUPS) % INBOX_GROUPS;
        }
        return idx;
    }
}
