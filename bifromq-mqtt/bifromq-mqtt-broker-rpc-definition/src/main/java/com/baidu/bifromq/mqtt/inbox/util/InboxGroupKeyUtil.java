/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

import static com.baidu.bifromq.sysprops.BifroMQSysProp.MQTT_INBOXGROUPS_PER_SERVER;

public class InboxGroupKeyUtil {
    private static final int INBOX_GROUPS = MQTT_INBOXGROUPS_PER_SERVER.get();

    public static String toInboxGroupKey(String inboxId, String serverId) {
        return serverId + ":" + groupIdx(inboxId);
    }

    public static String parseServerId(String inboxGroupKey) {
        return inboxGroupKey.substring(0, inboxGroupKey.lastIndexOf(":"));
    }

    private static int groupIdx(String inboxId) {
        int idx = inboxId.hashCode() % INBOX_GROUPS;
        if (idx < 0) {
            idx = (idx + INBOX_GROUPS) % INBOX_GROUPS;
        }
        return idx;
    }
}
