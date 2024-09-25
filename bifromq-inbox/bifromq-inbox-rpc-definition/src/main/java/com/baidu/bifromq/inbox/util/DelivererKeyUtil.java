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

package com.baidu.bifromq.inbox.util;

import com.baidu.bifromq.sysprops.props.InboxDelivererNum;

public class DelivererKeyUtil {
    private static final int MAX_INBOX_DELIVERER = InboxDelivererNum.INSTANCE.get();

    public static String getSubInboxId(String inboxId, long incarnation) {
        return inboxId + "_" + incarnation;
    }

    public static long getIncarnation(String subInboxId) {
        return Long.parseUnsignedLong(subInboxId.substring(subInboxId.lastIndexOf("_")));
    }

    public static String getDelivererKey(String tenantId, String inboxId) {
        int k = inboxId.hashCode() % MAX_INBOX_DELIVERER;
        if (k < 0) {
            k = (k + MAX_INBOX_DELIVERER) % MAX_INBOX_DELIVERER;
        }
        return tenantId + k;
    }
}
