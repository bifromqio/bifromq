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

import com.baidu.bifromq.inbox.record.InboxInstance;
import com.baidu.bifromq.sysprops.props.InboxDelivererNum;

public class InboxServiceUtil {
    private static final int MAX_INBOX_DELIVERER = InboxDelivererNum.INSTANCE.get();
    private static final String SEPARATOR = "_";

    public static String receiverId(String inboxId, long incarnation) {
        return inboxId + SEPARATOR + incarnation;
    }

    public static InboxInstance parseReceiverId(String receiverId) {
        int splitAt = receiverId.lastIndexOf(SEPARATOR);
        return new InboxInstance(receiverId.substring(0, splitAt),
            Long.parseUnsignedLong(receiverId.substring(splitAt + 1)));
    }

    public static String parseTenantId(String delivererKey) {
        return delivererKey.substring(0, delivererKey.lastIndexOf("_"));
    }

    public static String getDelivererKey(String tenantId, String inboxId) {
        int k = inboxId.hashCode() % MAX_INBOX_DELIVERER;
        if (k < 0) {
            k = (k + MAX_INBOX_DELIVERER) % MAX_INBOX_DELIVERER;
        }
        return tenantId + "_" + k;
    }
}
