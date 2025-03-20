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
    // Fixed width for the incarnation field, e.g. 20 digits
    private static final int INCARNATION_LENGTH = 20;
    // Fixed width for the deliverer number field, e.g. 5 digits
    private static final int DELIVERER_KEY_WIDTH = 5;

    public static String receiverId(String inboxId, long incarnation) {
        return inboxId + SEPARATOR + String.format("%0" + INCARNATION_LENGTH + "d", incarnation);
    }

    public static InboxInstance parseReceiverId(String receiverId) {
        // The incarnation is the last INCARNATION_LENGTH characters.
        int incarnationStart = receiverId.length() - INCARNATION_LENGTH;
        // The separator is at incarnationStart - 1.
        String inboxId = receiverId.substring(0, incarnationStart - 1);
        long incarnation = Long.parseUnsignedLong(receiverId.substring(incarnationStart));
        return new InboxInstance(inboxId, incarnation);
    }

    public static String parseTenantId(String delivererKey) {
        // The bucket field is fixed width (DELIVERER_KEY_WIDTH) and the separator takes one char.
        int tenantIdLength = delivererKey.length() - (DELIVERER_KEY_WIDTH + 1);
        return delivererKey.substring(0, tenantIdLength);
    }

    public static String getDelivererKey(String tenantId, String inboxId) {
        int k = inboxId.hashCode() % MAX_INBOX_DELIVERER;
        if (k < 0) {
            k = (k + MAX_INBOX_DELIVERER) % MAX_INBOX_DELIVERER;
        }
        return tenantId + SEPARATOR + String.format("%0" + DELIVERER_KEY_WIDTH + "d", k);
    }
}
