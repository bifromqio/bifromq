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

package com.baidu.bifromq.inbox.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.inbox.record.InboxInstance;
import com.baidu.bifromq.sysprops.props.InboxDelivererNum;
import org.testng.annotations.Test;

public class InboxServiceUtilTest {
    private static final String TENANT_ID = "tenant1";
    private static final String INBOX_ID = "inbox123";

    @Test
    void testParseReceiverId() {
        String inboxId = "InboxId";
        long inc = 12345;
        String receiverId = InboxServiceUtil.receiverId(inboxId, inc);
        InboxInstance inboxInstance = InboxServiceUtil.parseReceiverId(receiverId);
        assertEquals(inboxInstance.inboxId(), inboxId);
        assertEquals(inboxInstance.receiverId(), receiverId);
        assertEquals(inboxInstance.incarnation(), inc);
    }

    @Test
    void testParseTenantId() {
        String delivererKey = InboxServiceUtil.getDelivererKey(TENANT_ID, INBOX_ID);
        String tenantId = InboxServiceUtil.parseTenantId(delivererKey);
        assertEquals(tenantId, TENANT_ID);
    }

    @Test
    void testGetDelivererKey() {
        String delivererKey = InboxServiceUtil.getDelivererKey(TENANT_ID, INBOX_ID);
        assertNotNull(delivererKey);
        assertEquals(delivererKey.substring(0, delivererKey.lastIndexOf("_")), TENANT_ID);

        String[] parts = delivererKey.split("_");
        int k = Integer.parseInt(parts[1]);
        int maxDeliverer = InboxDelivererNum.INSTANCE.get();
        assertTrue(k >= 0 && k < maxDeliverer);
    }
}
