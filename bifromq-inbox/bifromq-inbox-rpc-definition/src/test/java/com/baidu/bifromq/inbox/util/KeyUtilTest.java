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
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import org.testng.annotations.Test;

public class KeyUtilTest {
    @Test
    public void buildAndParse() {
        String tenantId = "tenantA";
        String inboxId = "inboxA";
        long incarnation = System.nanoTime();
        ByteString tenantPrefix = KeyUtil.tenantPrefix(tenantId);
        ByteString inboxPrefix = KeyUtil.inboxPrefix(tenantId, inboxId);
        ByteString inboxKeyPrefix = KeyUtil.inboxKeyPrefix(tenantId, inboxId, incarnation);
        ByteString qos0MsgKey = KeyUtil.qos0InboxMsgKey(inboxKeyPrefix, 1);
        ByteString qos1MsgKey = KeyUtil.qos1InboxMsgKey(inboxKeyPrefix, 1);
        ByteString qos2InboxIndex = KeyUtil.qos2InboxIndex(inboxKeyPrefix, 1);

        assertTrue(inboxPrefix.startsWith(tenantPrefix));
        assertTrue(inboxKeyPrefix.startsWith(inboxPrefix));

        assertEquals(KeyUtil.parseTenantId(tenantPrefix), tenantId);
        assertEquals(KeyUtil.parseTenantId(inboxPrefix), tenantId);
        assertEquals(KeyUtil.parseTenantId(inboxKeyPrefix), tenantId);

        assertEquals(KeyUtil.parseInboxId(inboxPrefix), inboxId);
        assertEquals(KeyUtil.parseInboxId(inboxKeyPrefix), inboxId);

        assertEquals(KeyUtil.parseIncarnation(inboxKeyPrefix), incarnation);

        assertTrue(KeyUtil.isMetadataKey(inboxKeyPrefix));

        assertTrue(KeyUtil.isQoS0MessageKey(qos0MsgKey));
        assertEquals(KeyUtil.parseSeq(inboxKeyPrefix, qos0MsgKey), 1);
        assertEquals(KeyUtil.parseTenantId(qos0MsgKey), tenantId);
        assertEquals(KeyUtil.parseInboxId(qos0MsgKey), inboxId);
        assertEquals(KeyUtil.parseIncarnation(qos0MsgKey), incarnation);
        assertEquals(KeyUtil.parseInboxKeyPrefix(qos0MsgKey), inboxKeyPrefix);

        assertTrue(KeyUtil.isQoS1MessageKey(qos1MsgKey));
        assertEquals(KeyUtil.parseSeq(inboxKeyPrefix, qos1MsgKey), 1);
        assertEquals(KeyUtil.parseTenantId(qos1MsgKey), tenantId);
        assertEquals(KeyUtil.parseInboxId(qos1MsgKey), inboxId);
        assertEquals(KeyUtil.parseIncarnation(qos1MsgKey), incarnation);
        assertEquals(KeyUtil.parseInboxKeyPrefix(qos1MsgKey), inboxKeyPrefix);

        assertTrue(KeyUtil.isQoS2MessageIndexKey(qos2InboxIndex));
        assertTrue(KeyUtil.isQoS2MessageIndexKey(qos2InboxIndex, inboxKeyPrefix));
        assertEquals(KeyUtil.parseQoS2Index(inboxKeyPrefix, qos2InboxIndex), 1);
        assertEquals(KeyUtil.parseTenantId(qos2InboxIndex), tenantId);
        assertEquals(KeyUtil.parseInboxId(qos2InboxIndex), inboxId);
        assertEquals(KeyUtil.parseIncarnation(qos2InboxIndex), incarnation);
        assertEquals(KeyUtil.parseInboxKeyPrefix(qos2InboxIndex), inboxKeyPrefix);
    }
}
