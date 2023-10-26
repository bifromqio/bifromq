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

package com.baidu.bifromq.inbox.util;

import static com.baidu.bifromq.inbox.util.KeyUtil.hasScopedInboxId;
import static com.baidu.bifromq.inbox.util.KeyUtil.tenantPrefix;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import org.testng.annotations.Test;

public class KeyUtilTest {

    @Test
    public void testHasScopedInboxId() {
        assertFalse(hasScopedInboxId(ByteString.empty()));
        assertFalse(hasScopedInboxId(tenantPrefix("test")));
        assertTrue(hasScopedInboxId(KeyUtil.scopedInboxId("test", "inbox")));
        assertTrue(hasScopedInboxId(KeyUtil.qos0InboxMsgKey(KeyUtil.scopedInboxId("test", "inbox"), 1)));
        assertTrue(hasScopedInboxId(KeyUtil.qos1InboxMsgKey(KeyUtil.scopedInboxId("test", "inbox"), 1)));
        assertTrue(hasScopedInboxId(
            KeyUtil.qos2InboxMsgKey(KeyUtil.scopedInboxId("test", "inbox"), ByteString.copyFromUtf8("1"))));
        assertTrue(hasScopedInboxId(KeyUtil.qos2InboxIndex(KeyUtil.scopedInboxId("test", "inbox"), 1)));
    }
}
