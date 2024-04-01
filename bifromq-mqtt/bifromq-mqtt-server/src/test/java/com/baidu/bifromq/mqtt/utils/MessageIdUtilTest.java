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

package com.baidu.bifromq.mqtt.utils;

import static com.baidu.bifromq.mqtt.utils.MessageIdUtil.isSuccessive;
import static com.baidu.bifromq.mqtt.utils.MessageIdUtil.messageId;
import static com.baidu.bifromq.mqtt.utils.MessageIdUtil.messageSequence;
import static com.baidu.bifromq.mqtt.utils.MessageIdUtil.previousMessageId;
import static com.baidu.bifromq.mqtt.utils.MessageIdUtil.syncWindowSequence;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class MessageIdUtilTest {
    @Test
    public void testSyncWindowSequence() {
        long nowMillis = 1000;
        long syncWindowIntervalMillis = 100;
        long result = syncWindowSequence(nowMillis, syncWindowIntervalMillis);
        assertEquals(result, 10);
    }

    @Test
    public void testMessageSequence() {
        long syncWindowSequence = 100;
        long msgSeq = 10;
        long msgId = messageId(syncWindowSequence, msgSeq);
        assertEquals(msgSeq, messageSequence(msgId));
        assertEquals(syncWindowSequence, syncWindowSequence(msgId));
    }

    @Test
    public void testSuccessive() {
        assertTrue(isSuccessive(messageId(0, 1), messageId(0, 2)));
        assertTrue(isSuccessive(messageId(0, 1), messageId(1, 2)));
        assertTrue(isSuccessive(messageId(0, 1), messageId(3, 0)));

        assertFalse(isSuccessive(messageId(0, 2), messageId(0, 1)));
        assertFalse(isSuccessive(messageId(1, 2), messageId(0, 1)));
        assertFalse(isSuccessive(messageId(3, 0), messageId(0, 1)));

        assertFalse(isSuccessive(messageId(0, 1), messageId(0, 3)));
        assertFalse(isSuccessive(messageId(0, 1), messageId(1, 0)));
    }

    @Test
    public void testPreviousMessageId() {
        long messageId = messageId(0, 1);
        assertTrue(isSuccessive(previousMessageId(messageId), messageId));
        messageId = messageId(1, 0);
        assertTrue(isSuccessive(previousMessageId(messageId), messageId));
    }
}
