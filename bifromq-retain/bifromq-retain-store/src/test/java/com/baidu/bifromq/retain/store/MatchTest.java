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

package com.baidu.bifromq.retain.store;

import static com.google.common.collect.Sets.newHashSet;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.retain.rpc.proto.MatchResult;
import com.baidu.bifromq.type.TopicMessage;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MatchTest extends RetainStoreTest {
    private String tenantId;

    @BeforeMethod(alwaysRun = true)
    private void reset() {
        tenantId = "tenantA-" + System.nanoTime();
    }

    @Test(groups = "integration")
    public void wildcardTopicFilter() {
        TopicMessage message1 = message("/a/b/c", "hello");
        TopicMessage message2 = message("/a/b/", "hello");
        TopicMessage message3 = message("/c/", "hello");
        TopicMessage message4 = message("a", "hello");

        requestRetain(tenantId, message1);
        requestRetain(tenantId, message2);
        requestRetain(tenantId, message3);
        requestRetain(tenantId, message4);

        MatchResult matchReply = requestMatch(tenantId, "#", 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 4);
        assertEquals(newHashSet(matchReply.getOk().getMessagesList()),
            newHashSet(message1, message2, message3, message4));

        matchReply = requestMatch(tenantId, "+", 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 1);
        assertEquals(newHashSet(matchReply.getOk().getMessagesList()), newHashSet(message4));

        matchReply = requestMatch(tenantId, "+/#", 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 4);
        assertEquals(newHashSet(matchReply.getOk().getMessagesList()),
            newHashSet(message1, message2, message3, message4));

        matchReply = requestMatch(tenantId, "+/+/#", 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 3);
        assertEquals(newHashSet(matchReply.getOk().getMessagesList()), newHashSet(message1, message2, message3));

        matchReply = requestMatch(tenantId, "+/+/+", 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 1);
        assertEquals(newHashSet(matchReply.getOk().getMessagesList()), newHashSet(message3));

        matchReply = requestMatch(tenantId, "/#", 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 4);
        assertEquals(newHashSet(matchReply.getOk().getMessagesList()),
            newHashSet(message1, message2, message3, message4));

        matchReply = requestMatch(tenantId, "/c/#", 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 1);
        assertEquals(newHashSet(matchReply.getOk().getMessagesList()), newHashSet(message3));

        matchReply = requestMatch(tenantId, "/a/+", 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 0);

        matchReply = requestMatch(tenantId, "/a/#", 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 2);
        assertEquals(newHashSet(matchReply.getOk().getMessagesList()), newHashSet(message1, message2));

        matchReply = requestMatch(tenantId, "/a/+/+", 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 2);
        assertEquals(newHashSet(matchReply.getOk().getMessagesList()), newHashSet(message1, message2));

        matchReply = requestMatch(tenantId, "/a/+/#", 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 2);
        assertEquals(newHashSet(matchReply.getOk().getMessagesList()), newHashSet(message1, message2));

        matchReply = requestMatch(tenantId, "/+/b/", 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 1);
        assertEquals(newHashSet(matchReply.getOk().getMessagesList()), newHashSet(message2));

        matchReply = requestMatch(tenantId, "/+/b/#", 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 2);
        assertEquals(newHashSet(matchReply.getOk().getMessagesList()), newHashSet(message1, message2));

        matchReply = requestMatch(tenantId, "/a/b/c/#", 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 1);
        assertEquals(newHashSet(matchReply.getOk().getMessagesList()), newHashSet(message1));

        matchReply = requestMatch(tenantId, "/a/b/#", 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 2);
        assertEquals(newHashSet(matchReply.getOk().getMessagesList()), newHashSet(message1, message2));
    }

    @Test(groups = "integration")
    public void matchLimit() {
        TopicMessage message1 = message("/a/b/c", "hello");
        TopicMessage message2 = message("/a/b/", "hello");
        TopicMessage message3 = message("/c/", "hello");
        requestRetain(tenantId, message1);
        requestRetain(tenantId, message2);
        requestRetain(tenantId, message3);

        assertEquals(requestMatch(tenantId, "#", 0).getOk().getMessagesCount(), 0);
        assertEquals(requestMatch(tenantId, "#", 1).getOk().getMessagesCount(), 1);
    }
}
