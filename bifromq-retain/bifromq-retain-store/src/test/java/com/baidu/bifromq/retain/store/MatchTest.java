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

package com.baidu.bifromq.retain.store;

import static com.google.common.collect.Sets.newHashSet;
import static org.testng.AssertJUnit.assertEquals;

import com.baidu.bifromq.retain.rpc.proto.MatchCoProcReply;
import com.baidu.bifromq.type.TopicMessage;
import org.testng.annotations.Test;

public class MatchTest extends RetainStoreTest {

    @Test(groups = "integration")
    public void wildcardTopicFilter() {
        String trafficId = "trafficId";
        TopicMessage message1 = message("/a/b/c", "hello");
        TopicMessage message2 = message("/a/b/", "hello");
        TopicMessage message3 = message("/c/", "hello");
        requestRetain(trafficId, 10, message1);
        requestRetain(trafficId, 10, message2);
        requestRetain(trafficId, 10, message3);

        MatchCoProcReply matchReply = requestMatch(trafficId, "#", 10);
        assertEquals(3, matchReply.getMessagesCount());
        assertEquals(newHashSet(message1, message2, message3), newHashSet(matchReply.getMessagesList()));

        matchReply = requestMatch(trafficId, "/a/+", 10);
        assertEquals(0, matchReply.getMessagesCount());

        matchReply = requestMatch(trafficId, "/a/+/+", 10);
        assertEquals(2, matchReply.getMessagesCount());
        assertEquals(newHashSet(message1, message2), newHashSet(matchReply.getMessagesList()));

        matchReply = requestMatch(trafficId, "/+/b/", 10);
        assertEquals(1, matchReply.getMessagesCount());
        assertEquals(newHashSet(message2), newHashSet(matchReply.getMessagesList()));

        matchReply = requestMatch(trafficId, "/+/b/#", 10);
        assertEquals(2, matchReply.getMessagesCount());
        assertEquals(newHashSet(message1, message2), newHashSet(matchReply.getMessagesList()));
    }

    @Test(groups = "integration")
    public void matchLimit() {
        String trafficId = "trafficId";
        TopicMessage message1 = message("/a/b/c", "hello");
        TopicMessage message2 = message("/a/b/", "hello");
        TopicMessage message3 = message("/c/", "hello");
        requestRetain(trafficId, 10, message1);
        requestRetain(trafficId, 10, message2);
        requestRetain(trafficId, 10, message3);

        assertEquals(0, requestMatch(trafficId, "#", 0).getMessagesCount());
        assertEquals(1, requestMatch(trafficId, "#", 1).getMessagesCount());
    }
}
