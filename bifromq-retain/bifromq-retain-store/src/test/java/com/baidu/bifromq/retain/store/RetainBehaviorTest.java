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

import static org.junit.Assert.assertEquals;

import com.baidu.bifromq.retain.rpc.proto.MatchCoProcReply;
import com.baidu.bifromq.retain.rpc.proto.RetainCoProcReply;
import com.baidu.bifromq.type.TopicMessage;
import org.junit.Test;

public class RetainBehaviorTest extends RetainStoreTest {

    @Test
    public void retainFirstMessage() {
        String trafficId = "trafficId";
        String topic = "/a/b/c";
        TopicMessage message = message(topic, "hello");
        RetainCoProcReply reply = requestRetain(trafficId, 10, message);
        assertEquals(RetainCoProcReply.Result.RETAINED, reply.getResult());

        MatchCoProcReply matchReply = requestMatch(trafficId, topic, 10);
        assertEquals(1, matchReply.getMessagesCount());
        assertEquals(message, matchReply.getMessages(0));
    }

    @Test
    public void retainFirstMessageWithZeroMaxRetainedTopics() {
        String trafficId = "trafficId";
        String topic = "/a/b/c";
        RetainCoProcReply reply = requestRetain(trafficId, 0, message(topic, "hello"));
        assertEquals(RetainCoProcReply.Result.ERROR, reply.getResult());

        MatchCoProcReply matchReply = requestMatch(trafficId, topic, 10);
        assertEquals(0, matchReply.getMessagesCount());
    }

    @Test
    public void retainNewExceedLimit() {
        String trafficId = "trafficId";
        assertEquals(RetainCoProcReply.Result.RETAINED,
            requestRetain(trafficId, 1, message("/a", "hello")).getResult());

        assertEquals(RetainCoProcReply.Result.ERROR,
            requestRetain(trafficId, 1, message("/b", "hello")).getResult());

        MatchCoProcReply matchReply = requestMatch(trafficId, "/b", 10);
        assertEquals(0, matchReply.getMessagesCount());
    }
}
