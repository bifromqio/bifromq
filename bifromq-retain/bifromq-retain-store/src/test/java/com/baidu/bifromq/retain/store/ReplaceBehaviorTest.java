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

import static org.testng.AssertJUnit.assertEquals;

import com.baidu.bifromq.retain.rpc.proto.MatchCoProcReply;
import com.baidu.bifromq.retain.rpc.proto.RetainCoProcReply;
import com.baidu.bifromq.type.TopicMessage;
import org.testng.annotations.Test;

public class ReplaceBehaviorTest extends RetainStoreTest {

    @Test(groups = "integration")
    public void replaceWithinLimit() {
        String trafficId = "trafficId";
        String topic = "/a";
        TopicMessage message = message(topic, "hello");
        TopicMessage message1 = message(topic, "world");
        assertEquals(RetainCoProcReply.Result.RETAINED,
            requestRetain(trafficId, 1, message).getResult());

        assertEquals(RetainCoProcReply.Result.RETAINED,
            requestRetain(trafficId, 1, message1).getResult());

        MatchCoProcReply matchReply = requestMatch(trafficId, topic, 10);
        assertEquals(1, matchReply.getMessagesCount());
        assertEquals(message1, matchReply.getMessages(0));

    }

    @Test(groups = "integration")
    public void replaceButExceedLimit() {
        String trafficId = "trafficId";
        assertEquals(RetainCoProcReply.Result.RETAINED,
            requestRetain(trafficId, 2, message("/a", "hello")).getResult());

        TopicMessage message = message("/b", "world");
        assertEquals(RetainCoProcReply.Result.RETAINED,
            requestRetain(trafficId, 2, message).getResult());

        // limit now shrink to 1
        assertEquals(RetainCoProcReply.Result.ERROR,
            requestRetain(trafficId, 1, message("/b", "!!!")).getResult());

        MatchCoProcReply matchReply = requestMatch(trafficId, "/b", 10);
        assertEquals(1, matchReply.getMessagesCount());
        assertEquals(message, matchReply.getMessages(0));
    }
}
