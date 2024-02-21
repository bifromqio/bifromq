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

import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.retain.rpc.proto.MatchResult;
import com.baidu.bifromq.retain.rpc.proto.RetainResult;
import com.baidu.bifromq.type.TopicMessage;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ReplaceBehaviorTest extends RetainStoreTest {
    private String tenantId;

    @BeforeMethod(alwaysRun = true)
    private void reset() {
        tenantId = "tenantA-" + System.nanoTime();
    }

    @Test(groups = "integration")
    public void replaceWithinLimit() {
        String topic = "/a";
        TopicMessage message = message(topic, "hello");
        TopicMessage message1 = message(topic, "world");

        assertEquals(requestRetain(tenantId, message, 1), RetainResult.Code.RETAINED);
        assertEquals(requestRetain(tenantId, message1, 1), RetainResult.Code.RETAINED);

        MatchResult matchReply = requestMatch(tenantId, topic, 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 1);
        assertEquals(matchReply.getOk().getMessages(0), message1);
    }

    @Test(groups = "integration")
    public void replaceButExceedLimit() {
        assertEquals(requestRetain(tenantId, message("/a", "hello"), 2), RetainResult.Code.RETAINED);

        TopicMessage message = message("/b", "world");
        assertEquals(requestRetain(tenantId, message, 2), RetainResult.Code.RETAINED);

        // limit now shrink to 1
        assertEquals(requestRetain(tenantId, message("/b", "!!!"), 1), RetainResult.Code.EXCEED_LIMIT);

        MatchResult matchReply = requestMatch(tenantId, "/b", 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 1);
        assertEquals(matchReply.getOk().getMessages(0), message);
    }
}
