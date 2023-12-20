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

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.retain.rpc.proto.MatchResult;
import com.baidu.bifromq.retain.rpc.proto.RetainResult;
import com.baidu.bifromq.type.TopicMessage;
import org.testng.annotations.Test;

public class RetainBehaviorTest extends RetainStoreTest {

    @Test(groups = "integration")
    public void retainFirstMessage() {
        String tenantId = "tenantId";
        String topic = "/a/b/c";
        TopicMessage message = message(topic, "hello");
        when(settingProvider.provide(Setting.RetainedTopicLimit, tenantId)).thenReturn(10);

        RetainResult reply = requestRetain(tenantId, message);
        assertEquals(reply, RetainResult.RETAINED);

        MatchResult matchReply = requestMatch(tenantId, topic, 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 1);
        assertEquals(matchReply.getOk().getMessages(0), message);

        clearMessage(tenantId, topic);
    }

    @Test(groups = "integration")
    public void retainFirstMessageWithZeroMaxRetainedTopics() {
        String tenantId = "tenantId";
        String topic = "/a/b/c";
        when(settingProvider.provide(Setting.RetainedTopicLimit, tenantId)).thenReturn(0);
        RetainResult reply = requestRetain(tenantId, message(topic, "hello"));

        assertEquals(reply, RetainResult.ERROR);

        MatchResult matchReply = requestMatch(tenantId, topic, 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 0);
    }

    @Test(groups = "integration")
    public void retainNewExceedLimit() {
        String tenantId = "tenantId";
        when(settingProvider.provide(Setting.RetainedTopicLimit, tenantId)).thenReturn(1);
        assertEquals(requestRetain(tenantId, message("/a", "hello")), RetainResult.RETAINED);
        assertEquals(requestRetain(tenantId, message("/b", "hello")), RetainResult.ERROR);

        MatchResult matchReply = requestMatch(tenantId, "/b", 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 0);

        clearMessage(tenantId, "/a");
    }
}
