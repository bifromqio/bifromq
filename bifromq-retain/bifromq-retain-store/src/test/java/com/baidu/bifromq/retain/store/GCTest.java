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
import com.baidu.bifromq.retain.rpc.proto.Matched;
import com.baidu.bifromq.retain.rpc.proto.RetainResult;
import com.baidu.bifromq.type.TopicMessage;
import java.time.Clock;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GCTest extends RetainStoreTest {
    @Mock
    private Clock clock;

    @BeforeMethod(alwaysRun = true)
    public void reset() {
        when(clock.millis()).thenReturn(0L);
    }

    @Override
    protected Clock getClock() {
        return clock;
    }


    @Test(groups = "integration")
    public void retainAlreadyExpired() {
        String tenantId = "tenantA";
        String topic = "/a";
        TopicMessage message = message(topic, "hello", 0, 1);
        // make it expired
        when(clock.millis()).thenReturn(1100L);
        when(settingProvider.provide(Setting.RetainedTopicLimit, tenantId)).thenReturn(1);

        assertEquals(requestRetain(tenantId, message), RetainResult.ERROR);
    }

    @Test(groups = "integration")
    public void inlineGCDuringRetain() {
        String tenantId = "tenantA";
        String topic1 = "/a";
        String topic2 = "/b";
        TopicMessage message1 = message(topic1, "hello", 0, 1);
        when(settingProvider.provide(Setting.RetainedTopicLimit, tenantId)).thenReturn(1);

        requestRetain(tenantId, message1);
        MatchResult matchReply = requestMatch(tenantId, topic1, 10);
        Matched matched = matchReply.getOk();
        assertEquals(matched.getMessagesCount(), 1);
        assertEquals(matched.getMessages(0), message1);

        when(clock.millis()).thenReturn(1100L);

        // message1 has expired
        assertEquals(requestMatch(tenantId, topic1, 10).getOk().getMessagesCount(), 0);

        TopicMessage message2 = message(topic2, "world", 1000, 1);

        assertEquals(requestRetain(tenantId, message2), RetainResult.RETAINED);

        assertEquals(requestMatch(tenantId, topic1, 10).getOk().getMessagesCount(), 0);

        matchReply = requestMatch(tenantId, topic2, 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 1);
        assertEquals(matchReply.getOk().getMessages(0), message2);

        clearMessage(tenantId, topic2);
    }

    @Test(groups = "integration")
    public void inlineGCDuringDelete() {
        String tenantId = "tenantA";
        String topic = "/a";
        TopicMessage message1 = message(topic, "hello", 0, 1);
        when(settingProvider.provide(Setting.RetainedTopicLimit, tenantId)).thenReturn(1);

        requestRetain(tenantId, message1);

        when(clock.millis()).thenReturn(1100L);

        assertEquals(requestRetain(tenantId, message(topic, "")), RetainResult.CLEARED);
        assertEquals(requestMatch(tenantId, topic, 10).getOk().getMessagesCount(), 0);
    }

    @Test(groups = "integration")
    public void inlineGCDuringReplace() {
        String tenantId = "tenantA";
        String topic1 = "/a";
        String topic2 = "/b";
        TopicMessage message1 = message(topic1, "hello", 0, 1);
        TopicMessage message2 = message(topic2, "world", 0, 1);
        TopicMessage message3 = message(topic2, "world", 1000, 1);
        when(settingProvider.provide(Setting.RetainedTopicLimit, tenantId)).thenReturn(2);

        requestRetain(tenantId, message1);
        requestRetain(tenantId, message2);

        when(clock.millis()).thenReturn(1100L);


        assertEquals(requestRetain(tenantId, message3), RetainResult.RETAINED);

        assertEquals(requestMatch(tenantId, topic1, 10).getOk().getMessagesCount(), 0);
        assertEquals(requestMatch(tenantId, topic2, 10).getOk().getMessagesCount(), 1);
        assertEquals(requestMatch(tenantId, topic2, 10).getOk().getMessages(0), message3);

        // message1 will be removed as well, so retain set size should be 1
        assertEquals(requestRetain(tenantId, message("/c", "abc")), RetainResult.RETAINED);
        // no room
        assertEquals(requestRetain(tenantId, message("/d", "abc")), RetainResult.ERROR);

        clearMessage(tenantId, topic2);
        clearMessage(tenantId, "/c");
    }

    @Test(groups = "integration")
    public void estExpiryTimeUpdateByRetainNew() {
        String tenantId = "tenantA";
        String topic1 = "/a";
        String topic2 = "/b";
        TopicMessage message1 = message(topic1, "hello", 0, 2);
        TopicMessage message2 = message(topic2, "world", 0, 1);
        when(settingProvider.provide(Setting.RetainedTopicLimit, tenantId)).thenReturn(2);


        requestRetain(tenantId, message1);
        requestRetain(tenantId, message2);

        when(clock.millis()).thenReturn(1100L);
        // message2 expired
        assertEquals(requestMatch(tenantId, topic1, 10).getOk().getMessagesCount(), 1);
        assertEquals(requestMatch(tenantId, topic2, 10).getOk().getMessagesCount(), 0);

        assertEquals(requestRetain(tenantId, message("/c", "abc")), RetainResult.RETAINED);
        assertEquals(requestRetain(tenantId, message("/d", "abc")), RetainResult.ERROR);

        when(clock.millis()).thenReturn(2100L);
        // now message1 expired
        assertEquals(requestRetain(tenantId, message("/d", "abc")), RetainResult.RETAINED);

        clearMessage(tenantId, "/c");
        clearMessage(tenantId, "/d");
    }

    @Test(groups = "integration")
    public void estExpiryTimeUpdatedByReplaceNew() {
        String tenantId = "tenantA";
        String topic = "/a";
        TopicMessage message1 = message(topic, "hello", 0, 2);
        TopicMessage message2 = message(topic, "world", 0, 1);
        when(settingProvider.provide(Setting.RetainedTopicLimit, tenantId)).thenReturn(1);


        requestRetain(tenantId, message1);
        requestRetain(tenantId, message2);

        // no room for new message
        assertEquals(requestRetain(tenantId, message("/d", "abc")), RetainResult.ERROR);
        when(clock.millis()).thenReturn(1100L);
        assertEquals(requestRetain(tenantId, message("/d", "abc")), RetainResult.RETAINED);
        assertEquals(requestMatch(tenantId, topic, 10).getOk().getMessagesCount(), 0);

        clearMessage(tenantId, "/d");
    }

    @Test(groups = "integration")
    public void gc() {
        String tenantId = "tenantA";
        String topic = "/a";
        TopicMessage message = message(topic, "hello", 0, 1);
        when(settingProvider.provide(Setting.RetainedTopicLimit, tenantId)).thenReturn(1);

        requestRetain(tenantId, message);

        requestGC(tenantId);
        assertEquals(requestRetain(tenantId, message("/d", "abc")), RetainResult.ERROR);

        when(clock.millis()).thenReturn(1100L);
        requestGC(tenantId);

        assertEquals(requestRetain(tenantId, message("/d", "abc")), RetainResult.RETAINED);

        clearMessage(tenantId, "/d");
    }
}
