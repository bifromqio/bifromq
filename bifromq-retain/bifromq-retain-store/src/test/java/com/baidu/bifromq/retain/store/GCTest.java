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
import static org.mockito.Mockito.when;

import com.baidu.bifromq.retain.rpc.proto.MatchCoProcReply;
import com.baidu.bifromq.retain.rpc.proto.RetainCoProcReply;
import com.baidu.bifromq.type.TopicMessage;

import java.io.IOException;
import java.time.Clock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.mockito.Mock;

public class GCTest extends RetainStoreTest {
    @Mock
    private Clock clock;

    @BeforeMethod(alwaysRun = true)
    public void setup() throws IOException {
        super.setup();
        when(clock.millis()).thenReturn(0L);
    }

    @Override
    protected Clock getClock() {
        return clock;
    }


    @Test(groups = "integration")
    public void retainAlreadyExpired() {
        String trafficId = "trafficId";
        String topic = "/a";
        TopicMessage message = message(topic, "hello", 0, 1);
        // make it expired
        when(clock.millis()).thenReturn(1100L);

        assertEquals(RetainCoProcReply.Result.ERROR, requestRetain(trafficId, 1, message).getResult());
    }

    @Test(groups = "integration")
    public void inlineGCDuringRetain() {
        String trafficId = "trafficId";
        String topic1 = "/a";
        String topic2 = "/b";
        TopicMessage message1 = message(topic1, "hello", 0, 1);
        requestRetain(trafficId, 1, message1);
        MatchCoProcReply matchReply = requestMatch(trafficId, topic1, 10);
        assertEquals(1, matchReply.getMessagesCount());
        assertEquals(message1, matchReply.getMessages(0));

        when(clock.millis()).thenReturn(1100L);

        // message1 has expired
        assertEquals(0, requestMatch(trafficId, topic1, 10).getMessagesCount());

        TopicMessage message2 = message(topic2, "world", 1000, 1);
        assertEquals(RetainCoProcReply.Result.RETAINED, requestRetain(trafficId, 1, message2).getResult());

        assertEquals(0, requestMatch(trafficId, topic1, 10).getMessagesCount());

        matchReply = requestMatch(trafficId, topic2, 10);
        assertEquals(1, matchReply.getMessagesCount());
        assertEquals(message2, matchReply.getMessages(0));
    }

    @Test(groups = "integration")
    public void inlineGCDuringDelete() {
        String trafficId = "trafficId";
        String topic = "/a";
        TopicMessage message1 = message(topic, "hello", 0, 1);
        requestRetain(trafficId, 1, message1);

        when(clock.millis()).thenReturn(1100L);

        assertEquals(RetainCoProcReply.Result.CLEARED,
            requestRetain(trafficId, 1, message(topic, "")).getResult());
        assertEquals(0, requestMatch(trafficId, topic, 10).getMessagesCount());
    }

    @Test(groups = "integration")
    public void inlineGCDuringReplace() {
        String trafficId = "trafficId";
        String topic1 = "/a";
        String topic2 = "/b";
        TopicMessage message1 = message(topic1, "hello", 0, 1);
        TopicMessage message2 = message(topic2, "world", 0, 1);
        TopicMessage message3 = message(topic2, "world", 1000, 1);
        requestRetain(trafficId, 2, message1);
        requestRetain(trafficId, 2, message2);

        when(clock.millis()).thenReturn(1100L);


        assertEquals(RetainCoProcReply.Result.RETAINED,
            requestRetain(trafficId, 2, message3).getResult());

        assertEquals(0, requestMatch(trafficId, topic1, 10).getMessagesCount());
        assertEquals(1, requestMatch(trafficId, topic2, 10).getMessagesCount());
        assertEquals(message3, requestMatch(trafficId, topic2, 10).getMessages(0));

        // message1 will be removed as well, so retain set size should be 1
        assertEquals(RetainCoProcReply.Result.RETAINED,
            requestRetain(trafficId, 2, message("/c", "abc")).getResult());
        // no room
        assertEquals(RetainCoProcReply.Result.ERROR,
            requestRetain(trafficId, 2, message("/d", "abc")).getResult());
    }

    @Test(groups = "integration")
    public void estExpiryTimeUpdateByRetainNew() {
        String trafficId = "trafficId";
        String topic1 = "/a";
        String topic2 = "/b";
        TopicMessage message1 = message(topic1, "hello", 0, 2);
        TopicMessage message2 = message(topic2, "world", 0, 1);

        requestRetain(trafficId, 2, message1);
        requestRetain(trafficId, 2, message2);

        when(clock.millis()).thenReturn(1100L);
        // message2 expired
        assertEquals(1, requestMatch(trafficId, topic1, 10).getMessagesCount());
        assertEquals(0, requestMatch(trafficId, topic2, 10).getMessagesCount());

        assertEquals(RetainCoProcReply.Result.RETAINED, requestRetain(trafficId, 2,
            message("/c", "abc")).getResult());
        assertEquals(RetainCoProcReply.Result.ERROR, requestRetain(trafficId, 2,
            message("/d", "abc")).getResult());

        when(clock.millis()).thenReturn(2100L);
        // now message1 expired
        assertEquals(RetainCoProcReply.Result.RETAINED, requestRetain(trafficId, 2,
            message("/d", "abc")).getResult());
    }

    @Test(groups = "integration")
    public void estExpiryTimeUpdatedByReplaceNew() {
        String trafficId = "trafficId";
        String topic = "/a";
        TopicMessage message1 = message(topic, "hello", 0, 2);
        TopicMessage message2 = message(topic, "world", 0, 1);

        requestRetain(trafficId, 1, message1);
        requestRetain(trafficId, 1, message2);

        // no room for new message
        assertEquals(RetainCoProcReply.Result.ERROR, requestRetain(trafficId, 1,
            message("/d", "abc")).getResult());
        when(clock.millis()).thenReturn(1100L);
        assertEquals(RetainCoProcReply.Result.RETAINED, requestRetain(trafficId, 1,
            message("/d", "abc")).getResult());
        assertEquals(0, requestMatch(trafficId, topic, 10).getMessagesCount());
    }

    @Test(groups = "integration")
    public void gc() {
        String trafficId = "trafficId";
        String topic = "/a";
        TopicMessage message = message(topic, "hello", 0, 1);
        requestRetain(trafficId, 1, message);

        requestGC(trafficId);
        assertEquals(RetainCoProcReply.Result.ERROR, requestRetain(trafficId, 1,
            message("/d", "abc")).getResult());

        when(clock.millis()).thenReturn(1100L);
        requestGC(trafficId);

        assertEquals(RetainCoProcReply.Result.RETAINED, requestRetain(trafficId, 1,
            message("/d", "abc")).getResult());
    }
}
