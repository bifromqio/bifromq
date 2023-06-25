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

package com.baidu.bifromq.inbox.store;

import static com.baidu.bifromq.inbox.util.KeyUtil.scopedInboxId;
import static com.baidu.bifromq.type.QoS.AT_MOST_ONCE;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import com.baidu.bifromq.inbox.storage.proto.HasReply;
import com.baidu.bifromq.inbox.storage.proto.InboxFetchReply;
import com.baidu.bifromq.inbox.storage.proto.InboxInsertReply;
import com.baidu.bifromq.inbox.storage.proto.InboxInsertResult;
import com.baidu.bifromq.plugin.eventcollector.inboxservice.Overflowed;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import java.io.IOException;
import java.time.Clock;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class QoS0InboxTest extends InboxStoreTest {
    private String trafficId = "trafficId";
    private String inboxId = "inboxId";
    private SubInfo subInfo = SubInfo.newBuilder()
        .setTrafficId(trafficId)
        .setInboxId(inboxId)
        .setSubQoS(QoS.AT_MOST_ONCE)
        .setTopicFilter("greeting")
        .build();

    @Mock
    private Clock clock;

    @BeforeMethod(groups = "integration")
    public void setup() throws IOException {
        super.setup();
        when(clock.millis()).thenReturn(0L);
    }

    @Override
    protected Clock getClock() {
        return clock;
    }

    @Test(groups = "integration")
    public void implicitCleanExpiredInboxDuringCreate() {
        String topic = "greeting";
        String scopedInboxIdUtf8 = scopedInboxId(trafficId, inboxId).toStringUtf8();
        requestCreate(trafficId, inboxId, 10, 2, false);
        requestInsert(subInfo, topic,
            message(AT_MOST_ONCE, "hello"),
            message(AT_MOST_ONCE, "world"));

        // not expire
        requestCreate(trafficId, inboxId, 10, 2, false);
        InboxFetchReply reply = requestFetchQoS0(trafficId, inboxId, 10);
        assertEquals(2, reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount());

        when(clock.millis()).thenReturn(2100L);

        requestCreate(trafficId, inboxId, 10, 100, false);
        HasReply has = requestHas(trafficId, inboxId);
        assertTrue(has.getExistsMap().get(scopedInboxId(trafficId, inboxId).toStringUtf8()));
        reply = requestFetchQoS0(trafficId, inboxId, 10);
        assertEquals(0, reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount());
    }

    @Test(groups = "integration")
    public void insertToExpiredInbox() {
        requestCreate(trafficId, inboxId, 10, 1, false);
        when(clock.millis()).thenReturn(1100L);
        InboxInsertReply reply = requestInsert(subInfo, "greeting", message(AT_MOST_ONCE, "hello"));
        assertEquals(InboxInsertResult.Result.NO_INBOX, reply.getResults(0).getResult());
    }

    @Test(groups = "integration")
    public void insertToNonExistInbox() {
        InboxInsertReply reply = requestInsert(subInfo, "greeting", message(AT_MOST_ONCE, "hello"));
        assertEquals(InboxInsertResult.Result.NO_INBOX, reply.getResults(0).getResult());
    }

    @Test(groups = "integration")
    public void fetchWithoutStartAfter() {
        String scopedInboxIdUtf8 = scopedInboxId(trafficId, inboxId).toStringUtf8();
        String topic = "greeting";
        TopicMessagePack.SenderMessagePack msg1 = message(AT_MOST_ONCE, "hello");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_MOST_ONCE, "world");
        requestCreate(trafficId, inboxId, 10, 2, false);
        requestInsert(subInfo, topic, msg1, msg2);
        InboxFetchReply reply = requestFetchQoS0(trafficId, inboxId, 10);
        assertEquals(2, reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount());
        assertEquals(0, reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(0));
        assertEquals(1, reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(1));

        assertEquals(2, reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount());
        assertEquals(msg1.getMessage(0),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(0).getMsg().getMessage());
        assertEquals(msg2.getMessage(0),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(1).getMsg().getMessage());

        InboxFetchReply reply1 = requestFetchQoS0(trafficId, inboxId, 10);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqList(),
            reply1.getResultMap().get(scopedInboxIdUtf8).getQos0SeqList());
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgList(),
            reply1.getResultMap().get(scopedInboxIdUtf8).getQos0MsgList());
    }

    @Test(groups = "integration")
    public void fetchWithMaxLimit() {
        String scopedInboxIdUtf8 = scopedInboxId(trafficId, inboxId).toStringUtf8();
        String topic = "greeting";
        TopicMessagePack.SenderMessagePack msg1 = message(AT_MOST_ONCE, "hello");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_MOST_ONCE, "world");
        requestCreate(trafficId, inboxId, 10, 600, false);
        requestInsert(subInfo, topic, msg1, msg2);
        InboxFetchReply reply = requestFetchQoS0(trafficId, inboxId, 1);
        assertEquals(1, reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount());
        assertEquals(0, reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(0));

        assertEquals(1, reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount());
        assertEquals(msg1.getMessage(0),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(0).getMsg().getMessage());

        reply = requestFetchQoS0(trafficId, inboxId, 10);
        assertEquals(2, reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount());
        assertEquals(2, reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount());

        reply = requestFetchQoS0(trafficId, inboxId, 0);
        assertEquals(0, reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount());
        assertEquals(0, reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount());
    }

    @Test(groups = "integration")
    public void fetchWithStartAfter() {
        String scopedInboxIdUtf8 = scopedInboxId(trafficId, inboxId).toStringUtf8();

        String topic = "greeting";
        TopicMessagePack.SenderMessagePack msg1 = message(AT_MOST_ONCE, "a");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_MOST_ONCE, "b");
        TopicMessagePack.SenderMessagePack msg3 = message(AT_MOST_ONCE, "c");
        TopicMessagePack.SenderMessagePack msg4 = message(AT_MOST_ONCE, "d");
        TopicMessagePack.SenderMessagePack msg5 = message(AT_MOST_ONCE, "e");
        TopicMessagePack.SenderMessagePack msg6 = message(AT_MOST_ONCE, "f");
        requestCreate(trafficId, inboxId, 10, 600, false);
        requestInsert(subInfo, topic, msg1, msg2, msg3);
        requestInsert(subInfo, topic, msg4, msg5, msg6);

        InboxFetchReply reply = requestFetchQoS0(trafficId, inboxId, 1, 0L);
        assertEquals(1, reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount());
        assertEquals(1, reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(0));

        assertEquals(1, reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount());
        assertEquals(msg2.getMessage(0),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(0).getMsg().getMessage());

        reply = requestFetchQoS0(trafficId, inboxId, 10, 0L);
        assertEquals(5, reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount());
        assertEquals(1, reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(0));
        assertEquals(2, reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(1));
        assertEquals(3, reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(2));
        assertEquals(4, reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(3));
        assertEquals(5, reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(4));

        assertEquals(5, reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount());
        assertEquals(msg2.getMessage(0),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(0).getMsg().getMessage());
        assertEquals(msg3.getMessage(0),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(1).getMsg().getMessage());
        assertEquals(msg4.getMessage(0),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(2).getMsg().getMessage());
        assertEquals(msg5.getMessage(0),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(3).getMsg().getMessage());
        assertEquals(msg6.getMessage(0),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(4).getMsg().getMessage());

        reply = requestFetchQoS0(trafficId, inboxId, 10, 5L);
        assertEquals(0, reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount());
        assertEquals(0, reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount());
    }

    @Test(groups = "integration")
    public void commit() {
        String scopedInboxIdUtf8 = scopedInboxId(trafficId, inboxId).toStringUtf8();
        String topic = "greeting";
        TopicMessagePack.SenderMessagePack msg1 = message(AT_MOST_ONCE, "hello");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_MOST_ONCE, "world");
        TopicMessagePack.SenderMessagePack msg3 = message(AT_MOST_ONCE, "!!!!!");
        requestCreate(trafficId, inboxId, 10, 600, false);
        requestInsert(subInfo, topic, msg1, msg2, msg3);
        requestCommitQoS0(trafficId, inboxId, 1);

        InboxFetchReply reply = requestFetchQoS0(trafficId, inboxId, 10, null);
        assertEquals(1, reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount());
        assertEquals(2, reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(0));

        assertEquals(1, reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount());
        assertEquals(msg3.getMessage(0),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(0).getMsg().getMessage());

        // nothing should happen
        requestCommitQoS0(trafficId, inboxId, 1);

        reply = requestFetchQoS0(trafficId, inboxId, 10, null);
        assertEquals(1, reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount());
        assertEquals(2, reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(0));

        assertEquals(1, reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount());
        assertEquals(msg3.getMessage(0),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(0).getMsg().getMessage());

        requestCommitQoS0(trafficId, inboxId, 2);
        reply = requestFetchQoS0(trafficId, inboxId, 10, null);
        assertEquals(0, reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount());

        assertEquals(0, reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount());
    }

    @Test(groups = "integration")
    public void commitAll() {
        String scopedInboxIdUtf8 = scopedInboxId(trafficId, inboxId).toStringUtf8();
        String topic = "greeting";
        TopicMessagePack.SenderMessagePack msg1 = message(AT_MOST_ONCE, "a");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_MOST_ONCE, "b");
        TopicMessagePack.SenderMessagePack msg3 = message(AT_MOST_ONCE, "c");
        TopicMessagePack.SenderMessagePack msg4 = message(AT_MOST_ONCE, "d");
        TopicMessagePack.SenderMessagePack msg5 = message(AT_MOST_ONCE, "e");
        TopicMessagePack.SenderMessagePack msg6 = message(AT_MOST_ONCE, "f");
        requestCreate(trafficId, inboxId, 10, 600, false);
        requestInsert(subInfo, topic, msg1, msg2, msg3);
        requestInsert(subInfo, topic, msg4, msg5, msg6);
        requestCommitQoS0(trafficId, inboxId, 5);
        InboxFetchReply reply = requestFetchQoS0(trafficId, inboxId, 10, null);
        assertEquals(0, reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount());
        assertEquals(0, reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount());
    }

    @Test(groups = "integration")
    public void touch() {
        requestCreate(trafficId, inboxId, 10, 1, false);
        when(clock.millis()).thenReturn(900L);
        requestTouch(trafficId, inboxId);

        when(clock.millis()).thenReturn(1100L);
        assertTrue(requestHas(trafficId, inboxId).getExistsMap().get(scopedInboxId(trafficId, inboxId).toStringUtf8()));
    }

    @Test(groups = "integration")
    public void insertDropOldest() {
        String scopedInboxIdUtf8 = scopedInboxId(trafficId, inboxId).toStringUtf8();
        String topic = "greeting";
        TopicMessagePack.SenderMessagePack msg0 = message(AT_MOST_ONCE, "hello");
        TopicMessagePack.SenderMessagePack msg1 = message(AT_MOST_ONCE, "world");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_MOST_ONCE, "!!!!!");
        requestCreate(trafficId, inboxId, 2, 600, true);
        requestInsert(subInfo, topic, msg0, msg1);
        requestInsert(subInfo, topic, msg2);

        InboxFetchReply reply = requestFetchQoS0(trafficId, inboxId, 10);
        assertEquals(3, reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount());
        assertEquals(0, reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(0));
        assertEquals(1, reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(1));
        assertEquals(2, reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(2));

        assertEquals(3, reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount());
        assertEquals(msg0.getMessage(0),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(0).getMsg().getMessage());
        assertEquals(msg1.getMessage(0),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(1).getMsg().getMessage());
        assertEquals(msg2.getMessage(0),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(2).getMsg().getMessage());

        requestInsert(subInfo, topic, msg0, msg1, msg2);

        ArgumentCaptor<Overflowed> argCap = ArgumentCaptor.forClass(Overflowed.class);
        verify(eventCollector, times(1)).report(argCap.capture());
        Overflowed event = argCap.getValue();
        assertTrue(event.oldest());
        assertEquals(AT_MOST_ONCE, event.qos());
        assertEquals(4, event.dropCount());

        reply = requestFetchQoS0(trafficId, inboxId, 10);
        assertEquals(2, reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount());
        assertEquals(3, reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(0));
        assertEquals(4, reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(1));

        assertEquals(2, reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount());
        assertEquals(msg1.getMessage(0),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(0).getMsg().getMessage());
        assertEquals(msg2.getMessage(0),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(1).getMsg().getMessage());
    }

    @Test(groups = "integration")
    public void insertDropYoungest() {
        String scopedInboxIdUtf8 = scopedInboxId(trafficId, inboxId).toStringUtf8();
        String topic = "greeting";
        TopicMessagePack.SenderMessagePack msg0 = message(AT_MOST_ONCE, "hello");
        TopicMessagePack.SenderMessagePack msg1 = message(AT_MOST_ONCE, "world");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_MOST_ONCE, "!!!!!");
        requestCreate(trafficId, inboxId, 2, 600, false);
        requestInsert(subInfo, topic, msg0);
        requestInsert(subInfo, topic, msg1, msg2);

        InboxFetchReply reply = requestFetchQoS0(trafficId, inboxId, 10);
        assertEquals(2, reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount());
        assertEquals(0, reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(0));
        assertEquals(1, reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(1));

        assertEquals(2, reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount());
        assertEquals(msg0.getMessage(0),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(0).getMsg().getMessage());
        assertEquals(msg1.getMessage(0),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(1).getMsg().getMessage());

        requestInsert(subInfo, topic, msg0, msg1, msg2);

        ArgumentCaptor<Overflowed> argCap = ArgumentCaptor.forClass(Overflowed.class);
        verify(eventCollector, times(2)).report(argCap.capture());
        for (Overflowed event : argCap.getAllValues()) {
            assertFalse(event.oldest());
            assertEquals(AT_MOST_ONCE, event.qos());
            assertTrue(event.dropCount() == 1 || event.dropCount() == 3);
        }

        reply = requestFetchQoS0(trafficId, inboxId, 10);
        assertEquals(2, reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount());
        assertEquals(0, reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(0));
        assertEquals(1, reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(1));

        assertEquals(2, reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount());
        assertEquals(msg0.getMessage(0),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(0).getMsg().getMessage());
        assertEquals(msg1.getMessage(0),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(1).getMsg().getMessage());
    }

    @Test(groups = "integration")
    public void insert() {
        String topic = "greeting";
        TopicMessagePack.SenderMessagePack msg0 = message(AT_MOST_ONCE, "hello");
        TopicMessagePack.SenderMessagePack msg1 = message(AT_MOST_ONCE, "world");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_MOST_ONCE, "!!!!!");
        requestCreate(trafficId, inboxId, 2, 1, false);
        requestInsert(subInfo, topic, msg1, msg2);
        when(clock.millis()).thenReturn(1100L);
        InboxInsertReply reply = requestInsert(subInfo, topic, msg1);
        assertEquals(InboxInsertResult.Result.NO_INBOX, reply.getResults(0).getResult());
    }
}

