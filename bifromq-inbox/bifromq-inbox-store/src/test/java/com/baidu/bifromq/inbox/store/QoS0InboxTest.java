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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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
    private final String tenantId = "tenantA";
    private final String inboxId = "inboxId";
    private final SubInfo subInfo = SubInfo.newBuilder()
        .setTenantId(tenantId)
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
        String scopedInboxIdUtf8 = scopedInboxId(tenantId, inboxId).toStringUtf8();
        requestCreate(tenantId, inboxId, 10, 2, false);
        requestInsert(subInfo, topic,
            message(AT_MOST_ONCE, "hello"),
            message(AT_MOST_ONCE, "world"));

        // not expire
        requestCreate(tenantId, inboxId, 10, 2, false);
        InboxFetchReply reply = requestFetchQoS0(tenantId, inboxId, 10);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount(), 2);

        when(clock.millis()).thenReturn(2100L);

        requestCreate(tenantId, inboxId, 10, 100, false);
        HasReply has = requestHas(tenantId, inboxId);
        assertTrue(has.getExistsMap().get(scopedInboxId(tenantId, inboxId).toStringUtf8()));
        reply = requestFetchQoS0(tenantId, inboxId, 10);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount(), 0);
    }

    @Test(groups = "integration")
    public void insertToExpiredInbox() {
        requestCreate(tenantId, inboxId, 10, 1, false);
        when(clock.millis()).thenReturn(1100L);
        InboxInsertReply reply = requestInsert(subInfo, "greeting", message(AT_MOST_ONCE, "hello"));
        assertEquals(reply.getResults(0).getResult(), InboxInsertResult.Result.NO_INBOX);
    }

    @Test(groups = "integration")
    public void insertToNonExistInbox() {
        InboxInsertReply reply = requestInsert(subInfo, "greeting", message(AT_MOST_ONCE, "hello"));
        assertEquals(reply.getResults(0).getResult(), InboxInsertResult.Result.NO_INBOX);
    }

    @Test(groups = "integration")
    public void fetchWithoutStartAfter() {
        String scopedInboxIdUtf8 = scopedInboxId(tenantId, inboxId).toStringUtf8();
        String topic = "greeting";
        TopicMessagePack.SenderMessagePack msg1 = message(AT_MOST_ONCE, "hello");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_MOST_ONCE, "world");
        requestCreate(tenantId, inboxId, 10, 2, false);
        requestInsert(subInfo, topic, msg1, msg2);
        InboxFetchReply reply = requestFetchQoS0(tenantId, inboxId, 10);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(0), 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(1), 1);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(0).getMsg().getMessage(),
            msg1.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(1).getMsg().getMessage(),
            msg2.getMessage(0));

        InboxFetchReply reply1 = requestFetchQoS0(tenantId, inboxId, 10);
        assertEquals(reply1.getResultMap().get(scopedInboxIdUtf8).getQos0SeqList(),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqList());
        assertEquals(reply1.getResultMap().get(scopedInboxIdUtf8).getQos0MsgList(),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgList());
    }

    @Test(groups = "integration")
    public void fetchWithMaxLimit() {
        String scopedInboxIdUtf8 = scopedInboxId(tenantId, inboxId).toStringUtf8();
        String topic = "greeting";
        TopicMessagePack.SenderMessagePack msg1 = message(AT_MOST_ONCE, "hello");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_MOST_ONCE, "world");
        requestCreate(tenantId, inboxId, 10, 600, false);
        requestInsert(subInfo, topic, msg1, msg2);
        InboxFetchReply reply = requestFetchQoS0(tenantId, inboxId, 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(0), 0);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(0).getMsg().getMessage(),
            msg1.getMessage(0));

        reply = requestFetchQoS0(tenantId, inboxId, 10);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount(), 2);

        reply = requestFetchQoS0(tenantId, inboxId, 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount(), 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount(), 0);
    }

    @Test(groups = "integration")
    public void fetchWithStartAfter() {
        String scopedInboxIdUtf8 = scopedInboxId(tenantId, inboxId).toStringUtf8();

        String topic = "greeting";
        TopicMessagePack.SenderMessagePack msg1 = message(AT_MOST_ONCE, "a");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_MOST_ONCE, "b");
        TopicMessagePack.SenderMessagePack msg3 = message(AT_MOST_ONCE, "c");
        TopicMessagePack.SenderMessagePack msg4 = message(AT_MOST_ONCE, "d");
        TopicMessagePack.SenderMessagePack msg5 = message(AT_MOST_ONCE, "e");
        TopicMessagePack.SenderMessagePack msg6 = message(AT_MOST_ONCE, "f");
        requestCreate(tenantId, inboxId, 10, 600, false);
        requestInsert(subInfo, topic, msg1, msg2, msg3);
        requestInsert(subInfo, topic, msg4, msg5, msg6);

        InboxFetchReply reply = requestFetchQoS0(tenantId, inboxId, 1, 0L);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(0), 1);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(0).getMsg().getMessage(),
            msg2.getMessage(0));

        reply = requestFetchQoS0(tenantId, inboxId, 10, 0L);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount(), 5);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(0), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(1), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(2), 3);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(3), 4);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(4), 5);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount(), 5);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(0).getMsg().getMessage(),
            msg2.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(1).getMsg().getMessage(),
            msg3.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(2).getMsg().getMessage(),
            msg4.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(3).getMsg().getMessage(),
            msg5.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(4).getMsg().getMessage(),
            msg6.getMessage(0));

        reply = requestFetchQoS0(tenantId, inboxId, 10, 5L);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount(), 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount(), 0);
    }

    @Test(groups = "integration")
    public void commit() {
        String scopedInboxIdUtf8 = scopedInboxId(tenantId, inboxId).toStringUtf8();
        String topic = "greeting";
        TopicMessagePack.SenderMessagePack msg1 = message(AT_MOST_ONCE, "hello");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_MOST_ONCE, "world");
        TopicMessagePack.SenderMessagePack msg3 = message(AT_MOST_ONCE, "!!!!!");
        requestCreate(tenantId, inboxId, 10, 600, false);
        requestInsert(subInfo, topic, msg1, msg2, msg3);
        requestCommitQoS0(tenantId, inboxId, 1);

        InboxFetchReply reply = requestFetchQoS0(tenantId, inboxId, 10, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(0), 2);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(0).getMsg().getMessage(),
            msg3.getMessage(0));

        // nothing should happen
        requestCommitQoS0(tenantId, inboxId, 1);

        reply = requestFetchQoS0(tenantId, inboxId, 10, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(0), 2);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(0).getMsg().getMessage(),
            msg3.getMessage(0));

        requestCommitQoS0(tenantId, inboxId, 2);
        reply = requestFetchQoS0(tenantId, inboxId, 10, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount(), 0);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount(), 0);
    }

    @Test(groups = "integration")
    public void commitAll() {
        String scopedInboxIdUtf8 = scopedInboxId(tenantId, inboxId).toStringUtf8();
        String topic = "greeting";
        TopicMessagePack.SenderMessagePack msg1 = message(AT_MOST_ONCE, "a");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_MOST_ONCE, "b");
        TopicMessagePack.SenderMessagePack msg3 = message(AT_MOST_ONCE, "c");
        TopicMessagePack.SenderMessagePack msg4 = message(AT_MOST_ONCE, "d");
        TopicMessagePack.SenderMessagePack msg5 = message(AT_MOST_ONCE, "e");
        TopicMessagePack.SenderMessagePack msg6 = message(AT_MOST_ONCE, "f");
        requestCreate(tenantId, inboxId, 10, 600, false);
        requestInsert(subInfo, topic, msg1, msg2, msg3);
        requestInsert(subInfo, topic, msg4, msg5, msg6);
        requestCommitQoS0(tenantId, inboxId, 5);
        InboxFetchReply reply = requestFetchQoS0(tenantId, inboxId, 10, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount(), 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount(), 0);
    }

    @Test(groups = "integration")
    public void touch() {
        requestCreate(tenantId, inboxId, 10, 1, false);
        when(clock.millis()).thenReturn(900L);
        requestTouch(tenantId, inboxId);

        when(clock.millis()).thenReturn(1100L);
        assertTrue(requestHas(tenantId, inboxId).getExistsMap().get(scopedInboxId(tenantId, inboxId).toStringUtf8()));
    }

    @Test(groups = "integration")
    public void insertDropOldest() {
        String scopedInboxIdUtf8 = scopedInboxId(tenantId, inboxId).toStringUtf8();
        String topic = "greeting";
        TopicMessagePack.SenderMessagePack msg0 = message(AT_MOST_ONCE, "hello");
        TopicMessagePack.SenderMessagePack msg1 = message(AT_MOST_ONCE, "world");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_MOST_ONCE, "!!!!!");
        requestCreate(tenantId, inboxId, 2, 600, true);
        requestInsert(subInfo, topic, msg0, msg1);
        requestInsert(subInfo, topic, msg2);

        InboxFetchReply reply = requestFetchQoS0(tenantId, inboxId, 10);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount(), 3);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(0), 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(1), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(2), 2);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount(), 3);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(0).getMsg().getMessage(),
            msg0.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(1).getMsg().getMessage(),
            msg1.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(2).getMsg().getMessage(),
            msg2.getMessage(0));

        requestInsert(subInfo, topic, msg0, msg1, msg2);

        ArgumentCaptor<Overflowed> argCap = ArgumentCaptor.forClass(Overflowed.class);
        verify(eventCollector, times(1)).report(argCap.capture());
        Overflowed event = argCap.getValue();
        assertTrue(event.oldest());
        assertEquals(event.qos(), AT_MOST_ONCE);
        assertEquals(event.dropCount(), 4);

        reply = requestFetchQoS0(tenantId, inboxId, 10);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(0), 3);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(1), 4);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(0).getMsg().getMessage(),
            msg1.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(1).getMsg().getMessage(),
            msg2.getMessage(0));
    }

    @Test(groups = "integration")
    public void insertDropYoungest() {
        String scopedInboxIdUtf8 = scopedInboxId(tenantId, inboxId).toStringUtf8();
        String topic = "greeting";
        TopicMessagePack.SenderMessagePack msg0 = message(AT_MOST_ONCE, "hello");
        TopicMessagePack.SenderMessagePack msg1 = message(AT_MOST_ONCE, "world");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_MOST_ONCE, "!!!!!");
        requestCreate(tenantId, inboxId, 2, 600, false);
        requestInsert(subInfo, topic, msg0);
        requestInsert(subInfo, topic, msg1, msg2);

        InboxFetchReply reply = requestFetchQoS0(tenantId, inboxId, 10);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(0), 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(1), 1);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(0).getMsg().getMessage(),
            msg0.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(1).getMsg().getMessage(),
            msg1.getMessage(0));

        requestInsert(subInfo, topic, msg0, msg1, msg2);

        ArgumentCaptor<Overflowed> argCap = ArgumentCaptor.forClass(Overflowed.class);
        verify(eventCollector, times(2)).report(argCap.capture());
        for (Overflowed event : argCap.getAllValues()) {
            assertFalse(event.oldest());
            assertEquals(event.qos(), AT_MOST_ONCE);
            assertTrue(event.dropCount() == 1 || event.dropCount() == 3);
        }

        reply = requestFetchQoS0(tenantId, inboxId, 10);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(0), 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Seq(1), 1);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(0).getMsg().getMessage(),
            msg0.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(1).getMsg().getMessage(),
            msg1.getMessage(0));
    }

    @Test(groups = "integration")
    public void insert() {
        String topic = "greeting";
        TopicMessagePack.SenderMessagePack msg0 = message(AT_MOST_ONCE, "hello");
        TopicMessagePack.SenderMessagePack msg1 = message(AT_MOST_ONCE, "world");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_MOST_ONCE, "!!!!!");
        requestCreate(tenantId, inboxId, 2, 1, false);
        requestInsert(subInfo, topic, msg1, msg2);
        when(clock.millis()).thenReturn(1100L);
        InboxInsertReply reply = requestInsert(subInfo, topic, msg1);
        assertEquals(reply.getResults(0).getResult(), InboxInsertResult.Result.NO_INBOX);
    }
}

