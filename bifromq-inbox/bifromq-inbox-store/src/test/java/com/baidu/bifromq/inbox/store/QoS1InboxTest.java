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
import static com.baidu.bifromq.type.QoS.AT_LEAST_ONCE;
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
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import java.io.IOException;
import java.time.Clock;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class QoS1InboxTest extends InboxStoreTest {
    private final String tenantId = "tenantA";
    private final String topic = "greeting";
    private final String inboxId = "inboxId";
    private final SubInfo subInfo = SubInfo.newBuilder()
        .setTenantId(tenantId)
        .setInboxId(inboxId)
        .setSubQoS(AT_LEAST_ONCE)
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
        String scopedInboxIdUtf8 = scopedInboxId(tenantId, inboxId).toStringUtf8();

        requestCreate(tenantId, inboxId, 10, 2, true);
        requestInsert(subInfo, topic,
            message(AT_LEAST_ONCE, "hello"),
            message(AT_LEAST_ONCE, "world"));
        InboxFetchReply reply = requestFetchQoS1(tenantId, inboxId, 10);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1MsgCount(), 2);

        // not expire
        requestCreate(tenantId, inboxId, 10, 2, true);
        reply = requestFetchQoS1(tenantId, inboxId, 10);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1MsgCount(), 2);
        when(clock.millis()).thenReturn(2100L);

        requestCreate(tenantId, inboxId, 10, 100, true);
        HasReply has = requestHas(tenantId, inboxId);
        assertTrue(has.getExistsMap().get(scopedInboxId(tenantId, inboxId).toStringUtf8()));
        reply = requestFetchQoS1(tenantId, inboxId, 10);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1MsgCount(), 0);
    }

    @Test(groups = "integration")
    public void insertToExpiredInbox() {
        requestCreate(tenantId, inboxId, 10, 1, true);
        when(clock.millis()).thenReturn(1100L);
        SubInfo subInfo = SubInfo.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setSubQoS(AT_LEAST_ONCE)
            .setTopicFilter("greeting")
            .build();
        InboxInsertReply reply = requestInsert(subInfo, "greeting", message(AT_LEAST_ONCE, "hello"));
        assertEquals(reply.getResults(0).getSubInfo(), subInfo);
        assertEquals(reply.getResults(0).getResult(), InboxInsertResult.Result.NO_INBOX);
    }

    @Test(groups = "integration")
    public void insertToNonExistInbox() {
        String scopedInboxIdUtf8 = scopedInboxId(tenantId, inboxId).toStringUtf8();

        InboxInsertReply reply = requestInsert(subInfo,
            "greeting", message(AT_LEAST_ONCE, "hello"));
        assertEquals(reply.getResults(0).getResult(), InboxInsertResult.Result.NO_INBOX);
    }

    @Test(groups = "integration")
    public void fetchWithoutStartAfter() {
        String scopedInboxIdUtf8 = scopedInboxId(tenantId, inboxId).toStringUtf8();

        TopicMessagePack.SenderMessagePack msg1 = message(AT_LEAST_ONCE, "hello");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_LEAST_ONCE, "world");
        requestCreate(tenantId, inboxId, 10, 2, false);
        requestInsert(subInfo, topic, msg1, msg2);
        InboxFetchReply reply = requestFetchQoS1(tenantId, inboxId, 10);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1SeqCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Seq(0), 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Seq(1), 1);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1MsgCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Msg(0).getMsg().getMessage(),
            msg1.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Msg(1).getMsg().getMessage(),
            msg2.getMessage(0));

        InboxFetchReply reply1 = requestFetchQoS1(tenantId, inboxId, 10);
        assertEquals(reply1.getResultMap().get(scopedInboxIdUtf8).getQos1SeqList(),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos1SeqList());
        assertEquals(reply1.getResultMap().get(scopedInboxIdUtf8).getQos1MsgList(),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos1MsgList());
    }

    @Test(groups = "integration")
    public void fetchWithMaxLimit() {
        String scopedInboxIdUtf8 = scopedInboxId(tenantId, inboxId).toStringUtf8();

        TopicMessagePack.SenderMessagePack msg1 = message(AT_LEAST_ONCE, "hello");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_LEAST_ONCE, "world");
        requestCreate(tenantId, inboxId, 10, 600, false);
        requestInsert(subInfo, topic, msg1, msg2);
        InboxFetchReply reply = requestFetchQoS1(tenantId, inboxId, 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1SeqCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Seq(0), 0);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1MsgCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Msg(0).getMsg().getMessage(),
            msg1.getMessage(0));

        reply = requestFetchQoS1(tenantId, inboxId, 10);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1SeqCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1MsgCount(), 2);

        reply = requestFetchQoS1(tenantId, inboxId, 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1SeqCount(), 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1MsgCount(), 0);
    }

    @Test(groups = "integration")
    public void fetchWithStartAfter() {
        String scopedInboxIdUtf8 = scopedInboxId(tenantId, inboxId).toStringUtf8();

        TopicMessagePack.SenderMessagePack msg1 = message(AT_LEAST_ONCE, "hello");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_LEAST_ONCE, "world");
        TopicMessagePack.SenderMessagePack msg3 = message(AT_LEAST_ONCE, "!!!!!");
        requestCreate(tenantId, inboxId, 10, 600, false);
        requestInsert(subInfo, topic, msg1, msg2, msg3);

        InboxFetchReply reply = requestFetchQoS1(tenantId, inboxId, 1, 0L);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1SeqCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Seq(0), 1);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1MsgCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Msg(0).getMsg().getMessage(),
            msg2.getMessage(0));

        reply = requestFetchQoS1(tenantId, inboxId, 10, 0L);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1SeqCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Seq(0), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Seq(1), 2);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1MsgCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Msg(0).getMsg().getMessage(),
            msg2.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Msg(1).getMsg().getMessage(),
            msg3.getMessage(0));

        reply = requestFetchQoS1(tenantId, inboxId, 10, 2L);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1SeqCount(), 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1MsgCount(), 0);
    }

    @Test(groups = "integration")
    public void commit() {
        String scopedInboxIdUtf8 = scopedInboxId(tenantId, inboxId).toStringUtf8();

        TopicMessagePack.SenderMessagePack msg1 = message(AT_LEAST_ONCE, "hello");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_LEAST_ONCE, "world");
        TopicMessagePack.SenderMessagePack msg3 = message(AT_LEAST_ONCE, "!!!!!");
        requestCreate(tenantId, inboxId, 10, 600, false);
        requestInsert(subInfo, topic, msg1, msg2, msg3);
        requestCommitQoS1(tenantId, inboxId, 1);

        InboxFetchReply reply = requestFetchQoS1(tenantId, inboxId, 10, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1SeqCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Seq(0), 2);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1MsgCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Msg(0).getMsg().getMessage(),
            msg3.getMessage(0));

        // nothing should happen
        requestCommitQoS1(tenantId, inboxId, 1);

        reply = requestFetchQoS1(tenantId, inboxId, 10, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1SeqCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Seq(0), 2);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1MsgCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Msg(0).getMsg().getMessage(),
            msg3.getMessage(0));

        requestCommitQoS1(tenantId, inboxId, 2);
        reply = requestFetchQoS1(tenantId, inboxId, 10, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1SeqCount(), 0);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1MsgCount(), 0);
    }

    @Test(groups = "integration")
    public void commitAll() {
        String scopedInboxIdUtf8 = scopedInboxId(tenantId, inboxId).toStringUtf8();

        TopicMessagePack.SenderMessagePack msg1 = message(AT_LEAST_ONCE, "a");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_LEAST_ONCE, "b");
        TopicMessagePack.SenderMessagePack msg3 = message(AT_LEAST_ONCE, "c");
        TopicMessagePack.SenderMessagePack msg4 = message(AT_LEAST_ONCE, "d");
        TopicMessagePack.SenderMessagePack msg5 = message(AT_LEAST_ONCE, "e");
        TopicMessagePack.SenderMessagePack msg6 = message(AT_LEAST_ONCE, "f");
        requestCreate(tenantId, inboxId, 10, 600, false);
        requestInsert(subInfo, topic, msg1, msg2, msg3);
        requestInsert(subInfo, topic, msg4, msg5, msg6);
        requestCommitQoS1(tenantId, inboxId, 5);
        InboxFetchReply reply = requestFetchQoS1(tenantId, inboxId, 10, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1SeqCount(), 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1MsgCount(), 0);
    }

    @Test(groups = "integration")
    public void insertDropOldest() {
        String scopedInboxIdUtf8 = scopedInboxId(tenantId, inboxId).toStringUtf8();

        TopicMessagePack.SenderMessagePack msg0 = message(AT_LEAST_ONCE, "hello");
        TopicMessagePack.SenderMessagePack msg1 = message(AT_LEAST_ONCE, "world");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_LEAST_ONCE, "!!!!!");
        requestCreate(tenantId, inboxId, 2, 600, true);
        requestInsert(subInfo, topic, msg0, msg1);
        requestInsert(subInfo, topic, msg2);

        InboxFetchReply reply = requestFetchQoS1(tenantId, inboxId, 10);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1SeqCount(), 3);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Seq(0), 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Seq(1), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Seq(2), 2);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1MsgCount(), 3);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Msg(0).getMsg().getMessage(),
            msg0.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Msg(1).getMsg().getMessage(),
            msg1.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Msg(2).getMsg().getMessage(),
            msg2.getMessage(0));

        requestInsert(subInfo, topic, msg0, msg1, msg2);

        ArgumentCaptor<Overflowed> argCap = ArgumentCaptor.forClass(Overflowed.class);
        verify(eventCollector, times(1)).report(argCap.capture());
        Overflowed event = argCap.getValue();
        assertTrue(event.oldest());
        assertEquals(event.qos(), AT_LEAST_ONCE);
        assertEquals(event.dropCount(), 4);

        reply = requestFetchQoS1(tenantId, inboxId, 10);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1SeqCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Seq(0), 3);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Seq(1), 4);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1MsgCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Msg(0).getMsg().getMessage(),
            msg1.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Msg(1).getMsg().getMessage(),
            msg2.getMessage(0));
    }

    @Test(groups = "integration")
    public void insertDropYoungest() {
        String scopedInboxIdUtf8 = scopedInboxId(tenantId, inboxId).toStringUtf8();

        TopicMessagePack.SenderMessagePack msg0 = message(AT_LEAST_ONCE, "hello");
        TopicMessagePack.SenderMessagePack msg1 = message(AT_LEAST_ONCE, "world");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_LEAST_ONCE, "!!!!!");
        requestCreate(tenantId, inboxId, 2, 600, false);
        requestInsert(subInfo, topic, msg0);
        requestInsert(subInfo, topic, msg1, msg2);

        InboxFetchReply reply = requestFetchQoS1(tenantId, inboxId, 10);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1SeqCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Seq(0), 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Seq(1), 1);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1MsgCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Msg(0).getMsg().getMessage(),
            msg0.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Msg(1).getMsg().getMessage(),
            msg1.getMessage(0));

        requestInsert(subInfo, topic, msg0, msg1, msg2);

        ArgumentCaptor<Overflowed> argCap = ArgumentCaptor.forClass(Overflowed.class);
        verify(eventCollector, times(2)).report(argCap.capture());

        for (Overflowed event : argCap.getAllValues()) {
            assertFalse(event.oldest());
            assertEquals(event.qos(), AT_LEAST_ONCE);
            assertTrue(event.dropCount() == 1 || event.dropCount() == 3);
        }

        reply = requestFetchQoS1(tenantId, inboxId, 10);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1SeqCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Seq(0), 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Seq(1), 1);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1MsgCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Msg(0).getMsg().getMessage(),
            msg0.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos1Msg(1).getMsg().getMessage(),
            msg1.getMessage(0));
    }

    @Test(groups = "integration")
    public void insert() {
        String tenantId = "tenantId";
        String inboxId = "inboxId";

        TopicMessagePack.SenderMessagePack msg0 = message(AT_LEAST_ONCE, "hello");
        TopicMessagePack.SenderMessagePack msg1 = message(AT_LEAST_ONCE, "world");
        TopicMessagePack.SenderMessagePack msg2 = message(AT_LEAST_ONCE, "!!!!!");
        requestCreate(tenantId, inboxId, 2, 1, false);
        requestInsert(subInfo, topic, msg1, msg2);
        when(clock.millis()).thenReturn(1100L);
        InboxInsertReply reply = requestInsert(subInfo, topic, msg1);
        assertEquals(reply.getResults(0).getResult(), InboxInsertResult.Result.NO_INBOX);
    }
}

