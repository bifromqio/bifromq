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

import static com.baidu.bifromq.type.QoS.AT_MOST_ONCE;
import static com.baidu.bifromq.type.QoS.EXACTLY_ONCE;
import static org.mockito.Mockito.clearInvocations;
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
import com.baidu.bifromq.inbox.util.KeyUtil;
import com.baidu.bifromq.plugin.eventcollector.inboxservice.Overflowed;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import java.io.IOException;
import java.time.Clock;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class QoS2InboxTest extends InboxStoreTest {
    private final String tenantId = "tenantA";

    private final String topic = "greeting";
    private final String inboxId = "inboxId";
    private final SubInfo subInfo = SubInfo.newBuilder()
        .setTenantId(tenantId)
        .setInboxId(inboxId)
        .setSubQoS(EXACTLY_ONCE)
        .setTopicFilter("greeting")
        .build();

    @Mock
    private Clock clock;

    @BeforeMethod(groups = "integration")
    public void resetClock() {
        when(clock.millis()).thenReturn(0L);
    }

    @AfterMethod(groups = "integration")
    public void clearInbox() {
        requestDelete(tenantId, inboxId);
    }

    @Override
    protected Clock getClock() {
        return clock;
    }


    @Test(groups = "integration")
    public void implicitCleanExpiredInboxDuringCreate() {

        String scopedInboxIdUtf8 = KeyUtil.scopedInboxId(tenantId, inboxId).toStringUtf8();

        requestCreate(tenantId, inboxId, 10, 2, false);
        requestSub(tenantId, inboxId, subInfo.getTopicFilter(), AT_MOST_ONCE);
        requestInsert(subInfo, "greeting",
            message(EXACTLY_ONCE, "hello"),
            message(EXACTLY_ONCE, "world"));

        // not expire
        requestCreate(tenantId, inboxId, 10, 2, false);
        requestSub(tenantId, inboxId, subInfo.getTopicFilter(), AT_MOST_ONCE);
        InboxFetchReply reply = requestFetchQoS2(tenantId, inboxId, 10, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2MsgCount(), 2);

        when(clock.millis()).thenReturn(2100L);

        requestCreate(tenantId, inboxId, 10, 100, false);
        HasReply has = requestHas(tenantId, inboxId);
        assertTrue(has.getExistsMap().get(KeyUtil.scopedInboxId(tenantId, inboxId).toStringUtf8()));
        reply = requestFetchQoS2(tenantId, inboxId, 10, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2MsgCount(), 0);
    }

    @Test(groups = "integration")
    public void insertToExpiredInbox() {
        String tenantId = "tenantId";
        String inboxId = "inboxId";
        SubInfo subInfo = SubInfo.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setSubQoS(EXACTLY_ONCE)
            .setTopicFilter("greeting")
            .build();

        requestCreate(tenantId, inboxId, 10, 1, false);
        when(clock.millis()).thenReturn(1100L);
        InboxInsertReply reply = requestInsert(subInfo, "greeting",
            message(EXACTLY_ONCE, "hello"));
        assertEquals(reply.getResults(0).getResult(), InboxInsertResult.Result.NO_INBOX);
    }

    @Test(groups = "integration")
    public void insertToNonExistInbox() {
        InboxInsertReply reply = requestInsert(subInfo, "greeting",
            message(EXACTLY_ONCE, "hello"));
        assertEquals(reply.getResults(0).getResult(), InboxInsertResult.Result.NO_INBOX);
    }

    @Test(groups = "integration")
    public void fetchWithoutStartAfter() {
        String scopedInboxIdUtf8 = KeyUtil.scopedInboxId(tenantId, inboxId).toStringUtf8();

        TopicMessagePack.PublisherPack msg1 = message(EXACTLY_ONCE, "hello");
        TopicMessagePack.PublisherPack msg2 = message(EXACTLY_ONCE, "world");
        requestCreate(tenantId, inboxId, 10, 2, false);
        requestSub(tenantId, inboxId, subInfo.getTopicFilter(), AT_MOST_ONCE);
        requestInsert(subInfo, topic, msg1, msg2);
        InboxFetchReply reply = requestFetchQoS2(tenantId, inboxId, 10, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2SeqCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(0), 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(1), 1);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2MsgCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(0).getMsg().getMessage(),
            msg1.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(1).getMsg().getMessage(),
            msg2.getMessage(0));

        InboxFetchReply reply1 = requestFetchQoS2(tenantId, inboxId, 10, null);
        assertEquals(reply1.getResultMap().get(scopedInboxIdUtf8).getQos2SeqList(),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos2SeqList());
        assertEquals(reply1.getResultMap().get(scopedInboxIdUtf8).getQos2MsgList(),
            reply.getResultMap().get(scopedInboxIdUtf8).getQos2MsgList());
    }

    @Test(groups = "integration")
    public void fetchWithMaxLimit() {
        String scopedInboxIdUtf8 = KeyUtil.scopedInboxId(tenantId, inboxId).toStringUtf8();

        TopicMessagePack.PublisherPack msg1 = message(EXACTLY_ONCE, "hello");
        TopicMessagePack.PublisherPack msg2 = message(EXACTLY_ONCE, "world");
        requestCreate(tenantId, inboxId, 10, 600, false);
        requestSub(tenantId, inboxId, subInfo.getTopicFilter(), AT_MOST_ONCE);
        requestInsert(subInfo, topic, msg1, msg2);
        InboxFetchReply reply = requestFetchQoS2(tenantId, inboxId, 1, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2SeqCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(0), 0);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2MsgCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(0).getMsg().getMessage(),
            msg1.getMessage(0));

        reply = requestFetchQoS2(tenantId, inboxId, 10, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2SeqCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2MsgCount(), 2);

        reply = requestFetchQoS2(tenantId, inboxId, 0, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2SeqCount(), 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2MsgCount(), 0);
    }

    @Test(groups = "integration")
    public void fetchWithStartAfter() {
        String scopedInboxIdUtf8 = KeyUtil.scopedInboxId(tenantId, inboxId).toStringUtf8();

        TopicMessagePack.PublisherPack msg1 = message(EXACTLY_ONCE, "hello");
        TopicMessagePack.PublisherPack msg2 = message(EXACTLY_ONCE, "world");
        TopicMessagePack.PublisherPack msg3 = message(EXACTLY_ONCE, "!!!!!");
        requestCreate(tenantId, inboxId, 10, 600, false);
        requestSub(tenantId, inboxId, subInfo.getTopicFilter(), AT_MOST_ONCE);
        requestInsert(subInfo, topic, msg1, msg2, msg3);

        InboxFetchReply reply = requestFetchQoS2(tenantId, inboxId, 1, 0L);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2SeqCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(0), 1);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2MsgCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(0).getMsg().getMessage(),
            msg2.getMessage(0));

        reply = requestFetchQoS2(tenantId, inboxId, 10, 0L);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2SeqCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(0), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(1), 2);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2MsgCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(0).getMsg().getMessage(),
            msg2.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(1).getMsg().getMessage(),
            msg3.getMessage(0));

        reply = requestFetchQoS2(tenantId, inboxId, 10, 2L);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2SeqCount(), 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2MsgCount(), 0);
    }

    @Test(groups = "integration")
    public void commit() {
        String scopedInboxIdUtf8 = KeyUtil.scopedInboxId(tenantId, inboxId).toStringUtf8();

        TopicMessagePack.PublisherPack msg1 = message(EXACTLY_ONCE, "hello");
        TopicMessagePack.PublisherPack msg2 = message(EXACTLY_ONCE, "world");
        TopicMessagePack.PublisherPack msg3 = message(EXACTLY_ONCE, "!!!!!");
        requestCreate(tenantId, inboxId, 10, 600, false);
        requestSub(tenantId, inboxId, subInfo.getTopicFilter(), AT_MOST_ONCE);
        requestInsert(subInfo, topic, msg1, msg2, msg3);
        requestCommitQoS2(tenantId, inboxId, 1);

        InboxFetchReply reply = requestFetchQoS2(tenantId, inboxId, 10, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2SeqCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(0), 2);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2MsgCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(0).getMsg().getMessage(),
            msg3.getMessage(0));

        // nothing should happen
        requestCommitQoS2(tenantId, inboxId, 1);

        reply = requestFetchQoS2(tenantId, inboxId, 10, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2SeqCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(0), 2);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2MsgCount(), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(0).getMsg().getMessage(),
            msg3.getMessage(0));

        requestCommitQoS2(tenantId, inboxId, 2);
        reply = requestFetchQoS2(tenantId, inboxId, 10, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2SeqCount(), 0);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2MsgCount(), 0);
    }

    @Test(groups = "integration")
    public void insertDuplicated() {
        String scopedInboxIdUtf8 = KeyUtil.scopedInboxId(tenantId, inboxId).toStringUtf8();

        TopicMessagePack.PublisherPack msg0 = message(EXACTLY_ONCE, "hello");
        TopicMessagePack.PublisherPack msg1 = message(EXACTLY_ONCE, "world");
        TopicMessagePack.PublisherPack msg2 = message(EXACTLY_ONCE, "a");
        requestCreate(tenantId, inboxId, 10, 600, true);
        requestSub(tenantId, inboxId, subInfo.getTopicFilter(), AT_MOST_ONCE);
        requestInsert(subInfo, topic, msg0, msg0, msg1);
        requestInsert(subInfo, topic, msg0, msg2);
        InboxFetchReply reply = requestFetchQoS2(tenantId, inboxId, 10, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2SeqCount(), 3);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(0), 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(1), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(2), 2);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2MsgCount(), 3);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(0).getMsg().getMessage(),
            msg0.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(1).getMsg().getMessage(),
            msg1.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(2).getMsg().getMessage(),
            msg2.getMessage(0));
    }

    @Test(groups = "integration")
    public void insertSameMessageIdFromDifferentClients() {
        String scopedInboxIdUtf8 = KeyUtil.scopedInboxId(tenantId, inboxId).toStringUtf8();

        TopicMessagePack.PublisherPack msg0 = message(0, EXACTLY_ONCE, "hello", ClientInfo.newBuilder()
            .setTenantId(tenantId)
            .putMetadata("userId", "user1")
            .build());
        TopicMessagePack.PublisherPack msg1 = message(0, EXACTLY_ONCE, "world", ClientInfo.newBuilder()
            .setTenantId(tenantId)
            .putMetadata("userId", "user2")
            .build());
        requestCreate(tenantId, inboxId, 10, 600, true);
        requestSub(tenantId, inboxId, subInfo.getTopicFilter(), AT_MOST_ONCE);
        requestInsert(subInfo, topic, msg0, msg1);
        InboxFetchReply reply = requestFetchQoS2(tenantId, inboxId, 10, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2SeqCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(0), 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(1), 1);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2MsgCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(0).getMsg().getMessage(),
            msg0.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(1).getMsg().getMessage(),
            msg1.getMessage(0));
    }


    @Test(groups = "integration")
    public void insertDropOldest() {
        clearInvocations(eventCollector);
        String scopedInboxIdUtf8 = KeyUtil.scopedInboxId(tenantId, inboxId).toStringUtf8();

        TopicMessagePack.PublisherPack msg0 = message(EXACTLY_ONCE, "hello");
        TopicMessagePack.PublisherPack msg1 = message(EXACTLY_ONCE, "world");
        TopicMessagePack.PublisherPack msg2 = message(EXACTLY_ONCE, "a");
        TopicMessagePack.PublisherPack msg3 = message(EXACTLY_ONCE, "b");
        TopicMessagePack.PublisherPack msg4 = message(EXACTLY_ONCE, "c");
        TopicMessagePack.PublisherPack msg5 = message(EXACTLY_ONCE, "d");
        requestCreate(tenantId, inboxId, 2, 600, true);
        requestSub(tenantId, inboxId, subInfo.getTopicFilter(), AT_MOST_ONCE);
        requestInsert(subInfo, topic, msg0, msg1);
        requestInsert(subInfo, topic, msg2);

        InboxFetchReply reply = requestFetchQoS2(tenantId, inboxId, 10, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2SeqCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(0), 1);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(1), 2);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2MsgCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(0).getMsg().getMessage(),
            msg1.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(1).getMsg().getMessage(),
            msg2.getMessage(0));

        requestInsert(subInfo, topic, msg3, msg4, msg5);

        ArgumentCaptor<Overflowed> argCap = ArgumentCaptor.forClass(Overflowed.class);
        verify(eventCollector, times(2)).report(argCap.capture());

        for (Overflowed event : argCap.getAllValues()) {
            assertTrue(event.oldest());
            assertEquals(event.qos(), EXACTLY_ONCE);
            assertTrue(event.dropCount() == 1 || event.dropCount() == 3);
        }

        reply = requestFetchQoS2(tenantId, inboxId, 10, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2SeqCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(0), 3);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(1), 4);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2MsgCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(0).getMsg().getMessage(),
            msg4.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(1).getMsg().getMessage(),
            msg5.getMessage(0));
    }

    @Test(groups = "integration")
    public void insertDropYoungest() {
        clearInvocations(eventCollector);
        String scopedInboxIdUtf8 = KeyUtil.scopedInboxId(tenantId, inboxId).toStringUtf8();

        TopicMessagePack.PublisherPack msg0 = message(EXACTLY_ONCE, "hello");
        TopicMessagePack.PublisherPack msg1 = message(EXACTLY_ONCE, "world");
        TopicMessagePack.PublisherPack msg2 = message(EXACTLY_ONCE, "a");
        TopicMessagePack.PublisherPack msg3 = message(EXACTLY_ONCE, "b");
        TopicMessagePack.PublisherPack msg4 = message(EXACTLY_ONCE, "c");
        TopicMessagePack.PublisherPack msg5 = message(EXACTLY_ONCE, "d");

        requestCreate(tenantId, inboxId, 2, 600, false);
        requestSub(tenantId, inboxId, subInfo.getTopicFilter(), AT_MOST_ONCE);
        requestInsert(subInfo, topic, msg0);
        requestInsert(subInfo, topic, msg1, msg2);

        InboxFetchReply reply = requestFetchQoS2(tenantId, inboxId, 10, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2SeqCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(0), 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(1), 1);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2MsgCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(0).getMsg().getMessage(),
            msg0.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(1).getMsg().getMessage(),
            msg1.getMessage(0));

        requestInsert(subInfo, topic, msg3, msg4, msg5);

        ArgumentCaptor<Overflowed> argCap = ArgumentCaptor.forClass(Overflowed.class);
        verify(eventCollector, times(2)).report(argCap.capture());

        for (Overflowed event : argCap.getAllValues()) {
            assertFalse(event.oldest());
            assertEquals(event.qos(), EXACTLY_ONCE);
            assertTrue(event.dropCount() == 1 || event.dropCount() == 3);
        }

        reply = requestFetchQoS2(tenantId, inboxId, 10, null);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2SeqCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(0), 0);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(1), 1);

        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2MsgCount(), 2);
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(0).getMsg().getMessage(),
            msg0.getMessage(0));
        assertEquals(reply.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(1).getMsg().getMessage(),
            msg1.getMessage(0));
    }

    @Test(groups = "integration")
    public void insert() {
        String scopedInboxIdUtf8 = KeyUtil.scopedInboxId(tenantId, inboxId).toStringUtf8();

        TopicMessagePack.PublisherPack msg0 = message(EXACTLY_ONCE, "hello");
        TopicMessagePack.PublisherPack msg1 = message(EXACTLY_ONCE, "world");
        TopicMessagePack.PublisherPack msg2 = message(EXACTLY_ONCE, "!!!!!");
        requestCreate(tenantId, inboxId, 2, 1, false);
        requestInsert(subInfo, topic, msg1, msg2);
        when(clock.millis()).thenReturn(1100L);
        InboxInsertReply reply = requestInsert(subInfo, topic, msg1);
        assertEquals(reply.getResults(0).getResult(), InboxInsertResult.Result.NO_INBOX);
    }
}
