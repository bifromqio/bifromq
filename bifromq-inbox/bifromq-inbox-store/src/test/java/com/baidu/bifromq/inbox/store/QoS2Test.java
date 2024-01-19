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

package com.baidu.bifromq.inbox.store;

import static com.baidu.bifromq.type.QoS.EXACTLY_ONCE;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.inbox.storage.proto.BatchCreateRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchFetchRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchSubRequest;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.InboxSubMessagePack;
import com.baidu.bifromq.inbox.storage.proto.SubMessagePack;
import com.baidu.bifromq.plugin.eventcollector.inboxservice.Overflowed;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessagePack;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class QoS2Test extends InboxInsertTest {
    @Test(groups = "integration")
    public void fetchWithoutStartAfter() {
        fetchWithoutStartAfter(EXACTLY_ONCE);
    }

    @Test(groups = "integration")
    public void fetchWithMaxLimit() {
        fetchWithMaxLimit(EXACTLY_ONCE);
    }

    @Test(groups = "integration")
    public void fetchWithStartAfter() {
        fetchWithStartAfter(EXACTLY_ONCE);
    }

    @Test(groups = "integration")
    public void commit() {
        commit(EXACTLY_ONCE);
    }

    @Test(groups = "integration")
    public void commitAll() {
        commitAll(EXACTLY_ONCE);
    }

    @Test(groups = "integration")
    public void insertDropOldest() {
        clearInvocations(eventCollector);
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        String topicFilter = "/a/b/c";
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        requestCreate(BatchCreateRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(2)
            .setExpirySeconds(2)
            .setDropOldest(true)
            .setLimit(2)
            .setClient(client)
            .setNow(now)
            .build());
        requestSub(BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setSubQoS(EXACTLY_ONCE)
            .setNow(now)
            .build());

        TopicMessagePack.PublisherPack msg0 = message(EXACTLY_ONCE, "hello");
        TopicMessagePack.PublisherPack msg1 = message(EXACTLY_ONCE, "world");
        TopicMessagePack.PublisherPack msg2 = message(EXACTLY_ONCE, "a");
        TopicMessagePack.PublisherPack msg3 = message(EXACTLY_ONCE, "b");
        TopicMessagePack.PublisherPack msg4 = message(EXACTLY_ONCE, "c");
        TopicMessagePack.PublisherPack msg5 = message(EXACTLY_ONCE, "d");
        requestInsert(InboxSubMessagePack.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(QoS.EXACTLY_ONCE)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg0)
                    .build())
                .build())
            .build());
        requestInsert(InboxSubMessagePack.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(QoS.EXACTLY_ONCE)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg1)
                    .build())
                .build())
            .build());
        requestInsert(InboxSubMessagePack.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(QoS.EXACTLY_ONCE)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg2)
                    .build())
                .build())
            .build());

        ArgumentCaptor<Overflowed> argCap = ArgumentCaptor.forClass(Overflowed.class);
        verify(eventCollector).report(argCap.capture());
        Overflowed event = argCap.getValue();
        assertTrue(event.oldest());
        assertEquals(event.qos(), EXACTLY_ONCE);
        assertEquals(event.dropCount(), 1);

        Fetched fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build())
            .get(0);
        assertEquals(fetched.getQos2SeqCount(), 2);
        assertEquals(fetched.getQos2Seq(0), 1);
        assertEquals(fetched.getQos2Seq(1), 2);
        assertEquals(fetched.getQos2MsgCount(), 2);
        assertEquals(fetched.getQos2Msg(0).getMsg().getMessage(), msg1.getMessage(0));
        assertEquals(fetched.getQos2Msg(1).getMsg().getMessage(), msg2.getMessage(0));

        requestInsert(InboxSubMessagePack.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(QoS.EXACTLY_ONCE)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg3)
                    .addMessage(msg4)
                    .addMessage(msg5)
                    .build())
                .build())
            .build());

        verify(eventCollector, times(2)).report(argCap.capture());
        event = argCap.getValue();
        assertTrue(event.oldest());
        assertEquals(event.qos(), EXACTLY_ONCE);
        assertEquals(event.dropCount(), 3);

        fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build())
            .get(0);
        assertEquals(fetched.getQos2SeqCount(), 2);
        assertEquals(fetched.getQos2Seq(0), 3);
        assertEquals(fetched.getQos2Seq(1), 4);

        assertEquals(fetched.getQos2MsgCount(), 2);
        assertEquals(fetched.getQos2Msg(0).getMsg().getMessage(), msg4.getMessage(0));
        assertEquals(fetched.getQos2Msg(1).getMsg().getMessage(), msg5.getMessage(0));
    }

    @Test(groups = "integration")
    public void insertDropYoungest() {
        insertDropYoungest(EXACTLY_ONCE);
    }

    @Test(groups = "integration")
    public void insertDuplicated() {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        String topicFilter = "/a/b/c";
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        requestCreate(BatchCreateRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(2)
            .setExpirySeconds(2)
            .setDropOldest(false)
            .setLimit(10)
            .setClient(client)
            .setNow(now)
            .build());
        requestSub(BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setSubQoS(EXACTLY_ONCE)
            .setNow(now)
            .build());
        TopicMessagePack.PublisherPack msg0 = message(EXACTLY_ONCE, "hello");
        TopicMessagePack.PublisherPack msg1 = message(EXACTLY_ONCE, "world");
        TopicMessagePack.PublisherPack msg2 = message(EXACTLY_ONCE, "a");
        requestInsert(InboxSubMessagePack.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(QoS.EXACTLY_ONCE)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg0)
                    .addMessage(msg0)
                    .addMessage(msg1)
                    .build())
                .build())
            .build());
        requestInsert(InboxSubMessagePack.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(QoS.EXACTLY_ONCE)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg0)
                    .addMessage(msg2)
                    .build())
                .build())
            .build());
        Fetched fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build())
            .get(0);
        assertEquals(fetched.getQos2SeqCount(), 3);
        assertEquals(fetched.getQos2Seq(0), 0);
        assertEquals(fetched.getQos2Seq(1), 1);
        assertEquals(fetched.getQos2Seq(2), 2);

        assertEquals(fetched.getQos2MsgCount(), 3);
        assertEquals(fetched.getQos2Msg(0).getMsg().getMessage(), msg0.getMessage(0));
        assertEquals(fetched.getQos2Msg(1).getMsg().getMessage(), msg1.getMessage(0));
        assertEquals(fetched.getQos2Msg(2).getMsg().getMessage(), msg2.getMessage(0));
    }

    @Test(groups = "integration")
    public void insertSameMessageIdFromDifferentClients() {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        String topicFilter = "/a/b/c";
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        requestCreate(BatchCreateRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(2)
            .setExpirySeconds(2)
            .setDropOldest(false)
            .setLimit(10)
            .setClient(client)
            .setNow(now)
            .build());
        requestSub(BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setSubQoS(EXACTLY_ONCE)
            .setNow(now)
            .build());
        TopicMessagePack.PublisherPack msg0 = message(0, EXACTLY_ONCE, "hello", ClientInfo.newBuilder()
            .setTenantId(tenantId)
            .putMetadata("userId", "user1")
            .build());
        TopicMessagePack.PublisherPack msg1 = message(0, EXACTLY_ONCE, "world", ClientInfo.newBuilder()
            .setTenantId(tenantId)
            .putMetadata("userId", "user2")
            .build());
        requestInsert(InboxSubMessagePack.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(QoS.EXACTLY_ONCE)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg0)
                    .addMessage(msg1)
                    .build())
                .build())
            .build());
        Fetched fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build())
            .get(0);
        assertEquals(fetched.getQos2SeqCount(), 2);
        assertEquals(fetched.getQos2Seq(0), 0);
        assertEquals(fetched.getQos2Seq(1), 1);

        assertEquals(fetched.getQos2MsgCount(), 2);
        assertEquals(fetched.getQos2Msg(0).getMsg().getMessage(), msg0.getMessage(0));
        assertEquals(fetched.getQos2Msg(1).getMsg().getMessage(), msg1.getMessage(0));
    }
}

