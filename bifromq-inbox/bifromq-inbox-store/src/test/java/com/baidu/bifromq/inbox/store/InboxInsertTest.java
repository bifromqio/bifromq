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

import static com.baidu.bifromq.type.QoS.AT_MOST_ONCE;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.inbox.storage.proto.BatchCommitReply;
import com.baidu.bifromq.inbox.storage.proto.BatchCommitRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchFetchRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchInsertReply;
import com.baidu.bifromq.inbox.storage.proto.BatchSubRequest;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.InboxMessage;
import com.baidu.bifromq.inbox.storage.proto.InboxSubMessagePack;
import com.baidu.bifromq.inbox.storage.proto.SubMessagePack;
import com.baidu.bifromq.plugin.eventcollector.inboxservice.Overflowed;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessagePack;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class InboxInsertTest extends InboxStoreTest {
    @Test(groups = "integration")
    public void insertNoInbox() {
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        String topicFilter = "/a/b/c";
        BatchInsertReply.Result insertResult = requestInsert(InboxSubMessagePack.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(QoS.AT_MOST_ONCE)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(message(AT_MOST_ONCE, "hello"))
                    .build())
                .build())
            .build()).get(0);
        assertEquals(insertResult.getCode(), BatchInsertReply.Code.NO_INBOX);
    }

    @Test(groups = "integration")
    public void commitNoInbox() {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        BatchCommitReply.Code commitCode = requestCommit(BatchCommitRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(1)
            .setQos0UpToSeq(1)
            .setNow(now)
            .build()).get(0);
        assertEquals(commitCode, BatchCommitReply.Code.NO_INBOX);
    }

    protected void fetchWithoutStartAfter(QoS qos) {
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
            .setSubQoS(qos)
            .setNow(now)
            .build());

        TopicMessagePack.PublisherPack msg1 = message(qos, "hello");
        TopicMessagePack.PublisherPack msg2 = message(qos, "world");
        BatchInsertReply.Result insertResult = requestInsert(InboxSubMessagePack.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(qos)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg1)
                    .addMessage(msg2)
                    .build())
                .build())
            .build()).get(0);
        assertEquals(insertResult.getCode(), BatchInsertReply.Code.OK);
        assertEquals(insertResult.getInsertionResult(0).getTopicFilter(), topicFilter);
        assertFalse(insertResult.getInsertionResult(0).getRejected());

        Fetched fetched = requestFetch(
            BatchFetchRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setMaxFetch(10)
                .build())
            .get(0);
        assertEquals(seqCountGetter(qos).apply(fetched), 2);
        assertEquals(seqGetter(qos).apply(fetched, 0), 0);
        assertEquals(seqGetter(qos).apply(fetched, 1), 1);

        assertEquals(msgCountGetter(qos).apply(fetched), 2);
        assertEquals(msgGetter(qos).apply(fetched, 0).getMsg().getMessage(), msg1.getMessage(0));
        assertEquals(msgGetter(qos).apply(fetched, 1).getMsg().getMessage(), msg2.getMessage(0));

        Fetched fetched1 = requestFetch(
            BatchFetchRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setMaxFetch(10)
                .build())
            .get(0);
        assertEquals(fetched, fetched1);
    }

    protected void fetchWithMaxLimit(QoS qos) {
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
            .setSubQoS(qos)
            .setNow(now)
            .build());
        TopicMessagePack.PublisherPack msg1 = message(qos, "hello");
        TopicMessagePack.PublisherPack msg2 = message(qos, "world");
        BatchInsertReply.Result insertResult = requestInsert(InboxSubMessagePack.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(qos)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg1)
                    .addMessage(msg2)
                    .build())
                .build())
            .build()).get(0);
        assertEquals(insertResult.getCode(), BatchInsertReply.Code.OK);
        assertEquals(insertResult.getInsertionResult(0).getTopicFilter(), topicFilter);
        assertFalse(insertResult.getInsertionResult(0).getRejected());

        Fetched fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(1)
            .build())
            .get(0);
        assertEquals(seqCountGetter(qos).apply(fetched), 1);
        assertEquals(seqGetter(qos).apply(fetched, 0), 0);
        assertEquals(msgCountGetter(qos).apply(fetched), 1);
        assertEquals(msgGetter(qos).apply(fetched, 0).getMsg().getMessage(), msg1.getMessage(0));

        fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build())
            .get(0);
        assertEquals(seqCountGetter(qos).apply(fetched), 2);
        assertEquals(msgCountGetter(qos).apply(fetched), 2);

        fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(0)
            .build())
            .get(0);
        assertEquals(seqCountGetter(qos).apply(fetched), 0);
        assertEquals(msgCountGetter(qos).apply(fetched), 0);
    }

    protected void fetchWithStartAfter(QoS qos) {
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
            .setSubQoS(qos)
            .setNow(now)
            .build());

        TopicMessagePack.PublisherPack msg1 = message(qos, "a");
        TopicMessagePack.PublisherPack msg2 = message(qos, "b");
        TopicMessagePack.PublisherPack msg3 = message(qos, "c");
        TopicMessagePack.PublisherPack msg4 = message(qos, "d");
        TopicMessagePack.PublisherPack msg5 = message(qos, "e");
        TopicMessagePack.PublisherPack msg6 = message(qos, "f");
        requestInsert(InboxSubMessagePack.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(qos)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg1)
                    .addMessage(msg2)
                    .addMessage(msg3)
                    .build())
                .build())
            .build());
        requestInsert(InboxSubMessagePack.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(qos)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg4)
                    .addMessage(msg5)
                    .addMessage(msg6)
                    .build())
                .build())
            .build());

        BatchFetchRequest.Params.Builder paramsBuilder = BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(1);
        switch (qos) {
            case AT_MOST_ONCE -> paramsBuilder.setQos0StartAfter(0);
            case AT_LEAST_ONCE -> paramsBuilder.setQos1StartAfter(0);
            case EXACTLY_ONCE -> paramsBuilder.setQos2StartAfter(0);
        }
        Fetched fetched = requestFetch(paramsBuilder.build()).get(0);

        assertEquals(seqCountGetter(qos).apply(fetched), 1);
        assertEquals(seqGetter(qos).apply(fetched, 0), 1);
        assertEquals(msgCountGetter(qos).apply(fetched), 1);
        assertEquals(msgGetter(qos).apply(fetched, 0).getMsg().getMessage(), msg2.getMessage(0));

        paramsBuilder = BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10);
        switch (qos) {
            case AT_MOST_ONCE -> paramsBuilder.setQos0StartAfter(0);
            case AT_LEAST_ONCE -> paramsBuilder.setQos1StartAfter(0);
            case EXACTLY_ONCE -> paramsBuilder.setQos2StartAfter(0);
        }

        fetched = requestFetch(paramsBuilder.build()).get(0);
        assertEquals(seqCountGetter(qos).apply(fetched), 5);
        assertEquals(seqGetter(qos).apply(fetched, 0), 1);
        assertEquals(seqGetter(qos).apply(fetched, 1), 2);
        assertEquals(seqGetter(qos).apply(fetched, 2), 3);
        assertEquals(seqGetter(qos).apply(fetched, 3), 4);
        assertEquals(seqGetter(qos).apply(fetched, 4), 5);
        assertEquals(msgCountGetter(qos).apply(fetched), 5);
        assertEquals(msgGetter(qos).apply(fetched, 0).getMsg().getMessage(), msg2.getMessage(0));
        assertEquals(msgGetter(qos).apply(fetched, 1).getMsg().getMessage(), msg3.getMessage(0));
        assertEquals(msgGetter(qos).apply(fetched, 2).getMsg().getMessage(), msg4.getMessage(0));
        assertEquals(msgGetter(qos).apply(fetched, 3).getMsg().getMessage(), msg5.getMessage(0));
        assertEquals(msgGetter(qos).apply(fetched, 4).getMsg().getMessage(), msg6.getMessage(0));

        paramsBuilder = BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10);
        switch (qos) {
            case AT_MOST_ONCE -> paramsBuilder.setQos0StartAfter(5);
            case AT_LEAST_ONCE -> paramsBuilder.setQos1StartAfter(5);
            case EXACTLY_ONCE -> paramsBuilder.setQos2StartAfter(5);
        }
        fetched = requestFetch(paramsBuilder.build()).get(0);
        assertEquals(seqCountGetter(qos).apply(fetched), 0);
        assertEquals(msgCountGetter(qos).apply(fetched), 0);
    }

    protected void commit(QoS qos) {
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
            .setSubQoS(qos)
            .setNow(now)
            .build());
        TopicMessagePack.PublisherPack msg1 = message(qos, "hello");
        TopicMessagePack.PublisherPack msg2 = message(qos, "world");
        TopicMessagePack.PublisherPack msg3 = message(qos, "!!!!!");

        requestInsert(InboxSubMessagePack.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(qos)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg1)
                    .addMessage(msg2)
                    .addMessage(msg3)
                    .build())
                .build())
            .build());

        BatchCommitRequest.Params.Builder paramsBuilder = BatchCommitRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setNow(now);
        switch (qos) {
            case AT_MOST_ONCE -> paramsBuilder.setQos0UpToSeq(1);
            case AT_LEAST_ONCE -> paramsBuilder.setQos1UpToSeq(1);
            case EXACTLY_ONCE -> paramsBuilder.setQos2UpToSeq(1);
        }
        BatchCommitReply.Code commitCode = requestCommit(paramsBuilder.build()).get(0);
        assertEquals(commitCode, BatchCommitReply.Code.OK);

        Fetched fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build())
            .get(0);
        assertEquals(seqCountGetter(qos).apply(fetched), 1);
        assertEquals(seqGetter(qos).apply(fetched, 0), 2);
        assertEquals(msgCountGetter(qos).apply(fetched), 1);
        assertEquals(msgGetter(qos).apply(fetched, 0).getMsg().getMessage(), msg3.getMessage(0));

        // nothing should happen
        paramsBuilder = BatchCommitRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setNow(now);
        switch (qos) {
            case AT_MOST_ONCE -> paramsBuilder.setQos0UpToSeq(1);
            case AT_LEAST_ONCE -> paramsBuilder.setQos1UpToSeq(1);
            case EXACTLY_ONCE -> paramsBuilder.setQos2UpToSeq(1);
        }
        commitCode = requestCommit(paramsBuilder.build()).get(0);
        assertEquals(commitCode, BatchCommitReply.Code.OK);

        fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build())
            .get(0);
        assertEquals(seqCountGetter(qos).apply(fetched), 1);
        assertEquals(seqGetter(qos).apply(fetched, 0), 2);
        assertEquals(msgCountGetter(qos).apply(fetched), 1);
        assertEquals(msgGetter(qos).apply(fetched, 0).getMsg().getMessage(), msg3.getMessage(0));

        paramsBuilder = BatchCommitRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setNow(now);
        switch (qos) {
            case AT_MOST_ONCE -> paramsBuilder.setQos0UpToSeq(2);
            case AT_LEAST_ONCE -> paramsBuilder.setQos1UpToSeq(2);
            case EXACTLY_ONCE -> paramsBuilder.setQos2UpToSeq(2);
        }
        commitCode = requestCommit(paramsBuilder.build()).get(0);
        assertEquals(commitCode, BatchCommitReply.Code.OK);
        fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build())
            .get(0);
        assertEquals(seqCountGetter(qos).apply(fetched), 0);
        assertEquals(msgCountGetter(qos).apply(fetched), 0);
    }

    protected void commitAll(QoS qos) {
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
            .setSubQoS(qos)
            .setNow(now)
            .build());

        TopicMessagePack.PublisherPack msg1 = message(qos, "a");
        TopicMessagePack.PublisherPack msg2 = message(qos, "b");
        TopicMessagePack.PublisherPack msg3 = message(qos, "c");
        TopicMessagePack.PublisherPack msg4 = message(qos, "d");
        TopicMessagePack.PublisherPack msg5 = message(qos, "e");
        TopicMessagePack.PublisherPack msg6 = message(qos, "f");
        requestInsert(InboxSubMessagePack.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(qos)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg1)
                    .addMessage(msg2)
                    .addMessage(msg3)
                    .build())
                .build())
            .build());
        requestInsert(InboxSubMessagePack.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(qos)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg4)
                    .addMessage(msg5)
                    .addMessage(msg6)
                    .build())
                .build())
            .build());

        BatchCommitRequest.Params.Builder paramsBuilder = BatchCommitRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setNow(now);
        switch (qos) {
            case AT_MOST_ONCE -> paramsBuilder.setQos0UpToSeq(5);
            case AT_LEAST_ONCE -> paramsBuilder.setQos1UpToSeq(5);
            case EXACTLY_ONCE -> paramsBuilder.setQos2UpToSeq(5);
        }
        BatchCommitReply.Code commitCode = requestCommit(paramsBuilder.build()).get(0);
        assertEquals(commitCode, BatchCommitReply.Code.OK);

        Fetched fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build())
            .get(0);
        assertEquals(seqCountGetter(qos).apply(fetched), 0);
        assertEquals(msgCountGetter(qos).apply(fetched), 0);
    }

    protected void insertDropOldest(QoS qos) {
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
            .setSubQoS(qos)
            .setNow(now)
            .build());

        TopicMessagePack.PublisherPack msg0 = message(qos, "hello");
        TopicMessagePack.PublisherPack msg1 = message(qos, "world");
        TopicMessagePack.PublisherPack msg2 = message(qos, "!!!!!");
        requestInsert(InboxSubMessagePack.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(qos)
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
                .setSubQoS(qos)
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
                .setSubQoS(qos)
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
        assertEquals(event.qos(), qos);
        assertEquals(event.dropCount(), 1);

        Fetched fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build())
            .get(0);
        assertEquals(seqCountGetter(qos).apply(fetched), 2);
        assertEquals(seqGetter(qos).apply(fetched, 0), 1);
        assertEquals(seqGetter(qos).apply(fetched, 1), 2);

        assertEquals(msgCountGetter(qos).apply(fetched), 2);
        assertEquals(msgGetter(qos).apply(fetched, 0).getMsg().getMessage(), msg1.getMessage(0));
        assertEquals(msgGetter(qos).apply(fetched, 1).getMsg().getMessage(), msg2.getMessage(0));

        requestInsert(InboxSubMessagePack.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(qos)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg0)
                    .addMessage(msg1)
                    .addMessage(msg2)
                    .build())
                .build())
            .build());

        verify(eventCollector, times(2)).report(argCap.capture());
        event = argCap.getValue();
        assertTrue(event.oldest());
        assertEquals(event.qos(), qos);
        assertEquals(event.dropCount(), 3);

        fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build())
            .get(0);
        assertEquals(seqCountGetter(qos).apply(fetched), 2);
        assertEquals(seqGetter(qos).apply(fetched, 0), 4);
        assertEquals(seqGetter(qos).apply(fetched, 1), 5);

        assertEquals(msgCountGetter(qos).apply(fetched), 2);
        assertEquals(msgGetter(qos).apply(fetched, 0).getMsg().getMessage(), msg1.getMessage(0));
        assertEquals(msgGetter(qos).apply(fetched, 1).getMsg().getMessage(), msg2.getMessage(0));
    }

    protected void insertDropYoungest(QoS qos) {
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
            .setDropOldest(false)
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
            .setSubQoS(qos)
            .setNow(now)
            .build());

        TopicMessagePack.PublisherPack msg0 = message(qos, "hello");
        TopicMessagePack.PublisherPack msg1 = message(qos, "world");
        TopicMessagePack.PublisherPack msg2 = message(qos, "!!!!!");
        requestInsert(InboxSubMessagePack.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(qos)
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
                .setSubQoS(qos)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg1)
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
        assertEquals(seqCountGetter(qos).apply(fetched), 2);
        assertEquals(seqGetter(qos).apply(fetched, 0), 0);
        assertEquals(seqGetter(qos).apply(fetched, 1), 1);

        assertEquals(msgCountGetter(qos).apply(fetched), 2);
        assertEquals(msgGetter(qos).apply(fetched, 0).getMsg().getMessage(), msg0.getMessage(0));
        assertEquals(msgGetter(qos).apply(fetched, 1).getMsg().getMessage(), msg1.getMessage(0));

        requestInsert(InboxSubMessagePack.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS((qos))
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg0)
                    .addMessage(msg1)
                    .addMessage(msg2)
                    .build())
                .build())
            .build());

        ArgumentCaptor<Overflowed> argCap = ArgumentCaptor.forClass(Overflowed.class);
        verify(eventCollector, times(2)).report(argCap.capture());
        for (Overflowed event : argCap.getAllValues()) {
            assertFalse(event.oldest());
            assertEquals(event.qos(), qos);
            assertTrue(event.dropCount() == 1 || event.dropCount() == 3);
        }

        fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build())
            .get(0);
        assertEquals(seqCountGetter(qos).apply(fetched), 2);
        assertEquals(seqGetter(qos).apply(fetched, 0), 0);
        assertEquals(seqGetter(qos).apply(fetched, 1), 1);

        assertEquals(msgCountGetter(qos).apply(fetched), 2);
        assertEquals(msgGetter(qos).apply(fetched, 0).getMsg().getMessage(), msg0.getMessage(0));
        assertEquals(msgGetter(qos).apply(fetched, 1).getMsg().getMessage(), msg1.getMessage(0));
    }

    @Test(groups = "integration")
    public void insertQoS012() {
        long now = 0;
        String tenantId = "tenantId";
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        String topicFilter = "greeting";
        TopicMessagePack.PublisherPack msg0 = message(QoS.AT_MOST_ONCE, "hello");
        TopicMessagePack.PublisherPack msg1 = message(QoS.AT_LEAST_ONCE, "world");
        TopicMessagePack.PublisherPack msg2 = message(QoS.EXACTLY_ONCE, "!!!!!");
        requestCreate(BatchCreateRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(5)
            .setExpirySeconds(5)
            .setLimit(3)
            .setDropOldest(false)
            .setClient(client)
            .setNow(now)
            .build());
        requestSub(BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setSubQoS(QoS.EXACTLY_ONCE)
            .setNow(now)
            .build());
        BatchInsertReply.Result insertResult = requestInsert(InboxSubMessagePack.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(QoS.AT_MOST_ONCE)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg0)
                    .build())
                .build())
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(QoS.AT_LEAST_ONCE)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg1)
                    .build())
                .build())
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(QoS.EXACTLY_ONCE)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg2)
                    .build())
                .build())
            .build()).get(0);
        assertEquals(insertResult.getCode(), BatchInsertReply.Code.OK);
        Fetched fetched = requestFetch(
            BatchFetchRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setMaxFetch(10)
                .build())
            .get(0);
        Assert.assertEquals(fetched.getQos0SeqCount(), 1);
        Assert.assertEquals(fetched.getQos0Seq(0), 0);
        Assert.assertEquals(fetched.getQos0MsgCount(), 1);
        Assert.assertEquals(fetched.getQos0Msg(0).getMsg().getMessage(), msg0.getMessage(0));

        Assert.assertEquals(fetched.getQos1SeqCount(), 1);
        Assert.assertEquals(fetched.getQos1Seq(0), 0);
        Assert.assertEquals(fetched.getQos1MsgCount(), 1);
        Assert.assertEquals(fetched.getQos1Msg(0).getMsg().getMessage(), msg1.getMessage(0));

        Assert.assertEquals(fetched.getQos2SeqCount(), 1);
        Assert.assertEquals(fetched.getQos2Seq(0), 0);
        Assert.assertEquals(fetched.getQos2MsgCount(), 1);
        Assert.assertEquals(fetched.getQos2Msg(0).getMsg().getMessage(), msg2.getMessage(0));
    }

    @Test(groups = "integration")
    public void testInsertSameQoS2MultipleTimes() {
        long now = 0;
        String tenantId = "tenantId";
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        String topicFilter = "greeting";
        TopicMessagePack.PublisherPack msg = message(QoS.EXACTLY_ONCE, "hello world");
        requestCreate(BatchCreateRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(5)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(false)
            .setClient(client)
            .setNow(now)
            .build());
        requestSub(BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setSubQoS(QoS.EXACTLY_ONCE)
            .setNow(now)
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
                    .addMessage(msg)
                    .build())
                .build())
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(QoS.EXACTLY_ONCE)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg)
                    .build())
                .build())
            .addMessagePack(SubMessagePack.newBuilder()
                .setTopicFilter(topicFilter)
                .setSubQoS(QoS.EXACTLY_ONCE)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg)
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
        assertEquals(fetched.getQos0SeqCount(), 0);
        assertEquals(fetched.getQos0MsgCount(), 0);

        assertEquals(fetched.getQos1SeqCount(), 0);
        assertEquals(fetched.getQos1MsgCount(), 0);

        assertEquals(fetched.getQos2SeqCount(), 1);
        assertEquals(fetched.getQos2Seq(0), 0);
        assertEquals(fetched.getQos2Msg(0).getMsg().getMessage(), msg.getMessage(0));
    }

    private Function<Fetched, Integer> seqCountGetter(QoS qos) {
        return switch (qos) {
            case AT_MOST_ONCE -> Fetched::getQos0SeqCount;
            case AT_LEAST_ONCE -> Fetched::getQos1SeqCount;
            default -> Fetched::getQos2SeqCount;
        };
    }

    private BiFunction<Fetched, Integer, Long> seqGetter(QoS qos) {
        return switch (qos) {
            case AT_MOST_ONCE -> Fetched::getQos0Seq;
            case AT_LEAST_ONCE -> Fetched::getQos1Seq;
            default -> Fetched::getQos2Seq;
        };
    }

    private Function<Fetched, Integer> msgCountGetter(QoS qos) {
        return switch (qos) {
            case AT_MOST_ONCE -> Fetched::getQos0MsgCount;
            case AT_LEAST_ONCE -> Fetched::getQos1MsgCount;
            default -> Fetched::getQos2MsgCount;
        };
    }

    private BiFunction<Fetched, Integer, InboxMessage> msgGetter(QoS qos) {
        return switch (qos) {
            case AT_MOST_ONCE -> Fetched::getQos0Msg;
            case AT_LEAST_ONCE -> Fetched::getQos1Msg;
            default -> Fetched::getQos2Msg;
        };
    }
}

