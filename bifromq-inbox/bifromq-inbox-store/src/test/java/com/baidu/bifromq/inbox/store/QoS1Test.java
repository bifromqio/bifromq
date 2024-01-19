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

import static com.baidu.bifromq.type.QoS.AT_LEAST_ONCE;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.inbox.storage.proto.BatchCreateRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchFetchRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchInsertReply;
import com.baidu.bifromq.inbox.storage.proto.BatchSubRequest;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.InboxSubMessagePack;
import com.baidu.bifromq.inbox.storage.proto.SubMessagePack;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessagePack;
import org.testng.Assert;
import org.testng.annotations.Test;

public class QoS1Test extends InboxInsertTest {
    @Test(groups = "integration")
    public void fetchWithoutStartAfter() {
        fetchWithoutStartAfter(AT_LEAST_ONCE);
    }

    @Test(groups = "integration")
    public void fetchWithMaxLimit() {
        fetchWithMaxLimit(AT_LEAST_ONCE);
    }

    @Test(groups = "integration")
    public void fetchWithStartAfter() {
        fetchWithStartAfter(AT_LEAST_ONCE);
    }

    @Test(groups = "integration")
    public void commit() {
        commit(AT_LEAST_ONCE);
    }

    @Test(groups = "integration")
    public void commitAll() {
        commitAll(AT_LEAST_ONCE);
    }

    @Test(groups = "integration")
    public void insertDropOldest() {
        insertDropOldest(AT_LEAST_ONCE);
    }

    @Test(groups = "integration")
    public void insertDropYoungest() {
        insertDropYoungest(AT_LEAST_ONCE);
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
}

