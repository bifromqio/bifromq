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

import static org.testng.AssertJUnit.assertEquals;

import com.baidu.bifromq.inbox.storage.proto.InboxFetchReply;
import com.baidu.bifromq.inbox.storage.proto.MessagePack;
import com.baidu.bifromq.inbox.util.KeyUtil;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BatchInsertTest extends InboxStoreTest {

    @Test(groups = "integration")
    public void insertQoS012() {
        String trafficId = "trafficId";
        String inboxId = "inboxId";
        SubInfo subInfo1 = SubInfo.newBuilder()
            .setTrafficId(trafficId)
            .setInboxId(inboxId)
            .setSubQoS(QoS.AT_MOST_ONCE)
            .setTopicFilter("greeting")
            .build();
        SubInfo subInfo2 = SubInfo.newBuilder()
            .setTrafficId(trafficId)
            .setInboxId(inboxId)
            .setSubQoS(QoS.AT_LEAST_ONCE)
            .setTopicFilter("greeting")
            .build();
        SubInfo subInfo3 = SubInfo.newBuilder()
            .setTrafficId(trafficId)
            .setInboxId(inboxId)
            .setSubQoS(QoS.EXACTLY_ONCE)
            .setTopicFilter("greeting")
            .build();

        String scopedInboxIdUtf8 = KeyUtil.scopedInboxId(trafficId, inboxId).toStringUtf8();

        TopicMessagePack.SenderMessagePack msg0 = message(QoS.AT_MOST_ONCE, "hello");
        TopicMessagePack.SenderMessagePack msg1 = message(QoS.AT_LEAST_ONCE, "world");
        TopicMessagePack.SenderMessagePack msg2 = message(QoS.EXACTLY_ONCE, "!!!!!");
        requestCreate(trafficId, inboxId, 3, 600, false);
        requestInsert(trafficId, inboxId,
            MessagePack.newBuilder()
                .setSubInfo(subInfo1)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic("greeting")
                    .addMessage(msg0)
                    .build())
                .build(),
            MessagePack.newBuilder()
                .setSubInfo(subInfo2)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic("greeting")
                    .addMessage(msg1)
                    .build())
                .build(),
            MessagePack.newBuilder()
                .setSubInfo(subInfo3)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic("greeting")
                    .addMessage(msg2)
                    .build())
                .build());

        InboxFetchReply reply0 = requestFetchQoS0(trafficId, inboxId, 10);
        Assert.assertEquals(1, reply0.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount());

        Assert.assertEquals(1, reply0.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount());
        Assert.assertEquals(msg0.getMessage(0),
            reply0.getResultMap().get(scopedInboxIdUtf8).getQos0Msg(0).getMsg().getMessage());

        InboxFetchReply reply1 = requestFetchQoS1(trafficId, inboxId, 10);
        Assert.assertEquals(1, reply1.getResultMap().get(scopedInboxIdUtf8).getQos1SeqCount());
        Assert.assertEquals(0, reply1.getResultMap().get(scopedInboxIdUtf8).getQos1Seq(0));

        Assert.assertEquals(1, reply1.getResultMap().get(scopedInboxIdUtf8).getQos1MsgCount());
        Assert.assertEquals(msg1.getMessage(0),
            reply1.getResultMap().get(scopedInboxIdUtf8).getQos1Msg(0).getMsg().getMessage());

        InboxFetchReply reply2 = requestFetchQoS2(trafficId, inboxId, 10, null);
        Assert.assertEquals(1, reply2.getResultMap().get(scopedInboxIdUtf8).getQos2SeqCount());
        Assert.assertEquals(0, reply2.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(0));

        Assert.assertEquals(1, reply2.getResultMap().get(scopedInboxIdUtf8).getQos2MsgCount());
        Assert.assertEquals(msg2.getMessage(0),
            reply2.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(0).getMsg().getMessage());
    }

    @Test(groups = "integration")
    public void testInsertSameQoS2MultipleTimes() {
        String trafficId = "trafficId";
        String inboxId = "inboxId";
        SubInfo subInfo = SubInfo.newBuilder()
            .setTrafficId(trafficId)
            .setInboxId(inboxId)
            .setSubQoS(QoS.EXACTLY_ONCE)
            .setTopicFilter("greeting")
            .build();

        String scopedInboxIdUtf8 = KeyUtil.scopedInboxId(trafficId, inboxId).toStringUtf8();

        TopicMessagePack.SenderMessagePack msg = message(QoS.EXACTLY_ONCE, "hello world");
        requestCreate(trafficId, inboxId, 10, 600, false);
        requestInsert(trafficId, inboxId,
            MessagePack.newBuilder()
                .setSubInfo(subInfo)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic("greeting")
                    .addMessage(msg)
                    .build())
                .build(),
            MessagePack.newBuilder()
                .setSubInfo(subInfo)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic("greeting")
                    .addMessage(msg)
                    .build())
                .build(),
            MessagePack.newBuilder()
                .setSubInfo(subInfo)
                .addMessages(TopicMessagePack.newBuilder()
                    .setTopic("greeting")
                    .addMessage(msg)
                    .build())
                .build());

        InboxFetchReply reply0 = requestFetchQoS0(trafficId, inboxId, 10);
        assertEquals(0, reply0.getResultMap().get(scopedInboxIdUtf8).getQos0SeqCount());
        assertEquals(0, reply0.getResultMap().get(scopedInboxIdUtf8).getQos0MsgCount());

        InboxFetchReply reply1 = requestFetchQoS1(trafficId, inboxId, 10);
        assertEquals(0, reply1.getResultMap().get(scopedInboxIdUtf8).getQos1SeqCount());
        assertEquals(0, reply1.getResultMap().get(scopedInboxIdUtf8).getQos1MsgCount());

        InboxFetchReply reply2 = requestFetchQoS2(trafficId, inboxId, 10, null);
        assertEquals(1, reply2.getResultMap().get(scopedInboxIdUtf8).getQos2SeqCount());
        assertEquals(0, reply2.getResultMap().get(scopedInboxIdUtf8).getQos2Seq(0));
        assertEquals(msg.getMessage(0),
            reply2.getResultMap().get(scopedInboxIdUtf8).getQos2Msg(0).getMsg().getMessage());
    }
}
