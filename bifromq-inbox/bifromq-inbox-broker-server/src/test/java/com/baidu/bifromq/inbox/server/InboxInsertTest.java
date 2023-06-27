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

package com.baidu.bifromq.inbox.server;

import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.inbox.client.IInboxReaderClient;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxReply;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.plugin.inboxbroker.IInboxWriter;
import com.baidu.bifromq.plugin.inboxbroker.InboxPack;
import com.baidu.bifromq.plugin.inboxbroker.WriteResult;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;

public class InboxInsertTest extends InboxServiceTest {
    @Test(groups = "integration")
    public void insert() throws InterruptedException {
        String trafficId = "trafficA";
        String inboxId = "inbox1";
        String inboxGroupKey = "inboxGroup1";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTrafficId(trafficId).build();
        long reqId = System.nanoTime();
        CreateInboxReply createInboxReply = inboxReaderClient.create(reqId, inboxId, clientInfo).join();
        assertEquals(createInboxReply.getReqId(), reqId);
        assertEquals(createInboxReply.getResult(), CreateInboxReply.Result.OK);

        IInboxWriter writer = inboxBrokerClient.openInboxWriter(inboxGroupKey);
        Message msg = Message.newBuilder()
            .setPubQoS(QoS.AT_LEAST_ONCE)
            .build();
        TopicMessagePack.SenderMessagePack senderMsgPack = TopicMessagePack.SenderMessagePack
            .newBuilder()
            .addMessage(msg)
            .build();
        TopicMessagePack pack = TopicMessagePack.newBuilder()
            .setTopic("topic")
            .addMessage(senderMsgPack)
            .build();
        SubInfo subInfo = SubInfo.newBuilder()
            .setTrafficId(trafficId)
            .setInboxId(inboxId)
            .setTopicFilter("topic")
            .setSubQoS(QoS.AT_LEAST_ONCE)
            .build();
        List<InboxPack> msgPack = new LinkedList<>();
        msgPack.add(new InboxPack(pack, singletonList(subInfo)));
        Map<SubInfo, WriteResult> result = writer.write(msgPack).join();
        assertEquals(result.get(subInfo), WriteResult.OK);

        IInboxReaderClient.IInboxReader reader = inboxReaderClient.openInboxReader(inboxId, inboxGroupKey, clientInfo);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Fetched> fetchedRef = new AtomicReference<>();
        reader.fetch(fetched -> {
            fetchedRef.set(fetched);
            latch.countDown();
        });
        latch.await();
        assertEquals(fetchedRef.get().getResult(), Fetched.Result.OK);
        assertEquals(fetchedRef.get().getQos1MsgCount(), 1);
        assertEquals(fetchedRef.get().getQos1Msg(0).getMsg().getMessage(), msg);

        reader.close();
        writer.close();
    }

    @Test(groups = "integration")
    public void insertMultiMsgPackWithSameInbox() throws InterruptedException {
        String trafficId = "trafficA";
        String inboxId = "inbox1";
        String inboxGroupKey = "inboxGroup1";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTrafficId(trafficId).build();
        long reqId = System.nanoTime();
        CreateInboxReply createInboxReply = inboxReaderClient.create(reqId, inboxId, clientInfo).join();
        assertEquals(createInboxReply.getReqId(), reqId);
        assertEquals(createInboxReply.getResult(), CreateInboxReply.Result.OK);

        IInboxWriter writer = inboxBrokerClient.openInboxWriter(inboxGroupKey);
        Message msg = Message.newBuilder()
            .setPubQoS(QoS.AT_LEAST_ONCE)
            .build();
        TopicMessagePack.SenderMessagePack senderMsgPack = TopicMessagePack.SenderMessagePack
            .newBuilder()
            .addMessage(msg)
            .build();
        TopicMessagePack pack1 = TopicMessagePack.newBuilder()
            .setTopic("topic")
            .addMessage(senderMsgPack)
            .build();
        TopicMessagePack pack2 = TopicMessagePack.newBuilder()
            .setTopic("topic")
            .addMessage(senderMsgPack)
            .build();
        SubInfo subInfo = SubInfo.newBuilder()
            .setTrafficId(trafficId)
            .setInboxId(inboxId)
            .setTopicFilter("topic")
            .setSubQoS(QoS.AT_LEAST_ONCE)
            .build();
        List<InboxPack> msgPack1 = new LinkedList<>();
        msgPack1.add(new InboxPack(pack1, singletonList(subInfo)));
        List<InboxPack> msgPack2 = new LinkedList<>();
        msgPack2.add(new InboxPack(pack2, singletonList(subInfo)));
        CompletableFuture<Map<SubInfo, WriteResult>> writeFuture1 = writer.write(msgPack1);
        CompletableFuture<Map<SubInfo, WriteResult>> writeFuture2 = writer.write(msgPack2);
        CompletableFuture<Map<SubInfo, WriteResult>> writeFuture3 = writer.write(msgPack2);

        Map<SubInfo, WriteResult> result1 = writeFuture1.join();
        assertEquals(result1.get(subInfo), WriteResult.OK);
        Map<SubInfo, WriteResult> result2 = writeFuture2.join();
        assertEquals(result2.get(subInfo), WriteResult.OK);
        Map<SubInfo, WriteResult> result3 = writeFuture3.join();
        assertEquals(result3.get(subInfo), WriteResult.OK);

        IInboxReaderClient.IInboxReader reader = inboxReaderClient.openInboxReader(inboxId, inboxGroupKey, clientInfo);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Fetched> fetchedRef = new AtomicReference<>();
        reader.fetch(fetched -> {
            fetchedRef.set(fetched);
            latch.countDown();
        });
        latch.await();
        assertEquals(fetchedRef.get().getResult(), Fetched.Result.OK);
        assertEquals(fetchedRef.get().getQos1MsgCount(), 3);
        assertEquals(fetchedRef.get().getQos1Msg(0).getMsg().getMessage(), msg);

        reader.close();
        writer.close();
    }
}
