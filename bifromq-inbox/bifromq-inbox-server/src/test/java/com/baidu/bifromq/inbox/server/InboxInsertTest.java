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

import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxReply;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.plugin.subbroker.DeliveryPack;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
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
        String tenantId = "tenantA";
        String inboxId = "insert_inbox";
        String delivererKey = "deliverer1";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        long reqId = System.nanoTime();
        CreateInboxReply createInboxReply = inboxClient.create(reqId, inboxId, clientInfo).join();
        assertEquals(createInboxReply.getReqId(), reqId);
        assertEquals(createInboxReply.getResult(), CreateInboxReply.Result.OK);

        IDeliverer writer = inboxClient.open(delivererKey);
        Message msg = Message.newBuilder()
            .setPubQoS(QoS.AT_LEAST_ONCE)
            .build();
        TopicMessagePack.PublisherPack publisherPack = TopicMessagePack.PublisherPack
            .newBuilder()
            .addMessage(msg)
            .build();
        TopicMessagePack pack = TopicMessagePack.newBuilder()
            .setTopic("topic")
            .addMessage(publisherPack)
            .build();
        SubInfo subInfo = SubInfo.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setTopicFilter("topic")
            .setSubQoS(QoS.AT_LEAST_ONCE)
            .build();
        List<DeliveryPack> msgPack = new LinkedList<>();
        msgPack.add(new DeliveryPack(pack, singletonList(subInfo)));

        inboxClient.sub(System.nanoTime(), inboxId, "topic", QoS.AT_LEAST_ONCE, clientInfo);

        Map<SubInfo, DeliveryResult> result = writer.deliver(msgPack).join();
        assertEquals(result.get(subInfo), DeliveryResult.OK);

        IInboxClient.IInboxReader reader = inboxClient.openInboxReader(inboxId, clientInfo);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Fetched> fetchedRef = new AtomicReference<>();
        reader.fetch((fetched, throwable) -> {
            fetchedRef.set(fetched);
            latch.countDown();
        });
        latch.await();
        assertEquals(fetchedRef.get().getResult(), Fetched.Result.OK);
        assertEquals(fetchedRef.get().getQos1MsgCount(), 1);
        assertEquals(fetchedRef.get().getQos1Msg(0).getMsg().getMessage(), msg);

        reader.close();
        writer.close();
        inboxClient.delete(reqId, inboxId, clientInfo).join();
    }

    @Test(groups = "integration")
    public void insertMultiMsgPackWithSameInbox() throws InterruptedException {
        String tenantId = "trafficA";
        String inboxId = "insertMultiMsgPackWithSameInbox_inbox";
        String delivererKey = "deliverer1";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        long reqId = System.nanoTime();
        CreateInboxReply createInboxReply = inboxClient.create(reqId, inboxId, clientInfo).join();
        assertEquals(createInboxReply.getReqId(), reqId);
        assertEquals(createInboxReply.getResult(), CreateInboxReply.Result.OK);

        IDeliverer writer = inboxClient.open(delivererKey);
        Message msg = Message.newBuilder()
            .setPubQoS(QoS.AT_LEAST_ONCE)
            .build();
        TopicMessagePack.PublisherPack publisherPack = TopicMessagePack.PublisherPack
            .newBuilder()
            .addMessage(msg)
            .build();
        TopicMessagePack pack1 = TopicMessagePack.newBuilder()
            .setTopic("topic")
            .addMessage(publisherPack)
            .build();
        TopicMessagePack pack2 = TopicMessagePack.newBuilder()
            .setTopic("topic")
            .addMessage(publisherPack)
            .build();
        SubInfo subInfo = SubInfo.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setTopicFilter("topic")
            .setSubQoS(QoS.AT_LEAST_ONCE)
            .build();
        List<DeliveryPack> msgPack1 = new LinkedList<>();
        msgPack1.add(new DeliveryPack(pack1, singletonList(subInfo)));
        List<DeliveryPack> msgPack2 = new LinkedList<>();
        msgPack2.add(new DeliveryPack(pack2, singletonList(subInfo)));

        inboxClient.sub(System.nanoTime(), inboxId, "topic", QoS.AT_LEAST_ONCE, clientInfo);

        CompletableFuture<Map<SubInfo, DeliveryResult>> writeFuture1 = writer.deliver(msgPack1);
        CompletableFuture<Map<SubInfo, DeliveryResult>> writeFuture2 = writer.deliver(msgPack2);
        CompletableFuture<Map<SubInfo, DeliveryResult>> writeFuture3 = writer.deliver(msgPack2);

        Map<SubInfo, DeliveryResult> result1 = writeFuture1.join();
        assertEquals(result1.get(subInfo), DeliveryResult.OK);
        Map<SubInfo, DeliveryResult> result2 = writeFuture2.join();
        assertEquals(result2.get(subInfo), DeliveryResult.OK);
        Map<SubInfo, DeliveryResult> result3 = writeFuture3.join();
        assertEquals(result3.get(subInfo), DeliveryResult.OK);

        IInboxClient.IInboxReader reader = inboxClient.openInboxReader(inboxId, clientInfo);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Fetched> fetchedRef = new AtomicReference<>();
        reader.fetch((fetched, throwable) -> {
            fetchedRef.set(fetched);
            latch.countDown();
        });
        latch.await();
        assertEquals(fetchedRef.get().getResult(), Fetched.Result.OK);
        assertEquals(fetchedRef.get().getQos1MsgCount(), 3);
        assertEquals(fetchedRef.get().getQos1Msg(0).getMsg().getMessage(), msg);

        reader.close();
        writer.close();

        inboxClient.delete(reqId, inboxId, clientInfo).join();
    }
}
