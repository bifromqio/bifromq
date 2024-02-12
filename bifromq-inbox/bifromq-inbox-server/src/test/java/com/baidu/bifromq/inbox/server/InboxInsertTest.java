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

package com.baidu.bifromq.inbox.server;

import static com.baidu.bifromq.inbox.records.ScopedInbox.distInboxId;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.rpc.proto.CreateReply;
import com.baidu.bifromq.inbox.rpc.proto.CreateRequest;
import com.baidu.bifromq.inbox.rpc.proto.SubReply;
import com.baidu.bifromq.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterOption;
import com.baidu.bifromq.plugin.subbroker.DeliveryPack;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.MatchInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessagePack;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;

public class InboxInsertTest extends InboxServiceTest {
    @Test(groups = "integration")
    public void insert() throws InterruptedException {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenant-" + now;
        String inboxId = "insert_inbox-" + now;
        long incarnation = HLC.INST.getPhysical();
        String delivererKey = "deliverer1";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        long reqId = System.nanoTime();
        CreateReply createReply = inboxClient.create(CreateRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(5)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        assertEquals(createReply.getReqId(), reqId);
        assertEquals(createReply.getCode(), CreateReply.Code.OK);

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
        MatchInfo subInfo = MatchInfo.newBuilder()
            .setTenantId(tenantId)
            .setReceiverId(distInboxId(inboxId, incarnation))
            .setTopicFilter("topic")
            .build();
        List<DeliveryPack> msgPack = new LinkedList<>();
        msgPack.add(new DeliveryPack(pack, singletonList(subInfo)));

        SubReply reply2 = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter("topic")
            .setOption(TopicFilterOption.newBuilder().setQos(QoS.AT_LEAST_ONCE).build())
            .setNow(now)
            .build()).join();

        Map<MatchInfo, DeliveryResult> result = writer.deliver(msgPack).join();
        assertEquals(result.get(subInfo), DeliveryResult.OK);

        IInboxClient.IInboxReader reader = inboxClient.openInboxReader(tenantId, inboxId, incarnation);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Fetched> fetchedRef = new AtomicReference<>();
        reader.fetch(fetched -> {
            fetchedRef.set(fetched);
            latch.countDown();
        });
        reader.hint(100);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(fetchedRef.get().getResult(), Fetched.Result.OK);
        assertEquals(fetchedRef.get().getSendBufferMsgCount(), 1);
        assertEquals(fetchedRef.get().getSendBufferMsg(0).getMsg().getMessage(), msg);

        reader.close();
        writer.close();
    }

    @Test(groups = "integration")
    public void insertMultiMsgPackWithSameInbox() throws InterruptedException {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenant-" + now;
        String inboxId = "insert_inbox-" + now;
        long incarnation = HLC.INST.getPhysical();
        String delivererKey = "deliverer1";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        long reqId = System.nanoTime();
        CreateReply createReply = inboxClient.create(CreateRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(5)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        assertEquals(createReply.getReqId(), reqId);
        assertEquals(createReply.getCode(), CreateReply.Code.OK);

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
        MatchInfo subInfo = MatchInfo.newBuilder()
            .setTenantId(tenantId)
            .setReceiverId(distInboxId(inboxId, incarnation))
            .setTopicFilter("topic")
            .build();
        List<DeliveryPack> msgPack1 = new LinkedList<>();
        msgPack1.add(new DeliveryPack(pack1, singletonList(subInfo)));
        List<DeliveryPack> msgPack2 = new LinkedList<>();
        msgPack2.add(new DeliveryPack(pack2, singletonList(subInfo)));

        inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter("topic")
            .setOption(TopicFilterOption.newBuilder().setQos(QoS.AT_LEAST_ONCE).build())
            .setNow(now)
            .build()).join();

        CompletableFuture<Map<MatchInfo, DeliveryResult>> writeFuture1 = writer.deliver(msgPack1);
        CompletableFuture<Map<MatchInfo, DeliveryResult>> writeFuture2 = writer.deliver(msgPack2);
        CompletableFuture<Map<MatchInfo, DeliveryResult>> writeFuture3 = writer.deliver(msgPack2);

        Map<MatchInfo, DeliveryResult> result1 = writeFuture1.join();
        assertEquals(result1.get(subInfo), DeliveryResult.OK);
        Map<MatchInfo, DeliveryResult> result2 = writeFuture2.join();
        assertEquals(result2.get(subInfo), DeliveryResult.OK);
        Map<MatchInfo, DeliveryResult> result3 = writeFuture3.join();
        assertEquals(result3.get(subInfo), DeliveryResult.OK);

        IInboxClient.IInboxReader reader = inboxClient.openInboxReader(tenantId, inboxId, incarnation);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Fetched> fetchedRef = new AtomicReference<>();
        reader.fetch(fetched -> {
            fetchedRef.set(fetched);
            latch.countDown();
        });
        reader.hint(100);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(fetchedRef.get().getResult(), Fetched.Result.OK);
        assertEquals(fetchedRef.get().getSendBufferMsgCount(), 3);
        assertEquals(fetchedRef.get().getSendBufferMsg(0).getMsg().getMessage(), msg);

        reader.close();
        writer.close();
    }
}
