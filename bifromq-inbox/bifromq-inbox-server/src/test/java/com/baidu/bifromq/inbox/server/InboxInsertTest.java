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

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.rpc.proto.CreateReply;
import com.baidu.bifromq.inbox.rpc.proto.CreateRequest;
import com.baidu.bifromq.inbox.rpc.proto.SubReply;
import com.baidu.bifromq.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterOption;
import com.baidu.bifromq.plugin.subbroker.DeliveryPack;
import com.baidu.bifromq.plugin.subbroker.DeliveryPackage;
import com.baidu.bifromq.plugin.subbroker.DeliveryReply;
import com.baidu.bifromq.plugin.subbroker.DeliveryRequest;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.MatchInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessagePack;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;

import static com.baidu.bifromq.inbox.util.InboxServiceUtil.getDelivererKey;
import static com.baidu.bifromq.inbox.util.InboxServiceUtil.receiverId;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class InboxInsertTest extends InboxServiceTest {
    @Test(groups = "integration")
    public void insert() throws InterruptedException {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenant" + now;
        String inboxId = "insertInbox" + now;
        long incarnation = HLC.INST.getPhysical();
        String delivererKey = getDelivererKey(tenantId, inboxId);
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        long reqId = System.nanoTime();
        CreateReply createReply = inboxClient.create(
            CreateRequest.newBuilder().setReqId(reqId).setInboxId(inboxId).setIncarnation(incarnation)
                .setKeepAliveSeconds(5).setExpirySeconds(5).setLimit(10).setDropOldest(true).setClient(clientInfo)
                .setNow(now).build()).join();
        assertEquals(createReply.getReqId(), reqId);
        assertEquals(createReply.getCode(), CreateReply.Code.OK);

        IDeliverer writer = inboxClient.open(delivererKey);
        DeliveryRequest.Builder reqBuilder = DeliveryRequest.newBuilder();
        Message msg = Message.newBuilder().setPubQoS(QoS.AT_LEAST_ONCE).build();
        TopicMessagePack.PublisherPack publisherPack =
            TopicMessagePack.PublisherPack.newBuilder().addMessage(msg).build();
        TopicMessagePack pack = TopicMessagePack.newBuilder().setTopic("topic").addMessage(publisherPack).build();
        MatchInfo matchInfo =
            MatchInfo.newBuilder().setReceiverId(receiverId(inboxId, incarnation)).setTopicFilter("topic").build();
        reqBuilder.putPackage(tenantId, DeliveryPackage.newBuilder()
            .addPack(DeliveryPack.newBuilder().setMessagePack(pack).addMatchInfo(matchInfo).build()).build());

        SubReply reply2 = inboxClient.sub(
                SubRequest.newBuilder().setReqId(reqId).setTenantId(tenantId).setInboxId(inboxId)
                    .setIncarnation(incarnation).setVersion(0).setTopicFilter("topic")
                    .setOption(TopicFilterOption.newBuilder().setQos(QoS.AT_LEAST_ONCE).build()).setNow(now).build())
            .join();
        assertEquals(reply2.getCode(), SubReply.Code.OK);

        DeliveryReply reply = writer.deliver(reqBuilder.build()).join();
        assertTrue(reply.getResultMap().containsKey(tenantId) && reply.getResultMap().size() == 1);
        assertEquals(reply.getResultMap().get(tenantId).getResultCount(), 1);
        assertEquals(reply.getResultMap().get(tenantId).getResult(0).getMatchInfo(), matchInfo);
        assertEquals(reply.getResultMap().get(tenantId).getResult(0).getCode(), DeliveryResult.Code.OK);

        IInboxClient.IInboxReader reader = inboxClient.openInboxReader(tenantId, inboxId, incarnation);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Fetched> fetchedRef = new AtomicReference<>();
        reader.fetch(fetched -> {
            fetchedRef.set(fetched);
            latch.countDown();
        });
        reader.hint(100);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        if (fetchedRef.get().getResult() == Fetched.Result.OK) {
            assertEquals(fetchedRef.get().getSendBufferMsgCount(), 1);
            assertEquals(fetchedRef.get().getSendBufferMsg(0).getMsg().getMessage(), msg);
        }

        reader.close();
        writer.close();
    }

    @Test(groups = "integration")
    public void insertMultiMsgPackWithSameInbox() throws InterruptedException {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenant" + now;
        String inboxId = "insertInbox" + now;
        long incarnation = HLC.INST.getPhysical();
        String delivererKey = getDelivererKey(tenantId, inboxId);
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        long reqId = System.nanoTime();
        CreateReply createReply = inboxClient.create(
            CreateRequest.newBuilder().setReqId(reqId).setInboxId(inboxId).setIncarnation(incarnation)
                .setKeepAliveSeconds(5).setExpirySeconds(5).setLimit(10).setDropOldest(true).setClient(clientInfo)
                .setNow(now).build()).join();
        assertEquals(createReply.getReqId(), reqId);
        assertEquals(createReply.getCode(), CreateReply.Code.OK);

        IDeliverer writer = inboxClient.open(delivererKey);
        Message msg = Message.newBuilder().setPubQoS(QoS.AT_LEAST_ONCE).build();
        TopicMessagePack.PublisherPack publisherPack =
            TopicMessagePack.PublisherPack.newBuilder().addMessage(msg).build();
        TopicMessagePack pack1 = TopicMessagePack.newBuilder().setTopic("topic").addMessage(publisherPack).build();
        TopicMessagePack pack2 = TopicMessagePack.newBuilder().setTopic("topic").addMessage(publisherPack).build();
        MatchInfo matchInfo =
            MatchInfo.newBuilder().setReceiverId(receiverId(inboxId, incarnation)).setTopicFilter("topic").build();
        DeliveryRequest req1 = DeliveryRequest.newBuilder().putPackage(tenantId, DeliveryPackage.newBuilder()
            .addPack(DeliveryPack.newBuilder().setMessagePack(pack1).addMatchInfo(matchInfo).build()).build()).build();
        DeliveryRequest req2 = DeliveryRequest.newBuilder().putPackage(tenantId, DeliveryPackage.newBuilder()
            .addPack(DeliveryPack.newBuilder().setMessagePack(pack2).addMatchInfo(matchInfo).build()).build()).build();

        inboxClient.sub(SubRequest.newBuilder().setReqId(reqId).setTenantId(tenantId).setInboxId(inboxId)
            .setIncarnation(incarnation).setVersion(0).setTopicFilter("topic")
            .setOption(TopicFilterOption.newBuilder().setQos(QoS.AT_LEAST_ONCE).build()).setNow(now).build()).join();

        CompletableFuture<DeliveryReply> writeFuture1 = writer.deliver(req1);
        CompletableFuture<DeliveryReply> writeFuture2 = writer.deliver(req2);
        CompletableFuture<DeliveryReply> writeFuture3 = writer.deliver(req2);

        DeliveryReply result1 = writeFuture1.join();
        assertEquals(result1.getResultMap().get(tenantId).getResult(0).getMatchInfo(), matchInfo);
        assertEquals(result1.getResultMap().get(tenantId).getResult(0).getCode(), DeliveryResult.Code.OK);
        DeliveryReply result2 = writeFuture2.join();
        assertEquals(result2.getResultMap().get(tenantId).getResult(0).getMatchInfo(), matchInfo);
        assertEquals(result2.getResultMap().get(tenantId).getResult(0).getCode(), DeliveryResult.Code.OK);
        DeliveryReply result3 = writeFuture3.join();
        assertEquals(result3.getResultMap().get(tenantId).getResult(0).getMatchInfo(), matchInfo);
        assertEquals(result3.getResultMap().get(tenantId).getResult(0).getCode(), DeliveryResult.Code.OK);

        IInboxClient.IInboxReader reader = inboxClient.openInboxReader(tenantId, inboxId, incarnation);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Fetched> fetchedRef = new AtomicReference<>();
        reader.fetch(fetched -> {
            fetchedRef.set(fetched);
            latch.countDown();
        });
        reader.hint(100);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        if (fetchedRef.get().getResult() == Fetched.Result.OK) {
            assertEquals(fetchedRef.get().getSendBufferMsgCount(), 3);
            assertEquals(fetchedRef.get().getSendBufferMsg(0).getMsg().getMessage(), msg);
        }

        reader.close();
        writer.close();
    }
}
