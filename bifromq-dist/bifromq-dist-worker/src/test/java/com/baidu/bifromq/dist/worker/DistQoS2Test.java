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

package com.baidu.bifromq.dist.worker;

import static com.baidu.bifromq.type.QoS.AT_LEAST_ONCE;
import static com.baidu.bifromq.type.QoS.AT_MOST_ONCE;
import static com.baidu.bifromq.type.QoS.EXACTLY_ONCE;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.dist.rpc.proto.BatchDistReply;
import com.baidu.bifromq.plugin.subbroker.DeliveryPack;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Slf4j
public class DistQoS2Test extends DistWorkerTest {
    @AfterMethod(groups = "integration")
    public void clearMock() {
        Mockito.reset(writer1, writer2, writer3);
        Mockito.reset(distClient);
    }

    @Test(groups = "integration")
    public void succeedWithNoSub() {
        String topic = "/a/b/c";
        ByteString payload = copyFromUtf8("hello");

        BatchDistReply reply = dist(tenantA, EXACTLY_ONCE, topic, payload, "orderKey1");
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().getOrDefault(topic, 0).intValue(), 0);
    }

    @Test(groups = "integration")
    public void distQoS2ToVariousSubQoS() {
        // pub: qos2
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0),(/#, qos1)], inbox2 -> [(/#,qos2)]
        // expected behavior: inbox1 gets 2 messages, inbox2 get 1 message
        when(receiverManager.get(MqttBroker)).thenReturn(mqttBroker);
        when(mqttBroker.open("server1")).thenReturn(writer1);
        when(mqttBroker.open("server2")).thenReturn(writer2);

        when(writer1.deliver(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, DeliveryResult>>>) invocation -> {
                Iterable<DeliveryPack> inboxPacks = invocation.getArgument(0);
                Map<SubInfo, DeliveryResult> resultMap = new HashMap<>();
                for (DeliveryPack inboxWrite : inboxPacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, DeliveryResult.OK);
                    }
                }
                return CompletableFuture.completedFuture(resultMap);
            });
        when(writer2.deliver(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, DeliveryResult>>>) invocation -> {
                Iterable<DeliveryPack> inboxPacks = invocation.getArgument(0);
                Map<SubInfo, DeliveryResult> resultMap = new HashMap<>();
                for (DeliveryPack inboxWrite : inboxPacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, DeliveryResult.OK);
                    }
                }
                return CompletableFuture.completedFuture(resultMap);
            });

        sub(tenantA, "/a/b/c", AT_MOST_ONCE, MqttBroker, "inbox1", "server1");
        sub(tenantA, "/#", AT_LEAST_ONCE, MqttBroker, "inbox1", "server1");
        sub(tenantA, "/#", EXACTLY_ONCE, MqttBroker, "inbox2", "server2");
        BatchDistReply reply = dist(tenantA, EXACTLY_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("/a/b/c").intValue(), 3);


        ArgumentCaptor<Iterable<DeliveryPack>> msgCap = ArgumentCaptor.forClass(Iterable.class);

        verify(writer1, timeout(100).atLeastOnce()).deliver(msgCap.capture());
        for (DeliveryPack pack : msgCap.getValue()) {
            TopicMessagePack msgs = pack.messagePack;
            Iterable<SubInfo> subInfos = pack.inboxes;
            assertEquals(msgs.getTopic(), "/a/b/c");
            for (TopicMessagePack.PublisherPack publisherPack : msgs.getMessageList()) {
                for (Message msg : publisherPack.getMessageList()) {
                    for (SubInfo subInfo : subInfos) {
                        assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                    }
                }
            }
        }

//        // '/#' must come first
//        List<TopicMessage> inbox1Msgs = msgCap.getValue().get("tenantA").get("inbox1");
//        assertEquals(inbox1Msgs.get(0).getSubQoS(), AT_LEAST_ONCE);
//        assertEquals(inbox1Msgs.get(1).getSubQoS(), AT_MOST_ONCE);

        msgCap = ArgumentCaptor.forClass(Iterable.class);
        verify(writer2, timeout(100).atLeastOnce()).deliver(msgCap.capture());

        assertEquals(msgCap.getAllValues().size(), 1);
        for (DeliveryPack pack : msgCap.getValue()) {
            TopicMessagePack inbox2Msgs = pack.messagePack;
            assertEquals(inbox2Msgs.getTopic(), "/a/b/c");
            TopicMessagePack.PublisherPack publisherPack = inbox2Msgs.getMessageList().iterator().next();
            Message msg = publisherPack.getMessage(0);
            assertEquals(msg.getPubQoS(), EXACTLY_ONCE);
            assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
        }

        unsub(tenantA, "/a/b/c", MqttBroker, "inbox1", "server1");
        unsub(tenantA, "/#", MqttBroker, "inbox1", "server1");
        unsub(tenantA, "/#", MqttBroker, "inbox2", "server2");
    }
}
