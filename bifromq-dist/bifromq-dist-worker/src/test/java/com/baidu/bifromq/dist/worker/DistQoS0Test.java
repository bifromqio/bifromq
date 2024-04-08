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

package com.baidu.bifromq.dist.worker;

import static com.baidu.bifromq.type.QoS.AT_MOST_ONCE;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.dist.rpc.proto.TenantDistReply;
import com.baidu.bifromq.plugin.subbroker.DeliveryPack;
import com.baidu.bifromq.plugin.subbroker.DeliveryPackage;
import com.baidu.bifromq.plugin.subbroker.DeliveryRequest;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.type.MatchInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class DistQoS0Test extends DistWorkerTest {

    @BeforeMethod(groups = "integration")
    public void mock() {
        when(receiverManager.get(MqttBroker)).thenReturn(mqttBroker);
        when(receiverManager.get(InboxService)).thenReturn(inboxBroker);

        when(writer1.deliver(any())).thenAnswer(answer(DeliveryResult.Code.OK));
        when(writer2.deliver(any())).thenAnswer(answer(DeliveryResult.Code.OK));
        when(writer3.deliver(any())).thenAnswer(answer(DeliveryResult.Code.OK));
    }

    @AfterMethod(groups = "integration")
    public void clearMock() {
        Mockito.reset(writer1, writer2, writer3);
    }

    @Test(groups = "integration")
    public void succeedWithNoSub() {
        String topic = "/a/b/c";
        ByteString payload = copyFromUtf8("hello");

        TenantDistReply reply = tenantDist(tenantA, AT_MOST_ONCE, topic, payload, "orderKey1");
        assertEquals(reply.getResultsMap().get(topic).getFanout(), 0);
    }

    @Test(groups = "integration")
    public void testDistCase1() {
        // pub: qos0
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0),(/#, qos0)], inbox2 -> [(/#,qos1)]
        // expected behavior: inbox1 gets 2 messages, inbox2 get 1 message
        when(mqttBroker.open("batch1")).thenReturn(writer1);

        match(tenantA, "TopicA/#", MqttBroker, "inbox1", "batch1");

        TenantDistReply reply = tenantDist(tenantA, AT_MOST_ONCE, "TopicB", copyFromUtf8("Hello"), "orderKey1");
        assertEquals(reply.getResultsMap().get("TopicB").getFanout(), 0);

        unmatch(tenantA, "TopicA/#", InboxService, "inbox1", "batch1");
    }

    @Test(groups = "integration")
    public void testDistCase2() {
        // pub: qos0
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0),(/#, qos0)], inbox2 -> [(/#,qos1)]
        // expected behavior: inbox1 gets 2 messages, inbox2 get 1 message
        when(mqttBroker.open("batch1")).thenReturn(writer1);
        when(inboxBroker.open("batch2")).thenReturn(writer2);

        match(tenantA, "/你好/hello/😄", MqttBroker, "inbox1", "batch1");
        match(tenantA, "/#", MqttBroker, "inbox1", "batch1");
        match(tenantA, "/#", InboxService, "inbox2", "batch2");

        TenantDistReply reply = tenantDist(tenantA, AT_MOST_ONCE, "/你好/hello/😄", copyFromUtf8("Hello"), "orderKey1");
        assertEquals(reply.getResultsMap().get("/你好/hello/😄").getFanout(), 3);

        ArgumentCaptor<DeliveryRequest> msgCap = ArgumentCaptor.forClass(DeliveryRequest.class);
        verify(writer1, after(1000).atMost(2)).deliver(msgCap.capture());
        int msgCount = 0;
        for (DeliveryRequest request : msgCap.getAllValues()) {
            for (String tenantId : request.getPackageMap().keySet()) {
                DeliveryPackage deliveryPackage = request.getPackageMap().get(tenantId);
                assertEquals(tenantId, tenantA);
                for (DeliveryPack pack : deliveryPackage.getPackList()) {
                    TopicMessagePack msgPack = pack.getMessagePack();
                    Set<MatchInfo> subInfos = Sets.newHashSet(pack.getMatchInfoList());
                    assertEquals(msgPack.getTopic(), "/你好/hello/😄");
                    for (TopicMessagePack.PublisherPack publisherPack : msgPack.getMessageList()) {
                        for (Message msg : publisherPack.getMessageList()) {
                            assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                            msgCount += subInfos.size();
                        }
                    }
                }
            }
        }
        assertEquals(msgCount, 2);

        msgCap = ArgumentCaptor.forClass(DeliveryRequest.class);
        verify(writer2, timeout(1000).times(1)).deliver(msgCap.capture());
        for (DeliveryRequest request : msgCap.getAllValues()) {
            for (String tenantId : request.getPackageMap().keySet()) {
                DeliveryPackage deliveryPackage = request.getPackageMap().get(tenantId);
                assertEquals(tenantId, tenantA);
                for (DeliveryPack pack : deliveryPackage.getPackList()) {
                    TopicMessagePack msgs = pack.getMessagePack();
                    assertEquals(msgs.getTopic(), "/你好/hello/😄");
                    for (TopicMessagePack.PublisherPack publisherPack : msgs.getMessageList()) {
                        for (Message msg : publisherPack.getMessageList()) {
                            assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                        }
                    }
                }
            }
        }

        unmatch(tenantA, "/你好/hello/😄", MqttBroker, "inbox1", "batch1");
        unmatch(tenantA, "/#", MqttBroker, "inbox1", "batch1");
        unmatch(tenantA, "/#", InboxService, "inbox2", "batch2");
    }

    @Test(groups = "integration")
    public void testDistCase3() {
        // pub: qos0
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0)], inbox2 -> [(/a/b/c, qos0)]
        // expected behavior: inbox1 gets 1 messages
        when(mqttBroker.open("batch1")).thenReturn(writer1);

        match(tenantA, "/a/b/c", MqttBroker, "inbox1", "batch1");
        match(tenantA, "/a/b/c", MqttBroker, "inbox2", "batch1");
        TenantDistReply reply = tenantDist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        assertEquals(reply.getResultsMap().get("/a/b/c").getFanout(), 2);
        ArgumentCaptor<DeliveryRequest> list1 = ArgumentCaptor.forClass(DeliveryRequest.class);
        verify(writer1, after(1000).atMost(2)).deliver(list1.capture());
        log.info("Case3: verify writer1, list size is {}", list1.getAllValues().size());
        int msgCount = 0;
        Set<MatchInfo> matchInfos = new HashSet<>();
        for (DeliveryRequest request : list1.getAllValues()) {
            for (String tenantId : request.getPackageMap().keySet()) {
                for (DeliveryPack pack : request.getPackageMap().get(tenantId).getPackList()) {
                    TopicMessagePack msgs = pack.getMessagePack();
                    matchInfos.addAll(pack.getMatchInfoList());
                    assertEquals(msgs.getTopic(), "/a/b/c");
                    for (TopicMessagePack.PublisherPack publisherPack : msgs.getMessageList()) {
                        for (Message msg : publisherPack.getMessageList()) {
                            msgCount++;
                            assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                        }
                    }
                }
            }
        }
        assertEquals(matchInfos.size(), 2);
        assertEquals(msgCount, 2);

        unmatch(tenantA, "/a/b/c", MqttBroker, "inbox1", "batch1");
        unmatch(tenantA, "/a/b/c", MqttBroker, "inbox2", "batch1");
    }

    @Test(groups = "integration")
    public void testDistCase4() {
        // pub: qos0
        // topic: "/a/b/c"
        // sub: inbox1 -> [($share/group//a/b/c, qos0),(/#, qos0)], inbox2 -> [($share/group//a/b/c,qos1)]
        // expected behavior: total 10 messages, inbox1 gets N messages, inbox2 get M message, N + M == 10, N > 0, M > 0
        when(mqttBroker.open("batch1")).thenReturn(writer1);
        when(mqttBroker.open("batch2")).thenReturn(writer2);

        match(tenantA, "$share/group//a/b/c", MqttBroker, "inbox1", "batch1");
        match(tenantA, "$share/group//a/b/c", MqttBroker, "inbox2", "batch2");
        for (int i = 0; i < 10; i++) {
            TenantDistReply reply = tenantDist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            assertEquals(reply.getResultsMap().get("/a/b/c").getFanout(), 1);
        }

        ArgumentCaptor<DeliveryRequest> list1 = ArgumentCaptor.forClass(DeliveryRequest.class);
        verify(writer1, after(1000).atMost(10)).deliver(list1.capture());
        for (DeliveryRequest request : list1.getAllValues()) {
            for (String tenantId : request.getPackageMap().keySet()) {
                for (DeliveryPack pack : request.getPackageMap().get(tenantId).getPackList()) {
                    TopicMessagePack msgs = pack.getMessagePack();
                    assertEquals(msgs.getTopic(), "/a/b/c");
                    for (TopicMessagePack.PublisherPack publisherPack : msgs.getMessageList()) {
                        for (Message msg : publisherPack.getMessageList()) {
                            assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                        }
                    }
                }
            }
        }

        ArgumentCaptor<DeliveryRequest> list2 = ArgumentCaptor.forClass(DeliveryRequest.class);

        verify(writer2, after(200).atMost(10)).deliver(list2.capture());
        for (DeliveryRequest request : list2.getAllValues()) {
            for (String tenantId : request.getPackageMap().keySet()) {
                for (DeliveryPack pack : request.getPackageMap().get(tenantId).getPackList()) {
                    TopicMessagePack msgs = pack.getMessagePack();
                    assertEquals(msgs.getTopic(), "/a/b/c");
                    for (TopicMessagePack.PublisherPack publisherPack : msgs.getMessageList()) {
                        for (Message msg : publisherPack.getMessageList()) {
                            assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                        }
                    }
                }
            }
        }
        assertEquals(list1.getAllValues().size() + list2.getAllValues().size(), 10);

        unmatch(tenantA, "$share/group//a/b/c", MqttBroker, "inbox1", "batch1");
        unmatch(tenantA, "$share/group//a/b/c", MqttBroker, "inbox2", "batch2");
    }

    @Test(groups = "integration")
    public void testDistCase5() {
        // pub: qos0
        // topic: "/a/b/c"
        // sub: inbox1 -> [($oshare/group//a/b/c, qos0)], inbox2 -> [($oshare/group//a/b/c, qos1)]
        // expected behavior: total 10 messages, inbox1 gets N messages, inbox2 get M message, either N or M is 10
        Mockito.reset(distClient);
        when(mqttBroker.open("batch1")).thenReturn(writer1);
        when(mqttBroker.open("batch2")).thenReturn(writer2);

        match(tenantA, "$oshare/group//a/b/c", MqttBroker, "inbox1", "batch1");
        match(tenantA, "$oshare/group//a/b/c", MqttBroker, "inbox2", "batch2");

        for (int i = 0; i < 10; i++) {
            TenantDistReply reply = tenantDist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            assertEquals(reply.getResultsMap().get("/a/b/c").getFanout(), 1);
        }

        ArgumentCaptor<DeliveryRequest> list1 = ArgumentCaptor.forClass(DeliveryRequest.class);
        verify(writer1, after(200).atMost(10)).deliver(list1.capture());

        ArgumentCaptor<DeliveryRequest> list2 = ArgumentCaptor.forClass(DeliveryRequest.class);
        verify(writer2, after(200).atMost(10)).deliver(list2.capture());

        List<DeliveryRequest> captured = list1.getAllValues().isEmpty() ?
            list2.getAllValues() : list1.getAllValues();

        for (DeliveryRequest request : captured) {
            for (String tenantId : request.getPackageMap().keySet()) {
                for (DeliveryPack pack : request.getPackageMap().get(tenantId).getPackList()) {
                    TopicMessagePack msgs = pack.getMessagePack();
                    assertEquals(msgs.getTopic(), "/a/b/c");
                    for (TopicMessagePack.PublisherPack publisherPack : msgs.getMessageList()) {
                        for (Message msg : publisherPack.getMessageList()) {
                            assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                        }
                    }
                }
            }
        }
        assertTrue(list1.getAllValues().isEmpty() || list2.getAllValues().isEmpty());

        unmatch(tenantA, "$oshare/group//a/b/c", MqttBroker, "inbox1", "batch1");
        unmatch(tenantA, "$oshare/group//a/b/c", MqttBroker, "inbox2", "batch2");
    }

    @Test(groups = "integration")
    public void testDistCase6() {
        // pub: qos0
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0),(/#, qos0)], inbox2 -> [($share/group//a/b/c, qos0)]
        //      inbox3 -> [($oshare/group//a/b/c, qos0)]
        // subbroker: inbox1 -> NO_INBOX, inbox2 -> NO_INBOX, inbox3 -> NO_INBOX
        // expected behavior: unsub gets called 3 times
        when(mqttBroker.open("batch1")).thenReturn(writer1);
        when(mqttBroker.open("batch2")).thenReturn(writer2);
        when(mqttBroker.open("batch3")).thenReturn(writer3);

        match(tenantA, "/a/b/c", MqttBroker, "inbox1", "batch1");
        match(tenantA, "$share/group//a/b/c", MqttBroker, "inbox2", "batch2");
        match(tenantA, "$oshare/group//a/b/c", MqttBroker, "inbox3", "batch3");
        for (int i = 0; i < 1; ++i) {
            TenantDistReply reply = tenantDist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            assertEquals(reply.getResultsMap().get("/a/b/c").getFanout(), 3);
        }

        verify(writer1, timeout(1000).times(1)).deliver(any());
        verify(writer2, timeout(1000).times(1)).deliver(any());
        verify(writer3, timeout(1000).times(1)).deliver(any());

        unmatch(tenantA, "$oshare/group//a/b/c", MqttBroker, "inbox3", "batch3");
        unmatch(tenantA, "$share/group//a/b/c", MqttBroker, "inbox2", "batch2");
        unmatch(tenantA, "/a/b/c", MqttBroker, "inbox1", "batch1");
    }

    @Test(groups = "integration")
    public void testDistCase7() {
        // pub: qos0
        // topic: "/a/b/c"
        // sub: tenantA, inbox1 -> [(/a/b/c, qos0)]; tenantB, inbox2 -> [(#, qos0)]
        // expected behavior: inbox1 gets 1 messages
        when(mqttBroker.open("batch1")).thenReturn(writer1);

        match(tenantA, "/a/b/c", MqttBroker, "inbox1", "batch1");
        match(tenantB, "#", MqttBroker, "inbox1", "batch1");
        TenantDistReply reply = tenantDist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        assertEquals(reply.getResultsMap().get("/a/b/c").getFanout(), 1);

        ArgumentCaptor<DeliveryRequest> list1 = ArgumentCaptor.forClass(DeliveryRequest.class);
        verify(writer1, timeout(1000).times(1)).deliver(list1.capture());
        int msgCount = 0;
        for (DeliveryRequest request : list1.getAllValues()) {
            for (String tenantId : request.getPackageMap().keySet()) {
                for (DeliveryPack pack : request.getPackageMap().get(tenantId).getPackList()) {
                    TopicMessagePack msgs = pack.getMessagePack();
                    assertEquals(msgs.getTopic(), "/a/b/c");
                    for (TopicMessagePack.PublisherPack publisherPack : msgs.getMessageList()) {
                        for (Message msg : publisherPack.getMessageList()) {
                            msgCount++;
                            assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                        }
                    }
                }
            }
        }
        assertEquals(msgCount, 1);

        unmatch(tenantA, "/a/b/c", MqttBroker, "inbox1", "batch1");
        unmatch(tenantB, "#", MqttBroker, "inbox1", "batch1");
    }

    @Test(groups = "integration")
    public void testRouteRefresh() {
        when(mqttBroker.open("batch1")).thenReturn(writer1);
        when(mqttBroker.open("batch2")).thenReturn(writer2);
        when(mqttBroker.open("batch3")).thenReturn(writer3);

//        when(distClient.clear(anyLong(), anyString(), anyString(), anyString(), anyInt()))
//                .thenReturn(CompletableFuture.completedFuture(null));

        // pub: qos0
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0)]
        // expected behavior: inbox1 gets 1 message
        match(tenantA, "/a/b/c", MqttBroker, "inbox1", "batch1");
        tenantDist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        verify(writer1, timeout(1000).times(1)).deliver(any());
        clearInvocations(writer1, writer2, writer3);

        // pub: qos0
        // topic: "/a/b/c"
        // action: delete inbox1 match record
        // sub: no sub
        // expected behavior: inbox1 gets no messages
        unmatch(tenantA, "/a/b/c", MqttBroker, "inbox1", "batch1");
        tenantDist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        verify(writer1, timeout(1000).times(0)).deliver(any());
        clearInvocations(writer1, writer2, writer3);

        // pub: qos0
        // topic: "/a/b/c"
        // action: inbox2 join shared group
        // sub: inbox2 -> [($share/group/a/b/c, qos0)]
        // expected behavior: inbox2 gets 1 message
        match(tenantA, "$share/group//a/b/c", MqttBroker, "inbox2", "batch2");
        tenantDist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        verify(writer2, timeout(1000).times(1)).deliver(any());
        clearInvocations(writer1, writer2, writer3);

        // pub: qos0
        // topic: "/a/b/c"
        // action: inbox2 leaves the shared group and inbox3 joins
        // sub: inbox3 -> [($share/group/a/b/c, qos0)]
        // expected behavior: inbox2 gets no messages and inbox3 gets 1
        unmatch(tenantA, "$share/group//a/b/c", MqttBroker, "inbox2", "batch2");
        match(tenantA, "$share/group//a/b/c", MqttBroker, "inbox3", "batch3");
        tenantDist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        verify(writer2, timeout(1000).times(0)).deliver(any());
        verify(writer3, timeout(1000).times(1)).deliver(any());
        clearInvocations(writer1, writer2, writer3);

        // pub: qos0
        // topic: "/a/b/c"
        // action: inbox2 joins an ordered shared group and inbox3 leaves the shared group
        // sub: inbox2 -> [($oshare/group/a/b/c, qos0)]
        // expected behavior: inbox2 gets 1 message and inbox3 gets none
        match(tenantA, "$oshare/group//a/b/c", MqttBroker, "inbox2", "batch2");
        unmatch(tenantA, "$share/group//a/b/c", MqttBroker, "inbox3", "batch3");
        tenantDist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        verify(writer2, timeout(1000).times(1)).deliver(any());
        verify(writer3, timeout(1000).times(0)).deliver(any());
        clearInvocations(writer1, writer2, writer3);

        // pub: qos0
        // topic: "/a/b/c"
        // action: inbox3 joins the ordered shared group and inbox2 leaves
        // sub: inbox3 -> [($oshare/group/a/b/c, qos0)]
        // expected behavior: inbox2 gets no messages and inbox3 gets 1
        unmatch(tenantA, "$oshare/group//a/b/c", MqttBroker, "inbox2", "batch2");
        match(tenantA, "$oshare/group//a/b/c", MqttBroker, "inbox3", "batch3");
        tenantDist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        verify(writer2, timeout(1000).times(0)).deliver(any());
        verify(writer3, timeout(1000).times(1)).deliver(any());

        // clear
        unmatch(tenantA, "$oshare/group//a/b/c", MqttBroker, "inbox3", "batch3");
    }

    @Test(groups = "integration")
    public void testRouteRefreshWithWildcardTopic() throws InterruptedException {
        // pub: qos0
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0)]
        // expected behavior: inbox1 gets 1 message
        when(mqttBroker.open("batch1")).thenReturn(writer1);
        when(mqttBroker.open("batch2")).thenReturn(writer2);
        when(mqttBroker.open("batch3")).thenReturn(writer3);
        match(tenantA, "/a/b/c", MqttBroker, "inbox1", "batch1");
        tenantDist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        verify(writer1, timeout(1000).times(1)).deliver(any());
        clearInvocations(writer1, writer2, writer3);

        // pub: qos0
        // topic: "/a/b/c", "/#"
        // action: inbox2 sub the wildcard topic "/#"
        // sub: inbox1 -> [(/a/b/c, qos0)], inbox2 -> [(/#, qos0)]
        // expected behavior: inbox1 gets 1 message and inbox2 gets 1 either
        match(tenantA, "/#", MqttBroker, "inbox2", "batch2");
        Thread.sleep(1100);
        tenantDist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        verify(writer1, timeout(1000).times(1)).deliver(any());
        verify(writer2, timeout(1000).times(1)).deliver(any());
        clearInvocations(writer1, writer2, writer3);

        // pub: qos0
        // topic: "/a/b/c", "/#"
        // action: inbox3 joins the shared and ordered shared wildcard topic "/#"
        // sub: inbox1 -> [(/a/b/c, qos0)],
        // inbox2 -> [(/#, qos0)],
        // inbox3 -> [(#share/group/#, qos0), (#oshare/group/#, qos0)]
        // expected behavior: inbox1 and inbox2 gets 1 message, inbox3 gets 2
        match(tenantA, "$share/group/#", MqttBroker, "inbox3", "batch3");
        match(tenantA, "$oshare/group/#", MqttBroker, "inbox3", "batch3");
        // wait for cache refresh after writing
        Thread.sleep(1100);
        tenantDist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        verify(writer1, timeout(1000).times(1)).deliver(any());
        verify(writer2, timeout(1000).times(1)).deliver(any());
        verify(writer3, timeout(1000).atLeastOnce()).deliver(any());

        // clear
        unmatch(tenantA, "/#", MqttBroker, "inbox2", "batch2");
        unmatch(tenantA, "$share/group/#", MqttBroker, "inbox3", "batch3");
        unmatch(tenantA, "$oshare/group/#", MqttBroker, "inbox3", "batch3");
    }
}