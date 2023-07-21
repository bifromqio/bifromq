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
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.dist.rpc.proto.BatchDistReply;
import com.baidu.bifromq.plugin.subbroker.DeliveryPack;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

@Slf4j
public class DistQoS0Test extends DistWorkerTest {

    @Test(groups = "integration")
    public void succeedWithNoSub() {
        String topic = "/a/b/c";
        ByteString payload = copyFromUtf8("hello");

        BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, topic, payload, "orderKey1");
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().getOrDefault(topic, 0).intValue(), 0);
    }

    @Test(groups = "integration")
    public void testDistCase2() {
        // pub: qos0
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0),(/#, qos0)], inbox2 -> [(/#,qos1)]
        // expected behavior: inbox1 gets 2 messages, inbox2 get 1 message
        when(receiverManager.get(MqttBroker)).thenReturn(mqttBroker);
        when(mqttBroker.open("batch1")).thenReturn(writer1);
        when(receiverManager.get(InboxService)).thenReturn(inboxBroker);
        when(inboxBroker.open("batch2")).thenReturn(writer2);
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

        insertMatchRecord(tenantA, "/你好/hello/😄", AT_MOST_ONCE, MqttBroker, "inbox1", "batch1");
        insertMatchRecord(tenantA, "/#", AT_MOST_ONCE, MqttBroker, tenantA, "batch1");
        insertMatchRecord(tenantA, "/#", AT_LEAST_ONCE, InboxService, "inbox2", "batch2");

        BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, "/你好/hello/😄", copyFromUtf8("Hello"), "orderKey1");
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("/你好/hello/😄").intValue(), 3);

        ArgumentCaptor<Iterable<DeliveryPack>> msgCap = ArgumentCaptor.forClass(Iterable.class);
        verify(writer1, timeout(1000).atLeastOnce()).deliver(msgCap.capture());
        for (DeliveryPack pack : msgCap.getValue()) {
            TopicMessagePack msgPack = pack.messagePack;
            assertEquals(msgPack.getTopic(), "/你好/hello/😄");
            for (TopicMessagePack.PublisherPack publisherPack : msgPack.getMessageList()) {
                for (Message msg : publisherPack.getMessageList()) {
                    assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                }
            }
        }

        msgCap = ArgumentCaptor.forClass(Iterable.class);
        verify(writer2, timeout(1000).times(1)).deliver(msgCap.capture());
        for (DeliveryPack pack : msgCap.getValue()) {
            TopicMessagePack msgs = pack.messagePack;
            assertEquals(msgs.getTopic(), "/你好/hello/😄");
            for (TopicMessagePack.PublisherPack publisherPack : msgs.getMessageList()) {
                for (Message msg : publisherPack.getMessageList()) {
                    assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                }
            }
        }
    }

    @Test(groups = "integration")
    public void testDistCase3() {
        // pub: qos0
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0)], inbox2 -> [(/a/b/c, qos0)]
        // expected behavior: inbox1 gets 1 messages
        when(receiverManager.get(MqttBroker)).thenReturn(mqttBroker);
        when(mqttBroker.open("batch1")).thenReturn(writer1);

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

        insertMatchRecord(tenantA, "/a/b/c", AT_MOST_ONCE,
            MqttBroker, "inbox1", "batch1");
        insertMatchRecord(tenantA, "/a/b/c", AT_MOST_ONCE,
            MqttBroker, "inbox2", "batch1");
        BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("/a/b/c").intValue(), 2);

        ArgumentCaptor<Iterable<DeliveryPack>> list1 = ArgumentCaptor.forClass(Iterable.class);
        verify(writer1, after(1000).atMost(2)).deliver(list1.capture());
        int msgCount = 0;
        Set<SubInfo> subInfos = new HashSet<>();
        for (Iterable<DeliveryPack> packs : list1.getAllValues()) {
            for (DeliveryPack pack : packs) {
                TopicMessagePack msgs = pack.messagePack;
                pack.inboxes.forEach(subInfos::add);
                assertEquals(msgs.getTopic(), "/a/b/c");
                for (TopicMessagePack.PublisherPack publisherPack : msgs.getMessageList()) {
                    for (Message msg : publisherPack.getMessageList()) {
                        msgCount++;
                        assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                    }
                }
            }
        }
        assertEquals(subInfos.size(), 2);
        assertEquals(msgCount, 2);
    }

    @Test(groups = "integration")
    public void testDistCase4() {
        // pub: qos0
        // topic: "/a/b/c"
        // sub: inbox1 -> [($share/group//a/b/c, qos0),(/#, qos0)], inbox2 -> [($share/group//a/b/c,qos1)]
        // expected behavior: total 10 messages, inbox1 gets N messages, inbox2 get M message, N + M == 10, N > 0, M > 0

        when(receiverManager.get(MqttBroker)).thenReturn(mqttBroker);
        when(mqttBroker.open("batch1")).thenReturn(writer1);

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
        when(mqttBroker.open("batch2")).thenReturn(writer2);
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

        joinMatchGroup(tenantA, "$share/group//a/b/c", AT_MOST_ONCE, MqttBroker, "inbox1", "batch1");
        joinMatchGroup(tenantA, "$share/group//a/b/c", AT_LEAST_ONCE, MqttBroker, "inbox2", "batch2");
        for (int i = 0; i < 10; i++) {
            BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("/a/b/c"), 1);
        }

        ArgumentCaptor<Iterable<DeliveryPack>> list1 = ArgumentCaptor.forClass(Iterable.class);
        verify(writer1, after(1000).atMost(10)).deliver(list1.capture());
        for (DeliveryPack pack : list1.getValue()) {
            TopicMessagePack msgs = pack.messagePack;
            assertEquals(msgs.getTopic(), "/a/b/c");
            for (TopicMessagePack.PublisherPack publisherPack : msgs.getMessageList()) {
                for (Message msg : publisherPack.getMessageList()) {
                    assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                }
            }
        }

        ArgumentCaptor<Iterable<DeliveryPack>> list2 = ArgumentCaptor.forClass(Iterable.class);

        verify(writer2, after(100).atMost(10)).deliver(list2.capture());
        for (DeliveryPack pack : list2.getValue()) {
            TopicMessagePack msgs = pack.messagePack;
            assertEquals(msgs.getTopic(), "/a/b/c");
            for (TopicMessagePack.PublisherPack publisherPack : msgs.getMessageList()) {
                for (Message msg : publisherPack.getMessageList()) {
                    assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                }
            }
        }
        assertEquals(list1.getAllValues().size() + list2.getAllValues().size(), 10);
    }

    @Test(groups = "integration")
    public void testDistCase5() {
        // pub: qos0
        // topic: "/a/b/c"
        // sub: inbox1 -> [($oshare/group//a/b/c, qos0)], inbox2 -> [($oshare/group//a/b/c, qos1)]
        // expected behavior: total 10 messages, inbox1 gets N messages, inbox2 get M message, either N or M is 10
        when(receiverManager.get(MqttBroker)).thenReturn(mqttBroker);
        when(mqttBroker.open("batch1")).thenReturn(writer1);

        lenient().when(writer1.deliver(any()))
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

        when(mqttBroker.open("batch2")).thenReturn(writer2);

        lenient().when(writer2.deliver(any()))
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

        joinMatchGroup(tenantA, "$oshare/group//a/b/c", AT_MOST_ONCE,
            MqttBroker, "inbox1", "batch1");
        joinMatchGroup(tenantA, "$oshare/group//a/b/c", AT_LEAST_ONCE,
            MqttBroker, "inbox2", "batch2");

        for (int i = 0; i < 10; i++) {
            BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            assertTrue(reply.getResultMap().get(tenantA).getFanoutMap().get("/a/b/c").intValue() == 1);
        }

        ArgumentCaptor<Iterable<DeliveryPack>> list1 = ArgumentCaptor.forClass(Iterable.class);
        verify(writer1, after(100).atMost(10)).deliver(list1.capture());

        ArgumentCaptor<Iterable<DeliveryPack>> list2 = ArgumentCaptor.forClass(Iterable.class);
        verify(writer2, after(100).atMost(10)).deliver(list2.capture());

        List<Iterable<DeliveryPack>> captured = list1.getAllValues().isEmpty() ?
            list2.getAllValues() : list1.getAllValues();

        for (Iterable<DeliveryPack> packs : captured) {
            for (DeliveryPack pack : packs) {
                TopicMessagePack msgs = pack.messagePack;
                assertEquals(msgs.getTopic(), "/a/b/c");
                for (TopicMessagePack.PublisherPack publisherPack : msgs.getMessageList()) {
                    for (Message msg : publisherPack.getMessageList()) {
                        assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                    }
                }
            }
        }
        assertTrue(list1.getAllValues().isEmpty() || list2.getAllValues().isEmpty());
    }

    @Test(groups = "integration")
    public void testDistCase6() {
        // pub: qos0
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0),(/#, qos0)], inbox2 -> [($share/group//a/b/c, qos0)]
        //      inbox3 -> [($oshare/group//a/b/c, qos0)]
        // subbroker: inbox1 -> NO_INBOX, inbox2 -> NO_INBOX, inbox3 -> NO_INBOX
        // expected behavior: clear gets called 3 times
        when(receiverManager.get(MqttBroker)).thenReturn(mqttBroker);
        when(mqttBroker.open("batch1")).thenReturn(writer1);

        when(writer1.deliver(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, DeliveryResult>>>) invocation -> {
                Iterable<DeliveryPack> inboxPacks = invocation.getArgument(0);
                Map<SubInfo, DeliveryResult> resultMap = new HashMap<>();
                for (DeliveryPack inboxWrite : inboxPacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, DeliveryResult.NO_INBOX);
                    }
                }
                return CompletableFuture.completedFuture(resultMap);
            });

        when(mqttBroker.open("batch2")).thenReturn(writer2);

        when(writer2.deliver(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, DeliveryResult>>>) invocation -> {
                Iterable<DeliveryPack> inboxPacks = invocation.getArgument(0);
                Map<SubInfo, DeliveryResult> resultMap = new HashMap<>();
                for (DeliveryPack inboxWrite : inboxPacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, DeliveryResult.NO_INBOX);
                    }
                }
                return CompletableFuture.completedFuture(resultMap);
            });

        when(mqttBroker.open("batch3")).thenReturn(writer3);

        when(writer3.deliver(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, DeliveryResult>>>) invocation -> {
                Iterable<DeliveryPack> inboxPacks = invocation.getArgument(0);
                Map<SubInfo, DeliveryResult> resultMap = new HashMap<>();
                for (DeliveryPack inboxWrite : inboxPacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, DeliveryResult.NO_INBOX);
                    }
                }
                return CompletableFuture.completedFuture(resultMap);
            });

        when(distClient.clear(anyLong(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(null));

        insertMatchRecord(tenantA, "/a/b/c", AT_MOST_ONCE,
            MqttBroker, "inbox1", "batch1");
        joinMatchGroup(tenantA, "$share/group//a/b/c", AT_MOST_ONCE,
            MqttBroker, "inbox2", "batch2");
        joinMatchGroup(tenantA, "$oshare/group//a/b/c", AT_MOST_ONCE,
            MqttBroker, "inbox3", "batch3");
        BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("/a/b/c").intValue(), 3);

        verify(writer1, timeout(1000).times(1)).deliver(any());
        verify(writer2, timeout(1000).times(1)).deliver(any());
        verify(writer3, timeout(1000).times(1)).deliver(any());
        await().until(() -> {
            try {
                verify(distClient, times(3))
                    .unsub(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt());
                return true;
            } catch (Throwable e) {
                return false;
            }
        });
    }

    @Test(groups = "integration")
    public void testDistCase7() {
        // pub: qos0
        // topic: "/a/b/c"
        // sub: tenantA, inbox1 -> [(/a/b/c, qos0)]; tenantB, inbox2 -> [(#, qos0)]
        // expected behavior: inbox1 gets 1 messages
        when(receiverManager.get(MqttBroker)).thenReturn(mqttBroker);
        when(mqttBroker.open("batch1")).thenReturn(writer1);

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

        insertMatchRecord(tenantA, "/a/b/c", AT_MOST_ONCE, MqttBroker, "inbox1", "batch1");
        insertMatchRecord(tenantB, "#", AT_MOST_ONCE, MqttBroker, "inbox1", "batch1");
        BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("/a/b/c").intValue(), 1);

        ArgumentCaptor<Iterable<DeliveryPack>> list1 = ArgumentCaptor.forClass(Iterable.class);
        verify(writer1, timeout(1000).times(1)).deliver(list1.capture());
        int msgCount = 0;
        for (Iterable<DeliveryPack> packs : list1.getAllValues()) {
            for (DeliveryPack pack : packs) {
                TopicMessagePack msgs = pack.messagePack;
                assertEquals(msgs.getTopic(), "/a/b/c");
                for (TopicMessagePack.PublisherPack publisherPack : msgs.getMessageList()) {
                    for (Message msg : publisherPack.getMessageList()) {
                        msgCount++;
                        assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                    }
                }
            }
        }
        assertEquals(msgCount, 1);
    }

    @Test(groups = "integration")
    public void testRouteRefresh() {
        when(receiverManager.get(MqttBroker)).thenReturn(mqttBroker);
        when(mqttBroker.open("batch1")).thenReturn(writer1);

        when(writer1.deliver(any()))
                .thenAnswer((Answer<CompletableFuture<Map<SubInfo, DeliveryResult>>>) invocation -> {
                    Iterable<DeliveryPack> inboxPacks = invocation.getArgument(0);
                    Map<SubInfo, DeliveryResult> resultMap = new HashMap<>();
                    for (DeliveryPack inboxWrite : inboxPacks) {
                        for (SubInfo subInfo : inboxWrite.inboxes) {
                            resultMap.put(subInfo, DeliveryResult.NO_INBOX);
                        }
                    }
                    return CompletableFuture.completedFuture(resultMap);
                });

        when(mqttBroker.open("batch2")).thenReturn(writer2);

        when(writer2.deliver(any()))
                .thenAnswer((Answer<CompletableFuture<Map<SubInfo, DeliveryResult>>>) invocation -> {
                    Iterable<DeliveryPack> inboxPacks = invocation.getArgument(0);
                    Map<SubInfo, DeliveryResult> resultMap = new HashMap<>();
                    for (DeliveryPack inboxWrite : inboxPacks) {
                        for (SubInfo subInfo : inboxWrite.inboxes) {
                            resultMap.put(subInfo, DeliveryResult.NO_INBOX);
                        }
                    }
                    return CompletableFuture.completedFuture(resultMap);
                });

        when(mqttBroker.open("batch3")).thenReturn(writer3);

        when(writer3.deliver(any()))
                .thenAnswer((Answer<CompletableFuture<Map<SubInfo, DeliveryResult>>>) invocation -> {
                    Iterable<DeliveryPack> inboxPacks = invocation.getArgument(0);
                    Map<SubInfo, DeliveryResult> resultMap = new HashMap<>();
                    for (DeliveryPack inboxWrite : inboxPacks) {
                        for (SubInfo subInfo : inboxWrite.inboxes) {
                            resultMap.put(subInfo, DeliveryResult.NO_INBOX);
                        }
                    }
                    return CompletableFuture.completedFuture(resultMap);
                });

        when(distClient.clear(anyLong(), anyString(), anyString(), anyString(), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(null));

        // pub: qos0
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0)]
        // expected behavior: inbox1 gets 1 message
        insertMatchRecord(tenantA, "/a/b/c", AT_MOST_ONCE,
                MqttBroker, "inbox1", "batch1");
        dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        verify(writer1, timeout(1000).times(1)).deliver(any());
        clearInvocations(writer1, writer2, writer3);

        // pub: qos0
        // topic: "/a/b/c"
        // action: delete inbox1 match record
        // sub: no sub
        // expected behavior: inbox1 gets no messages
        deleteMatchRecord(tenantA, "/a/b/c", MqttBroker, "inbox1", "batch1");
        dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        verify(writer1, timeout(1000).times(0)).deliver(any());
        clearInvocations(writer1, writer2, writer3);

        // pub: qos0
        // topic: "/a/b/c"
        // action: inbox2 join shared group
        // sub: inbox2 -> [($share/group/a/b/c, qos0)]
        // expected behavior: inbox2 gets 1 message
        joinMatchGroup(tenantA, "$share/group//a/b/c", AT_MOST_ONCE,
                MqttBroker, "inbox2", "batch2");
        dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        verify(writer2, timeout(1000).times(1)).deliver(any());
        clearInvocations(writer1, writer2, writer3);

        // pub: qos0
        // topic: "/a/b/c"
        // action: inbox2 leaves the shared group and inbox3 joins
        // sub: inbox3 -> [($share/group/a/b/c, qos0)]
        // expected behavior: inbox2 gets no messages and inbox3 gets 1
        leaveMatchGroup(tenantA, "$share/group//a/b/c", MqttBroker, "inbox2", "batch2");
        joinMatchGroup(tenantA, "$share/group//a/b/c", AT_MOST_ONCE,
                MqttBroker, "inbox3", "batch3");
        dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        verify(writer2, timeout(1000).times(0)).deliver(any());
        verify(writer3, timeout(1000).times(1)).deliver(any());
        clearInvocations(writer1, writer2, writer3);

        // pub: qos0
        // topic: "/a/b/c"
        // action: inbox2 joins an ordered shared group and inbox3 leaves the shared group
        // sub: inbox2 -> [($oshare/group/a/b/c, qos0)]
        // expected behavior: inbox2 gets 1 message and inbox3 gets none
        joinMatchGroup(tenantA, "$oshare/group//a/b/c", AT_MOST_ONCE,
                MqttBroker, "inbox2", "batch2");
        leaveMatchGroup(tenantA, "$share/group//a/b/c", MqttBroker, "inbox3", "batch3");
        dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        verify(writer2, timeout(1000).times(1)).deliver(any());
        verify(writer3, timeout(1000).times(0)).deliver(any());
        clearInvocations(writer1, writer2, writer3);

        // pub: qos0
        // topic: "/a/b/c"
        // action: inbox3 joins the ordered shared group and inbox2 leaves
        // sub: inbox3 -> [($oshare/group/a/b/c, qos0)]
        // expected behavior: inbox2 gets no messages and inbox3 gets 1
        leaveMatchGroup(tenantA, "$oshare/group//a/b/c", MqttBroker, "inbox2", "batch2");
        joinMatchGroup(tenantA, "$oshare/group//a/b/c", AT_MOST_ONCE,
                MqttBroker, "inbox3", "batch3");
        dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        verify(writer2, timeout(1000).times(0)).deliver(any());
        verify(writer3, timeout(1000).times(1)).deliver(any());
    }

    @Test(groups = "integration")
    public void testRouteRefreshWithWildcardTopic() throws InterruptedException {
        when(receiverManager.get(MqttBroker)).thenReturn(mqttBroker);
        when(mqttBroker.open("batch1")).thenReturn(writer1);

        when(writer1.deliver(any()))
                .thenAnswer((Answer<CompletableFuture<Map<SubInfo, DeliveryResult>>>) invocation -> {
                    Iterable<DeliveryPack> inboxPacks = invocation.getArgument(0);
                    Map<SubInfo, DeliveryResult> resultMap = new HashMap<>();
                    for (DeliveryPack inboxWrite : inboxPacks) {
                        for (SubInfo subInfo : inboxWrite.inboxes) {
                            resultMap.put(subInfo, DeliveryResult.NO_INBOX);
                        }
                    }
                    return CompletableFuture.completedFuture(resultMap);
                });

        when(mqttBroker.open("batch2")).thenReturn(writer2);

        when(writer2.deliver(any()))
                .thenAnswer((Answer<CompletableFuture<Map<SubInfo, DeliveryResult>>>) invocation -> {
                    Iterable<DeliveryPack> inboxPacks = invocation.getArgument(0);
                    Map<SubInfo, DeliveryResult> resultMap = new HashMap<>();
                    for (DeliveryPack inboxWrite : inboxPacks) {
                        for (SubInfo subInfo : inboxWrite.inboxes) {
                            resultMap.put(subInfo, DeliveryResult.NO_INBOX);
                        }
                    }
                    return CompletableFuture.completedFuture(resultMap);
                });

        when(mqttBroker.open("batch3")).thenReturn(writer3);

        when(writer3.deliver(any()))
                .thenAnswer((Answer<CompletableFuture<Map<SubInfo, DeliveryResult>>>) invocation -> {
                    Iterable<DeliveryPack> inboxPacks = invocation.getArgument(0);
                    Map<SubInfo, DeliveryResult> resultMap = new HashMap<>();
                    for (DeliveryPack inboxWrite : inboxPacks) {
                        for (SubInfo subInfo : inboxWrite.inboxes) {
                            resultMap.put(subInfo, DeliveryResult.NO_INBOX);
                        }
                    }
                    return CompletableFuture.completedFuture(resultMap);
                });

        // pub: qos0
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0)]
        // expected behavior: inbox1 gets 1 message
        insertMatchRecord(tenantA, "/a/b/c", AT_MOST_ONCE,
                MqttBroker, "inbox1", "batch1");
        dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        verify(writer1, timeout(1000).times(1)).deliver(any());
        clearInvocations(writer1, writer2, writer3);

        // pub: qos0
        // topic: "/a/b/c", "/#"
        // action: inbox2 sub the wildcard topic "/#"
        // sub: inbox1 -> [(/a/b/c, qos0)], inbox2 -> [(/#, qos0)]
        // expected behavior: inbox1 gets 1 message and inbox2 gets 1 either
        insertMatchRecord(tenantA, "/#", AT_MOST_ONCE,
                MqttBroker, "inbox2", "batch2");
        Thread.sleep(1100);
        dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
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
        joinMatchGroup(tenantA, "$share/group/#", AT_MOST_ONCE,
                MqttBroker, "inbox3", "batch3");
        joinMatchGroup(tenantA, "$oshare/group/#", AT_MOST_ONCE,
                MqttBroker, "inbox3", "batch3");
        // wait for cache refresh after writing
        Thread.sleep(1100);
        dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        verify(writer1, timeout(1000).times(1)).deliver(any());
        verify(writer2, timeout(1000).times(1)).deliver(any());
        verify(writer3, timeout(1000).times(2)).deliver(any());
    }
}