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
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.dist.client.ClearResult;
import com.baidu.bifromq.dist.rpc.proto.BatchDistReply;
import com.baidu.bifromq.plugin.inboxbroker.InboxPack;
import com.baidu.bifromq.plugin.inboxbroker.WriteResult;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

@Slf4j
public class DistQoS0Test extends DistWorkerTest {

    @Test(groups = "integration")
    public void succeedWithNoSub() {
        String trafficId = "trafficA";
        String topic = "/a/b/c";
        ByteString payload = copyFromUtf8("hello");

        BatchDistReply reply = dist(trafficId, AT_MOST_ONCE, topic, payload, "orderKey1");
        assertTrue(reply.getResultMap().get(trafficId).getFanoutMap().getOrDefault(topic, 0).intValue() == 0);
    }

    @Test(groups = "integration")
    public void testDistCase2() {
        // pub: qos0
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0),(/#, qos0)], inbox2 -> [(/#,qos1)]
        // expected behavior: inbox1 gets 2 messages, inbox2 get 1 message
        when(receiverManager.openWriter("batch1", MqttBroker))
            .thenReturn(writer1);
        when(receiverManager.openWriter("batch2", InboxService))
            .thenReturn(writer2);
        when(writer1.write(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                Iterable<InboxPack> inboxPacks = invocation.getArgument(0);
                Map<SubInfo, WriteResult> resultMap = new HashMap<>();
                for (InboxPack inboxWrite : inboxPacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, WriteResult.OK);
                    }
                }
                return CompletableFuture.completedFuture(resultMap);
            });
        when(writer2.write(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                Iterable<InboxPack> inboxPacks = invocation.getArgument(0);
                Map<SubInfo, WriteResult> resultMap = new HashMap<>();
                for (InboxPack inboxWrite : inboxPacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, WriteResult.OK);
                    }
                }
                return CompletableFuture.completedFuture(resultMap);
            });

        insertMatchRecord("trafficA", "/你好/hello/😄", AT_MOST_ONCE,
            MqttBroker, "inbox1", "batch1");
        insertMatchRecord("trafficA", "/#", AT_MOST_ONCE,
            MqttBroker, "inbox1", "batch1");
        insertMatchRecord("trafficA", "/#", AT_LEAST_ONCE,
            InboxService, "inbox2", "batch2");

        BatchDistReply reply = dist("trafficA", AT_MOST_ONCE, "/你好/hello/😄", copyFromUtf8("Hello"), "orderKey1");
        assertTrue(reply.getResultMap().get("trafficA").getFanoutMap().get("/你好/hello/😄").intValue() > 0);

        ArgumentCaptor<Iterable<InboxPack>> msgCap = ArgumentCaptor.forClass(Iterable.class);
        verify(writer1, timeout(1000).atLeastOnce()).write(msgCap.capture());
        for (InboxPack pack : msgCap.getValue()) {
            TopicMessagePack msgPack = pack.messagePack;
            assertEquals("/你好/hello/😄", msgPack.getTopic());
            for (TopicMessagePack.SenderMessagePack senderMsgPack : msgPack.getMessageList()) {
                for (Message msg : senderMsgPack.getMessageList()) {
                    assertEquals(copyFromUtf8("Hello"), msg.getPayload());
                }
            }
        }

        msgCap = ArgumentCaptor.forClass(Iterable.class);
        verify(writer2, timeout(1000).times(1)).write(msgCap.capture());
        for (InboxPack pack : msgCap.getValue()) {
            TopicMessagePack msgs = pack.messagePack;
            assertEquals("/你好/hello/😄", msgs.getTopic());
            for (TopicMessagePack.SenderMessagePack senderMsgPack : msgs.getMessageList()) {
                for (Message msg : senderMsgPack.getMessageList()) {
                    assertEquals(copyFromUtf8("Hello"), msg.getPayload());
                }
            }
        }
    }

    @Test(groups = "integration")
    public void testDistCase3() throws InterruptedException {
        // pub: qos0
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0),(/a/b/c, qos0)]
        // expected behavior: inbox1 gets 1 messages
        when(receiverManager.openWriter("batch1", MqttBroker))
            .thenReturn(writer1);
        when(writer1.write(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                Iterable<InboxPack> inboxPacks = invocation.getArgument(0);
                Map<SubInfo, WriteResult> resultMap = new HashMap<>();
                for (InboxPack inboxWrite : inboxPacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, WriteResult.OK);
                    }
                }
                return CompletableFuture.completedFuture(resultMap);
            });

        insertMatchRecord("trafficA", "/a/b/c", AT_MOST_ONCE,
            MqttBroker, "inbox1", "batch1");
        insertMatchRecord("trafficA", "/a/b/c", AT_MOST_ONCE,
            MqttBroker, "inbox1", "batch1");
        BatchDistReply reply = dist("trafficA", AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        assertTrue(reply.getResultMap().get("trafficA").getFanoutMap().get("/a/b/c").intValue() > 0);

        ArgumentCaptor<Iterable<InboxPack>> list1 = ArgumentCaptor.forClass(Iterable.class);
        verify(writer1, timeout(1000).times(1)).write(list1.capture());
        for (InboxPack pack : list1.getValue()) {
            TopicMessagePack msgs = pack.messagePack;
            assertEquals("/a/b/c", msgs.getTopic());
            for (TopicMessagePack.SenderMessagePack senderMsgPack : msgs.getMessageList()) {
                for (Message msg : senderMsgPack.getMessageList()) {
                    assertEquals(copyFromUtf8("Hello"), msg.getPayload());
                }
            }
        }
    }

    @Test(groups = "integration")
    public void testDistCase4() {
        // pub: qos0
        // topic: "/a/b/c"
        // sub: inbox1 -> [($share/group//a/b/c, qos0),(/#, qos0)], inbox2 -> [($share/group//a/b/c,qos1)]
        // expected behavior: total 10 messages, inbox1 gets N messages, inbox2 get M message, N + M == 10, N > 0, M > 0


        when(receiverManager.openWriter("batch1", MqttBroker))
            .thenReturn(writer1);
        when(writer1.write(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                Iterable<InboxPack> inboxPacks = invocation.getArgument(0);
                Map<SubInfo, WriteResult> resultMap = new HashMap<>();
                for (InboxPack inboxWrite : inboxPacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, WriteResult.OK);
                    }
                }
                return CompletableFuture.completedFuture(resultMap);
            });

        when(receiverManager.openWriter("batch2", MqttBroker))
            .thenReturn(writer2);
        when(writer2.write(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                Iterable<InboxPack> inboxPacks = invocation.getArgument(0);
                Map<SubInfo, WriteResult> resultMap = new HashMap<>();
                for (InboxPack inboxWrite : inboxPacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, WriteResult.OK);
                    }
                }
                return CompletableFuture.completedFuture(resultMap);
            });

        joinMatchGroup("trafficA", "$share/group//a/b/c", AT_MOST_ONCE,
            MqttBroker, "inbox1", "batch1");
        joinMatchGroup("trafficA", "$share/group//a/b/c", AT_LEAST_ONCE,
            MqttBroker, "inbox2", "batch2");
        for (int i = 0; i < 10; i++) {
            BatchDistReply reply = dist("trafficA", AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            assertTrue(reply.getResultMap().get("trafficA").getFanoutMap().get("/a/b/c").intValue() > 0);
        }

        ArgumentCaptor<Iterable<InboxPack>> list1 = ArgumentCaptor.forClass(Iterable.class);
        verify(writer1, after(100).atMost(10)).write(list1.capture());
        for (InboxPack pack : list1.getValue()) {
            TopicMessagePack msgs = pack.messagePack;
            assertEquals("/a/b/c", msgs.getTopic());
            for (TopicMessagePack.SenderMessagePack senderMsgPack : msgs.getMessageList()) {
                for (Message msg : senderMsgPack.getMessageList()) {
                    assertEquals(copyFromUtf8("Hello"), msg.getPayload());
                }
            }
        }

        ArgumentCaptor<Iterable<InboxPack>> list2 = ArgumentCaptor.forClass(Iterable.class);

        verify(writer2, after(100).atMost(10)).write(list2.capture());
        for (InboxPack pack : list2.getValue()) {
            TopicMessagePack msgs = pack.messagePack;
            assertEquals("/a/b/c", msgs.getTopic());
            for (TopicMessagePack.SenderMessagePack senderMsgPack : msgs.getMessageList()) {
                for (Message msg : senderMsgPack.getMessageList()) {
                    assertEquals(copyFromUtf8("Hello"), msg.getPayload());
                }
            }
        }
        assertEquals(10, list1.getAllValues().size() + list2.getAllValues().size());
    }

    @Test(groups = "integration")
    public void testDistCase5() {
        // pub: qos0
        // topic: "/a/b/c"
        // sub: inbox1 -> [($oshare/group//a/b/c, qos0)], inbox2 -> [($oshare/group//a/b/c, qos1)]
        // expected behavior: total 10 messages, inbox1 gets N messages, inbox2 get M message, either N or M is 10

        lenient().when(receiverManager.openWriter("batch1", MqttBroker))
            .thenReturn(writer1);
        lenient().when(writer1.write(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                Iterable<InboxPack> inboxPacks = invocation.getArgument(0);
                Map<SubInfo, WriteResult> resultMap = new HashMap<>();
                for (InboxPack inboxWrite : inboxPacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, WriteResult.OK);
                    }
                }
                return CompletableFuture.completedFuture(resultMap);
            });

        lenient().when(receiverManager.openWriter("batch2", MqttBroker))
            .thenReturn(writer2);
        lenient().when(writer2.write(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                Iterable<InboxPack> inboxPacks = invocation.getArgument(0);
                Map<SubInfo, WriteResult> resultMap = new HashMap<>();
                for (InboxPack inboxWrite : inboxPacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, WriteResult.OK);
                    }
                }
                return CompletableFuture.completedFuture(resultMap);
            });

        joinMatchGroup("trafficA", "$oshare/group//a/b/c", AT_MOST_ONCE,
            MqttBroker, "inbox1", "batch1");
        joinMatchGroup("trafficA", "$oshare/group//a/b/c", AT_LEAST_ONCE,
            MqttBroker, "inbox2", "batch2");

        for (int i = 0; i < 10; i++) {
            BatchDistReply reply = dist("trafficA", AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            assertTrue(reply.getResultMap().get("trafficA").getFanoutMap().get("/a/b/c").intValue() == 1);
        }

        ArgumentCaptor<Iterable<InboxPack>> list1 = ArgumentCaptor.forClass(Iterable.class);
        verify(writer1, after(100).atMost(10)).write(list1.capture());

        ArgumentCaptor<Iterable<InboxPack>> list2 = ArgumentCaptor.forClass(Iterable.class);
        verify(writer2, after(100).atMost(10)).write(list2.capture());

        List<Iterable<InboxPack>> captured = list1.getAllValues().isEmpty() ?
            list2.getAllValues() : list1.getAllValues();

        for (Iterable<InboxPack> packs : captured) {
            for (InboxPack pack : packs) {
                TopicMessagePack msgs = pack.messagePack;
                assertEquals("/a/b/c", msgs.getTopic());
                for (TopicMessagePack.SenderMessagePack senderMsgPack : msgs.getMessageList()) {
                    for (Message msg : senderMsgPack.getMessageList()) {
                        assertEquals(copyFromUtf8("Hello"), msg.getPayload());
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
        when(receiverManager.openWriter("batch1", MqttBroker))
            .thenReturn(writer1);
        when(writer1.write(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                Iterable<InboxPack> inboxPacks = invocation.getArgument(0);
                Map<SubInfo, WriteResult> resultMap = new HashMap<>();
                for (InboxPack inboxWrite : inboxPacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, WriteResult.NO_INBOX);
                    }
                }
                return CompletableFuture.completedFuture(resultMap);
            });

        when(receiverManager.openWriter("batch2", MqttBroker))
            .thenReturn(writer2);
        when(writer2.write(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                Iterable<InboxPack> inboxPacks = invocation.getArgument(0);
                Map<SubInfo, WriteResult> resultMap = new HashMap<>();
                for (InboxPack inboxWrite : inboxPacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, WriteResult.NO_INBOX);
                    }
                }
                return CompletableFuture.completedFuture(resultMap);
            });

        when(receiverManager.openWriter("batch3", MqttBroker))
            .thenReturn(writer3);
        when(writer3.write(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                Iterable<InboxPack> inboxPacks = invocation.getArgument(0);
                Map<SubInfo, WriteResult> resultMap = new HashMap<>();
                for (InboxPack inboxWrite : inboxPacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, WriteResult.NO_INBOX);
                    }
                }
                return CompletableFuture.completedFuture(resultMap);
            });

        when(distClient.clear(anyLong(), anyString(), anyString(), anyInt(), any(ClientInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(ClearResult.OK));

        insertMatchRecord("trafficA", "/a/b/c", AT_MOST_ONCE,
            MqttBroker, "inbox1", "batch1");
        joinMatchGroup("trafficA", "$share/group//a/b/c", AT_MOST_ONCE,
            MqttBroker, "inbox2", "batch2");
        joinMatchGroup("trafficA", "$oshare/group//a/b/c", AT_MOST_ONCE,
            MqttBroker, "inbox3", "batch3");
        BatchDistReply reply = dist("trafficA", AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        assertTrue(reply.getResultMap().get("trafficA").getFanoutMap().get("/a/b/c").intValue() > 0);

        verify(writer1, timeout(1000).times(1)).write(any());
        verify(writer2, timeout(1000).times(1)).write(any());
        verify(writer3, timeout(1000).times(1)).write(any());
        await().until(() -> {
            try {
                verify(distClient, times(3))
                    .clear(anyLong(), anyString(), anyString(), anyInt(), any(ClientInfo.class));
                return true;
            } catch (Throwable e) {
                return false;
            }
        });
    }
}