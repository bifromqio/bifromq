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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.dist.client.ClearResult;
import com.baidu.bifromq.dist.rpc.proto.BatchDistReply;
import com.baidu.bifromq.plugin.eventcollector.EventType;
import com.baidu.bifromq.plugin.inboxbroker.InboxPack;
import com.baidu.bifromq.plugin.inboxbroker.WriteResult;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class DistQoS1Test extends DistWorkerTest {
    @Test
    public void succeedWithNoSub() {
        String trafficId = "trafficA";
        String topic = "/a/b/c";
        ByteString payload = copyFromUtf8("hello");

        BatchDistReply reply = dist(trafficId, AT_LEAST_ONCE, topic, payload, "orderKey1");
        assertTrue(reply.getResultMap().get(trafficId).getFanoutMap().getOrDefault(topic, 0).intValue() == 0);
    }

    @Test
    public void testDistCase7() {
        // pub: qos1
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0)]
        // subBroker: inbox1 -> ERROR
        // expected behavior: pub succeed

        when(receiverManager.openWriter("server1", MqttBroker)).thenReturn(writer1);
        when(writer1.write(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                Iterable<InboxPack> inboxPacks = invocation.getArgument(0);
                Map<SubInfo, WriteResult> resultMap = new HashMap<>();
                for (InboxPack inboxWrite : inboxPacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, ThreadLocalRandom.current().nextDouble() <= 0.5 ? WriteResult.error(
                            new RuntimeException()) : WriteResult.OK);
                    }
                }
                return CompletableFuture.completedFuture(resultMap);
            });

        insertMatchRecord("trafficA", "/a/b/c", AT_MOST_ONCE, MqttBroker, "inbox1", "server1");

        for (int i = 0; i < 10; i++) {
            BatchDistReply reply = dist("trafficA", AT_LEAST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            assertTrue(reply.getResultMap().get("trafficA").getFanoutMap().get("/a/b/c").intValue() > 0);
        }

        ArgumentCaptor<Iterable<InboxPack>> messageListCap = ArgumentCaptor.forClass(Iterable.class);
        verify(writer1, after(100).atMost(10)).write(messageListCap.capture());

        for (InboxPack inboxPack : messageListCap.getValue()) {
            TopicMessagePack msgs = inboxPack.messagePack;
            assertEquals("/a/b/c", msgs.getTopic());
            for (TopicMessagePack.SenderMessagePack senderMsgPack : msgs.getMessageList()) {
                for (Message msg : senderMsgPack.getMessageList()) {
                    assertEquals(copyFromUtf8("Hello"), msg.getPayload());
                }
            }
        }
    }

    @Test
    public void testDistCase8() {
        // pub: qos1
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos1)]
        // subBroker: inbox1 -> ERROR
        // expected behavior: pub failed

        when(receiverManager.openWriter("server1", MqttBroker)).thenReturn(writer1);
        when(writer1.write(any()))
            .thenAnswer(
                (Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                    Iterable<InboxPack> inboxPacks = invocation.getArgument(0);
                    Map<SubInfo, WriteResult> resultMap = new HashMap<>();
                    for (InboxPack inboxWrite : inboxPacks) {
                        for (SubInfo subInfo : inboxWrite.inboxes) {
                            resultMap.put(subInfo, WriteResult.error(new RuntimeException("mocked error")));
                        }
                    }
                    return CompletableFuture.completedFuture(resultMap);
                });

        insertMatchRecord("trafficA", "/a/b/c", AT_LEAST_ONCE, MqttBroker, "inbox1", "server1");

        for (int i = 0; i < 10; i++) {
            BatchDistReply reply = dist("trafficA", AT_LEAST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            assertTrue(reply.getResultMap().get("trafficA").getFanoutMap().get("/a/b/c").intValue() > 0);
        }

        ArgumentCaptor<Iterable<InboxPack>> messageListCap = ArgumentCaptor.forClass(Iterable.class);

        verify(writer1, after(100).atMost(10)).write(messageListCap.capture());

        verify(eventCollector, after(100).atMost(10)).report(argThat(e -> e.type() == EventType.DELIVER_ERROR));

        for (InboxPack inboxPack : messageListCap.getValue()) {
            TopicMessagePack msgs = inboxPack.messagePack;
            assertEquals("/a/b/c", msgs.getTopic());
            for (TopicMessagePack.SenderMessagePack senderMsgPack : msgs.getMessageList()) {
                for (Message msg : senderMsgPack.getMessageList()) {
                    assertEquals(copyFromUtf8("Hello"), msg.getPayload());
                }
            }
        }
    }

    @Test
    public void testDistCase9() throws InterruptedException {
        // pub: qos1
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos1)]
        // subBroker: inbox1 -> NO_INBOX
        // expected behavior: pub succeed with no sub

        when(receiverManager.openWriter("server1", MqttBroker)).thenReturn(writer1);
        when(writer1.write(any())).thenAnswer(
            (Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                Iterable<InboxPack> inboxPacks = invocation.getArgument(0);
                Map<SubInfo, WriteResult> resultMap = new HashMap<>();
                for (InboxPack inboxWrite : inboxPacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, WriteResult.NO_INBOX);
                    }
                }
                return CompletableFuture.completedFuture(resultMap);
            });

        when(distClient.clear(anyLong(), anyString(), anyString(), anyInt(), any(ClientInfo.class))).thenReturn(
            CompletableFuture.completedFuture(ClearResult.OK));

        insertMatchRecord("trafficA", "/a/b/c", AT_LEAST_ONCE, MqttBroker, "inbox1", "server1");

        for (int i = 0; i < 10; i++) {
            BatchDistReply reply = dist("trafficA", AT_LEAST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            assertTrue(reply.getResultMap().get("trafficA").getFanoutMap().get("/a/b/c").intValue() > 0);
        }

        ArgumentCaptor<Iterable<InboxPack>> messageListCap = ArgumentCaptor.forClass(Iterable.class);
        verify(writer1, timeout(100).atLeastOnce()).write(messageListCap.capture());

        for (Iterable<InboxPack> packs : messageListCap.getAllValues()) {
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

        verify(distClient, timeout(100).atLeastOnce()).clear(anyLong(), anyString(), anyString(), anyInt(),
            any(ClientInfo.class));
    }

    @SneakyThrows
    @Test
    public void testDistCase10() {
        // pub: qos1
        // topic: "/a/b/c"
        // sub: inbox1 -> [($share/group/a/b/c, qos1)]
        // subBroker: inbox1 -> NO_INBOX
        // expected behavior: pub succeed with no sub

        when(receiverManager.openWriter("server1", MqttBroker)).thenReturn(writer1);
        when(writer1.write(any())).thenAnswer(
            (Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                Iterable<InboxPack> inboxPacks = invocation.getArgument(0);
                Map<SubInfo, WriteResult> resultMap = new HashMap<>();
                for (InboxPack inboxWrite : inboxPacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, WriteResult.NO_INBOX);
                    }
                }
                return CompletableFuture.completedFuture(resultMap);
            });

        when(distClient.clear(anyLong(), anyString(), anyString(), anyInt(), any(ClientInfo.class))).thenReturn(
            CompletableFuture.completedFuture(ClearResult.OK));

        joinMatchGroup("trafficA", "$share/group//a/b/c", AT_LEAST_ONCE, MqttBroker, "inbox1", "server1");

        for (int i = 0; i < 10; i++) {
            BatchDistReply reply = dist("trafficA", AT_LEAST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            assertTrue(reply.getResultMap().get("trafficA").getFanoutMap().get("/a/b/c").intValue() > 0);
        }


        ArgumentCaptor<Iterable<InboxPack>> messageListCap = ArgumentCaptor.forClass(Iterable.class);
        verify(writer1, timeout(100).atLeastOnce()).write(messageListCap.capture());

        for (Iterable<InboxPack> packs : messageListCap.getAllValues()) {
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


        verify(distClient, timeout(100).atLeastOnce()).clear(anyLong(), anyString(), anyString(), anyInt(),
            any(ClientInfo.class));

        verify(eventCollector, timeout(100).atLeastOnce()).report(argThat(e -> e.type() == EventType.DELIVER_NO_INBOX));
    }
}
