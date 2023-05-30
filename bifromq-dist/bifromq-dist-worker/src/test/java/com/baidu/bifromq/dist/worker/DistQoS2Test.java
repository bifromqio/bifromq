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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.dist.rpc.proto.BatchDistReply;
import com.baidu.bifromq.plugin.inboxbroker.WriteResult;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class DistQoS2Test extends DistWorkerTest {
    @Test
    public void succeedWithNoSub() {
        String trafficId = "trafficA";
        String topic = "/a/b/c";
        ByteString payload = copyFromUtf8("hello");

        BatchDistReply reply = dist(trafficId, EXACTLY_ONCE, topic, payload, "orderKey1");
        assertTrue(reply.getResultMap().get(trafficId).getFanoutMap().getOrDefault(topic, 0).intValue() == 0);
    }

    @Test
    public void distQoS2ToVariousSubQoS() {
        // pub: qos2
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0),(/#, qos1)], inbox2 -> [(/#,qos2)]
        // expected behavior: inbox1 gets 2 messages, inbox2 get 1 message
        when(receiverManager.openWriter("server1", MqttBroker))
            .thenReturn(writer1);
        when(receiverManager.openWriter("server2", MqttBroker))
            .thenReturn(writer2);
        when(writer1.write(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                Map<TopicMessagePack, List<SubInfo>> msgPack = invocation.getArgument(0);
                return CompletableFuture.completedFuture(msgPack.values().stream().flatMap(l -> l.stream())
                    .collect(Collectors.toMap(s -> s, s -> WriteResult.OK)));
            });
        when(writer2.write(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                Map<TopicMessagePack, List<SubInfo>> msgPack = invocation.getArgument(0);
                return CompletableFuture.completedFuture(msgPack.values().stream().flatMap(l -> l.stream())
                    .collect(Collectors.toMap(s -> s, s -> WriteResult.OK)));
            });

        insertMatchRecord("trafficA", "/a/b/c", AT_MOST_ONCE,
            MqttBroker, "inbox1", "server1");
        insertMatchRecord("trafficA", "/#", AT_LEAST_ONCE,
            MqttBroker, "inbox1", "server1");
        insertMatchRecord("trafficA", "/#", EXACTLY_ONCE,
            MqttBroker, "inbox2", "server2");
        BatchDistReply reply =
            dist("trafficA", EXACTLY_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        assertTrue(reply.getResultMap().get("trafficA").getFanoutMap().get("/a/b/c").intValue() > 0);


        ArgumentCaptor<Map<TopicMessagePack, List<SubInfo>>> msgCap = ArgumentCaptor.forClass(Map.class);

        verify(writer1, atLeastOnce()).write(msgCap.capture());
        for (TopicMessagePack msgs : msgCap.getValue().keySet()) {
            List<SubInfo> subInfos = msgCap.getValue().get(msgs);
            assertEquals("/a/b/c", msgs.getTopic());
            for (TopicMessagePack.SenderMessagePack senderMsgPack : msgs.getMessageList()) {
                for (Message msg : senderMsgPack.getMessageList()) {
                    for (SubInfo subInfo : subInfos) {
                        assertEquals(copyFromUtf8("Hello"), msg.getPayload());
                    }
                }
            }
        }

//        // '/#' must come first
//        List<TopicMessage> inbox1Msgs = msgCap.getValue().get("trafficA").get("inbox1");
//        assertEquals(AT_LEAST_ONCE, inbox1Msgs.get(0).getSubQoS());
//        assertEquals(AT_MOST_ONCE, inbox1Msgs.get(1).getSubQoS());

        msgCap = ArgumentCaptor.forClass(Map.class);
        verify(writer2, atLeastOnce()).write(msgCap.capture());

        assertEquals(1, msgCap.getAllValues().size());
        for (TopicMessagePack inbox2Msgs : msgCap.getValue().keySet()) {
            assertEquals("/a/b/c", inbox2Msgs.getTopic());
            TopicMessagePack.SenderMessagePack senderMsgPack = inbox2Msgs.getMessageList().iterator().next();
            Message msg = senderMsgPack.getMessage(0);
            assertEquals(EXACTLY_ONCE, msg.getPubQoS());
            assertEquals(copyFromUtf8("Hello"), msg.getPayload());
        }
    }

    @Test
    public void distQoS2ToErrorInbox() {
        // pub: qos2
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0)]
        // subBroker: inbox1 -> ERROR
        // expected behavior: pub succeed

        when(receiverManager.openWriter("server1", MqttBroker))
            .thenReturn(writer1);
        when(writer1.write(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                Map<TopicMessagePack, List<SubInfo>> msgPack = invocation.getArgument(0);
                return CompletableFuture.completedFuture(msgPack.values().stream().flatMap(l -> l.stream())
                    .collect(Collectors.toMap(s -> s, s -> ThreadLocalRandom.current().nextDouble() <= 0.5 ?
                        WriteResult.error(new RuntimeException()) : WriteResult.OK)));
            });
        insertMatchRecord("trafficA", "/a/b/c", AT_MOST_ONCE,
            MqttBroker, "inbox1", "server1");

        for (int i = 0; i < 10; i++) {
            BatchDistReply reply =
                dist("trafficA", EXACTLY_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            assertTrue(reply.getResultMap().get("trafficA").getFanoutMap().get("/a/b/c").intValue() > 0);
        }

        ArgumentCaptor<Map<TopicMessagePack, List<SubInfo>>> messageListCap =
            ArgumentCaptor.forClass(Map.class);

        verify(writer1, atLeastOnce()).write(messageListCap.capture());
        verify(writer1, atMost(10)).write(messageListCap.capture());
        for (TopicMessagePack msgs : messageListCap.getValue().keySet()) {
            assertEquals("/a/b/c", msgs.getTopic());
            for (TopicMessagePack.SenderMessagePack senderMsgPack : msgs.getMessageList()) {
                for (Message msg : senderMsgPack.getMessageList()) {
                    assertEquals(copyFromUtf8("Hello"), msg.getPayload());
                }
            }
        }
    }
}
