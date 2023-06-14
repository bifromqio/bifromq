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
import static org.mockito.Mockito.when;

import com.baidu.bifromq.dist.rpc.proto.BatchDistReply;
import com.baidu.bifromq.plugin.inboxbroker.WriteResult;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.MQTT3ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
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
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class BatchDistTest extends DistWorkerTest {
    @Test
    public void batchDistWithNoSub() {
        String trafficId = "trafficA";
        String topic = "/a/b/c";
        ByteString payload = copyFromUtf8("hello");

        BatchDistReply reply = dist(trafficId,
            List.of(TopicMessagePack.newBuilder()
                    .setTopic("a")
                    .addMessage(toMsg(trafficId, AT_MOST_ONCE, payload))
                    .build(),
                TopicMessagePack.newBuilder()
                    .setTopic("a/")
                    .addMessage(toMsg(trafficId, AT_MOST_ONCE, payload))
                    .build(),
                TopicMessagePack.newBuilder()
                    .setTopic("a/b")
                    .addMessage(toMsg(trafficId, AT_MOST_ONCE, payload))
                    .build()), "orderKey1");
        assertTrue(reply.getResultMap().get(trafficId).getFanoutMap().getOrDefault(topic, 0).intValue() == 0);
    }

    @Test
    public void batchDist() {
        when(receiverManager.openWriter("batch1", MqttBroker))
            .thenReturn(writer1);
        when(receiverManager.openWriter("batch2", InboxService))
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

        insertMatchRecord("trafficA", "/a/1", AT_MOST_ONCE,
            MqttBroker, "inbox1", "batch1");
        insertMatchRecord("trafficA", "/a/2", AT_MOST_ONCE,
            MqttBroker, "inbox1", "batch1");
        insertMatchRecord("trafficA", "/a/2", AT_MOST_ONCE,
            MqttBroker, "inbox3", "batch1");
        insertMatchRecord("trafficA", "/a/3", AT_LEAST_ONCE,
            InboxService, "inbox2", "batch2");
        insertMatchRecord("trafficA", "/a/4", AT_LEAST_ONCE,
            InboxService, "inbox2", "batch2");

        BatchDistReply reply = dist("trafficA",
            List.of(
                TopicMessagePack.newBuilder()
                    .setTopic("/a/1")
                    .addMessage(toMsg("trafficA", AT_MOST_ONCE, copyFromUtf8("Hello")))
                    .build(),
                TopicMessagePack.newBuilder()
                    .setTopic("/a/2")
                    .addMessage(toMsg("trafficA", AT_MOST_ONCE, copyFromUtf8("Hello")))
                    .build(),
                TopicMessagePack.newBuilder()
                    .setTopic("/a/3")
                    .addMessage(toMsg("trafficA", AT_MOST_ONCE, copyFromUtf8("Hello")))
                    .build(),
                TopicMessagePack.newBuilder()
                    .setTopic("/a/4")
                    .addMessage(toMsg("trafficA", AT_MOST_ONCE, copyFromUtf8("Hello")))
                    .build()), "orderKey1");

        assertEquals(1, reply.getResultMap().get("trafficA").getFanoutMap().get("/a/1").intValue());
        assertEquals(2, reply.getResultMap().get("trafficA").getFanoutMap().get("/a/2").intValue());
        assertEquals(1, reply.getResultMap().get("trafficA").getFanoutMap().get("/a/3").intValue());
        assertEquals(1, reply.getResultMap().get("trafficA").getFanoutMap().get("/a/4").intValue());
    }

    private TopicMessagePack.SenderMessagePack toMsg(String trafficId, QoS qos, ByteString payload) {
        return TopicMessagePack.SenderMessagePack.newBuilder()
            .setSender(ClientInfo.newBuilder()
                .setTrafficId(trafficId)
                .setUserId("testUser")
                .setMqtt3ClientInfo(MQTT3ClientInfo.newBuilder()
                    .setClientId("testClientId")
                    .setIp("127.0.0.1")
                    .setPort(8080)
                    .build())
                .build())
            .addMessage(Message.newBuilder()
                .setMessageId(ThreadLocalRandom.current().nextInt())
                .setPubQoS(qos)
                .setPayload(payload)
                .setTimestamp(System.currentTimeMillis())
                .setExpireTimestamp(Long.MAX_VALUE)
                .build())
            .build();
    }
}
