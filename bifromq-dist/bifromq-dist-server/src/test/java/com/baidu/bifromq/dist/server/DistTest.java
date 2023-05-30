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

package com.baidu.bifromq.dist.server;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.dist.client.DistResult;
import com.baidu.bifromq.dist.client.SubResult;
import com.baidu.bifromq.dist.client.SubResult.Type;
import com.baidu.bifromq.plugin.inboxbroker.WriteResult;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class DistTest extends DistServiceTest {
    @SneakyThrows
    @Test
    public void distWithNoSub() {
        long reqId = System.nanoTime();
        ByteBuffer payload = ByteString.EMPTY.asReadOnlyByteBuffer();
        ClientInfo clientInfo;
        int total = 10;
        CountDownLatch latch = new CountDownLatch(total);
        for (int i = 0; i < total; i++) {
            clientInfo = ClientInfo.newBuilder().setTrafficId("trafficA").setUserId("user" + i).build();
            distClient().dist(reqId, "/sport/tennis" + i, QoS.AT_LEAST_ONCE, payload, Integer.MAX_VALUE, clientInfo)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        log.info("Error", e);
                    }
                    latch.countDown();
                    assertEquals(DistResult.Succeed.type(), v.type());
                });
        }
        latch.await();
    }

    @SneakyThrows
    @Test
    public void distWithSub() {
        long reqId = System.nanoTime();
        ByteBuffer payload = ByteString.EMPTY.asReadOnlyByteBuffer();
        ClientInfo clientInfo;
        int total = 1;
        for (int i = 0; i < total; i++) {
            clientInfo = ClientInfo.newBuilder().setTrafficId("trafficA").setUserId("user" + i).build();
            distClient().sub(reqId, "/sport/tennis" + i, QoS.AT_LEAST_ONCE, "inbox" + i, "server1", 0, clientInfo)
                .join();
        }
        CountDownLatch latch = new CountDownLatch(total);
        for (int i = 0; i < total; i++) {
            clientInfo = ClientInfo.newBuilder().setTrafficId("trafficA").setUserId("user" + i).build();
            distClient().dist(reqId, "/sport/tennis" + i, QoS.AT_LEAST_ONCE, payload, Integer.MAX_VALUE, clientInfo)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        log.info("Error", e);
                    }
                    latch.countDown();
                    assertEquals(DistResult.Succeed.type(), v.type());
                });
        }
        latch.await();
    }

    @SneakyThrows
    @Test
    public void distWithSharedSub() {
        long reqId = System.nanoTime();
        ByteBuffer payload = ByteString.EMPTY.asReadOnlyByteBuffer();
        ClientInfo clientInfo;
        int totalSub = 5;
        int totalMsg = 1;
        for (int i = 0; i < totalSub; i++) {
            clientInfo = ClientInfo.newBuilder().setTrafficId("trafficA").setUserId("user" + i).build();
            SubResult subResult = distClient().sub(reqId, "$share/g1/sport/tennis" + i, QoS.AT_LEAST_ONCE, "inbox" + i,
                "server1", 0, clientInfo).get();
            assertEquals(Type.OK_QoS1, subResult.type());
        }
        CountDownLatch latch = new CountDownLatch(totalMsg);
        for (int i = 0; i < totalMsg; i++) {
            clientInfo = ClientInfo.newBuilder().setTrafficId("trafficA").setUserId("user" + i).build();
            distClient().dist(reqId, "/sport/tennis" + i, QoS.AT_LEAST_ONCE, payload, Integer.MAX_VALUE, clientInfo)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        log.info("Error", e);
                    }
                    latch.countDown();
                    assertEquals(DistResult.Succeed.type(), v.type());
                });
        }
        latch.await();
    }

    @SneakyThrows
    @Test
    public void distWithFanOutSub() {
        List<Map<TopicMessagePack, List<SubInfo>>> capturedArguments = new ArrayList<>();
        when(inboxWriter.write(anyMap()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                // the argument object will be reused, so make a clone
                Map<TopicMessagePack, List<SubInfo>> msgPack = new HashMap<>(invocation.getArgument(0));
                capturedArguments.add(msgPack);
                return CompletableFuture.completedFuture(msgPack.values().stream().flatMap(l -> l.stream())
                    .collect(Collectors.toMap(s -> s, s -> WriteResult.OK)));
            });

        long reqId = System.nanoTime();
        ByteBuffer payload = ByteString.EMPTY.asReadOnlyByteBuffer();
        ClientInfo clientInfo;
        int totalInbox = 100;
        for (int i = 0; i < totalInbox; i++) {
            clientInfo = ClientInfo.newBuilder().setTrafficId("trafficA").setUserId("subUser" + i).build();
            distClient().sub(reqId, "/sport/tennis", QoS.AT_LEAST_ONCE, "inbox" + i, "server1", 0, clientInfo)
                .join();
        }

        int totalPub = 2;
        ClientInfo pubClient = ClientInfo.newBuilder().setTrafficId("trafficA").setUserId("pubUser").build();
        for (int i = 0; i < totalPub; i++) {
            distClient().dist(reqId, "/sport/tennis", QoS.AT_LEAST_ONCE, payload, Integer.MAX_VALUE, pubClient).join();
        }


        Set<SubInfo> subInfos = new HashSet<>();
        int msgCount = 0;
        for (Map<TopicMessagePack, List<SubInfo>> writeReq : capturedArguments) {
            for (TopicMessagePack msgs : writeReq.keySet()) {
                subInfos.addAll(writeReq.get(msgs));
                msgCount += msgs.getMessageCount() * writeReq.get(msgs).size();
            }
        }
        assertEquals(totalInbox, subInfos.size());
        assertEquals(totalInbox * totalPub, msgCount);
    }
}
