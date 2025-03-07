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

package com.baidu.bifromq.deliverer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.plugin.subbroker.DeliveryReply;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.plugin.subbroker.DeliveryResults;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.baidu.bifromq.plugin.subbroker.ISubBroker;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.type.MatchInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeliverySchedulerTest {
    private final String tenantId = "tenant";
    @Mock
    private IDistClient distClient;
    @Mock
    private ISubBrokerManager subBrokerManager;
    @Mock
    private ISubBroker subBroker;
    @Mock
    private IDeliverer groupWriter;
    private IMessageDeliverer testDeliverer;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        when(subBrokerManager.get(0)).thenReturn(subBroker);
        when(subBroker.open(anyString())).thenReturn(groupWriter);
        when(subBroker.id()).thenReturn(0);
        testDeliverer = new MessageDeliverer(subBrokerManager, distClient);
    }

    @SneakyThrows
    @AfterMethod
    public void teardown() {
        closeable.close();
    }

    @Test
    public void writeSucceed() {
        MatchInfo matchInfo = MatchInfo.newBuilder().build();
        DeliveryCall request =
            new DeliveryCall(tenantId, matchInfo, 0, "group1", TopicMessagePack.newBuilder().build());

        when(groupWriter.deliver(any())).thenReturn(
            CompletableFuture.completedFuture(DeliveryReply.newBuilder()
                .putResult(tenantId, DeliveryResults.newBuilder()
                    .addResult(DeliveryResult.newBuilder()
                        .setMatchInfo(matchInfo)
                        .setCode(DeliveryResult.Code.OK)
                        .build())
                    .build())
                .build()));
        DeliveryResult.Code result = testDeliverer.schedule(request).join();
        assertEquals(result, DeliveryResult.Code.OK);
    }

    @Test
    public void writeIncompleteResult() {
        MatchInfo matchInfo = MatchInfo.newBuilder().build();
        DeliveryCall request =
            new DeliveryCall(tenantId, matchInfo, 0, "group1", TopicMessagePack.newBuilder().build());
        when(groupWriter.deliver(any())).thenReturn(
            CompletableFuture.completedFuture(DeliveryReply.newBuilder().build()));
        DeliveryResult.Code result = testDeliverer.schedule(request).join();
        assertEquals(result, DeliveryResult.Code.OK);
    }

    @Test
    public void writeNoSub() {
        MatchInfo matchInfo = MatchInfo.newBuilder()
            .setTopicFilter("topic")
            .setReceiverId("receiverInfo")
            .setIncarnation(1)
            .build();
        DeliveryCall request =
            new DeliveryCall(tenantId, matchInfo, 0, "group1", TopicMessagePack.newBuilder().build());
        when(groupWriter.deliver(any())).thenReturn(
            CompletableFuture.completedFuture(DeliveryReply.newBuilder()
                .putResult(tenantId, DeliveryResults.newBuilder()
                    .addResult(DeliveryResult.newBuilder()
                        .setMatchInfo(matchInfo)
                        .setCode(DeliveryResult.Code.NO_SUB)
                        .build())
                    .build())
                .build()));
        DeliveryResult.Code result = testDeliverer.schedule(request).join();
        assertEquals(result, DeliveryResult.Code.NO_SUB);
        verify(distClient).removeTopicMatch(anyLong(),
            eq(tenantId), eq(matchInfo.getTopicFilter()), eq(matchInfo.getReceiverId()),
            eq("group1"), eq(0), eq(matchInfo.getIncarnation()));
    }

    @Test
    public void writeNoReceiver() {
        MatchInfo matchInfo = MatchInfo.newBuilder()
            .setTopicFilter("topic")
            .setReceiverId("receiverInfo")
            .setIncarnation(1)
            .build();
        DeliveryCall request =
            new DeliveryCall(tenantId, matchInfo, 0, "group1", TopicMessagePack.newBuilder().build());
        when(groupWriter.deliver(any())).thenReturn(
            CompletableFuture.completedFuture(DeliveryReply.newBuilder()
                .putResult(tenantId, DeliveryResults.newBuilder()
                    .addResult(DeliveryResult.newBuilder()
                        .setMatchInfo(matchInfo)
                        .setCode(DeliveryResult.Code.NO_RECEIVER)
                        .build())
                    .build())
                .build()));
        DeliveryResult.Code result = testDeliverer.schedule(request).join();
        assertEquals(result, DeliveryResult.Code.NO_RECEIVER);
        verify(distClient).removeTopicMatch(anyLong(),
            eq(tenantId), eq(matchInfo.getTopicFilter()), eq(matchInfo.getReceiverId()),
            eq("group1"), eq(0), eq(matchInfo.getIncarnation()));
    }


    @Test(expectedExceptions = RuntimeException.class)
    public void writeFail() {
        MatchInfo matchInfo = MatchInfo.newBuilder().build();
        DeliveryCall request =
            new DeliveryCall(tenantId, matchInfo, 0, "group1", TopicMessagePack.newBuilder().build());
        when(groupWriter.deliver(any())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Mock Exception")));
        testDeliverer.schedule(request).join();
        fail();
    }
}
