/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.plugin.subbroker.CheckReply;
import com.baidu.bifromq.plugin.subbroker.CheckRequest;
import com.baidu.bifromq.plugin.subbroker.ISubBroker;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.type.MatchInfo;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SubscriptionCleanerTest {
    @Mock
    private ISubBrokerManager subBrokerManager;
    @Mock
    private IDistClient distClient;
    @Mock
    private ISubBroker subBroker;
    private SubscriptionCleaner subscriptionCleaner;
    private AutoCloseable openMocks;

    @BeforeMethod
    void setUp() {
        openMocks = openMocks(this);
        subscriptionCleaner = new SubscriptionCleaner(subBrokerManager, distClient);
    }

    @AfterMethod
    void tearDown() throws Exception {
        openMocks.close();
    }

    @Test
    void testSweepNoSub() {
        int subBrokerId = 1;
        String tenantId = "tenant1";
        String topicFilter = "topic1";
        String receiverId = "receiver1";
        String delivererKey = "deliverer1";

        CheckRequest request = CheckRequest.newBuilder()
            .setTenantId(tenantId)
            .setDelivererKey(delivererKey)
            .addMatchInfo(MatchInfo.newBuilder()
                .setTopicFilter(topicFilter)
                .setReceiverId(receiverId)
                .build())
            .build();

        CheckReply checkReply = CheckReply.newBuilder()
            .addCode(CheckReply.Code.NO_SUB)
            .build();

        when(subBrokerManager.get(subBrokerId)).thenReturn(subBroker);
        when(subBroker.check(request)).thenReturn(
            CompletableFuture.completedFuture(checkReply));
        when(distClient.removeTopicMatch(anyLong(), eq(tenantId), eq(topicFilter), eq(receiverId), eq(delivererKey),
            eq(subBrokerId)))
            .thenReturn(CompletableFuture.completedFuture(UnmatchResult.OK));

        subscriptionCleaner.sweep(subBrokerId, request).join();

        verify(subBrokerManager, times(1)).get(subBrokerId);
        verify(subBroker, times(1)).check(request);
        verify(distClient, times(1)).removeTopicMatch(anyLong(), eq(tenantId), eq(topicFilter), eq(receiverId),
            eq(delivererKey), eq(subBrokerId));
    }

    @Test
    void testSweepNoReceiver() {
        int subBrokerId = 1;
        String tenantId = "tenant1";
        String topicFilter = "topic1";
        String receiverId = "receiver1";
        String delivererKey = "deliverer1";

        CheckRequest request = CheckRequest.newBuilder()
            .setTenantId(tenantId)
            .setDelivererKey(delivererKey)
            .addMatchInfo(MatchInfo.newBuilder()
                .setTopicFilter(topicFilter)
                .setReceiverId(receiverId)
                .build())
            .build();

        CheckReply checkReply = CheckReply.newBuilder()
            .addCode(CheckReply.Code.NO_RECEIVER)
            .build();

        when(subBrokerManager.get(subBrokerId)).thenReturn(subBroker);
        when(subBroker.check(request)).thenReturn(
            CompletableFuture.completedFuture(checkReply));
        when(distClient.removeTopicMatch(anyLong(), eq(tenantId), eq(topicFilter), eq(receiverId), eq(delivererKey),
            eq(subBrokerId)))
            .thenReturn(CompletableFuture.completedFuture(UnmatchResult.OK));

        subscriptionCleaner.sweep(subBrokerId, request).join();

        verify(subBrokerManager, times(1)).get(subBrokerId);
        verify(subBroker, times(1)).check(request);
        verify(distClient, times(1)).removeTopicMatch(anyLong(), eq(tenantId), eq(topicFilter), eq(receiverId),
            eq(delivererKey), eq(subBrokerId));
    }

    @Test
    void testSweepNoAction() {
        int subBrokerId = 1;
        String tenantId = "tenant1";
        String topicFilter = "topic1";
        String receiverId = "receiver1";
        String delivererKey = "deliverer1";

        CheckRequest request = CheckRequest.newBuilder()
            .setTenantId(tenantId)
            .setDelivererKey(delivererKey)
            .addMatchInfo(MatchInfo.newBuilder()
                .setTopicFilter(topicFilter)
                .setReceiverId(receiverId)
                .build())
            .build();

        CheckReply checkReply = CheckReply.newBuilder()
            .addCode(CheckReply.Code.OK)
            .build();

        when(subBrokerManager.get(subBrokerId)).thenReturn(subBroker);
        when(subBroker.check(request)).thenReturn(
            CompletableFuture.completedFuture(checkReply));

        subscriptionCleaner.sweep(subBrokerId, request).join();

        verify(subBrokerManager, times(1)).get(subBrokerId);
        verify(subBroker, times(1)).check(request);
        verify(distClient, times(0)).removeTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(),
            anyInt());
    }
}