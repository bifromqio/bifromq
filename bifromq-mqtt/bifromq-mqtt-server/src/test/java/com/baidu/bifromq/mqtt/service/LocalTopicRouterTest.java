/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.mqtt.service;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.client.MatchResult;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.mqtt.MockableTest;
import com.baidu.bifromq.sysprops.props.DeliverersPerMqttServer;
import com.baidu.bifromq.type.MatchInfo;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LocalTopicRouterTest extends MockableTest {
    static final int TOPIC_FILTER_BUCKET_NUM = DeliverersPerMqttServer.INSTANCE.get();

    private final String serverId = "serverId";

    @Mock
    private IDistClient distClient;
    private LocalTopicRouter router;

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) {
        super.setup(method);
        router = new LocalTopicRouter(serverId, distClient);
    }

    @Test
    public void addRoutes() {
        String tenantId = "tenantId";
        String topicFilter = "topicFilter";
        when(distClient.addTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(new CompletableFuture<>());
        for (int i = 0; i < 10 * TOPIC_FILTER_BUCKET_NUM; i++) {
            String channelId = "channelId" + i;
            long reqId = System.nanoTime();
            router.addTopicRoute(reqId, tenantId, topicFilter, channelId);
        }
        verify(distClient, atMost(TOPIC_FILTER_BUCKET_NUM)).addTopicMatch(
            anyLong(),
            eq(tenantId),
            eq(topicFilter),
            argThat(receiverId -> !ILocalDistService.isGlobal(receiverId)),
            anyString(),
            eq(0));
    }

    @Test
    public void removeRoutes() {
        String topicFilter = "topicFilter";
        when(distClient.addTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(new CompletableFuture<>());
        String tenantId = "tenantId";
        List<String> sessions = new ArrayList<>();
        for (int i = 0; i < 10 * TOPIC_FILTER_BUCKET_NUM; i++) {
            String channelId = "channelId" + i;
            sessions.add(channelId);
            long reqId = System.nanoTime();
            router.addTopicRoute(reqId, tenantId, topicFilter, channelId);
        }
        for (String channelId : sessions) {
            long reqId = System.nanoTime();
            router.removeTopicRoute(reqId, tenantId, topicFilter, channelId);
        }
        verify(distClient, atMost(TOPIC_FILTER_BUCKET_NUM)).removeTopicMatch(
            anyLong(),
            eq(tenantId),
            eq(topicFilter),
            argThat(receiverId -> !ILocalDistService.isGlobal(receiverId)),
            anyString(),
            eq(0));
    }

    @Test
    public void addSameRoute() {
        String tenantId = "tenantId";
        String topicFilter = "topicFilter";
        String channelId = "channelId";
        when(distClient.addTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(new CompletableFuture<>());

        router.addTopicRoute(System.nanoTime(), tenantId, topicFilter, channelId);
        router.addTopicRoute(System.nanoTime(), tenantId, topicFilter, channelId);

        verify(distClient).addTopicMatch(
            anyLong(),
            eq(tenantId),
            eq(topicFilter),
            argThat(receiverId -> !ILocalDistService.isGlobal(receiverId)),
            anyString(),
            eq(0));
    }

    @Test
    public void distClientMatchError() {
        String tenantId = "tenantId";
        String topicFilter = "topicFilter";
        CompletableFuture<MatchResult> matchFuture = new CompletableFuture<>();
        when(distClient.addTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(matchFuture);
        List<CompletableFuture<MatchResult>> matchFutures = new ArrayList<>();
        for (int i = 0; i < 10 * TOPIC_FILTER_BUCKET_NUM; i++) {
            String channelId = "channelId" + i;
            matchFutures.add(router.addTopicRoute(System.nanoTime(), tenantId, topicFilter, channelId));
        }
        matchFuture.complete(MatchResult.ERROR);
        for (CompletableFuture<MatchResult> future : matchFutures) {
            assertEquals(future.join(), MatchResult.ERROR);
        }
    }

    @Test
    public void distClientMatchException() {
        String tenantId = "tenantId";
        String topicFilter = "topicFilter";
        CompletableFuture<MatchResult> matchFuture = new CompletableFuture<>();
        when(distClient.addTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(matchFuture);
        List<CompletableFuture<MatchResult>> matchFutures = new ArrayList<>();
        for (int i = 0; i < 10 * TOPIC_FILTER_BUCKET_NUM; i++) {
            String channelId = "channelId" + i;
            matchFutures.add(router.addTopicRoute(System.nanoTime(), tenantId, topicFilter, channelId));
        }
        matchFuture.completeExceptionally(new RuntimeException("match failed"));
        for (CompletableFuture<MatchResult> future : matchFutures) {
            assertEquals(future.join(), MatchResult.ERROR);
        }
    }

    @Test
    public void distClientMatchOK() {
        String tenantId = "tenantId";
        String topicFilter = "topicFilter";
        CompletableFuture<MatchResult> matchFuture = new CompletableFuture<>();
        when(distClient.addTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(matchFuture);
        List<CompletableFuture<MatchResult>> matchFutures = new ArrayList<>();
        for (int i = 0; i < 10 * TOPIC_FILTER_BUCKET_NUM; i++) {
            String channelId = "channelId" + i;
            matchFutures.add(router.addTopicRoute(System.nanoTime(), tenantId, topicFilter, channelId));
        }
        matchFuture.complete(MatchResult.OK);
        for (CompletableFuture<MatchResult> future : matchFutures) {
            assertEquals(future.join(), MatchResult.OK);
        }
    }

    @Test
    public void removeNotExistRoute() {
        String tenantId = "tenantId";
        String topicFilter = "topicFilter";
        String channelId = "channelId";
        UnmatchResult result = router.removeTopicRoute(System.nanoTime(), tenantId, topicFilter, channelId).join();
        assertEquals(result, UnmatchResult.OK);
    }

    @Test
    public void distClientUnmatchError() {
        String topicFilter = "topicFilter";
        String tenantId = "tenantId";
        String channelId = "channelId";
        ArgumentCaptor<String> receiverIdCaptor = ArgumentCaptor.forClass(String.class);
        when(distClient.addTopicMatch(anyLong(), anyString(), anyString(), receiverIdCaptor.capture(), anyString(),
            anyInt()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        router.addTopicRoute(System.nanoTime(), tenantId, topicFilter, channelId);
        String receiverId = receiverIdCaptor.getValue();

        when(distClient.removeTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(UnmatchResult.ERROR));
        CompletableFuture<UnmatchResult> result =
            router.removeTopicRoute(System.nanoTime(), tenantId, topicFilter, channelId);
        assertEquals(result.join(), UnmatchResult.ERROR);

        Optional<CompletableFuture<? extends ILocalTopicRouter.ILocalRoutes>> localRoutes =
            router.getTopicRoutes(tenantId, MatchInfo.newBuilder()
                .setReceiverId(receiverId)
                .setTopicFilter(topicFilter).build());
        assertFalse(localRoutes.isPresent());
    }

    @Test
    public void distClientUnmatchException() {
        String topicFilter = "topicFilter";
        String tenantId = "tenantId";
        String channelId = "channelId";
        ArgumentCaptor<String> receiverIdCaptor = ArgumentCaptor.forClass(String.class);
        when(distClient.addTopicMatch(anyLong(), anyString(), anyString(), receiverIdCaptor.capture(), anyString(),
            anyInt()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        router.addTopicRoute(System.nanoTime(), tenantId, topicFilter, channelId);
        String receiverId = receiverIdCaptor.getValue();
        when(distClient.removeTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("unmatch failed")));

        CompletableFuture<UnmatchResult> result =
            router.removeTopicRoute(System.nanoTime(), tenantId, topicFilter, channelId);
        assertEquals(result.join(), UnmatchResult.ERROR);
        Optional<CompletableFuture<? extends ILocalTopicRouter.ILocalRoutes>> localRoutes =
            router.getTopicRoutes(tenantId, MatchInfo.newBuilder()
                .setReceiverId(receiverId)
                .setTopicFilter(topicFilter).build());
        assertFalse(localRoutes.isPresent());
    }

    @Test
    public void distClientUnmatchOK() {
        String topicFilter = "topicFilter";
        String tenantId = "tenantId";
        String channelId = "channelId";
        ArgumentCaptor<String> receiverIdCaptor = ArgumentCaptor.forClass(String.class);
        when(distClient.addTopicMatch(anyLong(), anyString(), anyString(), receiverIdCaptor.capture(), anyString(),
            anyInt()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        router.addTopicRoute(System.nanoTime(), tenantId, topicFilter, channelId);
        String receiverId = receiverIdCaptor.getValue();
        when(distClient.removeTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(UnmatchResult.OK));

        CompletableFuture<UnmatchResult> result =
            router.removeTopicRoute(System.nanoTime(), tenantId, topicFilter, channelId);
        assertEquals(result.join(), UnmatchResult.OK);
        Optional<CompletableFuture<? extends ILocalTopicRouter.ILocalRoutes>> localRoutes =
            router.getTopicRoutes(tenantId, MatchInfo.newBuilder()
                .setReceiverId(receiverId)
                .setTopicFilter(topicFilter).build());
        assertFalse(localRoutes.isPresent());
    }
}
