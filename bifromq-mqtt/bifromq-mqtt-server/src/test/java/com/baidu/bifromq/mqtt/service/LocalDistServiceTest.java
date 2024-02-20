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

import static com.baidu.bifromq.mqtt.inbox.util.DeliveryGroupKeyUtil.toDelivererKey;
import static com.baidu.bifromq.mqtt.service.LocalDistService.TOPIC_FILTER_BUCKET_NUM;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.client.MatchResult;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.mqtt.MockableTest;
import com.baidu.bifromq.mqtt.session.IMQTTTransientSession;
import com.baidu.bifromq.plugin.subbroker.DeliveryPack;
import com.baidu.bifromq.plugin.subbroker.DeliveryPackage;
import com.baidu.bifromq.plugin.subbroker.DeliveryReply;
import com.baidu.bifromq.plugin.subbroker.DeliveryRequest;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.MatchInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.Test;

@Slf4j
public class LocalDistServiceTest extends MockableTest {
    private final String serverId = "serverId";
    @Mock
    private IDistClient distClient;

    @Test
    public void matchSharedSubTopicFilter() {
        String topicFilter = "$share/group/topicFilter";
        LocalDistService localDistService = new LocalDistService(serverId, distClient);
        for (int i = 0; i < 100; i++) {
            IMQTTTransientSession session = mock(IMQTTTransientSession.class);
            String tenantId = "tenantId" + i;
            String channelId = "channelId" + i;
            ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
            when(session.clientInfo()).thenReturn(clientInfo);
            when(session.channelId()).thenReturn(channelId);
            long reqId = System.nanoTime();
            localDistService.match(reqId, topicFilter, session);
            verify(distClient).match(eq(reqId), eq(tenantId), eq(topicFilter),
                eq(ILocalDistService.globalize(channelId)),
                eq(toDelivererKey(ILocalDistService.globalize(channelId), serverId)), eq(0));
            reset(distClient);
        }
    }

    @Test
    public void unmatchSharedSubTopicFilter() {
        String topicFilter = "$share/group/topicFilter";
        LocalDistService localDistService = new LocalDistService(serverId, distClient);
        for (int i = 0; i < 100; i++) {
            IMQTTTransientSession session = mock(IMQTTTransientSession.class);
            String tenantId = "tenantId" + i;
            String channelId = "channelId" + i;
            ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
            when(session.clientInfo()).thenReturn(clientInfo);
            when(session.channelId()).thenReturn(channelId);
            long reqId = System.nanoTime();
            localDistService.unmatch(reqId, topicFilter, session);
            verify(distClient).unmatch(eq(reqId), eq(tenantId), eq(topicFilter),
                eq(ILocalDistService.globalize(channelId)),
                eq(toDelivererKey(ILocalDistService.globalize(channelId), serverId)), eq(0));
            reset(distClient);
        }
    }

    @Test
    public void matchSameNonSharedTopicFilter() {
        String topicFilter = "topicFilter";
        LocalDistService localDistService = new LocalDistService(serverId, distClient);
        when(distClient.match(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(new CompletableFuture<>());
        String tenantId = "tenantId";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        for (int i = 0; i < 10 * TOPIC_FILTER_BUCKET_NUM; i++) {
            IMQTTTransientSession session = mock(IMQTTTransientSession.class);
            String channelId = "channelId" + i;
            when(session.clientInfo()).thenReturn(clientInfo);
            when(session.channelId()).thenReturn(channelId);
            long reqId = System.nanoTime();
            localDistService.match(reqId, topicFilter, session);
        }
        verify(distClient, atMost(TOPIC_FILTER_BUCKET_NUM)).match(
            anyLong(),
            eq(tenantId),
            eq(topicFilter),
            argThat(receiverId -> !ILocalDistService.isGlobal(receiverId)),
            anyString(),
            eq(0));
    }

    @Test
    public void unmatchSameNonSharedTopicFilter() {
        String topicFilter = "topicFilter";
        LocalDistService localDistService = new LocalDistService(serverId, distClient);
        when(distClient.match(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        String tenantId = "tenantId";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        List<IMQTTTransientSession> sessions = new ArrayList<>();
        for (int i = 0; i < 10 * TOPIC_FILTER_BUCKET_NUM; i++) {
            IMQTTTransientSession session = mock(IMQTTTransientSession.class);
            sessions.add(session);
            String channelId = "channelId" + i;
            when(session.clientInfo()).thenReturn(clientInfo);
            when(session.channelId()).thenReturn(channelId);
            long reqId = System.nanoTime();
            localDistService.match(reqId, topicFilter, session);
        }
        for (IMQTTTransientSession session : sessions) {
            long reqId = System.nanoTime();
            localDistService.unmatch(reqId, topicFilter, session);
        }
        verify(distClient, atMost(TOPIC_FILTER_BUCKET_NUM)).unmatch(
            anyLong(),
            eq(tenantId),
            eq(topicFilter),
            argThat(receiverId -> !ILocalDistService.isGlobal(receiverId)),
            anyString(),
            eq(0));
    }

    @Test
    public void distClientMatchError() {
        String topicFilter = "topicFilter";
        LocalDistService localDistService = new LocalDistService(serverId, distClient);
        CompletableFuture<MatchResult> matchFuture = new CompletableFuture<>();
        when(distClient.match(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(matchFuture);
        String tenantId = "tenantId";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        List<IMQTTTransientSession> sessions = new ArrayList<>();
        List<CompletableFuture<MatchResult>> matchFutures = new ArrayList<>();
        for (int i = 0; i < 10 * TOPIC_FILTER_BUCKET_NUM; i++) {
            IMQTTTransientSession session = mock(IMQTTTransientSession.class);
            sessions.add(session);
            String channelId = "channelId" + i;
            when(session.clientInfo()).thenReturn(clientInfo);
            when(session.channelId()).thenReturn(channelId);
            long reqId = System.nanoTime();
            matchFutures.add(localDistService.match(reqId, topicFilter, session));
        }
        matchFuture.complete(MatchResult.ERROR);
        for (CompletableFuture<MatchResult> future : matchFutures) {
            assertEquals(future.join(), MatchResult.ERROR);
        }
        ArgumentCaptor<String> receiverIdCaptor = ArgumentCaptor.forClass(String.class);
        verify(distClient, atMost(TOPIC_FILTER_BUCKET_NUM)).match(
            anyLong(),
            eq(tenantId),
            eq(topicFilter),
            receiverIdCaptor.capture(),
            anyString(),
            eq(0));
        List<MatchInfo> matchInfos = receiverIdCaptor.getAllValues().stream()
            .map(receiverId -> MatchInfo.newBuilder()
                .setReceiverId(receiverId)
                .setTopicFilter(topicFilter).build())
            .toList();
        DeliveryReply reply = localDistService.dist(DeliveryRequest.newBuilder()
            .putPackage(tenantId, DeliveryPackage.newBuilder()
                .addPack(DeliveryPack.newBuilder()
                    .addAllMatchInfo(matchInfos)
                    .build())
                .build())
            .build()).join();
        assertEquals(matchInfos.size(), reply.getResultCount());
        assertTrue(reply.getResultMap().get(tenantId).getResultList().stream()
            .allMatch(result -> result.getCode() == DeliveryResult.Code.NO_SUB));
        for (IMQTTTransientSession session : sessions) {
            verify(session, never()).publish(any(), anyList());
        }
    }

    @Test
    public void distClientMatchException() {
        String topicFilter = "topicFilter";
        LocalDistService localDistService = new LocalDistService(serverId, distClient);
        CompletableFuture<MatchResult> matchFuture = new CompletableFuture<>();
        when(distClient.match(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(matchFuture);
        String tenantId = "tenantId";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        List<CompletableFuture<MatchResult>> matchFutures = new ArrayList<>();
        for (int i = 0; i < 10 * TOPIC_FILTER_BUCKET_NUM; i++) {
            IMQTTTransientSession session = mock(IMQTTTransientSession.class);
            String channelId = "channelId" + i;
            when(session.clientInfo()).thenReturn(clientInfo);
            when(session.channelId()).thenReturn(channelId);
            long reqId = System.nanoTime();
            matchFutures.add(localDistService.match(reqId, topicFilter, session));
        }
        matchFuture.completeExceptionally(new RuntimeException("match failed"));
        for (CompletableFuture<MatchResult> future : matchFutures) {
            assertEquals(future.join(), MatchResult.ERROR);
        }
    }

    @Test
    public void distClientMatchOK() {
        String topicFilter = "topicFilter";
        LocalDistService localDistService = new LocalDistService(serverId, distClient);
        CompletableFuture<MatchResult> matchFuture = new CompletableFuture<>();
        when(distClient.match(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(matchFuture);
        String tenantId = "tenantId";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        List<IMQTTTransientSession> sessions = new ArrayList<>();
        List<CompletableFuture<MatchResult>> matchFutures = new ArrayList<>();
        for (int i = 0; i < 10 * TOPIC_FILTER_BUCKET_NUM; i++) {
            IMQTTTransientSession session = mock(IMQTTTransientSession.class);
            sessions.add(session);
            String channelId = "channelId" + i;
            when(session.clientInfo()).thenReturn(clientInfo);
            when(session.channelId()).thenReturn(channelId);
            when(session.publish(any(), anyList())).thenReturn(true);
            long reqId = System.nanoTime();
            matchFutures.add(localDistService.match(reqId, topicFilter, session));
        }
        matchFuture.complete(MatchResult.OK);
        for (CompletableFuture<MatchResult> future : matchFutures) {
            assertEquals(future.join(), MatchResult.OK);
        }
        ArgumentCaptor<String> receiverIdCaptor = ArgumentCaptor.forClass(String.class);
        verify(distClient, atMost(TOPIC_FILTER_BUCKET_NUM)).match(
            anyLong(),
            eq(tenantId),
            eq(topicFilter),
            receiverIdCaptor.capture(),
            anyString(),
            eq(0));
        List<MatchInfo> matchInfos = receiverIdCaptor.getAllValues().stream()
            .map(receiverId -> MatchInfo.newBuilder()
                .setReceiverId(receiverId)
                .setTopicFilter(topicFilter).build())
            .toList();
        DeliveryReply reply = localDistService.dist(DeliveryRequest.newBuilder()
            .putPackage(tenantId, DeliveryPackage.newBuilder()
                .addPack(DeliveryPack.newBuilder()
                    .addAllMatchInfo(matchInfos)
                    .build())
                .build())
            .build()).join();
        assertEquals(matchInfos.size(), reply.getResultCount());
        assertTrue(reply.getResultMap().get(tenantId).getResultList().stream()
            .allMatch(result -> result.getCode() == DeliveryResult.Code.OK));
        for (IMQTTTransientSession session : sessions) {
            verify(session, times(1)).publish(any(), anyList());
        }
    }

    @Test
    public void matchSameTopicFilterTwice() {
        String topicFilter = "topicFilter";
        LocalDistService localDistService = new LocalDistService(serverId, distClient);
        when(distClient.match(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        String tenantId = "tenantId";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        IMQTTTransientSession session = mock(IMQTTTransientSession.class);
        String channelId = "channelId";
        when(session.clientInfo()).thenReturn(clientInfo);
        when(session.channelId()).thenReturn(channelId);
        when(session.publish(any(), anyList())).thenReturn(true);
        long reqId = System.nanoTime();
        localDistService.match(reqId, topicFilter, session);
        localDistService.match(reqId, topicFilter, session);

        ArgumentCaptor<String> receiverIdCaptor = ArgumentCaptor.forClass(String.class);
        verify(distClient, only()).match(
            anyLong(),
            eq(tenantId),
            eq(topicFilter),
            receiverIdCaptor.capture(),
            anyString(),
            eq(0));

        List<MatchInfo> matchInfos = receiverIdCaptor.getAllValues().stream()
            .map(receiverId -> MatchInfo.newBuilder()
                .setReceiverId(receiverId)
                .setTopicFilter(topicFilter).build())
            .toList();

        DeliveryReply reply = localDistService.dist(DeliveryRequest.newBuilder()
            .putPackage(tenantId, DeliveryPackage.newBuilder()
                .addPack(DeliveryPack.newBuilder()
                    .addAllMatchInfo(matchInfos)
                    .build())
                .build())
            .build()).join();
        assertEquals(matchInfos.size(), 1);
        assertEquals(matchInfos.size(), reply.getResultCount());
        verify(session, times(1)).publish(any(), anyList());
    }

    @Test
    public void unmatchNotExist() {
        String topicFilter = "topicFilter";
        LocalDistService localDistService = new LocalDistService(serverId, distClient);
        IMQTTTransientSession session = mock(IMQTTTransientSession.class);
        when(session.channelId()).thenReturn("channelId");
        when(session.clientInfo()).thenReturn(ClientInfo.newBuilder().setTenantId("tenantId").build());
        CompletableFuture<UnmatchResult> result = localDistService.unmatch(System.nanoTime(), topicFilter, session);
        assertEquals(result.join(), UnmatchResult.OK);
    }

    @Test
    public void unmatchError() {
        String topicFilter = "topicFilter";
        LocalDistService localDistService = new LocalDistService(serverId, distClient);
        when(distClient.match(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        String tenantId = "tenantId";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        IMQTTTransientSession session = mock(IMQTTTransientSession.class);
        String channelId = "channelId";
        when(session.clientInfo()).thenReturn(clientInfo);
        when(session.channelId()).thenReturn(channelId);
        when(session.publish(any(), anyList())).thenReturn(true);
        long reqId = System.nanoTime();
        localDistService.match(reqId, topicFilter, session);
        ArgumentCaptor<String> receiverIdCaptor = ArgumentCaptor.forClass(String.class);
        verify(distClient, only()).match(
            anyLong(),
            eq(tenantId),
            eq(topicFilter),
            receiverIdCaptor.capture(),
            anyString(),
            eq(0));

        List<MatchInfo> matchInfos = receiverIdCaptor.getAllValues().stream()
            .map(receiverId -> MatchInfo.newBuilder()
                .setReceiverId(receiverId)
                .setTopicFilter(topicFilter).build())
            .toList();

        when(distClient.unmatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(UnmatchResult.ERROR));
        CompletableFuture<UnmatchResult> result = localDistService.unmatch(System.nanoTime(), topicFilter, session);
        assertEquals(result.join(), UnmatchResult.ERROR);

        DeliveryReply reply = localDistService.dist(DeliveryRequest.newBuilder()
            .putPackage(tenantId, DeliveryPackage.newBuilder()
                .addPack(DeliveryPack.newBuilder()
                    .addAllMatchInfo(matchInfos)
                    .build())
                .build())
            .build()).join();
        assertEquals(reply.getResultMap().get(tenantId).getResult(0).getCode(), DeliveryResult.Code.NO_SUB);

        verify(session, never()).publish(any(), anyList());
    }

    @Test
    public void unmatchException() {
        String topicFilter = "topicFilter";
        LocalDistService localDistService = new LocalDistService(serverId, distClient);
        when(distClient.match(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        String tenantId = "tenantId";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        IMQTTTransientSession session = mock(IMQTTTransientSession.class);
        String channelId = "channelId";
        when(session.clientInfo()).thenReturn(clientInfo);
        when(session.channelId()).thenReturn(channelId);
        when(session.publish(any(), anyList())).thenReturn(true);
        long reqId = System.nanoTime();
        localDistService.match(reqId, topicFilter, session);
        ArgumentCaptor<String> receiverIdCaptor = ArgumentCaptor.forClass(String.class);
        verify(distClient, only()).match(
            anyLong(),
            eq(tenantId),
            eq(topicFilter),
            receiverIdCaptor.capture(),
            anyString(),
            eq(0));

        List<MatchInfo> matchInfos = receiverIdCaptor.getAllValues().stream()
            .map(receiverId -> MatchInfo.newBuilder()
                .setReceiverId(receiverId)
                .setTopicFilter(topicFilter).build())
            .toList();


        when(distClient.unmatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("unmatch failed")));
        CompletableFuture<UnmatchResult> result = localDistService.unmatch(System.nanoTime(), topicFilter, session);
        assertEquals(result.join(), UnmatchResult.ERROR);

        DeliveryReply reply = localDistService.dist(DeliveryRequest.newBuilder()
            .putPackage(tenantId, DeliveryPackage.newBuilder()
                .addPack(DeliveryPack.newBuilder()
                    .addAllMatchInfo(matchInfos)
                    .build())
                .build())
            .build()).join();
        assertEquals(reply.getResultMap().get(tenantId).getResult(0).getCode(), DeliveryResult.Code.NO_SUB);
        verify(session, never()).publish(any(), anyList());
    }

    @Test
    public void unmatchOK() {
        String topicFilter = "topicFilter";
        LocalDistService localDistService = new LocalDistService(serverId, distClient);
        when(distClient.match(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        String tenantId = "tenantId";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        IMQTTTransientSession session = mock(IMQTTTransientSession.class);
        String channelId = "channelId";
        when(session.clientInfo()).thenReturn(clientInfo);
        when(session.channelId()).thenReturn(channelId);
        when(session.publish(any(), anyList())).thenReturn(true);
        long reqId = System.nanoTime();
        localDistService.match(reqId, topicFilter, session);
        ArgumentCaptor<String> receiverIdCaptor = ArgumentCaptor.forClass(String.class);
        verify(distClient, only()).match(
            anyLong(),
            eq(tenantId),
            eq(topicFilter),
            receiverIdCaptor.capture(),
            anyString(),
            eq(0));

        List<MatchInfo> matchInfos = receiverIdCaptor.getAllValues().stream()
            .map(receiverId -> MatchInfo.newBuilder()
                .setReceiverId(receiverId)
                .setTopicFilter(topicFilter).build())
            .toList();


        when(distClient.unmatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(UnmatchResult.OK));
        CompletableFuture<UnmatchResult> result = localDistService.unmatch(System.nanoTime(), topicFilter, session);
        assertEquals(result.join(), UnmatchResult.OK);

        DeliveryReply reply = localDistService.dist(DeliveryRequest.newBuilder()
            .putPackage(tenantId, DeliveryPackage.newBuilder()
                .addPack(DeliveryPack.newBuilder()
                    .addAllMatchInfo(matchInfos)
                    .build())
                .build())
            .build()).join();
        assertEquals(reply.getResultMap().get(tenantId).getResult(0).getCode(), DeliveryResult.Code.NO_SUB);
        verify(session, never()).publish(any(), anyList());
    }
}
