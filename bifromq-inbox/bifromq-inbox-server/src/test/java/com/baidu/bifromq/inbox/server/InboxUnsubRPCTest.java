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

package com.baidu.bifromq.inbox.server;

import static com.baidu.bifromq.inbox.util.InboxServiceUtil.getDelivererKey;
import static com.baidu.bifromq.inbox.util.InboxServiceUtil.receiverId;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.dist.client.MatchResult;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.inbox.rpc.proto.CreateRequest;
import com.baidu.bifromq.inbox.rpc.proto.SubReply;
import com.baidu.bifromq.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.inbox.rpc.proto.UnsubReply;
import com.baidu.bifromq.inbox.rpc.proto.UnsubRequest;
import com.baidu.bifromq.inbox.storage.proto.LWT;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.plugin.subbroker.CheckReply;
import com.baidu.bifromq.plugin.subbroker.CheckRequest;
import com.baidu.bifromq.retain.rpc.proto.MatchReply;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.MatchInfo;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.Test;

public class InboxUnsubRPCTest extends InboxServiceTest {
    @Test(groups = "integration")
    public void unsubNoInbox() {
        clearInvocations(distClient);
        long now = System.nanoTime();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();
        String topicFilter = "/a/b/c";

        UnsubReply unsubReply = inboxClient.unsub(UnsubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(1)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build()).join();
        assertEquals(unsubReply.getReqId(), reqId);
        assertEquals(unsubReply.getCode(), UnsubReply.Code.NO_INBOX);
        verify(distClient, never())
            .removeTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong());
    }

    @Test(groups = "integration")
    public void unsubConflict() {
        clearInvocations(distClient);
        long now = System.nanoTime();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("LastWill").setDelaySeconds(5).build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();

        inboxClient.create(CreateRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(5)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setLwt(lwt)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();

        String topicFilter = "/a/b/c";
        UnsubReply unsubReply = inboxClient.unsub(UnsubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(1)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build()).join();
        assertEquals(unsubReply.getReqId(), reqId);
        assertEquals(unsubReply.getCode(), UnsubReply.Code.CONFLICT);
        verify(distClient, never())
            .removeTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong());
    }

    @Test(groups = "integration")
    public void unsubOK() {
        clearInvocations(distClient);
        long now = HLC.INST.getPhysical();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("LastWill").setDelaySeconds(5).build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();

        inboxClient.create(CreateRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(5)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setLwt(lwt)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();

        when(distClient
            .addTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        when(distClient
            .removeTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(UnmatchResult.OK));
        when(retainClient.match(any())).thenReturn(CompletableFuture.completedFuture(MatchReply.newBuilder()
            .setResult(MatchReply.Result.OK).build()));

        String topicFilter = "/a/b/c";
        SubReply subReply2 = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build()).join();
        assertEquals(subReply2.getReqId(), reqId);
        assertEquals(subReply2.getCode(), SubReply.Code.OK);

        UnsubReply unsubReply2 = inboxClient.unsub(UnsubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build()).join();
        assertEquals(unsubReply2.getReqId(), reqId);
        assertEquals(unsubReply2.getCode(), UnsubReply.Code.OK);

        CheckReply checkReply = inboxClient.check(CheckRequest.newBuilder()
            .setTenantId(tenantId)
            .setDelivererKey(getDelivererKey(inboxId, receiverId(inboxId, incarnation)))
            .addMatchInfo(MatchInfo.newBuilder()
                .setReceiverId(receiverId(inboxId, incarnation))
                .setTopicFilter(topicFilter)
                .build())
            .build()).join();
        assertEquals(checkReply.getCode(0), CheckReply.Code.NO_SUB);
        verify(distClient, times(1))
            .removeTopicMatch(anyLong(), eq(tenantId), eq(topicFilter), anyString(), anyString(), anyInt(), anyLong());
    }

    @Test(groups = "integration")
    public void unsubNoSub() {
        clearInvocations(distClient);
        long now = System.nanoTime();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("LastWill").setDelaySeconds(5).build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();

        inboxClient.create(CreateRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(5)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setLwt(lwt)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();

        String topicFilter = "/a/b/c";
        UnsubReply unsubReply = inboxClient.unsub(UnsubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build()).join();
        assertEquals(unsubReply.getReqId(), reqId);
        assertEquals(unsubReply.getCode(), UnsubReply.Code.NO_SUB);
        verify(distClient, never())
            .removeTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong());
    }

    @Test(groups = "integration")
    public void unsubUnderLimit() {
        clearInvocations(distClient);
        long now = System.nanoTime();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("LastWill").setDelaySeconds(5).build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();

        inboxClient.create(CreateRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(5)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setLwt(lwt)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();

        when(settingProvider.provide(Setting.MaxTopicFiltersPerInbox, tenantId)).thenReturn(1);
        when(distClient
            .addTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        when(distClient
            .removeTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(UnmatchResult.OK));
        when(retainClient.match(any())).thenReturn(CompletableFuture.completedFuture(MatchReply.newBuilder()
            .setResult(MatchReply.Result.OK).build()));
        SubReply subReply = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter("/a/b/c")
            .setNow(now)
            .build()).join();
        assertEquals(subReply.getReqId(), reqId);
        assertEquals(subReply.getCode(), SubReply.Code.OK);

        subReply = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter("/a/b")
            .setNow(now)
            .build()).join();
        assertEquals(subReply.getReqId(), reqId);
        assertEquals(subReply.getCode(), SubReply.Code.EXCEED_LIMIT);

        UnsubReply unsubReply = inboxClient.unsub(UnsubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter("/a/b/c")
            .setNow(now)
            .build()).join();
        assertEquals(unsubReply.getReqId(), reqId);
        assertEquals(unsubReply.getCode(), UnsubReply.Code.OK);

        subReply = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter("/a/b")
            .setNow(now)
            .build()).join();
        assertEquals(subReply.getReqId(), reqId);
        assertEquals(subReply.getCode(), SubReply.Code.OK);
    }

    @Test(groups = "integration")
    public void unsubWhenDistUnmatchError() {
        clearInvocations(distClient);
        long now = System.nanoTime();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("LastWill").setDelaySeconds(5).build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();

        inboxClient.create(CreateRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(5)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setLwt(lwt)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        when(distClient
            .addTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        when(retainClient.match(any())).thenReturn(CompletableFuture.completedFuture(MatchReply.newBuilder()
            .setResult(MatchReply.Result.OK).build()));

        String topicFilter = "/a/b/c";
        SubReply subReply2 = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build()).join();
        assertEquals(subReply2.getReqId(), reqId);
        assertEquals(subReply2.getCode(), SubReply.Code.OK);

        when(distClient
            .removeTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(UnmatchResult.ERROR));
        UnsubReply unsubReply2 = inboxClient.unsub(UnsubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build()).join();
        assertEquals(unsubReply2.getReqId(), reqId);
        assertEquals(unsubReply2.getCode(), UnsubReply.Code.ERROR);
    }
}
