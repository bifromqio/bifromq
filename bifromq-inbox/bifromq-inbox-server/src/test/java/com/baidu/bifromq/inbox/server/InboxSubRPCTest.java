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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.dist.client.MatchResult;
import com.baidu.bifromq.inbox.rpc.proto.CreateRequest;
import com.baidu.bifromq.inbox.rpc.proto.SubReply;
import com.baidu.bifromq.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.inbox.storage.proto.LWT;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterOption;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.plugin.subbroker.CheckReply;
import com.baidu.bifromq.plugin.subbroker.CheckRequest;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.MatchInfo;
import com.baidu.bifromq.type.QoS;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.Test;

public class InboxSubRPCTest extends InboxServiceTest {
    @Test(groups = "integration")
    public void subNoInbox() {
        clearInvocations(distClient);
        long now = System.nanoTime();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();
        String topicFilter = "/a/b/c";

        SubReply subReply2 = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(1)
            .setTopicFilter(topicFilter)
            .setOption(TopicFilterOption.newBuilder()
                .setIncarnation(1L)
                .build())
            .setNow(now)
            .build()).join();
        assertEquals(subReply2.getReqId(), reqId);
        assertEquals(subReply2.getCode(), SubReply.Code.NO_INBOX);

        CheckReply checkReply = inboxClient.check(CheckRequest.newBuilder()
            .setTenantId(tenantId)
            .setDelivererKey(getDelivererKey(inboxId, receiverId(inboxId, incarnation)))
            .addMatchInfo(MatchInfo.newBuilder()
                .setReceiverId(receiverId(inboxId, incarnation))
                .setTopicFilter(topicFilter)
                .build())
            .build()).join();
        assertEquals(checkReply.getCode(0), CheckReply.Code.NO_RECEIVER);

        verify(distClient, times(0))
            .addTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt(), eq(1L));

    }

    @Test(groups = "integration")
    public void subConflict() {
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
        SubReply subReply2 = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(1)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build()).join();
        assertEquals(subReply2.getReqId(), reqId);
        assertEquals(subReply2.getCode(), SubReply.Code.CONFLICT);

        verify(distClient, times(0)).addTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(),
            anyInt(), anyLong());
    }

    @Test(groups = "integration")
    public void subOK() {
        clearInvocations(distClient);
        long now = HLC.INST.getPhysical();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();
        String topicFilter = "/a/b/c";
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

        CheckReply checkReply = inboxClient.check(CheckRequest.newBuilder()
            .setTenantId(tenantId)
            .setDelivererKey(getDelivererKey(inboxId, receiverId(inboxId, incarnation)))
            .addMatchInfo(MatchInfo.newBuilder()
                .setReceiverId(receiverId(inboxId, incarnation))
                .setTopicFilter(topicFilter)
                .build())
            .build()).join();
        assertEquals(checkReply.getCode(0), CheckReply.Code.NO_SUB);

        when(distClient.addTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt(),
            anyLong()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        SubReply subReply2 = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setOption(TopicFilterOption.newBuilder().setQos(QoS.AT_LEAST_ONCE).build())
            .setNow(now)
            .build()).join();
        assertEquals(subReply2.getReqId(), reqId);
        assertEquals(subReply2.getCode(), SubReply.Code.OK);

        checkReply = inboxClient.check(CheckRequest.newBuilder()
            .setTenantId(tenantId)
            .setDelivererKey(getDelivererKey(inboxId, receiverId(inboxId, incarnation)))
            .addMatchInfo(MatchInfo.newBuilder()
                .setReceiverId(receiverId(inboxId, incarnation))
                .setTopicFilter(topicFilter)
                .build())
            .build()).join();
        assertEquals(checkReply.getCode(0), CheckReply.Code.OK);
        verify(distClient, times(1))
            .addTopicMatch(anyLong(), eq(tenantId), eq(topicFilter), anyString(), anyString(), anyInt(), anyLong());
    }

    @Test(groups = "integration")
    public void subExists() {
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

        when(distClient.addTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt(),
            anyLong()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        subReply2 = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build()).join();
        assertEquals(subReply2.getReqId(), reqId);
        assertEquals(subReply2.getCode(), SubReply.Code.EXISTS);

        verify(distClient, times(2))
            .addTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong());
    }

    @Test(groups = "integration")
    public void subExceedLimit() {
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
        when(distClient.addTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt(),
            anyLong()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        SubReply subReply2 = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter("/a/b/c")
            .setNow(now)
            .build()).join();
        assertEquals(subReply2.getReqId(), reqId);
        assertEquals(subReply2.getCode(), SubReply.Code.OK);

        subReply2 = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter("/a/b")
            .setNow(now)
            .build()).join();
        assertEquals(subReply2.getReqId(), reqId);
        assertEquals(subReply2.getCode(), SubReply.Code.EXCEED_LIMIT);

        verify(distClient, times(1))
            .addTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong());
    }

    @Test(groups = "integration")
    public void subErrorWhenExceedDistMatchLimit() {
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

        when(distClient.addTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt(),
            anyLong()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.EXCEED_LIMIT));

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
        assertEquals(subReply2.getCode(), SubReply.Code.EXCEED_LIMIT);
    }

    @Test(groups = "integration")
    public void subErrorWhenDistMatchError() {
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

        when(distClient.addTopicMatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt(),
            anyLong()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.ERROR));

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
        assertEquals(subReply2.getCode(), SubReply.Code.ERROR);
    }

}
