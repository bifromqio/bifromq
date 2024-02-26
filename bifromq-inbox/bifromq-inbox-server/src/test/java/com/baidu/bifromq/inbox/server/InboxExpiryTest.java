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

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.dist.client.DistResult;
import com.baidu.bifromq.dist.client.MatchResult;
import com.baidu.bifromq.inbox.rpc.proto.CreateRequest;
import com.baidu.bifromq.inbox.rpc.proto.DetachRequest;
import com.baidu.bifromq.inbox.rpc.proto.GetReply;
import com.baidu.bifromq.inbox.rpc.proto.GetRequest;
import com.baidu.bifromq.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.inbox.storage.proto.LWT;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.EventType;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.google.protobuf.ByteString;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class InboxExpiryTest extends InboxServiceTest {
    @Test(groups = "integration")
    public void lwtPub() {
        clearInvocations(distClient, eventCollector);
        long now = System.nanoTime();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();
        LWT lwt = LWT.newBuilder()
            .setTopic("LastWill")
            .setDelaySeconds(1)
            .setMessage(Message.newBuilder()
                .setPubQoS(QoS.AT_LEAST_ONCE)
                .setPayload(ByteString.copyFromUtf8("last will"))
                .build())
            .build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        when(distClient.pub(anyLong(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(DistResult.OK));
        inboxClient.create(CreateRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(1)
            .setExpirySeconds(2)
            .setLimit(10)
            .setDropOldest(true)
            .setLwt(lwt)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        verify(distClient, timeout(5000).times(1))
            .pub(anyLong(), eq(lwt.getTopic()),
                argThat(m -> m.getPubQoS() == QoS.AT_LEAST_ONCE &&
                    m.getPayload().equals(lwt.getMessage().getPayload())),
                any());
        verify(eventCollector, timeout(5000)).report(argThat(e -> e.type() == EventType.WILL_DISTED));
        await().until(() -> {
            GetReply getReply = inboxClient.get(GetRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setNow(0)
                .build()).join();
            return getReply.getCode() == GetReply.Code.NO_INBOX;
        });
    }

    @Test(groups = "integration")
    public void lwtRetained() {
        clearInvocations(retainClient, eventCollector);
        long now = System.nanoTime();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("LastWill").setMessage(Message.newBuilder()
                .setIsRetain(true)
                .setPayload(ByteString.copyFromUtf8("last will"))
                .build())
            .setDelaySeconds(1).build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        when(distClient.pub(anyLong(), anyString(), any(), any())).thenReturn(
            CompletableFuture.completedFuture(DistResult.OK));
        when(retainClient.retain(anyLong(), anyString(), any(), any(), anyInt(), any())).thenReturn(
            CompletableFuture.completedFuture(RetainReply.newBuilder().setResult(RetainReply.Result.RETAINED).build()));
        inboxClient.create(CreateRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(1)
            .setExpirySeconds(2)
            .setLimit(10)
            .setDropOldest(true)
            .setLwt(lwt)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        verify(retainClient, timeout(5000).times(1)).retain(anyLong(), anyString(), any(), any(), anyInt(), any());
        ArgumentCaptor<Event<?>> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventCollector, timeout(5000).times(2)).report(eventCaptor.capture());
        assertEquals(eventCaptor.getAllValues().get(0).type(), EventType.MSG_RETAINED);
        assertEquals(eventCaptor.getAllValues().get(1).type(), EventType.WILL_DISTED);
        await().until(() -> {
            GetReply getReply = inboxClient.get(GetRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setNow(0)
                .build()).join();
            return getReply.getCode() == GetReply.Code.NO_INBOX;
        });
    }

    @Test(groups = "integration")
    public void lwtRetryOnError() {
        clearInvocations(distClient, retainClient, eventCollector);
        long now = System.nanoTime();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("LastWill").setMessage(Message.newBuilder().setIsRetain(true).build())
            .setDelaySeconds(1).build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        when(distClient.pub(anyLong(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(DistResult.OK));
        when(retainClient.retain(anyLong(), anyString(), any(), any(), anyInt(), any()))
            .thenReturn(
                CompletableFuture.completedFuture(
                    RetainReply.newBuilder().setResult(RetainReply.Result.ERROR).build()),
                CompletableFuture.completedFuture(
                    RetainReply.newBuilder().setResult(RetainReply.Result.RETAINED).build()));
        inboxClient.create(CreateRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(1)
            .setExpirySeconds(2)
            .setLimit(10)
            .setDropOldest(true)
            .setLwt(lwt)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        verify(distClient, timeout(10000).times(2)).pub(anyLong(), anyString(), any(), any());
        verify(retainClient, timeout(10000).times(2)).retain(anyLong(), anyString(), any(), any(), anyInt(), any());
        await().until(() -> {
            GetReply getReply = inboxClient.get(GetRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setNow(0)
                .build()).join();
            return getReply.getCode() == GetReply.Code.NO_INBOX;
        });
    }

    @Test(groups = "integration")
    public void lwtAfterDetach() {
        clearInvocations(distClient);
        long now = System.currentTimeMillis();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("LastWill")
            .setDelaySeconds(1)
            .setMessage(Message.newBuilder()
                .setPubQoS(QoS.AT_LEAST_ONCE)
                .setPayload(ByteString.copyFromUtf8("last will"))
                .build())
            .build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        when(distClient.pub(anyLong(), anyString(), any(), any())).thenReturn(
            CompletableFuture.completedFuture(DistResult.OK));
        inboxClient.create(CreateRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(10)
            .setExpirySeconds(10)
            .setLimit(10)
            .setDropOldest(true)
            .setLwt(lwt)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        inboxClient.detach(DetachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setExpirySeconds(10)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        verify(distClient, timeout(2000).times(1))
            .pub(anyLong(), eq(lwt.getTopic()),
                argThat(m -> m.getPubQoS() == QoS.AT_LEAST_ONCE &&
                    m.getPayload().equals(lwt.getMessage().getPayload())),
                any());
        verify(eventCollector, timeout(2000)).report(argThat(e -> e.type() == EventType.WILL_DISTED));
    }

    @Test(groups = "integration")
    public void matchCleanupWhenInboxExpired() {
        clearInvocations(distClient, retainClient, eventCollector);
        long now = System.nanoTime();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        inboxClient.create(CreateRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(1)
            .setExpirySeconds(2)
            .setLimit(10)
            .setDropOldest(true)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        when(distClient.match(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        String topicFilter = "/a/b/c";
        inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build()).join();

        verify(distClient, timeout(5000).times(0)).pub(anyLong(), anyString(), any(), any());
        verify(distClient, timeout(5000).times(1)).unmatch(anyLong(), eq(tenantId), eq(topicFilter), anyString(),
            anyString(), anyInt());
        await().until(() -> {
            GetReply getReply = inboxClient.get(GetRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setNow(0)
                .build()).join();
            return getReply.getCode() == GetReply.Code.NO_INBOX;
        });
    }
}
