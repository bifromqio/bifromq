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

package com.baidu.bifromq.inbox.store;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.inbox.rpc.proto.DetachReply;
import com.baidu.bifromq.inbox.rpc.proto.DetachRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchDeleteRequest;
import com.baidu.bifromq.inbox.storage.proto.GCReply;
import com.baidu.bifromq.inbox.storage.proto.GCRequest;
import com.baidu.bifromq.type.ClientInfo;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class GCTest extends InboxStoreTest {
    @Test(groups = "integration")
    public void skipNonExpired() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        requestCreate(BatchCreateRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(5)
            .setExpirySeconds(5)
            .setClient(client)
            .setNow(now)
            .build());

        GCReply reply = requestGCScan(GCRequest.newBuilder()
            .setTenantId(tenantId)
            .setNow(now)
            .build());
        assertEquals(reply.getCandidateCount(), 0);

        requestDelete(BatchDeleteRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .build());
    }

    @Test(groups = "integration")
    public void scanWithRequestExpiry() {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        requestCreate(BatchCreateRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(5)
            .setExpirySeconds(5)
            .setClient(client)
            .setNow(now)
            .build());

        GCReply reply = requestGCScan(GCRequest.newBuilder()
            .setTenantId(tenantId)
            .setExpirySeconds(1)
            .setNow(Duration.ofSeconds(11).toMillis())
            .build());
        assertEquals(reply.getCandidateCount(), 1);
        GCReply.GCCandidate candidate = reply.getCandidate(0);
        assertEquals(candidate.getInboxId(), inboxId);
        assertEquals(candidate.getIncarnation(), incarnation);
        assertEquals(candidate.getVersion(), 0);
        assertEquals(candidate.getClient(), client);

        requestDelete(BatchDeleteRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .build());
    }

    @Test(groups = "integration")
    public void scanAll() {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        requestCreate(BatchCreateRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(5)
            .setExpirySeconds(5)
            .setClient(client)
            .setNow(now)
            .build());

        GCReply reply = requestGCScan(GCRequest.newBuilder()
            .setNow(Duration.ofSeconds(13).toMillis())
            .build());
        assertEquals(reply.getCandidateCount(), 1);
        GCReply.GCCandidate candidate = reply.getCandidate(0);
        assertEquals(candidate.getInboxId(), inboxId);
        assertEquals(candidate.getIncarnation(), incarnation);
        assertEquals(candidate.getVersion(), 0);
        assertEquals(candidate.getClient(), client);

        requestDelete(BatchDeleteRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .build());
    }

    @Test(groups = "integration")
    public void scanTenant() {
        long now = 0;
        String tenant1 = "tenant1-" + System.nanoTime();
        String tenant2 = "tenant2-" + System.nanoTime();
        String inboxId1 = "inboxId1-" + System.nanoTime();
        String inboxId2 = "inboxId2-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client1 = ClientInfo.newBuilder().setTenantId(tenant1).build();
        ClientInfo client2 = ClientInfo.newBuilder().setTenantId(tenant2).build();
        requestCreate(BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId1)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setClient(client1)
                .setNow(now)
                .build(),
            BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId2)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setClient(client2)
                .setNow(now)
                .build());

        GCReply reply = requestGCScan(GCRequest.newBuilder()
            .setTenantId(tenant1)
            .setNow(Duration.ofSeconds(13).toMillis())
            .build());
        assertEquals(reply.getCandidateCount(), 1);
        GCReply.GCCandidate candidate = reply.getCandidate(0);
        assertEquals(candidate.getInboxId(), inboxId1);
        assertEquals(candidate.getIncarnation(), incarnation);
        assertEquals(candidate.getVersion(), 0);
        assertEquals(candidate.getClient(), client1);

        reply = requestGCScan(GCRequest.newBuilder()
            .setTenantId(tenant2)
            .setNow(Duration.ofSeconds(13).toMillis())
            .build());
        assertEquals(reply.getCandidateCount(), 1);
        candidate = reply.getCandidate(0);
        assertEquals(candidate.getInboxId(), inboxId2);
        assertEquals(candidate.getIncarnation(), incarnation);
        assertEquals(candidate.getVersion(), 0);
        assertEquals(candidate.getClient(), client2);

        requestDelete(BatchDeleteRequest.Params.newBuilder()
            .setTenantId(tenant1)
            .setInboxId(inboxId1)
            .setIncarnation(incarnation)
            .setVersion(0)
            .build());
        requestDelete(BatchDeleteRequest.Params.newBuilder()
            .setTenantId(tenant2)
            .setInboxId(inboxId2)
            .setIncarnation(incarnation)
            .setVersion(0)
            .build());
    }

    @Test(groups = "integration")
    public void gcJob() {
        long now = System.currentTimeMillis();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        when(inboxClient.detach(any()))
            .thenReturn(CompletableFuture.completedFuture(DetachReply.newBuilder().build()));
        requestCreate(BatchCreateRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(1)
            .setExpirySeconds(1)
            .setClient(client)
            .setNow(now)
            .build());

        ArgumentCaptor<DetachRequest> detachCaptor = ArgumentCaptor.forClass(DetachRequest.class);
        verify(inboxClient, timeout(10000)).detach(detachCaptor.capture());
        DetachRequest request = detachCaptor.getValue();
        assertEquals(request.getInboxId(), inboxId);
        assertEquals(request.getClient(), client);
        assertEquals(request.getIncarnation(), incarnation);
        assertFalse(request.getDiscardLWT());
        assertEquals(request.getExpirySeconds(), 1);
    }
}
