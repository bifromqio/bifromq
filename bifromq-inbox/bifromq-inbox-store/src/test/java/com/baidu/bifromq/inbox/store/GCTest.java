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
import static org.testng.Assert.assertTrue;

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
        long now = 0;
        String tenantId = "tenantId";
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
            .setLimit(10)
            .setNow(0)
            .build());
        assertEquals(reply.getInboxCount(), 0);

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
            .setLimit(10)
            .setExpirySeconds(1)
            .setNow(Duration.ofSeconds(11).toMillis())
            .build());
        assertFalse(reply.hasCursor());
        assertEquals(reply.getInboxCount(), 1);
        GCReply.Inbox inbox = reply.getInbox(0);
        assertEquals(inbox.getInboxId(), inboxId);
        assertEquals(inbox.getIncarnation(), incarnation);
        assertEquals(inbox.getVersion(), 0);
        assertEquals(inbox.getClient(), client);

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
        String tenantId = "tenantId";
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
            .setLimit(10)
            .setNow(Duration.ofSeconds(13).toMillis())
            .build());
        assertFalse(reply.hasCursor());
        assertEquals(reply.getInboxCount(), 1);
        GCReply.Inbox inbox = reply.getInbox(0);
        assertEquals(inbox.getInboxId(), inboxId);
        assertEquals(inbox.getIncarnation(), incarnation);
        assertEquals(inbox.getVersion(), 0);
        assertEquals(inbox.getClient(), client);

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
        String tenant1 = "tenant1";
        String tenant2 = "tenant2";
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
            .setLimit(10)
            .setNow(Duration.ofSeconds(13).toMillis())
            .build());
        assertFalse(reply.hasCursor());
        assertEquals(reply.getInboxCount(), 1);
        GCReply.Inbox inbox = reply.getInbox(0);
        assertEquals(inbox.getInboxId(), inboxId1);
        assertEquals(inbox.getIncarnation(), incarnation);
        assertEquals(inbox.getVersion(), 0);
        assertEquals(inbox.getClient(), client1);

        reply = requestGCScan(GCRequest.newBuilder()
            .setTenantId(tenant2)
            .setLimit(10)
            .setNow(Duration.ofSeconds(13).toMillis())
            .build());
        assertFalse(reply.hasCursor());
        assertEquals(reply.getInboxCount(), 1);
        inbox = reply.getInbox(0);
        assertEquals(inbox.getInboxId(), inboxId2);
        assertEquals(inbox.getIncarnation(), incarnation);
        assertEquals(inbox.getVersion(), 0);
        assertEquals(inbox.getClient(), client2);

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
    public void scanWithLimit() {
        long now = 0;
        String tenant = "tenantId";
        String inboxId1 = "inboxId1-" + System.nanoTime();
        String inboxId2 = "inboxId2-" + System.nanoTime();
        String inboxId3 = "inboxId2-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenant).build();
        requestCreate(
            BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId1)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setClient(client)
                .setNow(now)
                .build(),
            BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId2)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setClient(client)
                .setNow(now)
                .build(),
            BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId3)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setClient(client)
                .setNow(now)
                .build());
        GCReply reply = requestGCScan(GCRequest.newBuilder()
            .setTenantId(tenant)
            .setLimit(2)
            .setNow(Duration.ofSeconds(13).toMillis())
            .build());
        assertTrue(reply.hasCursor());
        assertEquals(reply.getInboxCount(), 2);
    }

    @Test(groups = "integration")
    public void scanWithCursor() {
        long now = 0;
        String tenant = "tenantId";
        String inboxId1 = "inboxId1-" + System.nanoTime();
        String inboxId2 = "inboxId2-" + System.nanoTime();
        String inboxId3 = "inboxId2-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenant).build();
        requestCreate(
            BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId1)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setClient(client)
                .setNow(now)
                .build(),
            BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId2)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setClient(client)
                .setNow(now)
                .build(),
            BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId3)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setClient(client)
                .setNow(now)
                .build());
        GCReply reply = requestGCScan(GCRequest.newBuilder()
            .setTenantId(tenant)
            .setLimit(1)
            .setNow(Duration.ofSeconds(13).toMillis())
            .build());
        assertTrue(reply.hasCursor());
        assertEquals(reply.getInboxCount(), 1);
        assertEquals(reply.getInbox(0).getInboxId(), inboxId1);

        reply = requestGCScan(GCRequest.newBuilder()
            .setTenantId(tenant)
            .setLimit(1)
            .setCursor(reply.getCursor())
            .setNow(Duration.ofSeconds(13).toMillis())
            .build());
        assertTrue(reply.hasCursor());
        assertEquals(reply.getInboxCount(), 1);
        assertEquals(reply.getInbox(0).getInboxId(), inboxId2);

        reply = requestGCScan(GCRequest.newBuilder()
            .setTenantId(tenant)
            .setLimit(1)
            .setCursor(reply.getCursor())
            .setNow(Duration.ofSeconds(13).toMillis())
            .build());
        assertFalse(reply.hasCursor());
        assertEquals(reply.getInboxCount(), 1);
        assertEquals(reply.getInbox(0).getInboxId(), inboxId3);
    }

    @Test(groups = "integration")
    public void gcJob() {
        long now = System.currentTimeMillis();
        String tenantId = "tenantId";
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
        assertEquals(request.getExpirySeconds(), 0);
    }
}
