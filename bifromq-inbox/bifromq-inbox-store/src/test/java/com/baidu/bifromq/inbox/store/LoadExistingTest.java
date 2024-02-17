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

package com.baidu.bifromq.inbox.store;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.baidu.bifromq.inbox.rpc.proto.DetachReply;
import com.baidu.bifromq.inbox.rpc.proto.DetachRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateRequest;
import com.baidu.bifromq.type.ClientInfo;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class LoadExistingTest extends InboxStoreTest {
    @Test(groups = "integration")
    public void gcJobAfterRestart() {
        long now = System.currentTimeMillis();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        requestCreate(BatchCreateRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(5)
            .setExpirySeconds(10)
            .setClient(client)
            .setNow(now)
            .build());
        restartStoreServer();
        when(inboxClient.detach(any()))
            .thenReturn(CompletableFuture.completedFuture(DetachReply.newBuilder().build()));
        ArgumentCaptor<DetachRequest> detachCaptor = ArgumentCaptor.forClass(DetachRequest.class);
        verify(inboxClient, timeout(10000)).detach(detachCaptor.capture());
        DetachRequest request = detachCaptor.getValue();
        assertEquals(request.getInboxId(), inboxId);
        assertEquals(request.getClient(), client);
        assertEquals(request.getIncarnation(), incarnation);
        assertFalse(request.getDiscardLWT());
        assertEquals(request.getExpirySeconds(), 10);
    }
}
