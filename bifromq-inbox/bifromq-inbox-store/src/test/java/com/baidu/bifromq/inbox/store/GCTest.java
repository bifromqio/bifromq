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
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.inbox.rpc.proto.DeleteRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateRequest;
import com.baidu.bifromq.sessiondict.rpc.proto.GetReply;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.MQTTClientInfoConstants;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class GCTest extends InboxStoreTest {
    @Test(groups = "integration")
    public void gcJob() {
        reset(inboxClient);
        reset(sessionDictClient);
        long now = System.currentTimeMillis();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId)
            .putMetadata(MQTTClientInfoConstants.MQTT_USER_ID_KEY, "userId")
            .putMetadata(MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY, "clientId")
            .build();
        when(sessionDictClient.get(any())).thenReturn(
            CompletableFuture.completedFuture(GetReply.newBuilder().setResult(GetReply.Result.NOT_FOUND).build()));
        requestCreate(BatchCreateRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(1)
            .setClient(client)
            .setNow(now)
            .build());
        ArgumentCaptor<DeleteRequest> deleteCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        verify(inboxClient, timeout(10000)).delete(deleteCaptor.capture());
        DeleteRequest request = deleteCaptor.getValue();
        assertEquals(request.getTenantId(), tenantId);
        assertEquals(request.getInboxId(), inboxId);
        assertEquals(request.getIncarnation(), incarnation);
    }

    @Test(groups = "integration")
    public void gcJobNoClean() {
        reset(inboxClient);
        reset(sessionDictClient);
        long now = System.currentTimeMillis();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId)
            .putMetadata(MQTTClientInfoConstants.MQTT_USER_ID_KEY, "userId")
            .putMetadata(MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY, "clientId")
            .build();
        when(sessionDictClient.get(any())).thenReturn(
            CompletableFuture.completedFuture(GetReply.newBuilder().setResult(GetReply.Result.OK).build()));
        requestCreate(BatchCreateRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(1)
            .setClient(client)
            .setNow(now)
            .build());
        ArgumentCaptor<DeleteRequest> deleteCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        verify(inboxClient, timeout(3000).times(0)).delete(deleteCaptor.capture());
    }
}
