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

package com.baidu.bifromq.inbox.server;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.inbox.rpc.proto.AttachReply;
import com.baidu.bifromq.inbox.rpc.proto.AttachRequest;
import com.baidu.bifromq.inbox.rpc.proto.CreateReply;
import com.baidu.bifromq.inbox.rpc.proto.CreateRequest;
import com.baidu.bifromq.inbox.rpc.proto.DetachReply;
import com.baidu.bifromq.inbox.rpc.proto.DetachRequest;
import com.baidu.bifromq.inbox.rpc.proto.GetReply;
import com.baidu.bifromq.inbox.rpc.proto.GetRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxVersion;
import com.baidu.bifromq.inbox.storage.proto.LWT;
import com.baidu.bifromq.type.ClientInfo;
import org.testng.annotations.Test;

public class InboxAdminRPCTest extends InboxServiceTest {

    @Test(groups = "integration")
    public void createWithLWT() throws InterruptedException {
        long now = System.currentTimeMillis();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("LastWill").setDelaySeconds(5).build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();

        GetReply getReply = inboxClient.get(GetRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setNow(0)
            .build()).join();
        assertEquals(getReply.getReqId(), reqId);
        assertEquals(getReply.getCode(), GetReply.Code.NO_INBOX);
        assertTrue(getReply.getInboxList().isEmpty());

        CreateReply createReply = inboxClient.create(CreateRequest.newBuilder()
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
        assertEquals(createReply.getReqId(), reqId);
        assertEquals(createReply.getCode(), CreateReply.Code.OK);

        getReply = inboxClient.get(GetRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setNow(0)
            .build()).join();
        assertEquals(getReply.getReqId(), reqId);
        assertEquals(getReply.getCode(), GetReply.Code.EXIST);
        assertEquals(getReply.getInboxCount(), 1);

        InboxVersion inboxVersion = getReply.getInbox(0);
        assertEquals(inboxVersion.getIncarnation(), incarnation);
        assertEquals(inboxVersion.getKeepAliveSeconds(), 5);
        assertEquals(inboxVersion.getExpirySeconds(), 5);
        assertEquals(inboxVersion.getVersion(), 0);
        assertEquals(inboxVersion.getLwt(), lwt);
        assertEquals(inboxVersion.getClient(), clientInfo);
    }

    @Test(groups = "integration")
    public void createWithoutLWT() {
        long now = System.nanoTime();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();

        GetReply getReply = inboxClient.get(GetRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setNow(0)
            .build()).join();
        assertEquals(getReply.getReqId(), reqId);
        assertEquals(getReply.getCode(), GetReply.Code.NO_INBOX);
        assertTrue(getReply.getInboxList().isEmpty());

        CreateReply createReply = inboxClient.create(CreateRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setKeepAliveSeconds(5)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        assertEquals(createReply.getReqId(), reqId);
        assertEquals(createReply.getCode(), CreateReply.Code.OK);

        getReply = inboxClient.get(GetRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setNow(0)
            .build()).join();
        assertEquals(getReply.getReqId(), reqId);
        assertEquals(getReply.getCode(), GetReply.Code.EXIST);
        assertEquals(getReply.getInboxCount(), 1);

        InboxVersion inboxVersion = getReply.getInbox(0);
        assertEquals(inboxVersion.getIncarnation(), incarnation);
        assertEquals(inboxVersion.getKeepAliveSeconds(), 5);
        assertEquals(inboxVersion.getExpirySeconds(), 5);
        assertEquals(inboxVersion.getVersion(), 0);
        assertFalse(inboxVersion.hasLwt());
        assertEquals(inboxVersion.getClient(), clientInfo);
    }

    @Test(groups = "integration")
    public void attachNoInbox() {
        long now = System.nanoTime();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();

        AttachReply attachReply = inboxClient.attach(AttachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(1)
            .setKeepAliveSeconds(5)
            .setExpirySeconds(5)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        assertEquals(attachReply.getReqId(), reqId);
        assertEquals(attachReply.getCode(), AttachReply.Code.NO_INBOX);
    }

    @Test(groups = "integration")
    public void attachConflict() {
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

        AttachReply attachReply = inboxClient.attach(AttachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(1) // mismatched version
            .setKeepAliveSeconds(5)
            .setExpirySeconds(5)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        assertEquals(attachReply.getReqId(), reqId);
        assertEquals(attachReply.getCode(), AttachReply.Code.CONFLICT);
    }

    @Test(groups = "integration")
    public void attachWithLWT() {
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
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();

        AttachReply attachReply = inboxClient.attach(AttachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setKeepAliveSeconds(1)
            .setExpirySeconds(1)
            .setLwt(lwt)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        assertEquals(attachReply.getReqId(), reqId);
        assertEquals(attachReply.getCode(), AttachReply.Code.OK);

        GetReply getReply = inboxClient.get(GetRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setNow(0)
            .build()).join();
        assertEquals(getReply.getReqId(), reqId);
        assertEquals(getReply.getCode(), GetReply.Code.EXIST);
        assertEquals(getReply.getInboxCount(), 1);

        InboxVersion inboxVersion = getReply.getInbox(0);
        assertEquals(inboxVersion.getIncarnation(), incarnation);
        assertEquals(inboxVersion.getKeepAliveSeconds(), 1);
        assertEquals(inboxVersion.getExpirySeconds(), 1);
        assertEquals(inboxVersion.getVersion(), 1);
        assertEquals(inboxVersion.getLwt(), lwt);
        assertEquals(inboxVersion.getClient(), clientInfo);
    }

    @Test(groups = "integration")
    public void attachWithoutLWT() {
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

        AttachReply attachReply = inboxClient.attach(AttachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setKeepAliveSeconds(1)
            .setExpirySeconds(1)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        assertEquals(attachReply.getReqId(), reqId);
        assertEquals(attachReply.getCode(), AttachReply.Code.OK);

        GetReply getReply = inboxClient.get(GetRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setNow(0)
            .build()).join();
        assertEquals(getReply.getReqId(), reqId);
        assertEquals(getReply.getCode(), GetReply.Code.EXIST);
        assertEquals(getReply.getInboxCount(), 1);

        InboxVersion inboxVersion = getReply.getInbox(0);
        assertEquals(inboxVersion.getIncarnation(), incarnation);
        assertEquals(inboxVersion.getKeepAliveSeconds(), 1);
        assertEquals(inboxVersion.getExpirySeconds(), 1);
        assertEquals(inboxVersion.getVersion(), 1);
        assertFalse(inboxVersion.hasLwt());
        assertEquals(inboxVersion.getClient(), clientInfo);
    }

    @Test(groups = "integration")
    public void detachNoInbox() {
        long now = System.nanoTime();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();

        DetachReply detachReply = inboxClient.detach(DetachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(1)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        assertEquals(detachReply.getReqId(), reqId);
        assertEquals(detachReply.getCode(), DetachReply.Code.NO_INBOX);
    }

    @Test(groups = "integration")
    public void detachConflict() {
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
            .setKeepAliveSeconds(5)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();

        DetachReply detachReply = inboxClient.detach(DetachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(1) // mismatched version
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        assertEquals(detachReply.getReqId(), reqId);
        assertEquals(detachReply.getCode(), DetachReply.Code.CONFLICT);
    }

    @Test(groups = "integration")
    public void detach() {
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

        DetachReply detachReply = inboxClient.detach(DetachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setExpirySeconds(1)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        assertEquals(detachReply.getReqId(), reqId);
        assertEquals(detachReply.getCode(), DetachReply.Code.OK);
        assertEquals(detachReply.getLwt(), lwt);

        GetReply getReply = inboxClient.get(GetRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setNow(0)
            .build()).join();
        assertEquals(getReply.getReqId(), reqId);
        assertEquals(getReply.getCode(), GetReply.Code.EXIST);
        assertEquals(getReply.getInboxCount(), 1);

        InboxVersion inboxVersion = getReply.getInbox(0);
        assertEquals(inboxVersion.getIncarnation(), incarnation);
        assertEquals(inboxVersion.getKeepAliveSeconds(), 5);
        assertEquals(inboxVersion.getExpirySeconds(), 1);
        assertEquals(inboxVersion.getVersion(), 1);
        assertEquals(inboxVersion.getLwt(), lwt);
        assertEquals(inboxVersion.getClient(), clientInfo);
    }

    @Test(groups = "integration")
    public void detachAndDiscardLWT() {
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

        DetachReply detachReply = inboxClient.detach(DetachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setExpirySeconds(1)
            .setDiscardLWT(true)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        assertEquals(detachReply.getReqId(), reqId);
        assertEquals(detachReply.getCode(), DetachReply.Code.OK);
        assertFalse(detachReply.hasLwt());

        GetReply getReply = inboxClient.get(GetRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setNow(0)
            .build()).join();
        assertEquals(getReply.getReqId(), reqId);
        assertEquals(getReply.getCode(), GetReply.Code.EXIST);
        assertEquals(getReply.getInboxCount(), 1);

        InboxVersion inboxVersion = getReply.getInbox(0);
        assertEquals(inboxVersion.getIncarnation(), incarnation);
        assertEquals(inboxVersion.getKeepAliveSeconds(), 5);
        assertEquals(inboxVersion.getExpirySeconds(), 1);
        assertEquals(inboxVersion.getVersion(), 1);
        assertFalse(detachReply.hasLwt());
        assertEquals(inboxVersion.getClient(), clientInfo);
    }
}
