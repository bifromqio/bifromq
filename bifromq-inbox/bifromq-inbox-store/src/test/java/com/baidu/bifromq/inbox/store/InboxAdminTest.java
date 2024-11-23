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

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.inbox.storage.proto.BatchAttachReply;
import com.baidu.bifromq.inbox.storage.proto.BatchAttachRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchDeleteReply;
import com.baidu.bifromq.inbox.storage.proto.BatchDeleteRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchDetachReply;
import com.baidu.bifromq.inbox.storage.proto.BatchDetachRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchGetReply;
import com.baidu.bifromq.inbox.storage.proto.BatchGetRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchReply;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxVersion;
import com.baidu.bifromq.inbox.storage.proto.LWT;
import com.baidu.bifromq.plugin.eventcollector.EventType;
import com.baidu.bifromq.type.ClientInfo;
import java.time.Duration;
import org.testng.annotations.Test;

public class InboxAdminTest extends InboxStoreTest {
    @Test(groups = "integration")
    public void get() {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId1 = "inboxId1-" + System.nanoTime();
        String inboxId2 = "inboxId2-" + System.nanoTime();
        BatchGetRequest.Params getParams1 = BatchGetRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId1)
                .setNow(now)
                .build();
        BatchGetRequest.Params getParams2 = BatchGetRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId2)
                .setNow(now)
                .build();
        for (BatchGetReply.Result result : requestGet(getParams1, getParams2)) {
            assertEquals(result.getVersionCount(), 0);
        }
    }

    @Test(groups = "integration")
    public void getAfterExpired() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchCreateRequest.Params createParams = BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setClient(client)
                .setNow(now)
                .build();
        requestCreate(createParams);

        BatchGetRequest.Params getParams = BatchGetRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setNow(now)
                .build();
        BatchGetReply.Result result = requestGet(getParams).get(0);
        assertEquals(result.getVersionCount(), 1);

        getParams = BatchGetRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setNow(Duration.ofMillis(now).plusSeconds(13).toMillis())
                .build();
        result = requestGet(getParams).get(0);
        assertEquals(result.getVersionCount(), 0);
        verify(eventCollector).report(argThat(e -> e.type() == EventType.MQTT_SESSION_START));
    }

    @Test(groups = "integration")
    public void createWithLWT() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("lastWill").setDelaySeconds(5).build();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchGetRequest.Params getParams = BatchGetRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setNow(0)
                .build();
        BatchGetReply.Result result = requestGet(getParams).get(0);
        assertEquals(result.getVersionCount(), 0);

        BatchCreateRequest.Params createParams = BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setLwt(lwt)
                .setClient(client)
                .setNow(now)
                .build();
        boolean succeed = requestCreate(createParams).get(0);
        assertTrue(succeed);
        result = requestGet(getParams).get(0);
        verify(eventCollector).report(argThat(e -> e.type() == EventType.MQTT_SESSION_START));
        assertEquals(result.getVersionCount(), 1);
        InboxVersion inboxVersion = result.getVersion(0);
        assertEquals(inboxVersion.getVersion(), 0);
        assertEquals(inboxVersion.getIncarnation(), incarnation);
        assertEquals(inboxVersion.getKeepAliveSeconds(), 5);
        assertEquals(inboxVersion.getExpirySeconds(), 5);
        assertEquals(inboxVersion.getLwt(), lwt);
        assertEquals(inboxVersion.getClient(), client);
    }

    @Test(groups = "integration")
    public void createWithoutLWT() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchGetRequest.Params getParams = BatchGetRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setNow(now)
                .build();
        BatchGetReply.Result result = requestGet(getParams).get(0);
        assertEquals(result.getVersionCount(), 0);

        BatchCreateRequest.Params createParams = BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setClient(client)
                .setNow(now)
                .build();
        boolean succeed = requestCreate(createParams).get(0);
        assertTrue(succeed);
        verify(eventCollector).report(argThat(e -> e.type() == EventType.MQTT_SESSION_START));
        result = requestGet(getParams).get(0);
        assertEquals(result.getVersionCount(), 1);
        InboxVersion inboxVersion = result.getVersion(0);
        assertEquals(inboxVersion.getVersion(), 0);
        assertEquals(inboxVersion.getIncarnation(), incarnation);
        assertEquals(inboxVersion.getKeepAliveSeconds(), 5);
        assertEquals(inboxVersion.getExpirySeconds(), 5);
        assertFalse(inboxVersion.hasLwt());
        assertEquals(inboxVersion.getClient(), client);
    }

    @Test(groups = "integration")
    public void createOverExisting() {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchCreateRequest.Params createParams = BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setClient(client)
                .setNow(now)
                .build();
        boolean succeed = requestCreate(createParams).get(0);
        assertTrue(succeed);
        succeed = requestCreate(createParams).get(0);
        assertFalse(succeed);
        verify(eventCollector).report(argThat(e -> e.type() == EventType.MQTT_SESSION_START));
    }

    @Test(groups = "integration")
    public void attachNoInbox() {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setVersion(0)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setClient(client)
                .setNow(now)
                .build();
        BatchAttachReply.Result result = requestAttach(attachParams).get(0);
        assertEquals(result.getCode(), BatchAttachReply.Code.NO_INBOX);
    }

    @Test(groups = "integration")
    public void attachConflict() {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchCreateRequest.Params createParams = BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setClient(client)
                .setNow(now)
                .build();
        requestCreate(createParams);

        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setVersion(1)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setClient(client)
                .setNow(now)
                .build();
        BatchAttachReply.Result result = requestAttach(attachParams).get(0);
        assertSame(result.getCode(), BatchAttachReply.Code.CONFLICT);
        verify(eventCollector).report(argThat(e -> e.type() == EventType.MQTT_SESSION_START));
    }

    @Test(groups = "integration")
    public void attachWithoutLWT() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("lastWill").setDelaySeconds(5).build();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchCreateRequest.Params createParams = BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setLwt(lwt)
                .setClient(client)
                .setNow(now)
                .build();
        requestCreate(createParams);

        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setVersion(0)
                .setKeepAliveSeconds(1)
                .setExpirySeconds(1)
                .setClient(client)
                .setNow(now)
                .build();
        BatchAttachReply.Result result = requestAttach(attachParams).get(0);
        assertSame(result.getCode(), BatchAttachReply.Code.OK);

        BatchGetRequest.Params getParams = BatchGetRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setNow(now)
                .build();
        InboxVersion inboxVersion = requestGet(getParams).get(0).getVersion(0);
        assertEquals(inboxVersion.getVersion(), 1);
        assertEquals(inboxVersion.getKeepAliveSeconds(), 1);
        assertEquals(inboxVersion.getExpirySeconds(), 1);
        assertFalse(inboxVersion.hasLwt());

        getParams = BatchGetRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setNow(now + Duration.ofSeconds(3).toMillis())
                .build();
        assertEquals(requestGet(getParams).get(0).getVersionCount(), 0);
        verify(eventCollector).report(argThat(e -> e.type() == EventType.MQTT_SESSION_START));
    }

    @Test(groups = "integration")
    public void attachWithLWT() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("lastWill").setDelaySeconds(5).build();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchCreateRequest.Params createParams = BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setClient(client)
                .setNow(now)
                .build();
        requestCreate(createParams);

        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setVersion(0)
                .setKeepAliveSeconds(1)
                .setExpirySeconds(1)
                .setLwt(lwt)
                .setClient(client)
                .setNow(now)
                .build();
        BatchAttachReply.Result result = requestAttach(attachParams).get(0);
        assertSame(result.getCode(), BatchAttachReply.Code.OK);

        BatchGetRequest.Params getParams = BatchGetRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setNow(now)
                .build();
        InboxVersion inboxVersion = requestGet(getParams).get(0).getVersion(0);
        assertEquals(inboxVersion.getVersion(), 1);
        assertEquals(inboxVersion.getKeepAliveSeconds(), 1);
        assertEquals(inboxVersion.getExpirySeconds(), 1);
        assertEquals(inboxVersion.getLwt(), lwt);

        getParams = BatchGetRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setNow(now + Duration.ofSeconds(3).toMillis())
                .build();
        assertEquals(requestGet(getParams).get(0).getVersionCount(), 0);

        verify(eventCollector).report(argThat(e -> e.type() == EventType.MQTT_SESSION_START));
    }

    @Test(groups = "integration")
    public void detachNoInbox() {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        BatchDetachRequest.Params detachParams = BatchDetachRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setVersion(0)
                .setExpirySeconds(5)
                .setNow(now)
                .build();
        BatchDetachReply.Result result = requestDetach(detachParams).get(0);
        assertEquals(result.getCode(), BatchDetachReply.Code.NO_INBOX);
    }

    @Test(groups = "integration")
    public void detachConflict() {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchCreateRequest.Params createParams = BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setClient(client)
                .setNow(now)
                .build();
        requestCreate(createParams);

        BatchDetachRequest.Params detachParams = BatchDetachRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setVersion(1)
                .setExpirySeconds(5)
                .setNow(now)
                .build();
        BatchDetachReply.Result result = requestDetach(detachParams).get(0);
        assertEquals(result.getCode(), BatchDetachReply.Code.CONFLICT);

        verify(eventCollector).report(argThat(e -> e.type() == EventType.MQTT_SESSION_START));
    }

    @Test(groups = "integration")
    public void detach() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        LWT lwt = LWT.newBuilder().setTopic("lastWill").build();
        BatchCreateRequest.Params createParams = BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setLwt(lwt)
                .setClient(client)
                .setNow(now)
                .build();
        requestCreate(createParams);

        now = Duration.ofMillis(now).plusSeconds(5).toMillis();
        BatchDetachRequest.Params detachParams = BatchDetachRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setVersion(0)
                .setExpirySeconds(1)
                .setNow(now)
                .build();
        BatchDetachReply.Result result = requestDetach(detachParams).get(0);
        assertSame(result.getCode(), BatchDetachReply.Code.OK);
        assertEquals(result.getLwt(), lwt);

        BatchGetRequest.Params getParams = BatchGetRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setNow(now)
                .build();
        InboxVersion inboxVersion = requestGet(getParams).get(0).getVersion(0);
        assertEquals(inboxVersion.getVersion(), 1);
        assertEquals(inboxVersion.getKeepAliveSeconds(), 5);
        assertEquals(inboxVersion.getExpirySeconds(), 1);

        getParams = BatchGetRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setNow(now + Duration.ofSeconds(12).toMillis())
                .build();
        assertEquals(requestGet(getParams).get(0).getVersionCount(), 0);

        verify(eventCollector).report(argThat(e -> e.type() == EventType.MQTT_SESSION_START));
    }

    @Test(groups = "integration")
    public void detachAndDiscardLWT() {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        LWT lwt = LWT.newBuilder().setTopic("lastWill").build();
        BatchCreateRequest.Params createParams = BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setLwt(lwt)
                .setClient(client)
                .setNow(now)
                .build();
        requestCreate(createParams);

        now = Duration.ofSeconds(5).toMillis();
        BatchDetachRequest.Params detachParams = BatchDetachRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setVersion(0)
                .setExpirySeconds(1)
                .setDiscardLWT(true)
                .setNow(now)
                .build();
        BatchDetachReply.Result result = requestDetach(detachParams).get(0);
        assertSame(result.getCode(), BatchDetachReply.Code.OK);
        assertFalse(result.hasLwt());

        verify(eventCollector).report(argThat(e -> e.type() == EventType.MQTT_SESSION_START));
    }

    @Test(groups = "integration")
    public void touchNoInbox() {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        BatchTouchRequest.Params touchParams = BatchTouchRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setVersion(0)
                .setNow(now)
                .build();
        BatchTouchReply.Code code = requestTouch(touchParams).get(0);
        assertEquals(code, BatchTouchReply.Code.NO_INBOX);
    }

    @Test(groups = "integration")
    public void touchConflict() {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchCreateRequest.Params createParams = BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setClient(client)
                .setNow(now)
                .build();
        requestCreate(createParams);

        BatchTouchRequest.Params touchParams = BatchTouchRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setVersion(1)
                .setNow(now)
                .build();
        BatchTouchReply.Code code = requestTouch(touchParams).get(0);
        assertEquals(code, BatchTouchReply.Code.CONFLICT);

        verify(eventCollector).report(argThat(e -> e.type() == EventType.MQTT_SESSION_START));
    }

    @Test(groups = "integration")
    public void touch() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        LWT lwt = LWT.newBuilder().setTopic("lastWill").build();
        BatchCreateRequest.Params createParams = BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setLwt(lwt)
                .setClient(client)
                .setNow(now)
                .build();
        requestCreate(createParams);

        now = Duration.ofMillis(now).plusSeconds(5).toMillis();
        BatchTouchRequest.Params touchParams = BatchTouchRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setVersion(0)
                .setNow(now)
                .build();
        BatchTouchReply.Code code = requestTouch(touchParams).get(0);
        assertEquals(code, BatchTouchReply.Code.OK);

        BatchGetRequest.Params getParams = BatchGetRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setNow(now)
                .build();
        InboxVersion inboxVersion = requestGet(getParams).get(0).getVersion(0);
        assertEquals(inboxVersion.getVersion(), 0);
        assertEquals(inboxVersion.getKeepAliveSeconds(), 5);
        assertEquals(inboxVersion.getExpirySeconds(), 5);

        getParams = BatchGetRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setNow(now + Duration.ofSeconds(13).toMillis())
                .build();
        assertEquals(requestGet(getParams).get(0).getVersionCount(), 0);

        verify(eventCollector).report(argThat(e -> e.type() == EventType.MQTT_SESSION_START));
    }

    @Test(groups = "integration")
    public void deleteNoInbox() {
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        BatchDeleteRequest.Params deleteParams = BatchDeleteRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setVersion(0)
                .build();
        BatchDeleteReply.Result result = requestDelete(deleteParams).get(0);
        assertEquals(result.getCode(), BatchDeleteReply.Code.NO_INBOX);
    }

    @Test(groups = "integration")
    public void deleteConflict() {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchCreateRequest.Params createParams = BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setClient(client)
                .setNow(now)
                .build();
        requestCreate(createParams);

        BatchDeleteRequest.Params deleteParams = BatchDeleteRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setVersion(1)
                .build();
        BatchDeleteReply.Result result = requestDelete(deleteParams).get(0);
        assertEquals(result.getCode(), BatchDeleteReply.Code.CONFLICT);
    }

    @Test(groups = "integration")
    public void delete() {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        LWT lwt = LWT.newBuilder().setTopic("lastWill").build();
        BatchCreateRequest.Params createParams = BatchCreateRequest.Params.newBuilder()
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setKeepAliveSeconds(5)
                .setExpirySeconds(5)
                .setLwt(lwt)
                .setClient(client)
                .setNow(now)
                .build();
        requestCreate(createParams);
        // TODO: make some sub and verify the return values
        verify(eventCollector).report(argThat(e -> e.type() == EventType.MQTT_SESSION_START));

        reset(eventCollector);
        BatchDeleteRequest.Params deleteParams = BatchDeleteRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setVersion(0)
                .build();
        BatchDeleteReply.Result result = requestDelete(deleteParams).get(0);

        verify(eventCollector).report(argThat(e -> e.type() == EventType.MQTT_SESSION_STOP));

        assertEquals(result.getCode(), BatchDeleteReply.Code.OK);

        BatchGetRequest.Params getParams = BatchGetRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setNow(now)
                .build();
        assertEquals(requestGet(getParams).get(0).getVersionCount(), 0);
    }
}
