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

import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicFiltersPerInbox;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.inbox.storage.proto.BatchCreateRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchDeleteReply;
import com.baidu.bifromq.inbox.storage.proto.BatchDeleteRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchSubReply;
import com.baidu.bifromq.inbox.storage.proto.BatchSubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubReply;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubRequest;
import com.baidu.bifromq.type.ClientInfo;
import java.util.List;
import org.testng.annotations.Test;

public class InboxSubUnsubTest extends InboxStoreTest {
    @Test(groups = "integration")
    public void subNoInbox() {
        long now = 0;
        String tenantId = "tenantId";
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
        long incarnation = System.nanoTime();
        BatchSubRequest.Params subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        BatchSubReply.Code code = requestSub(subParams).get(0);
        assertEquals(code, BatchSubReply.Code.NO_INBOX);
    }

    @Test(groups = "integration")
    public void subConflict() {
        long now = 0;
        String tenantId = "tenantId";
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
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

        BatchSubRequest.Params subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(1)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        BatchSubReply.Code code = requestSub(subParams).get(0);
        assertEquals(code, BatchSubReply.Code.CONFLICT);
    }

    @Test(groups = "integration")
    public void sub() {
        long now = 0;
        String tenantId = "tenantId";
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
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

        when(settingProvider.provide(MaxTopicFiltersPerInbox, tenantId)).thenReturn(10);
        BatchSubRequest.Params subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        BatchSubReply.Code code = requestSub(subParams).get(0);
        assertEquals(code, BatchSubReply.Code.OK);

        subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        code = requestSub(subParams).get(0);
        assertEquals(code, BatchSubReply.Code.EXISTS);
    }

    @Test(groups = "integration")
    public void subAndDelete() {
        long now = 0;
        String tenantId = "tenantId";
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
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

        BatchSubRequest.Params subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        BatchSubReply.Code code = requestSub(subParams).get(0);
        assertEquals(code, BatchSubReply.Code.OK);

        BatchDeleteRequest.Params deleteParams = BatchDeleteRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .build();
        BatchDeleteReply.Result result = requestDelete(deleteParams).get(0);
        assertEquals(result.getCode(), BatchDeleteReply.Code.OK);
        assertEquals(result.getTopicFiltersList(), List.of(topicFilter));
    }

    @Test(groups = "integration")
    public void unsubNoInbox() {
        long now = 0;
        String tenantId = "tenantId";
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
        long incarnation = System.nanoTime();
        BatchUnsubRequest.Params unsubParams = BatchUnsubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        BatchUnsubReply.Code code = requestUnsub(unsubParams).get(0);
        assertEquals(code, BatchUnsubReply.Code.NO_INBOX);
    }

    @Test(groups = "integration")
    public void unsubConflict() {
        long now = 0;
        String tenantId = "tenantId";
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
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

        BatchUnsubRequest.Params unsubParams = BatchUnsubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(1)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        BatchUnsubReply.Code code = requestUnsub(unsubParams).get(0);
        assertEquals(code, BatchUnsubReply.Code.CONFLICT);
    }

    @Test(groups = "integration")
    public void unsubNoSub() {
        long now = 0;
        String tenantId = "tenantId";
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
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

        BatchUnsubRequest.Params unsubParams = BatchUnsubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        BatchUnsubReply.Code code = requestUnsub(unsubParams).get(0);
        assertEquals(code, BatchUnsubReply.Code.NO_SUB);
    }

    @Test(groups = "integration")
    public void unsub() {
        long now = 0;
        String tenantId = "tenantId";
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
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

        when(settingProvider.provide(MaxTopicFiltersPerInbox, tenantId)).thenReturn(10);
        BatchSubRequest.Params subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        requestSub(subParams);

        BatchUnsubRequest.Params unsubParams = BatchUnsubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        BatchUnsubReply.Code code = requestUnsub(unsubParams).get(0);
        assertEquals(code, BatchUnsubReply.Code.OK);
    }

    @Test(groups = "integration")
    public void subExceedLimit() {
        long now = 0;
        String tenantId = "tenantId";
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
        String topicFilter1 = "/a";
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

        when(settingProvider.provide(MaxTopicFiltersPerInbox, tenantId)).thenReturn(1);
        BatchSubRequest.Params subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        requestSub(subParams);

        subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter1)
            .setNow(now)
            .build();
        BatchSubReply.Code code = requestSub(subParams).get(0);
        assertEquals(code, BatchSubReply.Code.EXCEED_LIMIT);

        BatchUnsubRequest.Params unsubParams = BatchUnsubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        requestUnsub(unsubParams);

        // sub again
        subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter1)
            .setNow(now)
            .build();
        code = requestSub(subParams).get(0);
        assertEquals(code, BatchSubReply.Code.OK);
    }
}
