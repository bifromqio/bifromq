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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchDeleteRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchSubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubRequest;
import com.baidu.bifromq.inbox.storage.proto.CollectMetricsReply;
import com.baidu.bifromq.inbox.storage.proto.CollectMetricsRequest;
import com.baidu.bifromq.type.ClientInfo;
import org.testng.annotations.Test;

public class SubStatsTest extends InboxStoreTest {
    @Test(groups = "integration")
    public void collectAfterSub() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
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

        BatchSubRequest.Params subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        requestSub(subParams);

        long reqId = System.nanoTime();
        CollectMetricsReply reply = requestCollectMetrics(CollectMetricsRequest.newBuilder()
            .setReqId(reqId)
            .build());
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getSubCountsMap().get(tenantId), 1);
        assertTrue(reply.getSubUsedSpacesMap().get(tenantId) > 0);
    }

    @Test(groups = "integration")
    public void collectAfterSubExisting() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
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

        BatchSubRequest.Params subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        requestSub(subParams);
        requestSub(subParams);

        long reqId = System.nanoTime();
        CollectMetricsReply reply = requestCollectMetrics(CollectMetricsRequest.newBuilder()
            .setReqId(reqId)
            .build());
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getSubCountsMap().get(tenantId), 1);
        assertTrue(reply.getSubUsedSpacesMap().get(tenantId) > 0);
    }

    @Test(groups = "integration")
    public void collectAfterUnSub() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
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
        requestUnsub(unsubParams);

        long reqId = System.nanoTime();
        CollectMetricsReply reply = requestCollectMetrics(CollectMetricsRequest.newBuilder()
            .setReqId(reqId)
            .build());
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getSubCountsMap().get(tenantId), 0);
        assertEquals((long) reply.getSubUsedSpacesMap().get(tenantId), 0);
    }

    @Test(groups = "integration")
    public void collectAfterUnSubNonExists() {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
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
        requestUnsub(unsubParams);
        // unsub again
        requestUnsub(unsubParams);

        long reqId = System.nanoTime();
        CollectMetricsReply reply = requestCollectMetrics(CollectMetricsRequest.newBuilder()
            .setReqId(reqId)
            .build());
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getSubCountsMap().get(tenantId), 0);
        assertEquals((long) reply.getSubUsedSpacesMap().get(tenantId), 0);
    }

    @Test(groups = "integration")
    public void collectAfterDelete() {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        String inboxId1 = "inboxId1-" + System.nanoTime();
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
        createParams = BatchCreateRequest.Params.newBuilder()
            .setInboxId(inboxId1)
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
        requestSub(subParams);

        BatchDeleteRequest.Params deleteParams = BatchDeleteRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setVersion(0)
            .build();
        requestDelete(deleteParams);
        long reqId = System.nanoTime();
        CollectMetricsReply reply = requestCollectMetrics(CollectMetricsRequest.newBuilder()
            .setReqId(reqId)
            .build());
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getSubCountsMap().get(tenantId), 0);
        assertEquals((long) reply.getSubUsedSpacesMap().get(tenantId), 0);
    }
}
