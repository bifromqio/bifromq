/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

import static com.baidu.bifromq.inbox.util.KeyUtil.scopedInboxId;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.inbox.storage.proto.BatchAddSubReply;
import com.baidu.bifromq.inbox.storage.proto.BatchRemoveSubReply;
import com.baidu.bifromq.inbox.storage.proto.CreateReply;
import com.baidu.bifromq.inbox.storage.proto.HasReply;
import com.baidu.bifromq.inbox.storage.proto.TouchReply;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.type.QoS;
import java.time.Clock;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InboxAdminTest extends InboxStoreTest {
    @Mock
    private Clock clock;

    @BeforeMethod(groups = "integration")
    public void resetClock() {
        when(clock.millis()).thenReturn(0L);
    }

    @Override
    protected Clock getClock() {
        return clock;
    }

    @Test(groups = "integration")
    public void createAndHasCheck() {
        String tenantId = "tenantId";
        String inboxId = "inboxId";
        HasReply has = requestHas(tenantId, inboxId);
        assertFalse(has.getExistsMap().get(scopedInboxId(tenantId, inboxId).toStringUtf8()));
        requestCreate(tenantId, inboxId, 10, 100, false);
        has = requestHas(tenantId, inboxId);
        assertTrue(has.getExistsMap().get(scopedInboxId(tenantId, inboxId).toStringUtf8()));
        requestDelete(tenantId, inboxId);
    }

    @Test(groups = "integration")
    public void createOverExpiredOne() {
        String tenantId = "tenantId";
        String inboxId = "createOverExpiredOne_inboxId";
        requestCreate(tenantId, inboxId, 10, 2, false);
        BatchAddSubReply.Result result = requestSub(tenantId, inboxId, "/a/b/c", QoS.EXACTLY_ONCE);
        assertEquals(result, BatchAddSubReply.Result.OK);
        when(clock.millis()).thenReturn(2100L);

        CreateReply reply = requestCreate(tenantId, inboxId, 10, 100, false);
        assertEquals(reply.getSubsCount(), 1);
        assertEquals(reply.getSubsMap().get(scopedInboxId(tenantId, inboxId).toStringUtf8()).getTopicFilters(0),
            "/a/b/c");
    }

    @Test(groups = "integration")
    public void deleteAndReturnSubs() {
        String tenantId = "tenantId";
        String inboxId = "deleteAndReturnSubs_inboxId";
        requestCreate(tenantId, inboxId, 10, 2, false);
        BatchAddSubReply.Result result = requestSub(tenantId, inboxId, "/a/b/c", QoS.EXACTLY_ONCE);
        assertEquals(result, BatchAddSubReply.Result.OK);
        when(clock.millis()).thenReturn(2100L);

        TouchReply reply = requestDelete(tenantId, inboxId);
        assertEquals(reply.getSubsCount(), 1);
        assertEquals(reply.getSubsMap().get(scopedInboxId(tenantId, inboxId).toStringUtf8()).getTopicFilters(0),
            "/a/b/c");
    }

    @Test(groups = "integration")
    public void subUnsub() {
        String tenantId = "tenantId";
        String inboxId = "subUnsub_inbox";
        assertEquals(requestSub(tenantId, inboxId, "/a/", QoS.AT_MOST_ONCE), BatchAddSubReply.Result.NO_INBOX);
        assertEquals(requestUnsub(tenantId, inboxId, "/a/"), BatchRemoveSubReply.Result.NO_INBOX);

        requestCreate(tenantId, inboxId, 10, 2, false);
        assertEquals(requestUnsub(tenantId, inboxId, "/a/"), BatchRemoveSubReply.Result.NO_SUB);
        assertEquals(requestSub(tenantId, inboxId, "/a/", QoS.AT_MOST_ONCE), BatchAddSubReply.Result.OK);
        assertEquals(requestUnsub(tenantId, inboxId, "/a/"), BatchRemoveSubReply.Result.OK);
    }

    @Test(groups = "integration")
    public void subExceedLimit() {
        String tenantId = "tenantId";
        String inboxId = "subExceedLimit_inboxId";
        when(settingProvider.provide(Setting.MaxTopicFiltersPerInbox, tenantId)).thenReturn(2);
        requestCreate(tenantId, inboxId, 10, 2, false);
        BatchAddSubReply.Result result = requestSub(tenantId, inboxId, "/a/b/c", QoS.EXACTLY_ONCE);
        assertEquals(result, BatchAddSubReply.Result.OK);

        // same topic filter
        result = requestSub(tenantId, inboxId, "/a/b/c", QoS.EXACTLY_ONCE);
        assertEquals(result, BatchAddSubReply.Result.OK);

        result = requestSub(tenantId, inboxId, "/a/b/", QoS.EXACTLY_ONCE);
        assertEquals(result, BatchAddSubReply.Result.OK);

        result = requestSub(tenantId, inboxId, "/a/", QoS.EXACTLY_ONCE);
        assertEquals(result, BatchAddSubReply.Result.EXCEED_LIMIT);

        requestUnsub(tenantId, inboxId, "/a/b/");

        result = requestSub(tenantId, inboxId, "/a/", QoS.EXACTLY_ONCE);
        assertEquals(result, BatchAddSubReply.Result.OK);
    }

    @Test(groups = "integration")
    public void expireAndHasCheck() {
        String tenantId = "tenantId";
        String inboxId = "expireAndHasCheck_inboxId";
        HasReply has = requestHas(tenantId, inboxId);
        assertFalse(has.getExistsMap().get(scopedInboxId(tenantId, inboxId).toStringUtf8()));
        requestCreate(tenantId, inboxId, 10, 2, false);
        has = requestHas(tenantId, inboxId);
        assertTrue(has.getExistsMap().get(scopedInboxId(tenantId, inboxId).toStringUtf8()));
        when(clock.millis()).thenReturn(2100L);
        has = requestHas(tenantId, inboxId);
        assertFalse(has.getExistsMap().get(scopedInboxId(tenantId, inboxId).toStringUtf8()));
        requestDelete(tenantId, inboxId);
    }

    @Test(groups = "integration")
    public void createAndDelete() {
        String tenantId = "tenantId";
        String inboxId1 = "createAndDelete_inboxId1";
        String inboxId2 = "createAndDelete_inboxId2";
        requestCreate(tenantId, inboxId1, 10, 10, false);
        requestCreate(tenantId, inboxId2, 10, 10, false);
        assertTrue(requestHas(tenantId, inboxId1).getExistsMap()
            .get(scopedInboxId(tenantId, inboxId1).toStringUtf8()));
        requestDelete(tenantId, inboxId1);

        assertFalse(requestHas(tenantId, inboxId1).getExistsMap()
            .get(scopedInboxId(tenantId, inboxId1).toStringUtf8()));
        assertTrue(requestHas(tenantId, inboxId2).getExistsMap()
            .get(scopedInboxId(tenantId, inboxId2).toStringUtf8()));
        requestDelete(tenantId, inboxId1);
        requestDelete(tenantId, inboxId2);
    }

    @Test(groups = "integration")
    public void deleteNonExist() {
        String tenantId = "tenantId";
        String inboxId = "deleteNonExist_inboxId";
        requestDelete(tenantId, inboxId);
        HasReply has = requestHas(tenantId, inboxId);
        assertFalse(has.getExistsMap().get(scopedInboxId(tenantId, inboxId).toStringUtf8()));
        requestDelete(tenantId, inboxId);
    }
}
