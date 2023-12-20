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

import com.baidu.bifromq.inbox.client.InboxCheckResult;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.DeleteInboxReply;
import com.baidu.bifromq.type.ClientInfo;
import org.testng.annotations.Test;

public class InboxCreateTest extends InboxServiceTest {

    private final String tenantId = "trafficA";
    private final String inboxId = "inbox1";
    private final long reqId = System.nanoTime();

    @Test(groups = "integration")
    public void create() {
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        assertEquals(inboxClient.has(reqId, tenantId, inboxId).join(), InboxCheckResult.NO_INBOX);

        CreateInboxReply createInboxReply = inboxClient.create(reqId, inboxId, clientInfo).join();
        assertEquals(createInboxReply.getReqId(), reqId);
        assertEquals(createInboxReply.getResult(), CreateInboxReply.Result.OK);

        assertEquals(inboxClient.has(reqId, tenantId, inboxId).join(), InboxCheckResult.EXIST);
    }

    @Test(groups = "integration", dependsOnMethods = "create")
    public void delete() {
        DeleteInboxReply deleteInboxReply = inboxClient.delete(reqId, tenantId, inboxId).join();
        assertEquals(deleteInboxReply.getReqId(), reqId);
        assertEquals(deleteInboxReply.getResult(), DeleteInboxReply.Result.OK);

        assertEquals(inboxClient.has(reqId, tenantId, inboxId).join(), InboxCheckResult.NO_INBOX);
    }
}
