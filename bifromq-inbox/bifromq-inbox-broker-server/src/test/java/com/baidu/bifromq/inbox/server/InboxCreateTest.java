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

package com.baidu.bifromq.inbox.server;

import static org.junit.Assert.assertEquals;

import com.baidu.bifromq.inbox.rpc.proto.CreateInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.DeleteInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxReply;
import com.baidu.bifromq.type.ClientInfo;
import org.junit.Test;

public class InboxCreateTest extends InboxServiceTest {

    @Test
    public void create() {
        String trafficId = "trafficA";
        String inboxId = "inbox1";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTrafficId(trafficId).build();
        long reqId = System.nanoTime();
        HasInboxReply hasInboxReply = inboxReaderClient.has(reqId, inboxId, clientInfo).join();
        assertEquals(reqId, hasInboxReply.getReqId());
        assertEquals(HasInboxReply.Result.NO, hasInboxReply.getResult());

        CreateInboxReply createInboxReply = inboxReaderClient.create(reqId, inboxId, clientInfo).join();
        assertEquals(reqId, createInboxReply.getReqId());
        assertEquals(CreateInboxReply.Result.OK, createInboxReply.getResult());

        hasInboxReply = inboxReaderClient.has(reqId, inboxId, clientInfo).join();
        assertEquals(reqId, hasInboxReply.getReqId());
        assertEquals(HasInboxReply.Result.YES, hasInboxReply.getResult());
    }

    @Test
    public void delete() {
        String trafficId = "trafficA";
        String inboxId = "inbox1";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTrafficId(trafficId).build();
        long reqId = System.nanoTime();

        CreateInboxReply createInboxReply = inboxReaderClient.create(reqId, inboxId, clientInfo).join();
        assertEquals(reqId, createInboxReply.getReqId());
        assertEquals(CreateInboxReply.Result.OK, createInboxReply.getResult());

        DeleteInboxReply deleteInboxReply = inboxReaderClient.delete(reqId, inboxId, clientInfo).join();
        assertEquals(reqId, deleteInboxReply.getReqId());
        assertEquals(DeleteInboxReply.Result.OK, deleteInboxReply.getResult());

        HasInboxReply hasInboxReply = inboxReaderClient.has(reqId, inboxId, clientInfo).join();
        assertEquals(reqId, hasInboxReply.getReqId());
        assertEquals(HasInboxReply.Result.NO, hasInboxReply.getResult());
    }
}
