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

package com.baidu.bifromq.inbox.client;

import com.baidu.bifromq.baserpc.IConnectable;
import com.baidu.bifromq.inbox.rpc.proto.CommitReply;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.DeleteInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.ExpireInboxReply;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.plugin.subbroker.ISubBroker;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface IInboxClient extends ISubBroker, IConnectable {

    static InboxClientBuilder newBuilder() {
        return new InboxClientBuilder();
    }

    @Override
    default int id() {
        return 1;
    }

    CompletableFuture<InboxCheckResult> has(long reqId, String tenantId, String inboxId);

    CompletableFuture<CreateInboxReply> create(long reqId, String inboxId, ClientInfo owner);

    CompletableFuture<DeleteInboxReply> delete(long reqId, String tenantId, String inboxId);

    CompletableFuture<Void> touch(long reqId, String tenantId, String inboxId);

    CompletableFuture<InboxSubResult> sub(long reqId, String tenantId, String inboxId, String topicFilter, QoS qos);

    CompletableFuture<InboxUnsubResult> unsub(long reqId, String tenantId, String inboxId, String topicFilter);

    CompletableFuture<ExpireInboxReply> expireInbox(long reqId, String tenantId, int expirySeconds);

    IInboxReader openInboxReader(String tenantId, String inboxId);

    interface IInboxReader {
        void fetch(Consumer<Fetched> consumer);

        void hint(int bufferCapacity);

        void touch();

        CompletableFuture<CommitReply> commit(long reqId, QoS qos, long upToSeq);

        void close();
    }
}
