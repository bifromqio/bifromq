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

package com.baidu.bifromq.inbox.client;

import static com.baidu.bifromq.sysprops.BifroMQSysProp.INBOX_DELIVERERS;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.DeleteInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.DeleteInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import com.baidu.bifromq.type.ClientInfo;
import io.reactivex.rxjava3.core.Observable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class InboxReaderClient implements IInboxReaderClient {
    private static final int INBOX_GROUPS = INBOX_DELIVERERS.get();

    private final IRPCClient rpcClient;
    private final AtomicBoolean closed = new AtomicBoolean(false);


    InboxReaderClient(@NonNull IRPCClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    @Override
    public IInboxReader openInboxReader(String inboxId, String delivererKey, ClientInfo clientInfo) {
        return new InboxReaderPipeline(inboxId, delivererKey, clientInfo, rpcClient);
    }

    @Override
    public Observable<IRPCClient.ConnState> connState() {
        return rpcClient.connState();
    }

    @Override
    public CompletableFuture<Boolean> has(long reqId, String inboxId, ClientInfo clientInfo) {
        return rpcClient.invoke(clientInfo.getTenantId(), null, HasInboxRequest.newBuilder()
                .setReqId(reqId)
                .setInboxId(inboxId)
                .setClientInfo(clientInfo)
                .build(), InboxServiceGrpc.getHasInboxMethod())
            .thenApply(HasInboxReply::getResult);
    }

    @Override
    public CompletableFuture<CreateInboxReply> create(long reqId, String inboxId, ClientInfo clientInfo) {
        return rpcClient.invoke(clientInfo.getTenantId(), null, CreateInboxRequest.newBuilder()
                .setReqId(reqId)
                .setInboxId(inboxId)
                .setClientInfo(clientInfo)
                .build(), InboxServiceGrpc.getCreateInboxMethod())
            .exceptionally(e -> CreateInboxReply.newBuilder()
                .setReqId(reqId)
                .setResult(CreateInboxReply.Result.ERROR).build());
    }

    @Override
    public CompletableFuture<DeleteInboxReply> delete(long reqId, String inboxId, ClientInfo clientInfo) {
        return rpcClient.invoke(clientInfo.getTenantId(), null, DeleteInboxRequest.newBuilder()
                .setReqId(reqId)
                .setInboxId(inboxId)
                .setClientInfo(clientInfo)
                .build(), InboxServiceGrpc.getDeleteInboxMethod())
            .exceptionally(e -> {
                return DeleteInboxReply.newBuilder()
                    .setReqId(reqId)
                    .setResult(DeleteInboxReply.Result.ERROR)
                    .build();
            });
    }

    @Override
    public String getDelivererKey(String inboxId, ClientInfo clientInfo) {
        int k = inboxId.hashCode() % INBOX_GROUPS;
        if (k < 0) {
            k = (k + INBOX_GROUPS) % INBOX_GROUPS;
        }
        return k + "";
    }

    @Override
    public void stop() {
        if (closed.compareAndSet(false, true)) {
            log.info("Stopping inbox reader client");
            log.debug("Stopping rpc client");
            rpcClient.stop();
            log.info("Inbox reader client stopped");
        }
    }
}
