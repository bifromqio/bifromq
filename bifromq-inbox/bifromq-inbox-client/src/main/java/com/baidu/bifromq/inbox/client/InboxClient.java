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

import static com.baidu.bifromq.inbox.util.DelivererKeyUtil.getDelivererKey;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.inbox.RPCBluePrint;
import com.baidu.bifromq.inbox.rpc.proto.AddSubReply;
import com.baidu.bifromq.inbox.rpc.proto.AddSubRequest;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.DeleteInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.DeleteInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import com.baidu.bifromq.inbox.rpc.proto.RemoveSubReply;
import com.baidu.bifromq.inbox.rpc.proto.RemoveSubRequest;
import com.baidu.bifromq.inbox.rpc.proto.TouchInboxRequest;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import com.google.common.base.Preconditions;
import io.reactivex.rxjava3.core.Observable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class InboxClient implements IInboxClient {
    private final AtomicBoolean hasStopped = new AtomicBoolean();
    private final IRPCClient rpcClient;

    InboxClient(InboxClientBuilder builder) {
        this.rpcClient = IRPCClient.newBuilder()
            .bluePrint(RPCBluePrint.INSTANCE)
            .executor(builder.executor)
            .eventLoopGroup(builder.eventLoopGroup)
            .crdtService(builder.crdtService)
            .sslContext(builder.sslContext)
            .build();
    }

    @Override
    public IDeliverer open(String delivererKey) {
        Preconditions.checkState(!hasStopped.get());
        return new InboxDeliverPipeline(delivererKey, rpcClient);
    }

    @Override
    public Observable<IRPCClient.ConnState> connState() {
        return rpcClient.connState();
    }

    @Override
    public IInboxReader openInboxReader(String inboxId, ClientInfo clientInfo) {
        return new InboxFetchPipeline(inboxId, getDelivererKey(inboxId), clientInfo, rpcClient);
    }

    @Override
    public CompletableFuture<InboxCheckResult> has(long reqId, String inboxId, ClientInfo clientInfo) {
        return rpcClient.invoke(clientInfo.getTenantId(), null, HasInboxRequest.newBuilder()
                .setReqId(reqId)
                .setInboxId(inboxId)
                .setClientInfo(clientInfo)
                .build(), InboxServiceGrpc.getHasInboxMethod())
            .thenApply(v -> InboxCheckResult.values()[v.getResult().ordinal()]);
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
            .exceptionally(e -> DeleteInboxReply.newBuilder()
                .setReqId(reqId)
                .setResult(DeleteInboxReply.Result.ERROR)
                .build());
    }

    @Override
    public CompletableFuture<Void> touch(long reqId, String tenantId, String inboxId) {
        return rpcClient.invoke(tenantId, null, TouchInboxRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .build(), InboxServiceGrpc.getTouchInboxMethod())
            .handle((v, e) -> {
                if (e != null) {
                    log.error("Touch inbox failed: inboxId={}", inboxId, e);
                }
                return null;
            });
    }

    @Override
    public CompletableFuture<InboxSubResult> sub(long reqId, String inboxId, String topicFilter, QoS qos,
                                                 ClientInfo clientInfo) {
        return rpcClient.invoke(clientInfo.getTenantId(), null, AddSubRequest.newBuilder()
                .setReqId(reqId)
                .setInboxId(inboxId)
                .setTopicFilter(topicFilter)
                .setSubQoS(qos)
                .setClientInfo(clientInfo)
                .build(), InboxServiceGrpc.getAddSubMethod())
            .exceptionally(e -> AddSubReply.newBuilder()
                .setReqId(reqId)
                .setResult(AddSubReply.Result.ERROR)
                .build())
            .thenApply(v -> InboxSubResult.values()[v.getResult().ordinal()]);
    }

    @Override
    public CompletableFuture<InboxUnsubResult> unsub(long reqId, String inboxId, String topicFilter,
                                                     ClientInfo clientInfo) {
        return rpcClient.invoke(clientInfo.getTenantId(), null, RemoveSubRequest.newBuilder()
                .setReqId(reqId)
                .setInboxId(inboxId)
                .setTopicFilter(topicFilter)
                .setClientInfo(clientInfo)
                .build(), InboxServiceGrpc.getRemoveSubMethod())
            .exceptionally(e -> RemoveSubReply.newBuilder()
                .setReqId(reqId)
                .setResult(RemoveSubReply.Result.ERROR)
                .build())
            .thenApply(v -> InboxUnsubResult.values()[v.getResult().ordinal()]);
    }

    @Override
    public void close() {
        if (hasStopped.compareAndSet(false, true)) {
            log.info("Closing inbox client");
            log.debug("Stopping rpc client");
            rpcClient.stop();
            log.info("Inbox client closed");
        }
    }
}
