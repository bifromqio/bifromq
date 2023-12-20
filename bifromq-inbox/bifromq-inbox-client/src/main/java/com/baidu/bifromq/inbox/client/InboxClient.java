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

import static com.baidu.bifromq.inbox.util.DelivererKeyUtil.getDelivererKey;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.inbox.RPCBluePrint;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.DeleteInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.DeleteInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.ExpireInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.ExpireInboxReply.Result;
import com.baidu.bifromq.inbox.rpc.proto.ExpireInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import com.baidu.bifromq.inbox.rpc.proto.SubReply;
import com.baidu.bifromq.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.inbox.rpc.proto.TouchInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.UnsubReply;
import com.baidu.bifromq.inbox.rpc.proto.UnsubRequest;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import io.reactivex.rxjava3.core.Observable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class InboxClient implements IInboxClient {
    private final AtomicBoolean hasStopped = new AtomicBoolean();
    private final IRPCClient rpcClient;
    private final LoadingCache<FetchPipelineKey, InboxFetchPipeline> fetchPipelineCache;

    InboxClient(InboxClientBuilder builder) {
        this.rpcClient = IRPCClient.newBuilder()
            .bluePrint(RPCBluePrint.INSTANCE)
            .executor(builder.executor)
            .eventLoopGroup(builder.eventLoopGroup)
            .crdtService(builder.crdtService)
            .sslContext(builder.sslContext)
            .build();
        fetchPipelineCache = Caffeine.newBuilder()
            .weakValues()
            .executor(MoreExecutors.directExecutor())
            .build(key -> new InboxFetchPipeline(key.tenantId, key.delivererKey, rpcClient));
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
    public IInboxReader openInboxReader(String tenantId, String inboxId) {
        return new InboxReader(inboxId,
            fetchPipelineCache.get(new FetchPipelineKey(tenantId, getDelivererKey(inboxId))));
    }

    @Override
    public CompletableFuture<InboxCheckResult> has(long reqId, String tenantId, String inboxId) {
        return rpcClient.invoke(tenantId, null, HasInboxRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .build(), InboxServiceGrpc.getHasInboxMethod())
            .thenApply(v -> InboxCheckResult.values()[v.getResult().ordinal()]);
    }

    @Override
    public CompletableFuture<CreateInboxReply> create(long reqId, String inboxId, ClientInfo owner) {
        return rpcClient.invoke(owner.getTenantId(), null, CreateInboxRequest.newBuilder()
                .setReqId(reqId)
                .setInboxId(inboxId)
                .setClientInfo(owner)
                .build(), InboxServiceGrpc.getCreateInboxMethod())
            .exceptionally(e -> {
                log.debug("Failed to create inbox", e);
                return CreateInboxReply.newBuilder()
                    .setReqId(reqId)
                    .setResult(CreateInboxReply.Result.ERROR).build();
            });
    }

    @Override
    public CompletableFuture<DeleteInboxReply> delete(long reqId, String tenantId, String inboxId) {
        return rpcClient.invoke(tenantId, null, DeleteInboxRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setInboxId(inboxId)
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
    public CompletableFuture<InboxSubResult> sub(long reqId,
                                                 String tenantId,
                                                 String inboxId,
                                                 String topicFilter,
                                                 QoS qos) {
        return rpcClient.invoke(tenantId, null, SubRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setTopicFilter(topicFilter)
                .setSubQoS(qos)
                .build(), InboxServiceGrpc.getSubMethod())
            .exceptionally(e -> SubReply.newBuilder()
                .setReqId(reqId)
                .setResult(SubReply.Result.ERROR)
                .build())
            .thenApply(v -> InboxSubResult.values()[v.getResult().ordinal()]);
    }

    @Override
    public CompletableFuture<InboxUnsubResult> unsub(long reqId, String tenantId, String inboxId, String topicFilter) {
        return rpcClient.invoke(tenantId, null, UnsubRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setTopicFilter(topicFilter)
                .build(), InboxServiceGrpc.getUnsubMethod())
            .exceptionally(e -> UnsubReply.newBuilder()
                .setReqId(reqId)
                .setResult(UnsubReply.Result.ERROR)
                .build())
            .thenApply(v -> InboxUnsubResult.values()[v.getResult().ordinal()]);
    }

    @Override
    public CompletableFuture<ExpireInboxReply> expireInbox(long reqId, String tenantId, int expirySeconds) {
        return rpcClient.invoke(tenantId, null, ExpireInboxRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setExpirySeconds(expirySeconds)
                .build(), InboxServiceGrpc.getExpireInboxMethod())
            .exceptionally(e -> ExpireInboxReply.newBuilder()
                .setReqId(reqId)
                .setResult(Result.ERROR)
                .build());
    }

    @Override
    public void close() {
        if (hasStopped.compareAndSet(false, true)) {
            log.info("Closing inbox client");
            fetchPipelineCache.invalidateAll();
            log.debug("Stopping rpc client");
            rpcClient.stop();
            log.info("Inbox client closed");
        }
    }

    private record FetchPipelineKey(String tenantId, String delivererKey) {
    }
}
