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

import static com.baidu.bifromq.inbox.util.InboxServiceUtil.getDelivererKey;

import com.baidu.bifromq.baserpc.client.IRPCClient;
import com.baidu.bifromq.inbox.rpc.proto.AttachReply;
import com.baidu.bifromq.inbox.rpc.proto.AttachRequest;
import com.baidu.bifromq.inbox.rpc.proto.CommitReply;
import com.baidu.bifromq.inbox.rpc.proto.CommitRequest;
import com.baidu.bifromq.inbox.rpc.proto.CreateReply;
import com.baidu.bifromq.inbox.rpc.proto.CreateRequest;
import com.baidu.bifromq.inbox.rpc.proto.DetachReply;
import com.baidu.bifromq.inbox.rpc.proto.DetachRequest;
import com.baidu.bifromq.inbox.rpc.proto.ExpireAllReply;
import com.baidu.bifromq.inbox.rpc.proto.ExpireAllRequest;
import com.baidu.bifromq.inbox.rpc.proto.ExpireReply;
import com.baidu.bifromq.inbox.rpc.proto.ExpireRequest;
import com.baidu.bifromq.inbox.rpc.proto.GetReply;
import com.baidu.bifromq.inbox.rpc.proto.GetRequest;
import com.baidu.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import com.baidu.bifromq.inbox.rpc.proto.SubReply;
import com.baidu.bifromq.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.inbox.rpc.proto.TouchReply;
import com.baidu.bifromq.inbox.rpc.proto.TouchRequest;
import com.baidu.bifromq.inbox.rpc.proto.UnsubReply;
import com.baidu.bifromq.inbox.rpc.proto.UnsubRequest;
import com.baidu.bifromq.plugin.subbroker.CheckReply;
import com.baidu.bifromq.plugin.subbroker.CheckRequest;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.baidu.bifromq.type.MatchInfo;
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

    InboxClient(IRPCClient rpcClient) {
        this.rpcClient = rpcClient;
        fetchPipelineCache = Caffeine.newBuilder()
            .weakValues()
            .executor(MoreExecutors.directExecutor())
            .build(key -> new InboxFetchPipeline(key.tenantId, key.delivererKey, rpcClient));
    }

    @Override
    public CompletableFuture<CheckReply> check(CheckRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request, InboxServiceGrpc.getCheckSubscriptionsMethod())
            .exceptionally(e -> {
                log.debug("Failed to check subscription", e);
                CheckReply.Builder replyBuilder = CheckReply.newBuilder();
                for (MatchInfo matchInfo : request.getMatchInfoList()) {
                    replyBuilder.addCode(CheckReply.Code.ERROR);
                }
                return replyBuilder.build();
            });
    }

    @Override
    public IDeliverer open(String delivererKey) {
        Preconditions.checkState(!hasStopped.get());
        return new InboxDeliverPipeline(delivererKey, rpcClient);
    }

    @Override
    public Observable<ConnState> connState() {
        return rpcClient.connState();
    }

    @Override
    public IInboxReader openInboxReader(String tenantId, String inboxId, long incarnation) {
        return new InboxReader(inboxId, incarnation,
            fetchPipelineCache.get(new FetchPipelineKey(tenantId, getDelivererKey(tenantId, inboxId))));
    }

    @Override
    public CompletableFuture<CommitReply> commit(CommitRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request, InboxServiceGrpc.getCommitMethod())
            .exceptionally(e -> {
                log.debug("Failed to commit inbox", e);
                return CommitReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(CommitReply.Code.ERROR)
                    .build();
            });
    }

    @Override
    public CompletableFuture<GetReply> get(GetRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request, InboxServiceGrpc.getGetMethod())
            .exceptionally(e -> {
                log.debug("Failed to get inbox", e);
                return GetReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(GetReply.Code.ERROR)
                    .build();
            });
    }

    @Override
    public CompletableFuture<CreateReply> create(CreateRequest request) {
        return rpcClient.invoke(request.getClient().getTenantId(), null, request, InboxServiceGrpc.getCreateMethod())
            .exceptionally(e -> {
                log.debug("Failed to create inbox", e);
                return CreateReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(CreateReply.Code.ERROR).build();
            });
    }

    @Override
    public CompletableFuture<AttachReply> attach(AttachRequest request) {
        return rpcClient.invoke(request.getClient().getTenantId(), null, request, InboxServiceGrpc.getAttachMethod())
            .exceptionally(e -> {
                log.debug("Failed to attach inbox", e);
                return AttachReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(AttachReply.Code.ERROR).build();
            });
    }

    @Override
    public CompletableFuture<DetachReply> detach(DetachRequest request) {
        return rpcClient.invoke(request.getClient().getTenantId(), null, request, InboxServiceGrpc.getDetachMethod())
            .exceptionally(e -> {
                log.debug("Failed to attach inbox", e);
                return DetachReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(DetachReply.Code.ERROR).build();
            });
    }

    @Override
    public CompletableFuture<TouchReply> touch(TouchRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request, InboxServiceGrpc.getTouchMethod())
            .exceptionally(e -> {
                log.debug("Touch inbox failed", e);
                return TouchReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(TouchReply.Code.ERROR).build();
            });
    }

    @Override
    public CompletableFuture<SubReply> sub(SubRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request, InboxServiceGrpc.getSubMethod())
            .exceptionally(e -> {
                log.debug("Failed to sub inbox", e);
                return SubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(SubReply.Code.ERROR)
                    .build();
            });
    }

    @Override
    public CompletableFuture<UnsubReply> unsub(UnsubRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request, InboxServiceGrpc.getUnsubMethod())
            .exceptionally(e -> {
                log.debug("Failed to unsub inbox", e);
                return UnsubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(UnsubReply.Code.ERROR)
                    .build();
            });
    }

    @Override
    public CompletableFuture<ExpireReply> expire(ExpireRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request, InboxServiceGrpc.getExpireMethod())
            .exceptionally(e -> {
                log.debug("Failed to expire inbox", e);
                return ExpireReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(ExpireReply.Code.ERROR)
                    .build();
            });
    }

    @Override
    public CompletableFuture<ExpireAllReply> expireAll(ExpireAllRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request, InboxServiceGrpc.getExpireAllMethod())
            .exceptionally(e -> {
                log.debug("Failed to expire inboxes", e);
                return ExpireAllReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(ExpireAllReply.Code.ERROR)
                    .build();
            });
    }

    @Override
    public void close() {
        if (hasStopped.compareAndSet(false, true)) {
            log.debug("Closing inbox client");
            fetchPipelineCache.asMap().forEach((k, v) -> v.close());
            fetchPipelineCache.invalidateAll();
            log.debug("Stopping rpc client");
            rpcClient.stop();
            log.debug("Inbox client closed");
        }
    }

    private record FetchPipelineKey(String tenantId, String delivererKey) {
    }
}
