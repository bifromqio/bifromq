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

package com.baidu.bifromq.sessiondict.client;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.sessiondict.SessionRegisterKeyUtil;
import com.baidu.bifromq.sessiondict.rpc.proto.GetReply;
import com.baidu.bifromq.sessiondict.rpc.proto.GetRequest;
import com.baidu.bifromq.sessiondict.rpc.proto.KillAllReply;
import com.baidu.bifromq.sessiondict.rpc.proto.KillAllRequest;
import com.baidu.bifromq.sessiondict.rpc.proto.KillReply;
import com.baidu.bifromq.sessiondict.rpc.proto.KillRequest;
import com.baidu.bifromq.sessiondict.rpc.proto.SessionDictServiceGrpc;
import com.baidu.bifromq.sessiondict.rpc.proto.SubReply;
import com.baidu.bifromq.sessiondict.rpc.proto.SubRequest;
import com.baidu.bifromq.sessiondict.rpc.proto.UnsubReply;
import com.baidu.bifromq.sessiondict.rpc.proto.UnsubRequest;
import com.baidu.bifromq.type.ClientInfo;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.MoreExecutors;
import io.reactivex.rxjava3.core.Observable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class SessionDictClient implements ISessionDictClient {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final IRPCClient rpcClient;
    private final LoadingCache<String, SessionRegPipeline> regPipeline;

    SessionDictClient(IRPCClient rpcClient) {
        this.rpcClient = rpcClient;
        regPipeline = Caffeine.newBuilder()
            .weakValues()
            .executor(MoreExecutors.directExecutor())
            .build(registerKey -> new SessionRegPipeline(registerKey, rpcClient));
    }

    @Override
    public Observable<IRPCClient.ConnState> connState() {
        return rpcClient.connState();
    }

    @Override
    public ISessionRegister reg(ClientInfo owner, Consumer<ClientInfo> onKick) {
        return new SessionRegister(owner, onKick, regPipeline.get(SessionRegisterKeyUtil.toRegisterKey(owner)));
    }

    @Override
    public CompletableFuture<KillReply> kill(long reqId,
                                             String tenantId,
                                             String userId,
                                             String clientId,
                                             ClientInfo killer) {
        return rpcClient.invoke(tenantId, null, KillRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setUserId(userId)
                .setClientId(clientId)
                .setKiller(killer).build(), SessionDictServiceGrpc.getKillMethod())
            .exceptionally(e -> KillReply.newBuilder()
                .setReqId(reqId)
                .setResult(KillReply.Result.ERROR)
                .build());
    }

    @Override
    public CompletableFuture<KillAllReply> killAll(long reqId,
                                                   String tenantId,
                                                   @Nullable String userId,
                                                   ClientInfo killer) {
        KillAllRequest.Builder reqBuilder = KillAllRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setKiller(killer);
        if (!Strings.isNullOrEmpty(userId)) {
            reqBuilder.setUserId(userId);
        }
        return (CompletableFuture<KillAllReply>) rpcClient.serverList()
            .firstElement()
            .map(Map::keySet)
            .toCompletionStage()
            .thenApply(servers ->
                servers.stream().map(serverId -> rpcClient.invoke(tenantId, serverId, reqBuilder.build(),
                    SessionDictServiceGrpc.getKillAllMethod())).toList())
            .thenCompose(killAllFutures -> CompletableFuture.allOf(killAllFutures.toArray(CompletableFuture[]::new))
                .thenApply(v -> killAllFutures.stream().map(CompletableFuture::join).toList()))
            .thenApply(killAllReplies -> {
                if (killAllReplies.stream()
                    .allMatch(reply -> reply.getResult() == KillAllReply.Result.OK)) {
                    return KillAllReply.newBuilder()
                        .setReqId(reqId)
                        .setResult(KillAllReply.Result.OK)
                        .build();
                } else {
                    return KillAllReply.newBuilder()
                        .setReqId(reqId)
                        .setResult(KillAllReply.Result.ERROR)
                        .build();
                }
            })
            .exceptionally(e -> {
                log.debug("Kill all failed", e);
                return KillAllReply.newBuilder()
                    .setReqId(reqId)
                    .setResult(KillAllReply.Result.ERROR)
                    .build();
            });

    }

    @Override
    public CompletableFuture<GetReply> get(GetRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request,
                SessionDictServiceGrpc.getGetMethod())
            .exceptionally(e -> {
                log.debug("Get failed", e);
                return GetReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(GetReply.Result.ERROR)
                    .build();
            });
    }

    @Override
    public CompletableFuture<SubReply> sub(SubRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request,
                SessionDictServiceGrpc.getSubMethod())
            .exceptionally(e -> {
                log.debug("Sub failed", e);
                return SubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(SubReply.Result.ERROR)
                    .build();
            });
    }

    @Override
    public CompletableFuture<UnsubReply> unsub(UnsubRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request,
                SessionDictServiceGrpc.getUnsubMethod())
            .exceptionally(e -> {
                log.debug("Unsub failed", e);
                return UnsubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(UnsubReply.Result.ERROR)
                    .build();
            });
    }


    @Override
    public void stop() {
        if (closed.compareAndSet(false, true)) {
            log.info("Stopping session dict client");
            regPipeline.asMap().forEach((k, v) -> v.close());
            regPipeline.invalidateAll();
            log.debug("Stopping rpc client");
            rpcClient.stop();
            log.info("Session dict client stopped");
        }
    }
}
