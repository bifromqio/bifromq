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

package com.baidu.bifromq.dist.client;

import com.baidu.bifromq.baserpc.client.IRPCClient;
import com.baidu.bifromq.dist.client.scheduler.DistServerCallScheduler;
import com.baidu.bifromq.dist.client.scheduler.IDistServerCallScheduler;
import com.baidu.bifromq.dist.client.scheduler.PubCall;
import com.baidu.bifromq.dist.rpc.proto.DistServiceGrpc;
import com.baidu.bifromq.dist.rpc.proto.MatchRequest;
import com.baidu.bifromq.dist.rpc.proto.UnmatchRequest;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import io.reactivex.rxjava3.core.Observable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class DistClient implements IDistClient {
    private final IDistServerCallScheduler reqScheduler;
    private final IRPCClient rpcClient;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    DistClient(IRPCClient rpcClient) {
        this.rpcClient = rpcClient;
        reqScheduler = new DistServerCallScheduler(rpcClient);
    }

    @Override
    public Observable<IRPCClient.ConnState> connState() {
        return rpcClient.connState();
    }

    @Override
    public CompletableFuture<PubResult> pub(long reqId, String topic, Message message, ClientInfo publisher) {
        return reqScheduler.schedule(new PubCall(publisher, topic, message))
            .exceptionally(e -> {
                log.debug("Failed to pub", e);
                return PubResult.ERROR;
            });
    }

    @Override
    public CompletableFuture<MatchResult> addTopicMatch(long reqId,
                                                        String tenantId,
                                                        String topicFilter,
                                                        String receiverId,
                                                        String delivererKey,
                                                        int subBrokerId,
                                                        long incarnation) {
        MatchRequest request = MatchRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setTopicFilter(topicFilter)
            .setReceiverId(receiverId)
            .setDelivererKey(delivererKey)
            .setBrokerId(subBrokerId)
            .setIncarnation(incarnation)
            .build();
        log.trace("Handling match request:\n{}", request);
        return rpcClient.invoke(tenantId, null, request, DistServiceGrpc.getMatchMethod())
            .thenApply(v -> switch (v.getResult()) {
                case OK -> MatchResult.OK;
                case EXCEED_LIMIT -> MatchResult.EXCEED_LIMIT;
                case BACK_PRESSURE_REJECTED -> MatchResult.BACK_PRESSURE_REJECTED;
                default -> MatchResult.ERROR;
            })
            .exceptionally(e -> {
                log.debug("Failed to match", e);
                return MatchResult.ERROR;
            });
    }

    @Override
    public CompletableFuture<UnmatchResult> removeTopicMatch(long reqId,
                                                             String tenantId,
                                                             String topicFilter,
                                                             String receiverId,
                                                             String delivererKey,
                                                             int subBrokerId,
                                                             long incarnation) {
        UnmatchRequest request = UnmatchRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setTopicFilter(topicFilter)
            .setReceiverId(receiverId)
            .setDelivererKey(delivererKey)
            .setBrokerId(subBrokerId)
            .setIncarnation(incarnation)
            .build();
        log.trace("Handling unsub request:\n{}", request);
        return rpcClient.invoke(tenantId, null, request, DistServiceGrpc.getUnmatchMethod())
            .thenApply(v -> switch (v.getResult()) {
                case OK -> UnmatchResult.OK;
                case NOT_EXISTED -> UnmatchResult.NOT_EXISTED;
                case BACK_PRESSURE_REJECTED -> UnmatchResult.BACK_PRESSURE_REJECTED;
                default -> UnmatchResult.ERROR;
            })
            .exceptionally(e -> {
                log.debug("Failed to unmatch", e);
                return UnmatchResult.ERROR;
            });
    }

    @Override
    public void close() {
        // close tenant logger and drain logs before closing the dist client
        if (closed.compareAndSet(false, true)) {
            log.debug("Stopping dist client");
            log.debug("Closing request scheduler");
            reqScheduler.close();
            log.debug("Stopping rpc client");
            rpcClient.stop();
            log.debug("Dist client stopped");
        }
    }
}
