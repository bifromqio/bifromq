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

package com.baidu.bifromq.retain.server.scheduler;

import static com.baidu.bifromq.retain.utils.KeyUtil.tenantNS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DATA_PLANE_BURST_LATENCY_MS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DATA_PLANE_TOLERABLE_LATENCY_MS;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.retain.rpc.proto.BatchMatchCoProcRequest;
import com.baidu.bifromq.retain.rpc.proto.MatchParam;
import com.baidu.bifromq.retain.rpc.proto.MatchReply;
import com.baidu.bifromq.retain.rpc.proto.MatchRequest;
import com.baidu.bifromq.retain.rpc.proto.MatchResult;
import com.baidu.bifromq.retain.rpc.proto.MatchResultPack;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceROCoProcInput;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceROCoProcOutput;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MatchCallScheduler extends BatchCallScheduler<MatchRequest, MatchReply, KVRangeSetting>
    implements IMatchCallScheduler {
    private final IBaseKVStoreClient retainStoreClient;

    public MatchCallScheduler(IBaseKVStoreClient retainStoreClient) {
        super("retain_server_match_batcher", Duration.ofMillis(DATA_PLANE_TOLERABLE_LATENCY_MS.get()),
            Duration.ofMillis(DATA_PLANE_BURST_LATENCY_MS.get()));
        this.retainStoreClient = retainStoreClient;
    }

    @Override
    protected Batcher<MatchRequest, MatchReply, KVRangeSetting> newBatcher(String name,
                                                                           long tolerableLatencyNanos,
                                                                           long burstLatencyNanos,
                                                                           KVRangeSetting range) {
        return new MatchCallBatcher(range, name, tolerableLatencyNanos, burstLatencyNanos, retainStoreClient);
    }

    @Override
    protected Optional<KVRangeSetting> find(MatchRequest matchRequest) {
        return retainStoreClient.findByKey(tenantNS(matchRequest.getTenantId()));
    }

    private static class MatchCallBatcher extends Batcher<MatchRequest, MatchReply, KVRangeSetting> {
        private class BatchMatchCall implements IBatchCall<MatchRequest, MatchReply> {
            private Queue<CallTask<MatchRequest, MatchReply>> batchedTasks = new ArrayDeque<>();
            private Map<String, MatchParam.Builder> matchParamBuilders = new HashMap<>(128);

            @Override
            public void add(CallTask<MatchRequest, MatchReply> task) {
                MatchRequest request = task.call;
                batchedTasks.add(task);
                matchParamBuilders.computeIfAbsent(request.getTenantId(), k -> MatchParam.newBuilder())
                    .putTopicFilters(request.getTopicFilter(), request.getLimit());
            }

            @Override
            public void reset() {
                matchParamBuilders = new HashMap<>(128);
            }

            @Override
            public CompletableFuture<Void> execute() {
                long reqId = System.nanoTime();
                BatchMatchCoProcRequest.Builder reqBuilder = BatchMatchCoProcRequest
                    .newBuilder()
                    .setReqId(reqId);
                matchParamBuilders.forEach((tenantId, matchParamsBuilder) ->
                    reqBuilder.putMatchParams(tenantId, matchParamsBuilder.build()));
                KVRangeRORequest request = KVRangeRORequest.newBuilder()
                    .setReqId(reqId)
                    .setVer(batcherKey.ver)
                    .setKvRangeId(batcherKey.id)
                    .setRoCoProcInput(RetainServiceROCoProcInput.newBuilder()
                        .setMatch(reqBuilder.build())
                        .build().toByteString())
                    .build();
                return retainStoreClient.linearizedQuery(selectStore(batcherKey), request)
                    .thenApply(reply -> {
                        if (reply.getCode() == ReplyCode.Ok) {
                            try {
                                return RetainServiceROCoProcOutput.parseFrom(reply.getRoCoProcResult()).getMatch()
                                    .getResultPackMap();
                            } catch (InvalidProtocolBufferException e) {
                                log.error("Unable to parse rw co-proc output", e);
                                throw new RuntimeException(e);
                            }
                        }
                        log.warn("Failed to exec ro co-proc[code={}]", reply.getCode());
                        throw new RuntimeException();
                    })
                    .handle((reply, e) -> {
                        if (e != null) {
                            CallTask<MatchRequest, MatchReply> task;
                            while ((task = batchedTasks.poll()) != null) {
                                task.callResult.complete(MatchReply.newBuilder()
                                    .setReqId(task.call.getReqId())
                                    .setResult(MatchReply.Result.ERROR)
                                    .build());
                            }
                        } else {
                            CallTask<MatchRequest, MatchReply> task;
                            while ((task = batchedTasks.poll()) != null) {
                                task.callResult.complete(MatchReply.newBuilder()
                                    .setReqId(task.call.getReqId())
                                    .setResult(MatchReply.Result.OK)
                                    .addAllMessages(reply.getOrDefault(task.call.getTenantId(),
                                            MatchResultPack.getDefaultInstance())
                                        .getResultsOrDefault(task.call.getTopicFilter(),
                                            MatchResult.getDefaultInstance()).getOk()
                                        .getMessagesList())
                                    .build());
                            }
                        }
                        return null;
                    });
            }

            private String selectStore(KVRangeSetting setting) {
                return setting.allReplicas.get(ThreadLocalRandom.current().nextInt(setting.allReplicas.size()));
            }
        }

        private final IBaseKVStoreClient retainStoreClient;

        protected MatchCallBatcher(KVRangeSetting range,
                                   String name,
                                   long tolerableLatencyNanos,
                                   long burstLatencyNanos,
                                   IBaseKVStoreClient retainStoreClient) {
            super(range, name, tolerableLatencyNanos, burstLatencyNanos);
            this.retainStoreClient = retainStoreClient;
        }

        @Override
        protected IBatchCall<MatchRequest, MatchReply> newBatch() {
            return new BatchMatchCall();
        }
    }
}
