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

package com.baidu.bifromq.dist.server.scheduler;

import static com.baidu.bifromq.dist.entity.EntityUtil.toMatchRecordKey;
import static com.baidu.bifromq.dist.entity.EntityUtil.toQInboxId;
import static com.baidu.bifromq.dist.entity.EntityUtil.toScopedTopicFilter;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.CONTROL_PLANE_BURST_LATENCY_MS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.CONTROL_PLANE_TOLERABLE_LATENCY_MS;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.dist.rpc.proto.BatchMatchReply;
import com.baidu.bifromq.dist.rpc.proto.BatchMatchRequest;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcInput;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcOutput;
import com.baidu.bifromq.dist.rpc.proto.MatchReply;
import com.baidu.bifromq.dist.rpc.proto.MatchRequest;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MatchCallScheduler extends BatchCallScheduler<MatchRequest, MatchReply, KVRangeSetting>
    implements IMatchCallScheduler {
    private final IBaseKVStoreClient distWorkerClient;

    public MatchCallScheduler(IBaseKVStoreClient distWorkerClient) {
        super("dist_server_sub_batcher", Duration.ofMillis(CONTROL_PLANE_TOLERABLE_LATENCY_MS.get()),
            Duration.ofMillis(CONTROL_PLANE_BURST_LATENCY_MS.get()));
        this.distWorkerClient = distWorkerClient;
    }

    @Override
    protected Batcher<MatchRequest, MatchReply, KVRangeSetting> newBatcher(String name,
                                                                           long tolerableLatencyNanos,
                                                                           long burstLatencyNanos,
                                                                           KVRangeSetting range) {
        return new SubCallBatcher(name, tolerableLatencyNanos, burstLatencyNanos, range, distWorkerClient);
    }

    @Override
    protected Optional<KVRangeSetting> find(MatchRequest subCall) {
        return distWorkerClient.findByKey(rangeKey(subCall));
    }

    private ByteString rangeKey(MatchRequest call) {
        String qInboxId = toQInboxId(call.getBroker(), call.getInboxId(), call.getDelivererKey());
        return toMatchRecordKey(call.getTenantId(), call.getTopicFilter(), qInboxId);
    }

    private static class SubCallBatcher extends Batcher<MatchRequest, MatchReply, KVRangeSetting> {

        private class SubCallBatch implements IBatchCall<MatchRequest, MatchReply> {
            private final Queue<CallTask<MatchRequest, MatchReply>> batchedTasks = new ArrayDeque<>();
            private BatchMatchRequest.Builder reqBuilder = BatchMatchRequest.newBuilder();

            @Override
            public void reset() {
                reqBuilder = BatchMatchRequest.newBuilder();
            }

            @Override
            public void add(CallTask<MatchRequest, MatchReply> callTask) {
                MatchRequest subCall = callTask.call;
                batchedTasks.add(callTask);
                String qInboxId =
                    toQInboxId(subCall.getBroker(), subCall.getInboxId(), subCall.getDelivererKey());
                String scopedTopicFilter =
                    toScopedTopicFilter(subCall.getTenantId(), qInboxId, subCall.getTopicFilter());
                reqBuilder.putScopedTopicFilter(scopedTopicFilter, subCall.getSubQoS());
            }

            @Override
            public CompletableFuture<Void> execute() {
                long reqId = System.nanoTime();

                return distWorkerClient.execute(range.leader, KVRangeRWRequest.newBuilder()
                        .setReqId(reqId)
                        .setVer(range.ver)
                        .setKvRangeId(range.id)
                        .setRwCoProc(DistServiceRWCoProcInput.newBuilder()
                            .setBatchMatch(reqBuilder
                                .setReqId(reqId)
                                .build())
                            .build().toByteString())
                        .build())
                    .thenApply(reply -> {
                        if (reply.getCode() == ReplyCode.Ok) {
                            try {
                                return DistServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult()).getBatchMatch();
                            } catch (InvalidProtocolBufferException e) {
                                log.error("Unable to parse rw co-proc output", e);
                                throw new RuntimeException(e);
                            }
                        }
                        log.warn("Failed to exec rw co-proc[code={}]", reply.getCode());
                        throw new RuntimeException();
                    })
                    .handle((reply, e) -> {
                        if (e != null) {
                            CallTask<MatchRequest, MatchReply> callTask;
                            while ((callTask = batchedTasks.poll()) != null) {
                                callTask.callResult.complete(MatchReply.newBuilder()
                                    .setReqId(callTask.call.getReqId())
                                    .setResult(MatchReply.Result.ERROR)
                                    .build());
                            }
                        } else {
                            CallTask<MatchRequest, MatchReply> callTask;
                            while ((callTask = batchedTasks.poll()) != null) {
                                MatchRequest subCall = callTask.call;
                                String qInboxId =
                                    toQInboxId(subCall.getBroker(), subCall.getInboxId(),
                                        subCall.getDelivererKey());
                                String scopedTopicFilter =
                                    toScopedTopicFilter(subCall.getTenantId(), qInboxId, subCall.getTopicFilter());
                                BatchMatchReply.Result result =
                                    reply.getResultsOrDefault(scopedTopicFilter, BatchMatchReply.Result.ERROR);
                                callTask.callResult.complete(MatchReply.newBuilder()
                                    .setReqId(callTask.call.getReqId())
                                    .setResult(MatchReply.Result.forNumber(result.getNumber()))
                                    .build());
                            }
                        }
                        return null;
                    });
            }
        }

        private final IBaseKVStoreClient distWorkerClient;
        private final KVRangeSetting range;

        private SubCallBatcher(String name,
                               long tolerableLatencyNanos,
                               long burstLatencyNanos,
                               KVRangeSetting range,
                               IBaseKVStoreClient distWorkerClient) {
            super(range, name, tolerableLatencyNanos, burstLatencyNanos);
            this.distWorkerClient = distWorkerClient;
            this.range = range;
        }

        @Override
        protected IBatchCall<MatchRequest, MatchReply> newBatch() {
            return new SubCallBatch();
        }
    }
}
