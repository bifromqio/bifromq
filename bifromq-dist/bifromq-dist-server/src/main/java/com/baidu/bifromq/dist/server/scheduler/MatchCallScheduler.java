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

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
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
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MatchCallScheduler extends MutateCallScheduler<MatchRequest, MatchReply> implements IMatchCallScheduler {

    public MatchCallScheduler(IBaseKVStoreClient distWorkerClient) {
        super("dist_server_sub_batcher", distWorkerClient);
    }

    @Override
    protected Batcher<MatchRequest, MatchReply, KVRangeSetting> newBatcher(String name,
                                                                           long tolerableLatencyNanos,
                                                                           long burstLatencyNanos,
                                                                           KVRangeSetting range) {
        return new SubCallBatcher(name, tolerableLatencyNanos, burstLatencyNanos, range, distWorkerClient);
    }

    protected ByteString rangeKey(MatchRequest call) {
        String qInboxId = toQInboxId(call.getBroker(), call.getInboxId(), call.getDelivererKey());
        return toMatchRecordKey(call.getTenantId(), call.getTopicFilter(), qInboxId);
    }

    private static class SubCallBatcher extends MutateCallBatcher<MatchRequest, MatchReply> {

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

                return mutate(reqId, DistServiceRWCoProcInput.newBuilder()
                    .setBatchMatch(reqBuilder
                        .setReqId(reqId)
                        .build())
                    .build())
                    .thenApply(DistServiceRWCoProcOutput::getBatchMatch)
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


        private SubCallBatcher(String name,
                               long tolerableLatencyNanos,
                               long burstLatencyNanos,
                               KVRangeSetting range,
                               IBaseKVStoreClient distWorkerClient) {
            super(name, tolerableLatencyNanos, burstLatencyNanos, range, distWorkerClient);
        }

        @Override
        protected IBatchCall<MatchRequest, MatchReply> newBatch() {
            return new SubCallBatch();
        }
    }
}
