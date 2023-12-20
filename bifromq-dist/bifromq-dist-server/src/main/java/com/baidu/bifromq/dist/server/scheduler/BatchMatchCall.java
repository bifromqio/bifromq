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

package com.baidu.bifromq.dist.server.scheduler;

import static com.baidu.bifromq.dist.entity.EntityUtil.toQInboxId;
import static com.baidu.bifromq.dist.entity.EntityUtil.toScopedTopicFilter;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.BatchMutationCall;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.dist.rpc.proto.BatchMatchReply;
import com.baidu.bifromq.dist.rpc.proto.BatchMatchRequest;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcInput;
import com.baidu.bifromq.dist.rpc.proto.MatchReply;
import com.baidu.bifromq.dist.rpc.proto.MatchRequest;
import java.time.Duration;
import java.util.Iterator;
import java.util.Queue;

public class BatchMatchCall extends BatchMutationCall<MatchRequest, MatchReply> {
    BatchMatchCall(KVRangeId rangeId,
                   IBaseKVStoreClient distWorkerClient,
                   Duration pipelineExpiryTime) {
        super(rangeId, distWorkerClient, pipelineExpiryTime);
    }

    @Override
    protected RWCoProcInput makeBatch(Iterator<MatchRequest> reqIterator) {
        BatchMatchRequest.Builder reqBuilder = BatchMatchRequest.newBuilder();
        while (reqIterator.hasNext()) {
            MatchRequest subCall = reqIterator.next();
            String qInboxId =
                toQInboxId(subCall.getBroker(), subCall.getInboxId(), subCall.getDelivererKey());
            String scopedTopicFilter =
                toScopedTopicFilter(subCall.getTenantId(), qInboxId, subCall.getTopicFilter());
            reqBuilder.putScopedTopicFilter(scopedTopicFilter, subCall.getSubQoS());
        }
        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setDistService(DistServiceRWCoProcInput.newBuilder()
                .setBatchMatch(reqBuilder
                    .setReqId(reqId)
                    .build())
                .build())
            .build();
    }

    @Override
    protected void handleException(CallTask<MatchRequest, MatchReply, MutationCallBatcherKey> callTask, Throwable e) {
        callTask.callResult.completeExceptionally(e);
    }

    @Override
    protected void handleOutput(Queue<CallTask<MatchRequest, MatchReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        CallTask<MatchRequest, MatchReply, MutationCallBatcherKey> callTask;
        while ((callTask = batchedTasks.poll()) != null) {
            BatchMatchReply reply = output.getDistService().getBatchMatch();
            MatchRequest subCall = callTask.call;
            String qInboxId = toQInboxId(subCall.getBroker(), subCall.getInboxId(), subCall.getDelivererKey());
            String scopedTopicFilter = toScopedTopicFilter(subCall.getTenantId(), qInboxId, subCall.getTopicFilter());
            BatchMatchReply.Result result = reply.getResultsOrDefault(scopedTopicFilter, BatchMatchReply.Result.ERROR);
            callTask.callResult.complete(MatchReply.newBuilder()
                .setReqId(callTask.call.getReqId())
                .setResult(MatchReply.Result.forNumber(result.getNumber()))
                .build());
        }
    }
}
