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

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.BatchQueryCall;
import com.baidu.bifromq.basekv.client.scheduler.QueryCallBatcherKey;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ROCoProcOutput;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.retain.rpc.proto.BatchMatchRequest;
import com.baidu.bifromq.retain.rpc.proto.MatchParam;
import com.baidu.bifromq.retain.rpc.proto.MatchReply;
import com.baidu.bifromq.retain.rpc.proto.MatchRequest;
import com.baidu.bifromq.retain.rpc.proto.MatchResult;
import com.baidu.bifromq.retain.rpc.proto.MatchResultPack;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceROCoProcInput;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

public class BatchMatchCall extends BatchQueryCall<MatchRequest, MatchReply> {
    protected BatchMatchCall(KVRangeId rangeId,
                             IBaseKVStoreClient storeClient,
                             Duration pipelineExpiryTime) {
        super(rangeId, storeClient, true, pipelineExpiryTime);
    }

    @Override
    protected ROCoProcInput makeBatch(Iterator<MatchRequest> matchRequestIterator) {
        Map<String, MatchParam.Builder> matchParamBuilders = new HashMap<>(128);
        matchRequestIterator.forEachRemaining(request ->
            matchParamBuilders.computeIfAbsent(request.getTenantId(), k -> MatchParam.newBuilder())
                .putTopicFilters(request.getTopicFilter(), request.getLimit()));
        long reqId = System.nanoTime();
        BatchMatchRequest.Builder reqBuilder = BatchMatchRequest
            .newBuilder()
            .setReqId(reqId);
        matchParamBuilders.forEach((tenantId, matchParamsBuilder) ->
            reqBuilder.putMatchParams(tenantId, matchParamsBuilder.build()));

        return ROCoProcInput.newBuilder()
            .setRetainService(RetainServiceROCoProcInput.newBuilder()
                .setBatchMatch(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<CallTask<MatchRequest, MatchReply, QueryCallBatcherKey>> batchedTasks,
                                ROCoProcOutput output) {
        CallTask<MatchRequest, MatchReply, QueryCallBatcherKey> task;
        while ((task = batchedTasks.poll()) != null) {
            task.callResult.complete(MatchReply.newBuilder()
                .setReqId(task.call.getReqId())
                .setResult(MatchReply.Result.OK)
                .addAllMessages(output.getRetainService()
                    .getBatchMatch()
                    .getResultPackMap()
                    .getOrDefault(task.call.getTenantId(),
                        MatchResultPack.getDefaultInstance())
                    .getResultsOrDefault(task.call.getTopicFilter(),
                        MatchResult.getDefaultInstance()).getOk()
                    .getMessagesList())
                .build());
        }

    }

    @Override
    protected void handleException(CallTask<MatchRequest, MatchReply, QueryCallBatcherKey> callTask, Throwable e) {
        callTask.callResult.complete(MatchReply.newBuilder()
            .setReqId(callTask.call.getReqId())
            .setResult(MatchReply.Result.ERROR)
            .build());
    }
}
