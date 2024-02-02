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
import com.baidu.bifromq.retain.rpc.proto.MatchResult;
import com.baidu.bifromq.retain.rpc.proto.MatchResultPack;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceROCoProcInput;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

public class BatchMatchCall extends BatchQueryCall<MatchCall, MatchCallResult> {
    protected BatchMatchCall(KVRangeId rangeId,
                             IBaseKVStoreClient storeClient,
                             Duration pipelineExpiryTime) {
        super(rangeId, storeClient, true, pipelineExpiryTime);
    }

    @Override
    protected ROCoProcInput makeBatch(Iterator<MatchCall> matchRequestIterator) {
        Map<String, MatchParam.Builder> matchParamBuilders = new HashMap<>(128);
        matchRequestIterator.forEachRemaining(request ->
            matchParamBuilders.computeIfAbsent(request.tenantId(), k -> MatchParam.newBuilder())
                .putTopicFilters(request.topicFilter(), request.limit()));
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
    protected void handleOutput(Queue<CallTask<MatchCall, MatchCallResult, QueryCallBatcherKey>> batchedTasks,
                                ROCoProcOutput output) {
        CallTask<MatchCall, MatchCallResult, QueryCallBatcherKey> task;
        while ((task = batchedTasks.poll()) != null) {
            task.callResult.complete(new MatchCallResult(MatchReply.Result.OK, output.getRetainService()
                .getBatchMatch()
                .getResultPackMap()
                .getOrDefault(task.call.tenantId(),
                    MatchResultPack.getDefaultInstance())
                .getResultsOrDefault(task.call.topicFilter(),
                    MatchResult.getDefaultInstance()).getOk()
                .getMessagesList()));
        }
    }

    @Override
    protected void handleException(CallTask<MatchCall, MatchCallResult, QueryCallBatcherKey> callTask, Throwable e) {
        callTask.callResult.complete(new MatchCallResult(MatchReply.Result.ERROR, Collections.emptyList()));
    }
}
