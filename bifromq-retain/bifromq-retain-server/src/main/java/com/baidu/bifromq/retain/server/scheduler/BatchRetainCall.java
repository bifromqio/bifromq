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
import com.baidu.bifromq.basekv.client.scheduler.BatchMutationCall;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.retain.rpc.proto.BatchRetainRequest;
import com.baidu.bifromq.retain.rpc.proto.RetainMessage;
import com.baidu.bifromq.retain.rpc.proto.RetainMessagePack;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import com.baidu.bifromq.retain.rpc.proto.RetainRequest;
import com.baidu.bifromq.retain.rpc.proto.RetainResult;
import com.baidu.bifromq.retain.rpc.proto.RetainResultPack;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceRWCoProcInput;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

public class BatchRetainCall extends BatchMutationCall<RetainRequest, RetainReply> {
    protected BatchRetainCall(KVRangeId rangeId,
                              IBaseKVStoreClient distWorkerClient,
                              Duration pipelineExpiryTime) {
        super(rangeId, distWorkerClient, pipelineExpiryTime);
    }

    @Override
    protected RWCoProcInput makeBatch(Iterator<RetainRequest> retainRequestIterator) {
        Map<String, RetainMessagePack.Builder> retainMsgPackBuilders = new HashMap<>(128);
        retainRequestIterator.forEachRemaining(request -> {
            retainMsgPackBuilders.computeIfAbsent(request.getPublisher().getTenantId(),
                    k -> RetainMessagePack.newBuilder())
                .putTopicMessages(request.getTopic(), RetainMessage.newBuilder()
                    .setMessage(request.getMessage())
                    .setPublisher(request.getPublisher())
                    .build());
        });
        long reqId = System.nanoTime();
        BatchRetainRequest.Builder reqBuilder = BatchRetainRequest.newBuilder().setReqId(reqId);
        retainMsgPackBuilders.forEach((tenantId, retainMsgPackBuilder) ->
            reqBuilder.putRetainMessagePack(tenantId, retainMsgPackBuilder.build()));

        return RWCoProcInput.newBuilder()
            .setRetainService(RetainServiceRWCoProcInput.newBuilder()
                .setBatchRetain(reqBuilder.build())
                .build()).build();
    }

    @Override
    protected void handleOutput(Queue<CallTask<RetainRequest, RetainReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        CallTask<RetainRequest, RetainReply, MutationCallBatcherKey> task;
        while ((task = batchedTasks.poll()) != null) {
            RetainReply.Builder replyBuilder = RetainReply.newBuilder()
                .setReqId(task.call.getReqId());
            RetainResult result = output.getRetainService()
                .getBatchRetain()
                .getResultsMap()
                .getOrDefault(task.call.getPublisher().getTenantId(),
                    RetainResultPack.getDefaultInstance())
                .getResultsOrDefault(task.call.getTopic(), RetainResult.ERROR);
            switch (result) {
                case RETAINED -> replyBuilder.setResult(RetainReply.Result.RETAINED);
                case CLEARED -> replyBuilder.setResult(RetainReply.Result.CLEARED);
                case ERROR -> replyBuilder.setResult(RetainReply.Result.ERROR);
            }
            task.callResult.complete(replyBuilder.build());
        }
    }

    @Override
    protected void handleException(CallTask<RetainRequest, RetainReply, MutationCallBatcherKey> callTask, Throwable e) {
        callTask.callResult.complete(RetainReply.newBuilder()
            .setReqId(callTask.call.getReqId())
            .setResult(RetainReply.Result.ERROR)
            .build());
    }
}
