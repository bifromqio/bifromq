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

package com.baidu.bifromq.inbox.server.scheduler;

import static com.baidu.bifromq.inbox.util.KeyUtil.scopedTopicFilter;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.BatchMutationCall;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.inbox.rpc.proto.SubReply;
import com.baidu.bifromq.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchSubRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import java.time.Duration;
import java.util.Iterator;
import java.util.Queue;

public class BatchSubCall extends BatchMutationCall<SubRequest, SubReply> {
    protected BatchSubCall(KVRangeId rangeId,
                           IBaseKVStoreClient distWorkerClient,
                           Duration pipelineExpiryTime) {
        super(rangeId, distWorkerClient, pipelineExpiryTime);
    }

    @Override
    protected RWCoProcInput makeBatch(Iterator<SubRequest> subRequestIterator) {
        BatchSubRequest.Builder reqBuilder = BatchSubRequest.newBuilder();
        subRequestIterator.forEachRemaining(request -> reqBuilder.putTopicFilters(
            scopedTopicFilter(request.getTenantId(), request.getInboxId(),
                request.getTopicFilter()).toStringUtf8(), request.getSubQoS()));
        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchSub(reqBuilder
                    .setReqId(reqId)
                    .build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<CallTask<SubRequest, SubReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        CallTask<SubRequest, SubReply, MutationCallBatcherKey> task;
        while ((task = batchedTasks.poll()) != null) {
            task.callResult.complete(SubReply.newBuilder()
                .setReqId(task.call.getReqId())
                .setResult(SubReply.Result.forNumber(output.getInboxService()
                    .getBatchSub()
                    .getResultsMap()
                    .get(scopedTopicFilter(task.call.getTenantId(),
                        task.call.getInboxId(), task.call.getTopicFilter()).toStringUtf8())
                    .getNumber()))
                .build());
        }

    }

    @Override
    protected void handleException(CallTask<SubRequest, SubReply, MutationCallBatcherKey> callTask, Throwable e) {
        callTask.callResult.complete(SubReply.newBuilder()
            .setReqId(callTask.call.getReqId())
            .setResult(SubReply.Result.ERROR)
            .build());

    }
}
