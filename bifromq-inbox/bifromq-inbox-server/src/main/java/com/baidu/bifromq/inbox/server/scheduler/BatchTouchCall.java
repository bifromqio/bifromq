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

package com.baidu.bifromq.inbox.server.scheduler;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.BatchMutationCall;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterList;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

public class BatchTouchCall extends BatchMutationCall<IInboxTouchScheduler.Touch, List<String>> {
    protected BatchTouchCall(KVRangeId rangeId,
                             IBaseKVStoreClient distWorkerClient,
                             Duration pipelineExpiryTime) {
        super(rangeId, distWorkerClient, pipelineExpiryTime);
    }

    @Override
    protected RWCoProcInput makeBatch(Iterator<IInboxTouchScheduler.Touch> touchIterator) {
        BatchTouchRequest.Builder reqBuilder = BatchTouchRequest.newBuilder();
        touchIterator.forEachRemaining(request -> reqBuilder.putScopedInboxId(request.scopedInboxIdUtf8,
            request.keep && reqBuilder.getScopedInboxIdOrDefault(request.scopedInboxIdUtf8, true)));
        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchTouch(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(
        Queue<CallTask<IInboxTouchScheduler.Touch, List<String>, MutationCallBatcherKey>> batchedTasks,
        RWCoProcOutput output) {
        CallTask<IInboxTouchScheduler.Touch, List<String>, MutationCallBatcherKey> callTask;
        while ((callTask = batchedTasks.poll()) != null) {
            callTask.callResult.complete(
                output.getInboxService().getBatchTouch().getSubsOrDefault(callTask.call.scopedInboxIdUtf8,
                    TopicFilterList.getDefaultInstance()).getTopicFiltersList());
        }

    }

    @Override
    protected void handleException(CallTask<IInboxTouchScheduler.Touch, List<String>, MutationCallBatcherKey> callTask,
                                   Throwable e) {
        callTask.callResult.completeExceptionally(e);
    }
}
