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

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.BatchQueryCall;
import com.baidu.bifromq.basekv.client.scheduler.QueryCallBatcherKey;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ROCoProcOutput;
import com.baidu.bifromq.basescheduler.ICallTask;
import com.baidu.bifromq.inbox.storage.proto.BatchFetchRequest;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import java.time.Duration;
import java.util.Iterator;
import java.util.Queue;

class BatchFetchCall extends BatchQueryCall<IInboxFetchScheduler.InboxFetch, Fetched> {
    protected BatchFetchCall(KVRangeId rangeId,
                             IBaseKVStoreClient storeClient,
                             Duration pipelineExpiryTime) {
        super(rangeId, storeClient, false, pipelineExpiryTime);
    }

    @Override
    protected ROCoProcInput makeBatch(Iterator<IInboxFetchScheduler.InboxFetch> reqIterator) {
        BatchFetchRequest.Builder reqBuilder = BatchFetchRequest.newBuilder();
        reqIterator.forEachRemaining(request -> {
            BatchFetchRequest.Params.Builder paramsBuilder = BatchFetchRequest.Params.newBuilder()
                .setTenantId(request.tenantId)
                .setInboxId(request.inboxId)
                .setIncarnation(request.incarnation)
                .setMaxFetch(request.params.getMaxFetch());
            if (request.params.hasQos0StartAfter()) {
                paramsBuilder.setQos0StartAfter(request.params.getQos0StartAfter());
            }
            if (request.params.hasSendBufferStartAfter()) {
                paramsBuilder.setSendBufferStartAfter(request.params.getSendBufferStartAfter());
            }
            reqBuilder.addParams(paramsBuilder.build());
        });
        long reqId = System.nanoTime();
        return ROCoProcInput.newBuilder()
            .setInboxService(InboxServiceROCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchFetch(reqBuilder
                    .build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(
        Queue<ICallTask<IInboxFetchScheduler.InboxFetch, Fetched, QueryCallBatcherKey>> batchedTasks,
        ROCoProcOutput output) {
        ICallTask<IInboxFetchScheduler.InboxFetch, Fetched, QueryCallBatcherKey> task;
        int i = 0;
        while ((task = batchedTasks.poll()) != null) {
            task.resultPromise().complete(output.getInboxService().getBatchFetch().getResult(i++));
        }
    }

    @Override
    protected void handleException(ICallTask<IInboxFetchScheduler.InboxFetch, Fetched, QueryCallBatcherKey> callTask,
                                   Throwable e) {
        callTask.resultPromise().completeExceptionally(e);
    }
}
