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
import com.baidu.bifromq.inbox.rpc.proto.GetReply;
import com.baidu.bifromq.inbox.rpc.proto.GetRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchGetRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxVersion;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BatchGetCall extends BatchQueryCall<GetRequest, GetReply> {
    protected BatchGetCall(KVRangeId rangeId,
                           IBaseKVStoreClient storeClient,
                           Duration pipelineExpiryTime) {
        super(rangeId, storeClient, true, pipelineExpiryTime);
    }

    @Override
    protected ROCoProcInput makeBatch(Iterator<GetRequest> reqIterator) {
        BatchGetRequest.Builder reqBuilder = BatchGetRequest.newBuilder();
        reqIterator.forEachRemaining(
            request -> reqBuilder
                .addParams(BatchGetRequest.Params.newBuilder()
                    .setTenantId(request.getTenantId())
                    .setInboxId(request.getInboxId())
                    .setNow(request.getNow())
                    .build()));
        long reqId = System.nanoTime();
        return ROCoProcInput.newBuilder()
            .setInboxService(InboxServiceROCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchGet(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<ICallTask<GetRequest, GetReply, QueryCallBatcherKey>> batchedTasks,
                                ROCoProcOutput output) {
        ICallTask<GetRequest, GetReply, QueryCallBatcherKey> task;
        assert batchedTasks.size() == output.getInboxService().getBatchGet().getResultCount();
        int i = 0;
        while ((task = batchedTasks.poll()) != null) {
            List<InboxVersion> inboxVersions =
                output.getInboxService().getBatchGet().getResult(i++).getVersionList();
            if (inboxVersions.isEmpty()) {
                task.resultPromise().complete(GetReply.newBuilder()
                    .setReqId(task.call().getReqId())
                    .setCode(GetReply.Code.NO_INBOX)
                    .build());
            } else {
                task.resultPromise().complete(GetReply.newBuilder()
                    .setReqId(task.call().getReqId())
                    .setCode(GetReply.Code.EXIST)
                    .addAllInbox(inboxVersions) // always using highest incarnation
                    .build());
            }
        }
    }

    @Override
    protected void handleException(ICallTask<GetRequest, GetReply, QueryCallBatcherKey> callTask,
                                   Throwable e) {
        callTask.resultPromise().complete(GetReply.newBuilder()
            .setReqId(callTask.call().getReqId())
            .setCode(GetReply.Code.ERROR)
            .build());
    }
}
