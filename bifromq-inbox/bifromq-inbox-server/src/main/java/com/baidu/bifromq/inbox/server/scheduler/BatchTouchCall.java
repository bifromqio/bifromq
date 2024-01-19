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
import com.baidu.bifromq.basekv.client.scheduler.BatchMutationCall;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.inbox.records.ScopedInbox;
import com.baidu.bifromq.inbox.rpc.proto.TouchReply;
import com.baidu.bifromq.inbox.rpc.proto.TouchRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;

public class BatchTouchCall extends BatchMutationCall<TouchRequest, TouchReply> {
    protected BatchTouchCall(KVRangeId rangeId,
                             IBaseKVStoreClient distWorkerClient,
                             Duration pipelineExpiryTime) {
        super(rangeId, distWorkerClient, pipelineExpiryTime);
    }

    @Override
    protected BatchCallTask<TouchRequest, TouchReply> newBatch(String storeId, long ver) {
        return new BatchTouchCallTask(storeId, ver);
    }

    @Override
    protected RWCoProcInput makeBatch(Iterator<TouchRequest> touchIterator) {
        BatchTouchRequest.Builder reqBuilder = BatchTouchRequest.newBuilder();
        touchIterator.forEachRemaining(request -> reqBuilder.addParams(BatchTouchRequest.Params.newBuilder()
            .setTenantId(request.getTenantId())
            .setInboxId(request.getInboxId())
            .setIncarnation(request.getIncarnation())
            .setVersion(request.getVersion())
            .setNow(request.getNow())
            .build()));
        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchTouch(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<CallTask<TouchRequest, TouchReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        assert batchedTasks.size() == output.getInboxService().getBatchTouch().getCodeCount();
        CallTask<TouchRequest, TouchReply, MutationCallBatcherKey> callTask;
        int i = 0;
        while ((callTask = batchedTasks.poll()) != null) {
            TouchReply.Builder replyBuilder = TouchReply.newBuilder().setReqId(callTask.call.getReqId());
            switch (output.getInboxService().getBatchTouch().getCode(i++)) {
                case OK -> callTask.callResult.complete(replyBuilder.setCode(TouchReply.Code.OK).build());
                case NO_INBOX -> callTask.callResult.complete(replyBuilder.setCode(TouchReply.Code.NO_INBOX).build());
                case CONFLICT -> callTask.callResult.complete(replyBuilder.setCode(TouchReply.Code.CONFLICT).build());
                case ERROR -> callTask.callResult.complete(replyBuilder.setCode(TouchReply.Code.ERROR).build());
            }
        }
    }

    @Override
    protected void handleException(CallTask<TouchRequest, TouchReply, MutationCallBatcherKey> callTask,
                                   Throwable e) {
        callTask.callResult.complete(TouchReply.newBuilder().setCode(TouchReply.Code.ERROR).build());
    }

    private static class BatchTouchCallTask extends BatchCallTask<TouchRequest, TouchReply> {
        private final Set<ScopedInbox> inboxes = new HashSet<>();

        private BatchTouchCallTask(String storeId, long ver) {
            super(storeId, ver);
        }

        @Override
        protected void add(CallTask<TouchRequest, TouchReply, MutationCallBatcherKey> callTask) {
            super.add(callTask);
            inboxes.add(new ScopedInbox(
                callTask.call.getTenantId(),
                callTask.call.getInboxId(),
                callTask.call.getIncarnation())
            );
        }

        @Override
        protected boolean isBatchable(CallTask<TouchRequest, TouchReply, MutationCallBatcherKey> callTask) {
            return !inboxes.contains(new ScopedInbox(
                callTask.call.getTenantId(),
                callTask.call.getInboxId(),
                callTask.call.getIncarnation()));
        }
    }
}
