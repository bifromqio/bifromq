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
import com.baidu.bifromq.basekv.client.exception.BadVersionException;
import com.baidu.bifromq.basekv.client.exception.TryLaterException;
import com.baidu.bifromq.basekv.client.scheduler.BatchMutationCall;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.baserpc.client.exception.ServerNotFoundException;
import com.baidu.bifromq.basescheduler.ICallTask;
import com.baidu.bifromq.inbox.record.InboxInstance;
import com.baidu.bifromq.inbox.record.TenantInboxInstance;
import com.baidu.bifromq.inbox.rpc.proto.TouchReply;
import com.baidu.bifromq.inbox.rpc.proto.TouchRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchReply;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BatchTouchCall extends BatchMutationCall<TouchRequest, TouchReply> {
    protected BatchTouchCall(KVRangeId rangeId,
                             IBaseKVStoreClient distWorkerClient,
                             Duration pipelineExpiryTime) {
        super(rangeId, distWorkerClient, pipelineExpiryTime);
    }

    @Override
    protected MutationCallTaskBatch<TouchRequest, TouchReply> newBatch(String storeId, long ver) {
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
    protected void handleOutput(Queue<ICallTask<TouchRequest, TouchReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        assert batchedTasks.size() == output.getInboxService().getBatchTouch().getCodeCount();
        ICallTask<TouchRequest, TouchReply, MutationCallBatcherKey> callTask;
        int i = 0;
        while ((callTask = batchedTasks.poll()) != null) {
            TouchReply.Builder replyBuilder = TouchReply.newBuilder().setReqId(callTask.call().getReqId());
            BatchTouchReply.Code result = output.getInboxService().getBatchTouch().getCode(i++);
            switch (result) {
                case OK -> callTask.resultPromise().complete(replyBuilder.setCode(TouchReply.Code.OK).build());
                case NO_INBOX ->
                    callTask.resultPromise().complete(replyBuilder.setCode(TouchReply.Code.NO_INBOX).build());
                case CONFLICT ->
                    callTask.resultPromise().complete(replyBuilder.setCode(TouchReply.Code.CONFLICT).build());
                default -> {
                    log.error("Unexpected touch result: {}", result);
                    callTask.resultPromise().complete(replyBuilder.setCode(TouchReply.Code.ERROR).build());
                }
            }
        }
    }

    @Override
    protected void handleException(ICallTask<TouchRequest, TouchReply, MutationCallBatcherKey> callTask,
                                   Throwable e) {
        if (e instanceof ServerNotFoundException || e.getCause() instanceof ServerNotFoundException) {
            callTask.resultPromise().complete(TouchReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(TouchReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof BadVersionException || e.getCause() instanceof BadVersionException) {
            callTask.resultPromise().complete(TouchReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(TouchReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof TryLaterException || e.getCause() instanceof TryLaterException) {
            callTask.resultPromise().complete(TouchReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(TouchReply.Code.TRY_LATER)
                .build());
            return;
        }
        callTask.resultPromise().completeExceptionally(e);
    }

    private static class BatchTouchCallTask extends MutationCallTaskBatch<TouchRequest, TouchReply> {
        private final Set<TenantInboxInstance> inboxes = new HashSet<>();

        private BatchTouchCallTask(String storeId, long ver) {
            super(storeId, ver);
        }

        @Override
        protected void add(ICallTask<TouchRequest, TouchReply, MutationCallBatcherKey> callTask) {
            super.add(callTask);
            inboxes.add(new TenantInboxInstance(
                callTask.call().getTenantId(),
                new InboxInstance(callTask.call().getInboxId(), callTask.call().getIncarnation()))
            );
        }

        @Override
        protected boolean isBatchable(ICallTask<TouchRequest, TouchReply, MutationCallBatcherKey> callTask) {
            return !inboxes.contains(new TenantInboxInstance(
                callTask.call().getTenantId(),
                new InboxInstance(callTask.call().getInboxId(), callTask.call().getIncarnation()))
            );
        }
    }
}
