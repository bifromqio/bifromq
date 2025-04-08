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
import com.baidu.bifromq.basekv.client.scheduler.BatchQueryCall;
import com.baidu.bifromq.basekv.client.scheduler.QueryCallBatcherKey;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ROCoProcOutput;
import com.baidu.bifromq.baserpc.client.exception.ServerNotFoundException;
import com.baidu.bifromq.basescheduler.ICallTask;
import com.baidu.bifromq.inbox.rpc.proto.TouchReply;
import com.baidu.bifromq.inbox.rpc.proto.TouchRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchReply;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import java.time.Duration;
import java.util.Iterator;
import java.util.Queue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BatchTouchCall extends BatchQueryCall<TouchRequest, TouchReply> {
    protected BatchTouchCall(KVRangeId rangeId,
                             IBaseKVStoreClient storeClient,
                             Duration pipelineExpiryTime) {
        super(rangeId, storeClient, false, pipelineExpiryTime);
    }

    @Override
    protected ROCoProcInput makeBatch(Iterator<TouchRequest> reqIterator) {
        BatchTouchRequest.Builder reqBuilder = BatchTouchRequest.newBuilder();
        reqIterator.forEachRemaining(
            request -> reqBuilder
                .addParams(BatchTouchRequest.Params.newBuilder()
                    .setTenantId(request.getTenantId())
                    .setInboxId(request.getInboxId())
                    .setIncarnation(request.getIncarnation())
                    .setVersion(request.getVersion())
                    .setNow(request.getNow())
                    .build()));
        long reqId = System.nanoTime();
        return ROCoProcInput.newBuilder()
            .setInboxService(InboxServiceROCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchTouch(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<ICallTask<TouchRequest, TouchReply, QueryCallBatcherKey>> batchedTasks,
                                ROCoProcOutput output) {
        ICallTask<TouchRequest, TouchReply, QueryCallBatcherKey> task;
        assert batchedTasks.size() == output.getInboxService().getBatchTouch().getCodeCount();
        int i = 0;
        while ((task = batchedTasks.poll()) != null) {
            BatchTouchReply.Code code = output.getInboxService().getBatchTouch().getCode(i++);
            switch (code) {
                case OK -> task.resultPromise().complete(TouchReply.newBuilder()
                    .setReqId(task.call().getReqId())
                    .setCode(TouchReply.Code.OK)
                    .build());
                case NO_INBOX -> task.resultPromise().complete(TouchReply.newBuilder()
                    .setReqId(task.call().getReqId())
                    .setCode(TouchReply.Code.NO_INBOX)
                    .build());
                case CONFLICT -> task.resultPromise().complete(TouchReply.newBuilder()
                    .setReqId(task.call().getReqId())
                    .setCode(TouchReply.Code.CONFLICT)
                    .build());
                default -> {
                    log.error("Unexpected touch reply code: {}", code);
                    task.resultPromise().complete(TouchReply.newBuilder()
                        .setReqId(task.call().getReqId())
                        .setCode(TouchReply.Code.ERROR)
                        .build());
                }
            }
        }
    }

    @Override
    protected void handleException(ICallTask<TouchRequest, TouchReply, QueryCallBatcherKey> callTask, Throwable e) {
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
}
