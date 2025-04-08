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
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.baserpc.client.exception.ServerNotFoundException;
import com.baidu.bifromq.basescheduler.ICallTask;
import com.baidu.bifromq.inbox.record.InboxInstance;
import com.baidu.bifromq.inbox.record.TenantInboxInstance;
import com.baidu.bifromq.inbox.rpc.proto.SubReply;
import com.baidu.bifromq.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchSubReply;
import com.baidu.bifromq.inbox.storage.proto.BatchSubRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.Replica;
import java.time.Duration;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BatchSubCall extends BatchMutationCall<SubRequest, SubReply> {
    protected BatchSubCall(IBaseKVStoreClient distWorkerClient,
                           Duration pipelineExpiryTime,
                           MutationCallBatcherKey batcherKey) {
        super(distWorkerClient, pipelineExpiryTime, batcherKey);
    }

    @Override
    protected MutationCallTaskBatch<SubRequest, SubReply> newBatch(String storeId, long ver) {
        return new BatchSubCallTask(storeId, ver);
    }

    @Override
    protected RWCoProcInput makeBatch(Iterable<ICallTask<SubRequest, SubReply, MutationCallBatcherKey>> callTasks) {
        BatchSubRequest.Builder reqBuilder = BatchSubRequest.newBuilder()
            .setLeader(Replica.newBuilder()
                .setRangeId(batcherKey.id)
                .setStoreId(batcherKey.leaderStoreId)
                .build());
        callTasks.forEach(call -> {
            SubRequest request = call.call();
            BatchSubRequest.Params.Builder paramsBuilder = BatchSubRequest.Params.newBuilder()
                .setTenantId(request.getTenantId())
                .setInboxId(request.getInboxId())
                .setIncarnation(request.getIncarnation())
                .setVersion(request.getVersion())
                .setTopicFilter(request.getTopicFilter())
                .setOption(request.getOption())
                .setNow(request.getNow());
            reqBuilder.addParams(paramsBuilder.build());
        });
        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchSub(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<ICallTask<SubRequest, SubReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        assert batchedTasks.size() == output.getInboxService().getBatchSub().getCodeCount();
        ICallTask<SubRequest, SubReply, MutationCallBatcherKey> task;
        int i = 0;
        while ((task = batchedTasks.poll()) != null) {
            SubReply.Builder replyBuilder = SubReply.newBuilder().setReqId(task.call().getReqId());
            BatchSubReply.Code code = output.getInboxService().getBatchSub().getCode(i++);
            switch (code) {
                case OK -> task.resultPromise().complete(replyBuilder.setCode(SubReply.Code.OK).build());
                case EXISTS -> task.resultPromise().complete(replyBuilder.setCode(SubReply.Code.EXISTS).build());
                case NO_INBOX -> task.resultPromise().complete(replyBuilder.setCode(SubReply.Code.NO_INBOX).build());
                case EXCEED_LIMIT ->
                    task.resultPromise().complete(replyBuilder.setCode(SubReply.Code.EXCEED_LIMIT).build());
                case CONFLICT -> task.resultPromise().complete(replyBuilder.setCode(SubReply.Code.CONFLICT).build());
                default -> {
                    log.error("Unknown error code: {}", code);
                    task.resultPromise().complete(replyBuilder.setCode(SubReply.Code.ERROR).build());
                }
            }
        }
    }

    @Override
    protected void handleException(ICallTask<SubRequest, SubReply, MutationCallBatcherKey> callTask, Throwable e) {
        if (e instanceof ServerNotFoundException || e.getCause() instanceof ServerNotFoundException) {
            callTask.resultPromise().complete(SubReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(SubReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof BadVersionException || e.getCause() instanceof BadVersionException) {
            callTask.resultPromise().complete(SubReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(SubReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof TryLaterException || e.getCause() instanceof TryLaterException) {
            callTask.resultPromise().complete(SubReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(SubReply.Code.TRY_LATER)
                .build());
            return;
        }
        callTask.resultPromise().completeExceptionally(e);
    }

    private static class BatchSubCallTask extends MutationCallTaskBatch<SubRequest, SubReply> {
        private final Set<TenantInboxInstance> inboxes = new HashSet<>();

        private BatchSubCallTask(String storeId, long ver) {
            super(storeId, ver);
        }

        @Override
        protected void add(ICallTask<SubRequest, SubReply, MutationCallBatcherKey> callTask) {
            super.add(callTask);
            inboxes.add(new TenantInboxInstance(
                callTask.call().getTenantId(),
                new InboxInstance(callTask.call().getInboxId(), callTask.call().getIncarnation()))
            );
        }

        @Override
        protected boolean isBatchable(ICallTask<SubRequest, SubReply, MutationCallBatcherKey> callTask) {
            return !inboxes.contains(new TenantInboxInstance(
                callTask.call().getTenantId(),
                new InboxInstance(callTask.call().getInboxId(), callTask.call().getIncarnation()))
            );
        }
    }
}
