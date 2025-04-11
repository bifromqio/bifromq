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
import com.baidu.bifromq.inbox.rpc.proto.UnsubReply;
import com.baidu.bifromq.inbox.rpc.proto.UnsubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubReply;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.Replica;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BatchUnsubCall extends BatchMutationCall<UnsubRequest, UnsubReply> {
    protected BatchUnsubCall(IBaseKVStoreClient distWorkerClient, MutationCallBatcherKey batcherKey) {
        super(distWorkerClient, batcherKey);
    }

    @Override
    protected MutationCallTaskBatch<UnsubRequest, UnsubReply> newBatch(long ver) {
        return new BatchUnsubCallTask(ver);
    }

    @Override
    protected RWCoProcInput makeBatch(Iterable<ICallTask<UnsubRequest, UnsubReply, MutationCallBatcherKey>> callTasks) {
        BatchUnsubRequest.Builder reqBuilder = BatchUnsubRequest.newBuilder()
            .setLeader(Replica.newBuilder()
                .setRangeId(batcherKey.id)
                .setStoreId(batcherKey.leaderStoreId)
                .build());
        callTasks.forEach(call -> {
            UnsubRequest request = call.call();
            reqBuilder.addParams(BatchUnsubRequest.Params.newBuilder()
                .setTenantId(request.getTenantId())
                .setInboxId(request.getInboxId())
                .setIncarnation(request.getIncarnation())
                .setVersion(request.getVersion())
                .setTopicFilter(request.getTopicFilter())
                .setNow(request.getNow())
                .build());
        });
        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchUnsub(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<ICallTask<UnsubRequest, UnsubReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        assert batchedTasks.size() == output.getInboxService().getBatchUnsub().getResultCount();
        ICallTask<UnsubRequest, UnsubReply, MutationCallBatcherKey> task;
        int i = 0;
        while ((task = batchedTasks.poll()) != null) {
            UnsubReply.Builder replyBuilder = UnsubReply.newBuilder().setReqId(task.call().getReqId());
            BatchUnsubReply.Result result = output.getInboxService().getBatchUnsub().getResult(i++);
            switch (result.getCode()) {
                case OK -> task.resultPromise().complete(replyBuilder
                    .setCode(UnsubReply.Code.OK)
                    .setOption(result.getOption())
                    .build());
                case NO_INBOX -> task.resultPromise().complete(replyBuilder.setCode(UnsubReply.Code.NO_INBOX).build());
                case NO_SUB -> task.resultPromise().complete(replyBuilder.setCode(UnsubReply.Code.NO_SUB).build());
                case CONFLICT -> task.resultPromise().complete(replyBuilder.setCode(UnsubReply.Code.CONFLICT).build());
                default -> {
                    log.error("Unknown error code: {}", result.getCode());
                    task.resultPromise().complete(replyBuilder.setCode(UnsubReply.Code.ERROR).build());
                }
            }
        }
    }

    @Override
    protected void handleException(ICallTask<UnsubRequest, UnsubReply, MutationCallBatcherKey> callTask, Throwable e) {
        if (e instanceof ServerNotFoundException || e.getCause() instanceof ServerNotFoundException) {
            callTask.resultPromise().complete(UnsubReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(UnsubReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof BadVersionException || e.getCause() instanceof BadVersionException) {
            callTask.resultPromise().complete(UnsubReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(UnsubReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof TryLaterException || e.getCause() instanceof TryLaterException) {
            callTask.resultPromise().complete(UnsubReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(UnsubReply.Code.TRY_LATER)
                .build());
            return;
        }
        callTask.resultPromise().completeExceptionally(e);

    }

    private static class BatchUnsubCallTask extends MutationCallTaskBatch<UnsubRequest, UnsubReply> {
        private final Set<TenantInboxInstance> inboxes = new HashSet<>();

        private BatchUnsubCallTask(long ver) {
            super(ver);
        }

        @Override
        protected void add(ICallTask<UnsubRequest, UnsubReply, MutationCallBatcherKey> callTask) {
            super.add(callTask);
            inboxes.add(new TenantInboxInstance(
                callTask.call().getTenantId(),
                new InboxInstance(callTask.call().getInboxId(), callTask.call().getIncarnation()))
            );
        }

        @Override
        protected boolean isBatchable(ICallTask<UnsubRequest, UnsubReply, MutationCallBatcherKey> callTask) {
            return !inboxes.contains(new TenantInboxInstance(
                callTask.call().getTenantId(),
                new InboxInstance(callTask.call().getInboxId(), callTask.call().getIncarnation()))
            );
        }
    }
}
