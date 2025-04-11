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
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.basescheduler.ICallTask;
import com.baidu.bifromq.inbox.record.InboxInstance;
import com.baidu.bifromq.inbox.record.TenantInboxInstance;
import com.baidu.bifromq.inbox.storage.proto.BatchInsertRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxInsertResult;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxSubMessagePack;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

class BatchInsertCall extends BatchMutationCall<InboxSubMessagePack, InboxInsertResult> {
    protected BatchInsertCall(IBaseKVStoreClient storeClient, MutationCallBatcherKey batcherKey) {
        super(storeClient, batcherKey);
    }

    @Override
    protected MutationCallTaskBatch<InboxSubMessagePack, InboxInsertResult> newBatch(long ver) {
        return new BatchInsertCallTask(ver);
    }

    @Override
    protected RWCoProcInput makeBatch(
        Iterable<ICallTask<InboxSubMessagePack, InboxInsertResult, MutationCallBatcherKey>> callTasks) {
        BatchInsertRequest.Builder reqBuilder = BatchInsertRequest.newBuilder();
        callTasks.forEach(call -> reqBuilder.addInboxSubMsgPack(call.call()));
        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchInsert(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(
        Queue<ICallTask<InboxSubMessagePack, InboxInsertResult, MutationCallBatcherKey>> batchedTasks,
        RWCoProcOutput output) {
        assert batchedTasks.size() == output.getInboxService().getBatchInsert().getResultCount();
        ICallTask<InboxSubMessagePack, InboxInsertResult, MutationCallBatcherKey> task;
        int i = 0;
        while ((task = batchedTasks.poll()) != null) {
            task.resultPromise().complete(output.getInboxService().getBatchInsert().getResult(i++));
        }
    }

    @Override
    protected void handleException(ICallTask<InboxSubMessagePack, InboxInsertResult, MutationCallBatcherKey> callTask,
                                   Throwable e) {
        callTask.resultPromise().completeExceptionally(e);
    }

    private static class BatchInsertCallTask extends MutationCallTaskBatch<InboxSubMessagePack, InboxInsertResult> {
        private final Set<TenantInboxInstance> inboxes = new HashSet<>();

        private BatchInsertCallTask(long ver) {
            super(ver);
        }

        @Override
        protected void add(ICallTask<InboxSubMessagePack, InboxInsertResult, MutationCallBatcherKey> callTask) {
            super.add(callTask);
            inboxes.add(new TenantInboxInstance(
                callTask.call().getTenantId(),
                new InboxInstance(callTask.call().getInboxId(), callTask.call().getIncarnation()))
            );
        }

        @Override
        protected boolean isBatchable(
            ICallTask<InboxSubMessagePack, InboxInsertResult, MutationCallBatcherKey> callTask) {
            return !inboxes.contains(new TenantInboxInstance(callTask.call().getTenantId(),
                new InboxInstance(callTask.call().getInboxId(), callTask.call().getIncarnation()))
            );
        }
    }
}
