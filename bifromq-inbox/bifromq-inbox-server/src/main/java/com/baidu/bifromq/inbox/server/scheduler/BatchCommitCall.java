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
import com.baidu.bifromq.basescheduler.ICallTask;
import com.baidu.bifromq.inbox.record.InboxInstance;
import com.baidu.bifromq.inbox.record.TenantInboxInstance;
import com.baidu.bifromq.inbox.rpc.proto.CommitReply;
import com.baidu.bifromq.inbox.rpc.proto.CommitRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCommitRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;

class BatchCommitCall extends BatchMutationCall<CommitRequest, CommitReply> {
    protected BatchCommitCall(KVRangeId rangeId,
                              IBaseKVStoreClient storeClient,
                              Duration pipelineExpiryTime) {
        super(rangeId, storeClient, pipelineExpiryTime);
    }

    @Override
    protected MutationCallTaskBatch<CommitRequest, CommitReply> newBatch(String storeId, long ver) {
        return new BatchCommitCallTask(storeId, ver);
    }

    @Override
    protected RWCoProcInput makeBatch(Iterator<CommitRequest> reqIterator) {
        BatchCommitRequest.Builder reqBuilder = BatchCommitRequest.newBuilder();
        reqIterator.forEachRemaining(req -> {
            BatchCommitRequest.Params.Builder paramsBuilder = BatchCommitRequest.Params.newBuilder()
                .setTenantId(req.getTenantId())
                .setInboxId(req.getInboxId())
                .setIncarnation(req.getIncarnation())
                .setVersion(req.getVersion())
                .setNow(req.getNow());
            if (req.hasQos0UpToSeq()) {
                paramsBuilder.setQos0UpToSeq(req.getQos0UpToSeq());
            }
            if (req.hasSendBufferUpToSeq()) {
                paramsBuilder.setSendBufferUpToSeq(req.getSendBufferUpToSeq());
            }
            reqBuilder.addParams(paramsBuilder.build());
        });

        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchCommit(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<ICallTask<CommitRequest, CommitReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        assert batchedTasks.size() == output.getInboxService().getBatchCommit().getCodeCount();
        ICallTask<CommitRequest, CommitReply, MutationCallBatcherKey> task;
        int i = 0;
        while ((task = batchedTasks.poll()) != null) {
            CommitReply.Builder replyBuilder = CommitReply.newBuilder().setReqId(task.call().getReqId());
            switch (output.getInboxService().getBatchCommit().getCode(i++)) {
                case OK -> task.resultPromise().complete(replyBuilder.setCode(CommitReply.Code.OK).build());
                case NO_INBOX -> task.resultPromise().complete(replyBuilder.setCode(CommitReply.Code.NO_INBOX).build());
                case CONFLICT -> task.resultPromise().complete(replyBuilder.setCode(CommitReply.Code.CONFLICT).build());
                default -> task.resultPromise().complete(replyBuilder.setCode(CommitReply.Code.ERROR).build());
            }
        }
    }

    @Override
    protected void handleException(ICallTask<CommitRequest, CommitReply, MutationCallBatcherKey> callTask,
                                   Throwable e) {
        callTask.resultPromise().complete(CommitReply.newBuilder()
            .setReqId(callTask.call().getReqId())
            .setCode(CommitReply.Code.ERROR)
            .build());

    }

    private static class BatchCommitCallTask extends MutationCallTaskBatch<CommitRequest, CommitReply> {
        private final Set<TenantInboxInstance> inboxes = new HashSet<>();

        private BatchCommitCallTask(String storeId, long ver) {
            super(storeId, ver);
        }

        @Override
        protected void add(ICallTask<CommitRequest, CommitReply, MutationCallBatcherKey> callTask) {
            super.add(callTask);
            inboxes.add(new TenantInboxInstance(
                callTask.call().getTenantId(),
                new InboxInstance(callTask.call().getInboxId(), callTask.call().getIncarnation()))
            );
        }

        @Override
        protected boolean isBatchable(ICallTask<CommitRequest, CommitReply, MutationCallBatcherKey> callTask) {
            return !inboxes.contains(new TenantInboxInstance(
                callTask.call().getTenantId(),
                new InboxInstance(callTask.call().getInboxId(), callTask.call().getIncarnation()))
            );
        }
    }
}
