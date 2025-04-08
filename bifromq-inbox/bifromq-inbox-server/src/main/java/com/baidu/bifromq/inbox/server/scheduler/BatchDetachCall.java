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
import com.baidu.bifromq.inbox.rpc.proto.DetachReply;
import com.baidu.bifromq.inbox.rpc.proto.DetachRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchDetachReply;
import com.baidu.bifromq.inbox.storage.proto.BatchDetachRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.Replica;
import java.time.Duration;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BatchDetachCall extends BatchMutationCall<DetachRequest, DetachReply> {

    protected BatchDetachCall(IBaseKVStoreClient distWorkerClient,
                              Duration pipelineExpiryTime,
                              MutationCallBatcherKey batcherKey) {
        super(distWorkerClient, pipelineExpiryTime, batcherKey);
    }

    @Override
    protected MutationCallTaskBatch<DetachRequest, DetachReply> newBatch(String storeId, long ver) {
        return new BatchDetachCallTask(storeId, ver);
    }

    @Override
    protected RWCoProcInput makeBatch(
        Iterable<ICallTask<DetachRequest, DetachReply, MutationCallBatcherKey>> callTasks) {
        BatchDetachRequest.Builder reqBuilder = BatchDetachRequest.newBuilder()
            .setLeader(Replica.newBuilder()
                .setRangeId(batcherKey.id)
                .setStoreId(batcherKey.leaderStoreId)
                .build());
        for (ICallTask<DetachRequest, DetachReply, MutationCallBatcherKey> callTask : callTasks) {
            DetachRequest request = callTask.call();
            BatchDetachRequest.Params.Builder paramsBuilder = BatchDetachRequest.Params.newBuilder()
                .setTenantId(request.getClient().getTenantId())
                .setInboxId(request.getInboxId())
                .setIncarnation(request.getIncarnation()) // new incarnation
                .setVersion(request.getVersion())
                .setExpirySeconds(request.getExpirySeconds())
                .setDiscardLWT(request.getDiscardLWT())
                .setNow(request.getNow());
            if (request.hasSender()) {
                paramsBuilder.setSender(request.getSender());
            }
            reqBuilder.addParams(paramsBuilder.build());
        }

        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchDetach(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<ICallTask<DetachRequest, DetachReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        ICallTask<DetachRequest, DetachReply, MutationCallBatcherKey> callTask;
        assert batchedTasks.size() == output.getInboxService().getBatchDetach().getCodeCount();

        int i = 0;
        while ((callTask = batchedTasks.poll()) != null) {
            BatchDetachReply.Code code = output.getInboxService().getBatchDetach().getCode(i++);
            DetachReply.Builder replyBuilder = DetachReply.newBuilder().setReqId(callTask.call().getReqId());
            switch (code) {
                case OK -> replyBuilder.setCode(DetachReply.Code.OK);
                case NO_INBOX -> replyBuilder.setCode(DetachReply.Code.NO_INBOX);
                case CONFLICT -> replyBuilder.setCode(DetachReply.Code.CONFLICT);
                default -> {
                    log.error("Unexpected detach result: {}", code);
                    replyBuilder.setCode(DetachReply.Code.ERROR);
                }
            }
            callTask.resultPromise().complete(replyBuilder.build());
        }
    }

    @Override
    protected void handleException(ICallTask<DetachRequest, DetachReply, MutationCallBatcherKey> callTask,
                                   Throwable e) {
        if (e instanceof ServerNotFoundException || e.getCause() instanceof ServerNotFoundException) {
            callTask.resultPromise()
                .complete(DetachReply.newBuilder()
                    .setCode(DetachReply.Code.TRY_LATER)
                    .build());
            return;
        }
        if (e instanceof BadVersionException || e.getCause() instanceof BadVersionException) {
            callTask.resultPromise()
                .complete(DetachReply.newBuilder()
                    .setCode(DetachReply.Code.TRY_LATER)
                    .build());
            return;
        }
        if (e instanceof TryLaterException || e.getCause() instanceof TryLaterException) {
            callTask.resultPromise()
                .complete(DetachReply.newBuilder()
                    .setCode(DetachReply.Code.TRY_LATER)
                    .build());
            return;
        }
        callTask.resultPromise().completeExceptionally(e);
    }

    private static class BatchDetachCallTask extends MutationCallTaskBatch<DetachRequest, DetachReply> {
        private final Set<TenantInboxInstance> inboxes = new HashSet<>();

        private BatchDetachCallTask(String storeId, long ver) {
            super(storeId, ver);
        }

        @Override
        protected void add(ICallTask<DetachRequest, DetachReply, MutationCallBatcherKey> callTask) {
            super.add(callTask);
            inboxes.add(new TenantInboxInstance(callTask.call().getClient().getTenantId(),
                new InboxInstance(callTask.call().getInboxId(), callTask.call().getIncarnation()))
            );
        }

        @Override
        protected boolean isBatchable(ICallTask<DetachRequest, DetachReply, MutationCallBatcherKey> callTask) {
            return !inboxes.contains(new TenantInboxInstance(callTask.call().getClient().getTenantId(),
                new InboxInstance(callTask.call().getInboxId(), callTask.call().getIncarnation()))
            );
        }
    }
}
