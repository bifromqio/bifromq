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
import com.baidu.bifromq.inbox.rpc.proto.AttachReply;
import com.baidu.bifromq.inbox.rpc.proto.AttachRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchAttachReply;
import com.baidu.bifromq.inbox.storage.proto.BatchAttachRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.Replica;
import java.time.Duration;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BatchAttachCall extends BatchMutationCall<AttachRequest, AttachReply> {

    protected BatchAttachCall(IBaseKVStoreClient distWorkerClient,
                              Duration pipelineExpiryTime,
                              MutationCallBatcherKey batcherKey) {
        super(distWorkerClient, pipelineExpiryTime, batcherKey);
    }

    @Override
    protected MutationCallTaskBatch<AttachRequest, AttachReply> newBatch(String storeId, long ver) {
        return new BatchAttachCallTask(storeId, ver);
    }


    @Override
    protected RWCoProcInput makeBatch(
        Iterable<ICallTask<AttachRequest, AttachReply, MutationCallBatcherKey>> callTasks) {
        BatchAttachRequest.Builder reqBuilder = BatchAttachRequest.newBuilder()
            .setLeader(Replica.newBuilder()
                .setRangeId(batcherKey.id)
                .setStoreId(batcherKey.leaderStoreId)
                .build());
        callTasks.forEach(call -> {
            AttachRequest request = call.call();
            BatchAttachRequest.Params.Builder paramsBuilder = BatchAttachRequest.Params.newBuilder()
                .setInboxId(request.getInboxId())
                .setIncarnation(request.getIncarnation()) // new incarnation
                .setVersion(request.getVersion())
                .setExpirySeconds(request.getExpirySeconds())
                .setClient(request.getClient())
                .setNow(request.getNow());
            if (request.hasLwt()) {
                paramsBuilder.setLwt(request.getLwt());
            }
            reqBuilder.addParams(paramsBuilder.build());
        });
        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchAttach(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<ICallTask<AttachRequest, AttachReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        ICallTask<AttachRequest, AttachReply, MutationCallBatcherKey> callTask;
        assert batchedTasks.size() == output.getInboxService().getBatchAttach().getCodeCount();

        int i = 0;
        while ((callTask = batchedTasks.poll()) != null) {
            BatchAttachReply.Code code = output.getInboxService().getBatchAttach().getCode(i++);
            AttachReply.Builder replyBuilder = AttachReply.newBuilder().setReqId(callTask.call().getReqId());
            switch (code) {
                case OK -> callTask.resultPromise().complete(replyBuilder
                    .setCode(AttachReply.Code.OK)
                    .build());
                case NO_INBOX -> callTask.resultPromise().complete(replyBuilder
                    .setCode(AttachReply.Code.NO_INBOX)
                    .build());
                case CONFLICT -> callTask.resultPromise().complete(replyBuilder
                    .setCode(AttachReply.Code.CONFLICT)
                    .build());
                default -> {
                    log.error("Unexpected attach result: {}", code);
                    callTask.resultPromise().complete(replyBuilder
                        .setCode(AttachReply.Code.ERROR)
                        .build());
                }
            }
        }
    }

    @Override
    protected void handleException(ICallTask<AttachRequest, AttachReply, MutationCallBatcherKey> callTask,
                                   Throwable e) {
        if (e instanceof ServerNotFoundException || e.getCause() instanceof ServerNotFoundException) {
            callTask.resultPromise().complete(AttachReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(AttachReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof BadVersionException || e.getCause() instanceof BadVersionException) {
            callTask.resultPromise().complete(AttachReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(AttachReply.Code.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof TryLaterException || e.getCause() instanceof TryLaterException) {
            callTask.resultPromise().complete(AttachReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setCode(AttachReply.Code.TRY_LATER)
                .build());
            return;
        }
        callTask.resultPromise().completeExceptionally(e);
    }

    private static class BatchAttachCallTask extends MutationCallTaskBatch<AttachRequest, AttachReply> {
        private final Set<TenantInboxInstance> inboxes = new HashSet<>();

        private BatchAttachCallTask(String storeId, long ver) {
            super(storeId, ver);
        }

        @Override
        protected void add(ICallTask<AttachRequest, AttachReply, MutationCallBatcherKey> callTask) {
            super.add(callTask);
            inboxes.add(new TenantInboxInstance(
                callTask.call().getClient().getTenantId(),
                new InboxInstance(callTask.call().getInboxId(), callTask.call().getIncarnation()))
            );
        }

        @Override
        protected boolean isBatchable(ICallTask<AttachRequest, AttachReply, MutationCallBatcherKey> callTask) {
            return !inboxes.contains(new TenantInboxInstance(
                callTask.call().getClient().getTenantId(),
                new InboxInstance(callTask.call().getInboxId(), callTask.call().getIncarnation()))
            );
        }
    }
}
