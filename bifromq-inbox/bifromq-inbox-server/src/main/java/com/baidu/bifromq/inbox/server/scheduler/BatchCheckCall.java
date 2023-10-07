/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

import static com.baidu.bifromq.inbox.util.KeyUtil.scopedInboxId;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.BatchQueryCall;
import com.baidu.bifromq.basekv.client.scheduler.QueryCallBatcherKey;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ROCoProcOutput;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCheckRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;

public class BatchCheckCall extends BatchQueryCall<HasInboxRequest, HasInboxReply> {
    protected BatchCheckCall(KVRangeId rangeId,
                             IBaseKVStoreClient storeClient,
                             Duration pipelineExpiryTime) {
        super(rangeId, storeClient, true, pipelineExpiryTime);
    }

    @Override
    protected ROCoProcInput makeBatch(Iterator<HasInboxRequest> hasInboxRequestIterator) {
        Set<ByteString> checkInboxes = new HashSet<>();

        BatchCheckRequest.Builder reqBuilder = BatchCheckRequest.newBuilder();
        hasInboxRequestIterator.forEachRemaining(
            request -> {
                checkInboxes.add(scopedInboxId(request.getTenantId(), request.getInboxId()));
            });
        long reqId = System.nanoTime();
        return ROCoProcInput.newBuilder()
            .setInboxService(InboxServiceROCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchCheck(reqBuilder.addAllScopedInboxId(checkInboxes))
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<CallTask<HasInboxRequest, HasInboxReply, QueryCallBatcherKey>> batchedTasks,
                                ROCoProcOutput output) {
        CallTask<HasInboxRequest, HasInboxReply, QueryCallBatcherKey> task;
        while ((task = batchedTasks.poll()) != null) {
            Boolean exists = output.getInboxService().getBatchCheck().getExistsMap()
                .get(scopedInboxId(task.call.getTenantId(),
                    task.call.getInboxId()).toStringUtf8());
            // if query result doesn't contain the scoped inboxId, reply error
            if (exists == null) {
                task.callResult.completeExceptionally(new RuntimeException("Inbox not found"));
            } else {
                task.callResult.complete(HasInboxReply.newBuilder()
                    .setReqId(task.call.getReqId())
                    .setResult(exists ? HasInboxReply.Result.EXIST : HasInboxReply.Result.NO_INBOX)
                    .build());
            }
        }
    }

    @Override
    protected void handleException(CallTask<HasInboxRequest, HasInboxReply, QueryCallBatcherKey> callTask,
                                   Throwable e) {
        callTask.callResult.completeExceptionally(e);
    }
}
