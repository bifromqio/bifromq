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

import static com.baidu.bifromq.inbox.util.KeyUtil.scopedInboxId;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.BatchMutationCall;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.inbox.rpc.proto.CommitReply;
import com.baidu.bifromq.inbox.rpc.proto.CommitRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCommitRequest;
import com.baidu.bifromq.inbox.storage.proto.CommitParams;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.type.QoS;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

public class BatchCommitCall extends BatchMutationCall<CommitRequest, CommitReply> {
    protected BatchCommitCall(KVRangeId rangeId,
                              IBaseKVStoreClient storeClient,
                              Duration pipelineExpiryTime) {
        super(rangeId, storeClient, pipelineExpiryTime);
    }

    @Override
    protected RWCoProcInput makeBatch(Iterator<CommitRequest> commitRequestIterator) {
        Map<String, Long[]> commitParamsMap = new HashMap<>(128);
        commitRequestIterator.forEachRemaining(call -> {
            String tenantId = call.getTenantId();
            String scopedInboxIdUtf8 = scopedInboxId(tenantId, call.getInboxId()).toStringUtf8();
            Long[] upToSeqs = commitParamsMap.computeIfAbsent(scopedInboxIdUtf8, k -> new Long[3]);
            QoS qos = call.getQos();
            upToSeqs[qos.ordinal()] = upToSeqs[qos.ordinal()] == null ?
                call.getUpToSeq() : Math.max(upToSeqs[qos.ordinal()], call.getUpToSeq());
        });
        BatchCommitRequest.Builder reqBuilder = BatchCommitRequest.newBuilder();
        commitParamsMap.forEach((k, v) -> {
            CommitParams.Builder cb = CommitParams.newBuilder();
            if (v[0] != null) {
                cb.setQos0UpToSeq(v[0]);
            }
            if (v[1] != null) {
                cb.setQos1UpToSeq(v[1]);
            }
            if (v[2] != null) {
                cb.setQos2UpToSeq(v[2]);
            }
            reqBuilder.putInboxCommit(k, cb.build());
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
    protected void handleOutput(Queue<CallTask<CommitRequest, CommitReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        CallTask<CommitRequest, CommitReply, MutationCallBatcherKey> task;
        while ((task = batchedTasks.poll()) != null) {
            task.callResult.complete(CommitReply.newBuilder()
                .setReqId(task.call.getReqId())
                .setResult(output.getInboxService().getBatchCommit().getResultMap()
                    .get(scopedInboxId(
                        task.call.getTenantId(),
                        task.call.getInboxId()).toStringUtf8()) ?
                    CommitReply.Result.OK : CommitReply.Result.ERROR)
                .build());
        }
    }

    @Override
    protected void handleException(CallTask<CommitRequest, CommitReply, MutationCallBatcherKey> callTask, Throwable e) {
        callTask.callResult.complete(CommitReply.newBuilder()
            .setReqId(callTask.call.getReqId())
            .setResult(CommitReply.Result.ERROR)
            .build());

    }
}
