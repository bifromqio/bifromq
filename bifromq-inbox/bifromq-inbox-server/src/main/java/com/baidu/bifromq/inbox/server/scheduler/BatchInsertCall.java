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

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.BatchMutationCall;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.inbox.rpc.proto.SendResult;
import com.baidu.bifromq.inbox.storage.proto.BatchInsertRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InsertResult;
import com.baidu.bifromq.inbox.storage.proto.MessagePack;
import com.baidu.bifromq.type.SubInfo;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

public class BatchInsertCall extends BatchMutationCall<MessagePack, SendResult.Result> {
    protected BatchInsertCall(KVRangeId rangeId, IBaseKVStoreClient storeClient, Duration pipelineExpiryTime) {
        super(rangeId, storeClient, pipelineExpiryTime);
    }

    @Override
    protected RWCoProcInput makeBatch(Iterator<MessagePack> messagePackIterator) {
        BatchInsertRequest.Builder reqBuilder = BatchInsertRequest.newBuilder();
        messagePackIterator.forEachRemaining(reqBuilder::addSubMsgPack);
        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchInsert(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<CallTask<MessagePack, SendResult.Result, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        Map<SubInfo, SendResult.Result> insertResults = new HashMap<>();
        for (InsertResult result : output.getInboxService().getBatchInsert().getResultsList()) {
            switch (result.getResult()) {
                case OK -> insertResults.put(result.getSubInfo(), SendResult.Result.OK);
                case NO_INBOX -> insertResults.put(result.getSubInfo(), SendResult.Result.NO_INBOX);
                case ERROR -> insertResults.put(result.getSubInfo(), SendResult.Result.ERROR);
            }
        }
        CallTask<MessagePack, SendResult.Result, MutationCallBatcherKey> task;
        while ((task = batchedTasks.poll()) != null) {
            task.callResult.complete(insertResults.get(task.call.getSubInfo()));
        }
    }

    @Override
    protected void handleException(CallTask<MessagePack, SendResult.Result, MutationCallBatcherKey> callTask,
                                   Throwable e) {
        callTask.callResult.complete(SendResult.Result.ERROR);
    }
}
