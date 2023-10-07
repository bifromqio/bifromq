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

package com.baidu.bifromq.basekv.client.scheduler;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.basescheduler.CallTask;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Iterator;
import java.util.Queue;

public class TestBatchMutationCall extends BatchMutationCall<ByteString, ByteString> {
    protected TestBatchMutationCall(KVRangeId rangeId,
                                    IBaseKVStoreClient storeClient,
                                    Duration pipelineExpiryTime) {
        super(rangeId, storeClient, pipelineExpiryTime);
    }

    @Override
    protected RWCoProcInput makeBatch(Iterator<ByteString> byteStringIterator) {
        ByteString finalBS = ByteString.empty();
        while (byteStringIterator.hasNext()) {
            finalBS = finalBS.concat(byteStringIterator.next());
        }
        return RWCoProcInput.newBuilder()
            .setRaw(finalBS)
            .build();
    }

    @Override
    protected void handleOutput(Queue<CallTask<ByteString, ByteString, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        CallTask<ByteString, ByteString, MutationCallBatcherKey> task;
        while ((task = batchedTasks.poll()) != null) {
            // just echo the request
            task.callResult.complete(task.call);
        }
    }

    @Override
    protected void handleException(CallTask<ByteString, ByteString, MutationCallBatcherKey> callTask, Throwable e) {
        callTask.callResult.completeExceptionally(e);
    }
}
