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

package com.baidu.bifromq.basekv.client.scheduler;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.basescheduler.ICallTask;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;

public class TestBatchMutationCall extends BatchMutationCall<ByteString, ByteString> {
    protected TestBatchMutationCall(KVRangeId rangeId,
                                    IBaseKVStoreClient storeClient,
                                    Duration pipelineExpiryTime) {
        super(rangeId, storeClient, pipelineExpiryTime);
    }

    @Override
    protected MutationCallTaskBatch<ByteString, ByteString> newBatch(String storeId, long ver) {
        return new TestBatchCallTask(storeId, ver);
    }

    @Override
    protected RWCoProcInput makeBatch(Iterator<ByteString> byteStringIterator) {
        ByteString finalBS = byteStringIterator.hasNext() ? byteStringIterator.next() : ByteString.empty();
        while (byteStringIterator.hasNext()) {
            finalBS = finalBS.concat(ByteString.copyFromUtf8("_")).concat(byteStringIterator.next());
        }
        return RWCoProcInput.newBuilder()
            .setRaw(finalBS)
            .build();
    }

    @Override
    protected void handleOutput(Queue<ICallTask<ByteString, ByteString, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        ICallTask<ByteString, ByteString, MutationCallBatcherKey> task;
        while ((task = batchedTasks.poll()) != null) {
            // just echo the request
            task.resultPromise().complete(task.call());
        }
    }

    @Override
    protected void handleException(ICallTask<ByteString, ByteString, MutationCallBatcherKey> callTask, Throwable e) {
        callTask.resultPromise().completeExceptionally(e);
    }

    private static class TestBatchCallTask extends MutationCallTaskBatch<ByteString, ByteString> {
        private final Set<ByteString> keys = new HashSet<>();

        protected TestBatchCallTask(String storeId, long ver) {
            super(storeId, ver);
        }

        @Override
        protected void add(ICallTask<ByteString, ByteString, MutationCallBatcherKey> callTask) {
            super.add(callTask);
            keys.add(callTask.call());
        }

        @Override
        protected boolean isBatchable(ICallTask<ByteString, ByteString, MutationCallBatcherKey> callTask) {
            return !keys.contains(callTask.call());
        }
    }
}
