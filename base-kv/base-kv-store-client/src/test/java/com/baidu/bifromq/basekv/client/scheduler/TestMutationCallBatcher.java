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
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.google.protobuf.ByteString;
import java.time.Duration;

public class TestMutationCallBatcher extends MutationCallBatcher<ByteString, ByteString> {
    private final Duration pipelineExpiry;

    protected TestMutationCallBatcher(String name,
                                      long tolerableLatencyNanos,
                                      long burstLatencyNanos,
                                      MutationCallBatcherKey batcherKey,
                                      IBaseKVStoreClient storeClient,
                                      Duration pipelineExpiry) {
        super(name, tolerableLatencyNanos, burstLatencyNanos, batcherKey, storeClient);
        this.pipelineExpiry = pipelineExpiry;
    }

    @Override
    protected IBatchCall<ByteString, ByteString, MutationCallBatcherKey> newBatch() {
        return new TestBatchMutationCall(batcherKey.id, storeClient, pipelineExpiry);
    }
}
