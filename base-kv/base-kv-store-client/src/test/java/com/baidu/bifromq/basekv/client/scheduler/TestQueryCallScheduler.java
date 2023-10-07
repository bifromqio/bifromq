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
import com.baidu.bifromq.basescheduler.Batcher;
import com.google.protobuf.ByteString;
import java.time.Duration;

public class TestQueryCallScheduler extends QueryCallScheduler<ByteString, ByteString> {
    private final Duration pipelineExpire;
    private final boolean linearizable;

    public TestQueryCallScheduler(String name,
                                  IBaseKVStoreClient storeClient,
                                  Duration tolerableLatency,
                                  Duration burstLatency,
                                  Duration pipelineExpire,
                                  boolean linearizable) {
        super(name, storeClient, tolerableLatency, burstLatency);
        this.pipelineExpire = pipelineExpire;
        this.linearizable = linearizable;
    }

    public TestQueryCallScheduler(String name,
                                  IBaseKVStoreClient storeClient,
                                  Duration tolerableLatency,
                                  Duration burstLatency,
                                  Duration pipelineExpire,
                                  Duration batcherExpiry,
                                  boolean linearizable) {
        super(name, storeClient, tolerableLatency, burstLatency, batcherExpiry);
        this.pipelineExpire = pipelineExpire;
        this.linearizable = linearizable;
    }

    @Override
    protected int selectQueue(ByteString request) {
        return 0;
    }

    @Override
    protected ByteString rangeKey(ByteString call) {
        return call;
    }

    @Override
    protected Batcher<ByteString, ByteString, QueryCallBatcherKey> newBatcher(String name,
                                                                              long tolerableLatencyNanos,
                                                                              long burstLatencyNanos,
                                                                              QueryCallBatcherKey batcherKey) {
        return new TestQueryCallBatcher(
            name,
            tolerableLatencyNanos,
            burstLatencyNanos,
            batcherKey,
            storeClient,
            pipelineExpire,
            linearizable
        );
    }
}
