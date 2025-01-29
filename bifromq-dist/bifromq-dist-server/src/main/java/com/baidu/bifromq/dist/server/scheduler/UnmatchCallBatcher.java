/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.dist.server.scheduler;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcher;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.dist.rpc.proto.UnmatchReply;
import com.baidu.bifromq.dist.rpc.proto.UnmatchRequest;
import java.time.Duration;

class UnmatchCallBatcher extends MutationCallBatcher<UnmatchRequest, UnmatchReply> {
    UnmatchCallBatcher(String name,
                       long tolerableLatencyNanos,
                       long burstLatencyNanos,
                       MutationCallBatcherKey batcherKey,
                       IBaseKVStoreClient distWorkerClient) {
        super(name, tolerableLatencyNanos, burstLatencyNanos, batcherKey, distWorkerClient);
    }

    @Override
    protected IBatchCall<UnmatchRequest, UnmatchReply, MutationCallBatcherKey> newBatch() {
        return new BatchUnmatchCall(batcherKey.id, storeClient, Duration.ofMinutes(5));
    }
}
