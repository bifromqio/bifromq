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

package com.baidu.bifromq.retain.server.scheduler;

import static com.baidu.bifromq.retain.store.schema.KVSchemaUtil.retainMessageKey;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcher;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import com.baidu.bifromq.retain.rpc.proto.RetainRequest;
import com.baidu.bifromq.sysprops.props.DataPlaneBurstLatencyMillis;
import com.baidu.bifromq.sysprops.props.DataPlaneTolerableLatencyMillis;
import com.google.protobuf.ByteString;
import java.time.Duration;

public class DeleteCallScheduler extends MutationCallScheduler<RetainRequest, RetainReply>
    implements IRetainCallScheduler {
    private final IBaseKVStoreClient retainStoreClient;

    public DeleteCallScheduler(IBaseKVStoreClient retainStoreClient) {
        super("retain_server_delete_batcher", retainStoreClient,
            Duration.ofMillis(DataPlaneTolerableLatencyMillis.INSTANCE.get()),
            Duration.ofMillis(DataPlaneBurstLatencyMillis.INSTANCE.get()));
        this.retainStoreClient = retainStoreClient;
    }

    @Override
    protected ByteString rangeKey(RetainRequest request) {
        return retainMessageKey(request.getPublisher().getTenantId(), request.getTopic());
    }

    @Override
    protected Batcher<RetainRequest, RetainReply, MutationCallBatcherKey> newBatcher(String name,
                                                                                     long tolerableLatencyNanos,
                                                                                     long burstLatencyNanos,
                                                                                     MutationCallBatcherKey batchKey) {
        return new DeleteCallBatcher(batchKey, name, tolerableLatencyNanos, burstLatencyNanos, retainStoreClient);
    }

    private static class DeleteCallBatcher extends MutationCallBatcher<RetainRequest, RetainReply> {

        protected DeleteCallBatcher(MutationCallBatcherKey batchKey,
                                    String name,
                                    long tolerableLatencyNanos,
                                    long burstLatencyNanos,
                                    IBaseKVStoreClient retainStoreClient) {
            super(name, tolerableLatencyNanos, burstLatencyNanos, batchKey, retainStoreClient);
        }

        @Override
        protected IBatchCall<RetainRequest, RetainReply, MutationCallBatcherKey> newBatch() {
            return new BatchRetainCall(batcherKey.id, storeClient, Duration.ofMinutes(5));
        }
    }
}
