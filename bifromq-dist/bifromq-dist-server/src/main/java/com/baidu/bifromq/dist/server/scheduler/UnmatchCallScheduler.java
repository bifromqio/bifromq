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

package com.baidu.bifromq.dist.server.scheduler;

import static com.baidu.bifromq.dist.entity.EntityUtil.toMatchRecordKey;
import static com.baidu.bifromq.dist.entity.EntityUtil.toQInboxId;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.CONTROL_PLANE_BURST_LATENCY_MS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.CONTROL_PLANE_TOLERABLE_LATENCY_MS;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcher;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.dist.rpc.proto.UnmatchReply;
import com.baidu.bifromq.dist.rpc.proto.UnmatchRequest;
import com.google.protobuf.ByteString;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UnmatchCallScheduler extends MutationCallScheduler<UnmatchRequest, UnmatchReply>
    implements IUnmatchCallScheduler {

    public UnmatchCallScheduler(IBaseKVStoreClient distWorkerClient) {
        super("dist_server_unsub_batcher",
            distWorkerClient,
            Duration.ofMillis(CONTROL_PLANE_TOLERABLE_LATENCY_MS.get()),
            Duration.ofMillis(CONTROL_PLANE_BURST_LATENCY_MS.get()));
    }

    @Override
    protected Batcher<UnmatchRequest, UnmatchReply, MutationCallBatcherKey> newBatcher(String name,
                                                                                       long tolerableLatencyNanos,
                                                                                       long burstLatencyNanos,
                                                                                       MutationCallBatcherKey range) {
        return new UnsubCallBatcher(name, tolerableLatencyNanos, burstLatencyNanos, range, storeClient);
    }

    protected ByteString rangeKey(UnmatchRequest call) {
        String qInboxId = toQInboxId(call.getBroker(), call.getInboxId(), call.getDelivererKey());
        return toMatchRecordKey(call.getTenantId(), call.getTopicFilter(), qInboxId);
    }

    private static class UnsubCallBatcher extends MutationCallBatcher<UnmatchRequest, UnmatchReply> {
        private UnsubCallBatcher(String name,
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
}
