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

import static com.baidu.bifromq.inbox.store.schema.KVSchemaUtil.inboxStartKeyPrefix;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcher;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.rpc.proto.DetachReply;
import com.baidu.bifromq.inbox.rpc.proto.DetachRequest;
import com.baidu.bifromq.sysprops.props.ControlPlaneBurstLatencyMillis;
import com.baidu.bifromq.sysprops.props.ControlPlaneTolerableLatencyMillis;
import com.google.protobuf.ByteString;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxDetachScheduler extends MutationCallScheduler<DetachRequest, DetachReply>
    implements IInboxDetachScheduler {

    public InboxDetachScheduler(IBaseKVStoreClient inboxStoreClient) {
        super("inbox_server_detach", inboxStoreClient,
            Duration.ofMillis(ControlPlaneTolerableLatencyMillis.INSTANCE.get()),
            Duration.ofMillis(ControlPlaneBurstLatencyMillis.INSTANCE.get()));
    }

    @Override
    protected Batcher<DetachRequest, DetachReply, MutationCallBatcherKey> newBatcher(
        String name,
        long tolerableLatencyNanos,
        long burstLatencyNanos,
        MutationCallBatcherKey range) {
        return new InboxDetachBatcher(name, tolerableLatencyNanos, burstLatencyNanos, range, storeClient);
    }

    @Override
    protected ByteString rangeKey(DetachRequest request) {
        return inboxStartKeyPrefix(request.getClient().getTenantId(), request.getInboxId());
    }

    private static class InboxDetachBatcher extends MutationCallBatcher<DetachRequest, DetachReply> {
        private InboxDetachBatcher(String name,
                                   long expectLatencyNanos,
                                   long maxTolerableLatencyNanos,
                                   MutationCallBatcherKey range,
                                   IBaseKVStoreClient inboxStoreClient) {
            super(name, expectLatencyNanos, maxTolerableLatencyNanos, range, inboxStoreClient);
        }

        @Override
        protected IBatchCall<DetachRequest, DetachReply, MutationCallBatcherKey> newBatch() {
            return new BatchDetachCall(batcherKey.id, storeClient, Duration.ofMinutes(5));
        }
    }
}
