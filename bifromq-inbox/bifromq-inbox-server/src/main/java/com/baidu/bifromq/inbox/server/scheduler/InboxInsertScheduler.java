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
import com.baidu.bifromq.inbox.storage.proto.BatchInsertReply;
import com.baidu.bifromq.inbox.storage.proto.InboxSubMessagePack;
import com.baidu.bifromq.sysprops.props.DataPlaneBurstLatencyMillis;
import com.baidu.bifromq.sysprops.props.DataPlaneTolerableLatencyMillis;
import com.google.protobuf.ByteString;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxInsertScheduler extends MutationCallScheduler<InboxSubMessagePack, BatchInsertReply.Result>
    implements IInboxInsertScheduler {
    public InboxInsertScheduler(IBaseKVStoreClient inboxStoreClient) {
        super("inbox_server_insert", inboxStoreClient,
            Duration.ofMillis(DataPlaneTolerableLatencyMillis.INSTANCE.get()),
            Duration.ofMillis(DataPlaneBurstLatencyMillis.INSTANCE.get()));
    }

    @Override
    protected Batcher<InboxSubMessagePack, BatchInsertReply.Result, MutationCallBatcherKey> newBatcher(String name,
                                                                                                       long tolerableLatencyNanos,
                                                                                                       long burstLatencyNanos,
                                                                                                       MutationCallBatcherKey batcherKey) {
        return new InboxInsertBatcher(name, tolerableLatencyNanos, burstLatencyNanos, batcherKey, storeClient);
    }

    @Override
    protected ByteString rangeKey(InboxSubMessagePack request) {
        return inboxStartKeyPrefix(request.getTenantId(), request.getInboxId());
    }

    private static class InboxInsertBatcher extends MutationCallBatcher<InboxSubMessagePack, BatchInsertReply.Result> {
        InboxInsertBatcher(String name,
                           long tolerableLatencyNanos,
                           long burstLatencyNanos,
                           MutationCallBatcherKey batchKey,
                           IBaseKVStoreClient inboxStoreClient) {
            super(name, tolerableLatencyNanos, burstLatencyNanos, batchKey, inboxStoreClient);
        }

        @Override
        protected IBatchCall<InboxSubMessagePack, BatchInsertReply.Result, MutationCallBatcherKey> newBatch() {
            return new BatchInsertCall(batcherKey.id, storeClient, Duration.ofMinutes(5));
        }
    }
}
