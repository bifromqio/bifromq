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

import static com.baidu.bifromq.inbox.store.schema.KVSchemaUtil.inboxInstanceStartKey;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.QueryCallBatcher;
import com.baidu.bifromq.basekv.client.scheduler.QueryCallBatcherKey;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.sysprops.props.InboxFetchQueuesPerRange;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxFetchScheduler extends InboxReadScheduler<IInboxFetchScheduler.InboxFetch, Fetched>
    implements IInboxFetchScheduler {
    public InboxFetchScheduler(IBaseKVStoreClient inboxStoreClient) {
        super(InboxFetchQueuesPerRange.INSTANCE.get(), inboxStoreClient, "inbox_server_fetch");
    }

    @Override
    protected Batcher<InboxFetch, Fetched, QueryCallBatcherKey> newBatcher(String name, long tolerableLatencyNanos,
                                                                           long burstLatencyNanos,
                                                                           QueryCallBatcherKey inboxReadBatcherKey) {
        return new InboxFetchBatcher(inboxReadBatcherKey, name, tolerableLatencyNanos, burstLatencyNanos, storeClient);
    }

    @Override
    protected int selectQueue(InboxFetch request) {
        int idx = Objects.hash(request.tenantId, request.inboxId, request.incarnation) % queuesPerRange;
        if (idx < 0) {
            idx += queuesPerRange;
        }
        return idx;
    }

    @Override
    protected ByteString rangeKey(InboxFetch request) {
        return inboxInstanceStartKey(request.tenantId, request.inboxId, request.incarnation);
    }

    private static class InboxFetchBatcher extends QueryCallBatcher<InboxFetch, Fetched> {
        InboxFetchBatcher(QueryCallBatcherKey batcherKey, String name, long tolerableLatencyNanos,
                          long burstLatencyNanos, IBaseKVStoreClient storeClient) {
            super(name, tolerableLatencyNanos, burstLatencyNanos, batcherKey, storeClient);
        }

        @Override
        protected IBatchCall<InboxFetch, Fetched, QueryCallBatcherKey> newBatch() {
            return new BatchFetchCall(batcherKey.id, storeClient, Duration.ofMinutes(5));
        }
    }
}
