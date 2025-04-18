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
import com.baidu.bifromq.basekv.client.scheduler.QueryCallBatcher;
import com.baidu.bifromq.basekv.client.scheduler.QueryCallBatcherKey;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.rpc.proto.ExistReply;
import com.baidu.bifromq.inbox.rpc.proto.ExistRequest;
import com.baidu.bifromq.sysprops.props.InboxCheckQueuesPerRange;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxGetScheduler extends InboxReadScheduler<ExistRequest, ExistReply> implements IInboxGetScheduler {
    public InboxGetScheduler(IBaseKVStoreClient inboxStoreClient) {
        super(InboxCheckQueuesPerRange.INSTANCE.get(), inboxStoreClient, "inbox_server_check");
    }

    @Override
    protected Batcher<ExistRequest, ExistReply, QueryCallBatcherKey> newBatcher(String name,
                                                                                long tolerableLatencyNanos,
                                                                                long burstLatencyNanos,
                                                                                QueryCallBatcherKey batcherKey) {
        return new InboxGetBatcher(batcherKey, name, tolerableLatencyNanos, burstLatencyNanos, storeClient);
    }

    @Override
    protected ByteString rangeKey(ExistRequest request) {
        return inboxStartKeyPrefix(request.getTenantId(), request.getInboxId());
    }

    private static class InboxGetBatcher extends QueryCallBatcher<ExistRequest, ExistReply> {
        InboxGetBatcher(QueryCallBatcherKey batcherKey,
                        String name,
                        long tolerableLatencyNanos,
                        long burstLatencyNanos,
                        IBaseKVStoreClient storeClient) {
            super(name, tolerableLatencyNanos, burstLatencyNanos, batcherKey, storeClient);
        }

        @Override
        protected IBatchCall<ExistRequest, ExistReply, QueryCallBatcherKey> newBatch() {
            return new BatchExistCall(storeClient, batcherKey);
        }
    }
}
