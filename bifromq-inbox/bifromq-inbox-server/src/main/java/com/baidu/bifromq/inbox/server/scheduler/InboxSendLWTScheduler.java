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
import com.baidu.bifromq.inbox.rpc.proto.SendLWTReply;
import com.baidu.bifromq.inbox.rpc.proto.SendLWTRequest;
import com.baidu.bifromq.sysprops.props.InboxCheckQueuesPerRange;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxSendLWTScheduler extends InboxReadScheduler<SendLWTRequest, SendLWTReply>
    implements IInboxSendLWTScheduler {
    public InboxSendLWTScheduler(IBaseKVStoreClient inboxStoreClient) {
        super(InboxCheckQueuesPerRange.INSTANCE.get(), inboxStoreClient, "inbox_server_check");
    }

    @Override
    protected Batcher<SendLWTRequest, SendLWTReply, QueryCallBatcherKey> newBatcher(String name,
                                                                                    long tolerableLatencyNanos,
                                                                                    long burstLatencyNanos,
                                                                                    QueryCallBatcherKey batcherKey) {
        return new InboxSendLWTBatcher(batcherKey, name, tolerableLatencyNanos, burstLatencyNanos, storeClient);
    }

    @Override
    protected ByteString rangeKey(SendLWTRequest request) {
        return inboxStartKeyPrefix(request.getTenantId(), request.getInboxId());
    }

    private static class InboxSendLWTBatcher extends QueryCallBatcher<SendLWTRequest, SendLWTReply> {
        InboxSendLWTBatcher(QueryCallBatcherKey batcherKey,
                            String name,
                            long tolerableLatencyNanos,
                            long burstLatencyNanos,
                            IBaseKVStoreClient storeClient) {
            super(name, tolerableLatencyNanos, burstLatencyNanos, batcherKey, storeClient);
        }

        @Override
        protected IBatchCall<SendLWTRequest, SendLWTReply, QueryCallBatcherKey> newBatch() {
            return new BatchSendLWTCall(storeClient, batcherKey);
        }
    }
}
