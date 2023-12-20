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

import static com.baidu.bifromq.inbox.util.KeyUtil.scopedInboxId;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.INBOX_CHECK_QUEUES_PER_RANGE;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.QueryCallBatcher;
import com.baidu.bifromq.basekv.client.scheduler.QueryCallBatcherKey;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxRequest;
import com.google.protobuf.ByteString;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxCheckScheduler extends InboxReadScheduler<HasInboxRequest, HasInboxReply>
    implements IInboxCheckScheduler {
    public InboxCheckScheduler(IBaseKVStoreClient inboxStoreClient) {
        super(INBOX_CHECK_QUEUES_PER_RANGE.get(), inboxStoreClient, "inbox_server_check");
    }

    @Override
    protected Batcher<HasInboxRequest, HasInboxReply, QueryCallBatcherKey> newBatcher(String name,
                                                                                      long tolerableLatencyNanos,
                                                                                      long burstLatencyNanos,
                                                                                      QueryCallBatcherKey batcherKey) {
        return new InboxCheckBatcher(batcherKey, name, tolerableLatencyNanos, burstLatencyNanos, storeClient);
    }

    @Override
    protected ByteString rangeKey(HasInboxRequest request) {
        return scopedInboxId(request.getTenantId(), request.getInboxId());
    }

    private static class InboxCheckBatcher extends QueryCallBatcher<HasInboxRequest, HasInboxReply> {
        InboxCheckBatcher(QueryCallBatcherKey batcherKey,
                          String name,
                          long tolerableLatencyNanos,
                          long burstLatencyNanos,
                          IBaseKVStoreClient storeClient) {
            super(name, tolerableLatencyNanos, burstLatencyNanos, batcherKey, storeClient);
        }

        @Override
        protected IBatchCall<HasInboxRequest, HasInboxReply, QueryCallBatcherKey> newBatch() {
            return new BatchCheckCall(batcherKey.id, storeClient, Duration.ofMinutes(5));
        }
    }
}
