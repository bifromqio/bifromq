/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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
import com.baidu.bifromq.plugin.subbroker.CheckReply;
import com.baidu.bifromq.sysprops.props.InboxCheckQueuesPerRange;
import com.google.protobuf.ByteString;
import java.time.Duration;

public class InboxCheckSubScheduler
    extends InboxReadScheduler<IInboxCheckSubScheduler.CheckMatchInfo, CheckReply.Code>
    implements IInboxCheckSubScheduler {
    public InboxCheckSubScheduler(IBaseKVStoreClient inboxStoreClient) {
        super(InboxCheckQueuesPerRange.INSTANCE.get(), inboxStoreClient, "inbox_server_checksub");
    }

    @Override
    protected ByteString rangeKey(CheckMatchInfo request) {
        return inboxStartKeyPrefix(request.tenantId(), request.matchInfo().getReceiverId());
    }

    @Override
    protected Batcher<CheckMatchInfo, CheckReply.Code, QueryCallBatcherKey> newBatcher(String name,
                                                                                       long tolerableLatencyNanos,
                                                                                       long burstLatencyNanos,
                                                                                       QueryCallBatcherKey batcherKey) {
        return new InboxCheckMatchInfoBatcher(batcherKey, name, tolerableLatencyNanos, burstLatencyNanos, storeClient);
    }

    private static class InboxCheckMatchInfoBatcher
        extends QueryCallBatcher<IInboxCheckSubScheduler.CheckMatchInfo, CheckReply.Code> {
        InboxCheckMatchInfoBatcher(QueryCallBatcherKey batcherKey,
                                   String name,
                                   long tolerableLatencyNanos,
                                   long burstLatencyNanos,
                                   IBaseKVStoreClient storeClient) {
            super(name, tolerableLatencyNanos, burstLatencyNanos, batcherKey, storeClient);
        }

        @Override
        protected IBatchCall<IInboxCheckSubScheduler.CheckMatchInfo, CheckReply.Code, QueryCallBatcherKey> newBatch() {
            return new BatchCheckSubCall(batcherKey.id, storeClient, Duration.ofMinutes(5));
        }
    }
}
