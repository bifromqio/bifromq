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

import static com.baidu.bifromq.retain.utils.KeyUtil.tenantNS;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.QueryCallBatcher;
import com.baidu.bifromq.basekv.client.scheduler.QueryCallBatcherKey;
import com.baidu.bifromq.basekv.client.scheduler.QueryCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.sysprops.props.DataPlaneBurstLatencyMillis;
import com.baidu.bifromq.sysprops.props.DataPlaneTolerableLatencyMillis;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MatchCallScheduler extends QueryCallScheduler<MatchCall, MatchCallResult> implements IMatchCallScheduler {
    public MatchCallScheduler(IBaseKVStoreClient retainStoreClient) {
        super("retain_server_match_batcher", retainStoreClient,
            Duration.ofMillis(DataPlaneTolerableLatencyMillis.INSTANCE.get()),
            Duration.ofSeconds(DataPlaneBurstLatencyMillis.INSTANCE.get()));
    }

    @Override
    protected Batcher<MatchCall, MatchCallResult, QueryCallBatcherKey> newBatcher(String name,
                                                                                  long tolerableLatencyNanos,
                                                                                  long burstLatencyNanos,
                                                                                  QueryCallBatcherKey batcherKey) {
        return new MatchCallBatcher(batcherKey, name, tolerableLatencyNanos, burstLatencyNanos, storeClient);
    }

    @Override
    protected int selectQueue(MatchCall request) {
        return ThreadLocalRandom.current().nextInt(5);
    }

    @Override
    protected ByteString rangeKey(MatchCall request) {
        return tenantNS(request.tenantId());
    }

    public static class MatchCallBatcher extends QueryCallBatcher<MatchCall, MatchCallResult> {

        protected MatchCallBatcher(QueryCallBatcherKey batcherKey,
                                   String name,
                                   long tolerableLatencyNanos,
                                   long burstLatencyNanos,
                                   IBaseKVStoreClient storeClient) {
            super(name, tolerableLatencyNanos, burstLatencyNanos, batcherKey, storeClient);
        }

        @Override
        protected IBatchCall<MatchCall, MatchCallResult, QueryCallBatcherKey> newBatch() {
            return new BatchMatchCall(batcherKey.id, storeClient, Duration.ofMinutes(5));
        }
    }
}
