/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DATA_PLANE_BURST_LATENCY_MS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DATA_PLANE_TOLERABLE_LATENCY_MS;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.QueryCallBatcher;
import com.baidu.bifromq.basekv.client.scheduler.QueryCallBatcherKey;
import com.baidu.bifromq.basekv.client.scheduler.QueryCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.retain.rpc.proto.MatchReply;
import com.baidu.bifromq.retain.rpc.proto.MatchRequest;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MatchCallScheduler extends QueryCallScheduler<MatchRequest, MatchReply> implements IMatchCallScheduler {
    public MatchCallScheduler(IBaseKVStoreClient retainStoreClient) {
        super("retain_server_match_batcher", retainStoreClient,
            Duration.ofMillis(DATA_PLANE_TOLERABLE_LATENCY_MS.get()),
            Duration.ofSeconds(DATA_PLANE_BURST_LATENCY_MS.get()));
    }

    @Override
    protected Batcher<MatchRequest, MatchReply, QueryCallBatcherKey> newBatcher(String name,
                                                                                long tolerableLatencyNanos,
                                                                                long burstLatencyNanos,
                                                                                QueryCallBatcherKey batcherKey) {
        return new MatchCallBatcher(batcherKey, name, tolerableLatencyNanos, burstLatencyNanos, storeClient);
    }

    @Override
    protected int selectQueue(MatchRequest request) {
        return ThreadLocalRandom.current().nextInt(5);
    }

    @Override
    protected ByteString rangeKey(MatchRequest request) {
        return tenantNS(request.getTenantId());
    }

    public static class MatchCallBatcher extends QueryCallBatcher<MatchRequest, MatchReply> {

        protected MatchCallBatcher(QueryCallBatcherKey batcherKey,
                                   String name,
                                   long tolerableLatencyNanos,
                                   long burstLatencyNanos,
                                   IBaseKVStoreClient storeClient) {
            super(name, tolerableLatencyNanos, burstLatencyNanos, batcherKey, storeClient);
        }

        @Override
        protected IBatchCall<MatchRequest, MatchReply, QueryCallBatcherKey> newBatch() {
            return new BatchMatchCall(batcherKey.id, storeClient, Duration.ofMinutes(5));
        }
    }
}
