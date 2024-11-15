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

package com.baidu.bifromq.dist.client.scheduler;

import static java.util.Collections.emptyMap;

import com.baidu.bifromq.baserpc.client.IRPCClient;
import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.dist.client.PubResult;
import com.baidu.bifromq.dist.rpc.proto.DistServiceGrpc;
import com.baidu.bifromq.sysprops.props.DataPlaneBurstLatencyMillis;
import com.baidu.bifromq.sysprops.props.DataPlaneTolerableLatencyMillis;
import java.time.Duration;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DistServerCallScheduler extends BatchCallScheduler<PubCall, PubResult, PubCallBatcherKey>
    implements IDistServerCallScheduler {
    private final IRPCClient rpcClient;

    public DistServerCallScheduler(IRPCClient rpcClient) {
        super("dist_client_send_batcher", Duration.ofMillis(DataPlaneTolerableLatencyMillis.INSTANCE.get()),
            Duration.ofMillis(DataPlaneBurstLatencyMillis.INSTANCE.get()));
        this.rpcClient = rpcClient;
    }

    @Override
    protected Batcher<PubCall, PubResult, PubCallBatcherKey> newBatcher(String name,
                                                                        long tolerableLatencyNanos,
                                                                        long burstLatencyNanos,
                                                                        PubCallBatcherKey batcherKey) {
        return new PubCallBatcher(batcherKey, name, tolerableLatencyNanos, burstLatencyNanos,
            rpcClient.createRequestPipeline(batcherKey.tenantId(), null, null, emptyMap(),
                DistServiceGrpc.getDistMethod()));
    }

    @Override
    protected Optional<PubCallBatcherKey> find(PubCall clientCall) {
        return Optional.of(new PubCallBatcherKey(clientCall.publisher.getTenantId(), Thread.currentThread().getId()));
    }
}
