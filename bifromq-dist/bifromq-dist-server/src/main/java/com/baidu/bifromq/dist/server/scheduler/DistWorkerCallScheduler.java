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

package com.baidu.bifromq.dist.server.scheduler;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.sysprops.props.DataPlaneBurstLatencyMillis;
import com.baidu.bifromq.sysprops.props.DataPlaneTolerableLatencyMillis;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Scheduling dist worker calls by batching dist server calls.
 */
@Slf4j
public class DistWorkerCallScheduler
    extends BatchCallScheduler<DistServerCall, Map<String, Optional<Integer>>, DistServerCallBatcherKey>
    implements IDistWorkerCallScheduler {

    private final ISettingProvider settingProvider;
    private final IBaseKVStoreClient distWorkerClient;

    /**
     * Constructor of DistWorkerCallScheduler.
     *
     * @param distWorkerClient the dist worker client
     * @param settingProvider  the setting provider
     */
    public DistWorkerCallScheduler(IBaseKVStoreClient distWorkerClient,
                                   ISettingProvider settingProvider) {
        super("dist_server_dist_batcher",
            Duration.ofMillis(DataPlaneTolerableLatencyMillis.INSTANCE.get()),
            Duration.ofMillis(DataPlaneBurstLatencyMillis.INSTANCE.get()));
        this.settingProvider = settingProvider;
        this.distWorkerClient = distWorkerClient;
    }

    @Override
    protected Batcher<DistServerCall, Map<String, Optional<Integer>>, DistServerCallBatcherKey> newBatcher(
        String name, long tolerableLatencyNanos, long burstLatencyNanos, DistServerCallBatcherKey batchKey) {
        return new DistServerCallBatcher(batchKey, name, tolerableLatencyNanos, burstLatencyNanos,
            distWorkerClient, settingProvider);
    }

    @Override
    protected Optional<DistServerCallBatcherKey> find(DistServerCall request) {
        return Optional.of(new DistServerCallBatcherKey(request.tenantId(), request.callQueueIdx()));
    }
}
