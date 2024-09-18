/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.sysprops.props.DataPlaneBurstLatencyMillis;
import com.baidu.bifromq.sysprops.props.DataPlaneTolerableLatencyMillis;
import java.time.Duration;
import java.util.Optional;

public class MatchCallScheduler extends BatchCallScheduler<MatchCall, MatchCallResult, MatchCallBatcherKey>
    implements IMatchCallScheduler {
    private final IBaseKVStoreClient retainStoreClient;
    private final ISettingProvider settingProvider;

    public MatchCallScheduler(IBaseKVStoreClient retainStoreClient, ISettingProvider settingProvider) {
        super("retain_server_match_batcher",
            Duration.ofMillis(DataPlaneTolerableLatencyMillis.INSTANCE.get()),
            Duration.ofSeconds(DataPlaneBurstLatencyMillis.INSTANCE.get()));
        this.retainStoreClient = retainStoreClient;
        this.settingProvider = settingProvider;
    }

    @Override
    protected Batcher<MatchCall, MatchCallResult, MatchCallBatcherKey> newBatcher(String name,
                                                                                  long tolerableLatencyNanos,
                                                                                  long burstLatencyNanos,
                                                                                  MatchCallBatcherKey key) {
        return new MatchCallBatcher(key, name, tolerableLatencyNanos, burstLatencyNanos,
            retainStoreClient, settingProvider);
    }

    @Override
    protected Optional<MatchCallBatcherKey> find(MatchCall call) {
        // TODO: implement multi batcher for tenant
        return Optional.of(new MatchCallBatcherKey(call.tenantId(), 0));
    }
}
