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

package com.baidu.bifromq.basekv.server;

import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProcFactory;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Accessors(fluent = true)
@Setter
public class BaseKVStoreServiceBuilder<P extends AbstractBaseKVStoreServerBuilder<P>> {
    final P serverBuilder;
    final String clusterId;
    final boolean bootstrap;
    KVRangeStoreOptions storeOptions = new KVRangeStoreOptions();
    IKVRangeCoProcFactory coProcFactory;
    IAgentHost agentHost;
    Executor queryExecutor;
    ScheduledExecutorService tickTaskExecutor;
    ScheduledExecutorService bgTaskExecutor;

    BaseKVStoreServiceBuilder(String clusterId, boolean bootstrap,
                              P serverBuilder) {
        this.clusterId = clusterId;
        this.bootstrap = bootstrap;
        this.serverBuilder = serverBuilder;
    }

    public final P finish() {
        serverBuilder.serviceBuilders.put(clusterId, this);
        return serverBuilder;
    }
}
