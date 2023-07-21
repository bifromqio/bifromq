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

package com.baidu.bifromq.starter.config;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.balance.option.KVRangeBalanceControllerOptions;
import com.baidu.bifromq.starter.config.model.AgentHostConfig;
import com.baidu.bifromq.starter.config.model.ServerSSLContextConfig;
import com.baidu.bifromq.starter.config.model.StorageEngineConfig;
import com.baidu.bifromq.starter.config.model.StoreClientConfig;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RetainStoreConfig implements StarterConfig {
    private String bindAddress = StarterConfig.getHostFromSysProps();

    private int port;

    private int bgWorkerThreads = Math.max(1, EnvProvider.INSTANCE.availableProcessors() / 4);

    private boolean bootstrap;

    private String overrideIdentity;

    private int walFlushBufferSize = 1024;

    private StorageEngineConfig dataEngineConfig;

    private StorageEngineConfig walEngineConfig;

    private KVRangeBalanceControllerOptions balanceConfig;

    private ServerSSLContextConfig serverSSLCtxConfig = new ServerSSLContextConfig();

    private AgentHostConfig agentHostConfig;

    private StoreClientConfig retainStoreClientConfig = new StoreClientConfig();

}
