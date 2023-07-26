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

package com.baidu.bifromq.basekv.server;

import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProcFactory;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor(access = AccessLevel.PACKAGE)
@Slf4j
@Accessors(fluent = true)
@Setter
public class BaseKVStoreServerBuilder {
    KVRangeStoreOptions storeOptions = new KVRangeStoreOptions();
    String clusterId;
    String host;
    int port;
    EventLoopGroup bossEventLoopGroup;
    EventLoopGroup workerEventLoopGroup;
    SslContext sslContext;
    IAgentHost agentHost;
    ICRDTService crdtService;
    IKVRangeCoProcFactory coProcFactory;
    Executor ioExecutor;
    Executor queryExecutor;
    Executor mutationExecutor;
    ScheduledExecutorService tickTaskExecutor;
    ScheduledExecutorService bgTaskExecutor;

    public IBaseKVStoreServer build() {
        return new BaseKVStoreServer(this);
    }
}
