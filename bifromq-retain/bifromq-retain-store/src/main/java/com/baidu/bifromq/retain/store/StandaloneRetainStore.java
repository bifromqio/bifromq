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

package com.baidu.bifromq.retain.store;

import com.baidu.bifromq.basekv.server.IBaseKVStoreServer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class StandaloneRetainStore extends AbstractRetainStore<StandaloneRetainStoreBuilder> {
    private final IBaseKVStoreServer storeServer;

    public StandaloneRetainStore(StandaloneRetainStoreBuilder builder) {
        super(builder);
        storeServer = IBaseKVStoreServer.standaloneServer()
            .crdtService(builder.crdtService)
            // build basekv store service
            .addService(builder.clusterId, builder.bootstrap)
            .coProcFactory(coProcFactory)
            .storeOptions(builder.storeOptions)
            .agentHost(builder.agentHost)
            .queryExecutor(builder.queryExecutor)
            .tickTaskExecutor(builder.tickTaskExecutor)
            .bgTaskExecutor(builder.bgTaskExecutor)
            .finish()
            // build rpc server
            .host(builder.host)
            .port(builder.port)
            .bossEventLoopGroup(builder.bossEventLoopGroup)
            .workerEventLoopGroup(builder.workerEventLoopGroup)
            .ioExecutor(builder.ioExecutor)
            .sslContext(builder.sslContext)
            .build();
    }

    @Override
    protected IBaseKVStoreServer storeServer() {
        return storeServer;
    }
}
