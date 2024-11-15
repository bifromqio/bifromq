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

import com.baidu.bifromq.baserpc.server.IRPCServer;
import com.baidu.bifromq.baserpc.server.RPCServerBuilder;
import java.util.Collections;

final class StandaloneBaseKVStoreServer extends AbstractBaseKVStoreServer<StandaloneBaseKVStoreServerBuilder> {
    private final IRPCServer rpcServer;

    StandaloneBaseKVStoreServer(StandaloneBaseKVStoreServerBuilder builder) {
        super(builder);
        RPCServerBuilder rpcServerBuilder = IRPCServer.newBuilder()
            .host(builder.host)
            .port(builder.port)
            .trafficService(builder.trafficService)
            .sslContext(builder.sslContext)
            .bossEventLoopGroup(builder.bossEventLoopGroup)
            .workerEventLoopGroup(builder.workerEventLoopGroup);
        for (BindableStoreService bindable : bindableStoreServices) {
            rpcServerBuilder.bindService(
                bindable.serviceDefinition,
                bindable.bluePrint,
                bindable.metadata,
                Collections.emptySet(),
                bindable.executor);
        }
        rpcServer = rpcServerBuilder.build();
    }

    @Override
    protected void afterServiceStart() {
        rpcServer.start();
    }

    @Override
    protected void beforeServiceStop() {
        rpcServer.shutdown();
    }
}

