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

package com.baidu.bifromq.mqtt.service;

import com.baidu.bifromq.baserpc.server.IRPCServer;
import com.baidu.bifromq.mqtt.inbox.RPCBluePrint;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class StandaloneLocalSessionServer extends AbstractLocalSessionServer<StandaloneLocalSessionServerBuilder> {
    private final IRPCServer server;

    public StandaloneLocalSessionServer(StandaloneLocalSessionServerBuilder builder) {
        super(builder);
        server = IRPCServer.newBuilder()
            .bindService(service.bindService(),
                RPCBluePrint.INSTANCE,
                builder.attributes,
                builder.defaultGroupTags,
                builder.rpcExecutor)
            .id(builder.id)
            .host(builder.host)
            .port(builder.port)
            .bossEventLoopGroup(builder.bossEventLoopGroup)
            .workerEventLoopGroup(builder.workerEventLoopGroup)
            .sslContext(builder.sslContext)
            .build();
    }

    @Override
    protected void afterServiceStart() {
        try {
            server.start();
        } catch (Throwable e) {
            log.error("Failed to start rpc server", e);
        }
    }

    @Override
    public void beforeServiceStop() {
        try {
            server.shutdown();
        } catch (Throwable e) {
            log.warn("Failed to stop rpc server", e);
        }
    }
}
