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

package com.baidu.bifromq.inbox.server;

import com.baidu.bifromq.baserpc.server.IRPCServer;
import com.baidu.bifromq.inbox.RPCBluePrint;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class StandaloneInboxServer extends AbstractInboxServer {
    private final IRPCServer rpcServer;

    StandaloneInboxServer(StandaloneInboxServerBuilder builder) {
        super(builder);
        this.rpcServer = IRPCServer.newBuilder()
            .bindService(inboxService.bindService(),
                RPCBluePrint.INSTANCE,
                builder.attrs,
                builder.defaultGroupTags,
                builder.rpcExecutor)
            .host(builder.host)
            .port(builder.port)
            .trafficService(builder.trafficService)
            .bossEventLoopGroup(builder.bossEventLoopGroup)
            .workerEventLoopGroup(builder.workerEventLoopGroup)
            .sslContext(builder.sslContext)
            .build();
    }

    @Override
    public void start() {
        log.info("Starting inbox server");
        super.start();
        log.debug("Starting rpc server");
        rpcServer.start();
        log.info("Inbox server started");
    }

    @SneakyThrows
    @Override
    public void shutdown() {
        log.info("Shutting down inbox server");
        log.debug("Shutting down inbox rpc server");
        rpcServer.shutdown();
        super.shutdown();
        log.info("Inbox server shutdown");
    }
}
