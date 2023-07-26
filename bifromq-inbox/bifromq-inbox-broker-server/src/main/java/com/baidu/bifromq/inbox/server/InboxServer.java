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

package com.baidu.bifromq.inbox.server;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.baserpc.IRPCServer;
import com.baidu.bifromq.inbox.RPCBluePrint;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import java.util.concurrent.ScheduledExecutorService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class InboxServer implements IInboxServer {
    private final IRPCServer rpcServer;
    private final InboxService inboxService;

    InboxServer(InboxServerBuilder builder, ISettingProvider settingProvider,
                IBaseKVStoreClient inboxStoreClient,
                ScheduledExecutorService bgTaskExecutor) {
        this.inboxService = new InboxService(settingProvider, inboxStoreClient, bgTaskExecutor);
        this.rpcServer = IRPCServer.newBuilder()
            .bindService(inboxService.bindService(), RPCBluePrint.INSTANCE)
            .id(builder.id)
            .host(builder.host)
            .port(builder.port)
            .bossEventLoopGroup(builder.bossEventLoopGroup)
            .workerEventLoopGroup(builder.workerEventLoopGroup)
            .crdtService(builder.crdtService)
            .executor(builder.executor)
            .sslContext(builder.sslContext)
            .build();
        ;
    }

    @Override
    public void start() {
        log.info("Starting inbox server");
        log.debug("Starting inbox service");
        inboxService.start();
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
        log.debug("Stopping inbox service");
        inboxService.stop();
        log.info("Inbox server shutdown");
    }
}
