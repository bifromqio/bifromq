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

package com.baidu.bifromq.dist.server;

import static com.baidu.bifromq.basehookloader.BaseHookLoader.load;

import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.baserpc.IRPCServer;
import com.baidu.bifromq.dist.RPCBluePrint;
import com.baidu.bifromq.dist.server.scheduler.IGlobalDistCallRateSchedulerFactory;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class DistServer implements IDistServer {
    private final IRPCServer rpcServer;
    private final DistService distService;

    DistServer(DistServerBuilder builder,
               IBaseKVStoreClient storeClient,
               ISettingProvider settingProvider,
               IEventCollector eventCollector,
               ICRDTService crdtService) {
        this.distService = new DistService(storeClient, settingProvider, eventCollector, crdtService,
            distCallPreBatchSchedulerFactory(builder.distCallPreSchedulerFactoryClass));
        this.rpcServer = IRPCServer.newBuilder()
            .executor(builder.executor)
            .bindService(distService.bindService(), RPCBluePrint.INSTANCE)
            .id(builder.id)
            .host(builder.host)
            .port(builder.port)
            .bossEventLoopGroup(builder.bossEventLoopGroup)
            .workerEventLoopGroup(builder.workerEventLoopGroup)
            .crdtService(crdtService)
            .sslContext(builder.sslContext)
            .build();
    }

    @Override
    public void start() {
        log.info("Starting dist server");
        log.debug("Starting rpc server");
        rpcServer.start();
        log.info("Dist Server started");
    }

    @SneakyThrows
    @Override
    public void shutdown() {
        log.info("Stopping dist server");
        log.debug("Stop dist rpc server");
        rpcServer.shutdown();
        log.debug("Stop dist service");
        distService.stop();
        log.info("Dist server stopped");
    }

    private IGlobalDistCallRateSchedulerFactory distCallPreBatchSchedulerFactory(String factoryClass) {
        if (factoryClass == null) {
            log.info("DistCallPreBatchSchedulerFactory[DEFAULT] loaded");
            return IGlobalDistCallRateSchedulerFactory.DEFAULT;
        } else {
            Map<String, IGlobalDistCallRateSchedulerFactory> factoryMap =
                load(IGlobalDistCallRateSchedulerFactory.class);
            IGlobalDistCallRateSchedulerFactory factory =
                factoryMap.getOrDefault(factoryClass, IGlobalDistCallRateSchedulerFactory.DEFAULT);
            log.info("DistCallPreBatchSchedulerFactory[{}] loaded",
                factory != IGlobalDistCallRateSchedulerFactory.DEFAULT ? factoryClass : "DEFAULT");
            return factory;
        }
    }
}
