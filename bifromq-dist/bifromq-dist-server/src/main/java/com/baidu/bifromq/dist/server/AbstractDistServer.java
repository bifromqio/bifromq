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

package com.baidu.bifromq.dist.server;

import static com.baidu.bifromq.basehookloader.BaseHookLoader.load;

import com.baidu.bifromq.dist.server.scheduler.IGlobalDistCallRateSchedulerFactory;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractDistServer implements IDistServer {
    protected final DistService distService;

    AbstractDistServer(AbstractDistServerBuilder<?> builder) {
        this.distService = new DistService(
            builder.distWorkerClient,
            builder.settingProvider,
            builder.eventCollector,
            builder.crdtService,
            distCallPreBatchSchedulerFactory(builder.distCallPreSchedulerFactoryClass));
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

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {
        log.debug("Stop dist service");
        distService.stop();
    }
}
