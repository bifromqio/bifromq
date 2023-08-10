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

package com.baidu.bifromq.dist.worker;

import static com.baidu.bifromq.sysprops.BifroMQSysProp.DIST_MATCH_PARALLELISM;

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProc;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProcFactory;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.range.ILoadTracker;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.worker.scheduler.DeliveryScheduler;
import com.baidu.bifromq.dist.worker.scheduler.IDeliveryScheduler;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DistWorkerCoProcFactory implements IKVRangeCoProcFactory {
    private final IDistClient distClient;
    private final ISettingProvider settingProvider;
    private final IEventCollector eventCollector;
    private final ISubBrokerManager subBrokerManager;
    private final IDeliveryScheduler scheduler;
    private final ExecutorService matchExecutor;

    public DistWorkerCoProcFactory(IDistClient distClient,
                                   ISettingProvider settingProvider,
                                   IEventCollector eventCollector,
                                   ISubBrokerManager subBrokerManager) {
        this.distClient = distClient;
        this.settingProvider = settingProvider;
        this.eventCollector = eventCollector;
        this.subBrokerManager = subBrokerManager;
//        scheduler = new DeliveryScheduler(subBrokerManager);
        scheduler = new DeliveryScheduler(subBrokerManager);

        matchExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ForkJoinPool(DIST_MATCH_PARALLELISM.get(), new ForkJoinPool.ForkJoinWorkerThreadFactory() {
                final AtomicInteger index = new AtomicInteger(0);

                @Override
                public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                    ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                    worker.setName(String.format("topic-matcher-%d", index.incrementAndGet()));
                    worker.setDaemon(false);
                    return worker;
                }
            }, null, false), "topic-matcher");
    }

    @Override
    public IKVRangeCoProc create(KVRangeId id, Supplier<IKVRangeReader> rangeReaderProvider, ILoadTracker loadTracker) {
        return new DistWorkerCoProc(id, rangeReaderProvider, eventCollector, settingProvider, distClient,
            subBrokerManager, scheduler, matchExecutor, loadTracker);
    }

    public void close() {
        scheduler.close();
        matchExecutor.shutdown();
    }
}
