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

package com.baidu.bifromq.dist.worker;

import static com.baidu.bifromq.sysprops.BifroMQSysProp.DIST_MATCH_PARALLELISM;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DIST_WORKER_FANOUT_SPLIT_THRESHOLD;

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProc;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProcFactory;
import com.baidu.bifromq.basekv.store.api.IKVRangeSplitHinter;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.range.hinter.MutationKVLoadBasedSplitHinter;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.deliverer.IMessageDeliverer;
import com.baidu.bifromq.deliverer.MessageDeliverer;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.worker.hinter.FanoutSplitHinter;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
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
    private final IMessageDeliverer deliverer;
    private final ExecutorService matchExecutor;
    private final Duration loadEstWindow;
    private final int fanoutSplitThreshold = DIST_WORKER_FANOUT_SPLIT_THRESHOLD.get();

    public DistWorkerCoProcFactory(IDistClient distClient,
                                   ISettingProvider settingProvider,
                                   IEventCollector eventCollector,
                                   ISubBrokerManager subBrokerManager,
                                   Duration loadEstimateWindow) {
        this.distClient = distClient;
        this.settingProvider = settingProvider;
        this.eventCollector = eventCollector;
        this.subBrokerManager = subBrokerManager;
        this.loadEstWindow = loadEstimateWindow;
        deliverer = new MessageDeliverer(subBrokerManager);

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
    public List<IKVRangeSplitHinter> createHinters(String clusterId, String storeId, KVRangeId id,
                                                   Supplier<IKVReader> readerProvider) {
        return List.of(
            new FanoutSplitHinter(readerProvider, fanoutSplitThreshold,
                "clusterId", clusterId, "storeId", storeId, "rangeId", KVRangeIdUtil.toString(id)),
            new MutationKVLoadBasedSplitHinter(loadEstWindow, Optional::of,
                "clusterId", clusterId, "storeId", storeId, "rangeId", KVRangeIdUtil.toString(id)));
    }

    @Override
    public IKVRangeCoProc createCoProc(String clusterId, String storeId, KVRangeId id,
                                       Supplier<IKVReader> rangeReaderProvider) {
        return new DistWorkerCoProc(id, rangeReaderProvider, eventCollector, settingProvider, distClient,
            subBrokerManager, deliverer, matchExecutor);
    }

    public void close() {
        deliverer.close();
        matchExecutor.shutdown();
    }
}
