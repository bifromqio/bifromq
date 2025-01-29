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

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.api.IKVCloseableReader;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProc;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProcFactory;
import com.baidu.bifromq.basekv.store.api.IKVRangeSplitHinter;
import com.baidu.bifromq.basekv.store.range.hinter.MutationKVLoadBasedSplitHinter;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.deliverer.IMessageDeliverer;
import com.baidu.bifromq.deliverer.MessageDeliverer;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.worker.cache.ISubscriptionCache;
import com.baidu.bifromq.dist.worker.cache.SubscriptionCache;
import com.baidu.bifromq.dist.worker.hinter.FanoutSplitHinter;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.sysprops.props.DistFanOutParallelism;
import com.baidu.bifromq.sysprops.props.DistMatchParallelism;
import com.baidu.bifromq.sysprops.props.DistWorkerFanOutSplitThreshold;
import com.bifromq.plugin.resourcethrottler.IResourceThrottler;
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
    private final IEventCollector eventCollector;
    private final IResourceThrottler resourceThrottler;
    private final IMessageDeliverer deliverer;
    private final ISubscriptionCleaner subscriptionChecker;
    private final ExecutorService matchExecutor;
    private final Duration loadEstWindow;
    private final int fanoutSplitThreshold = DistWorkerFanOutSplitThreshold.INSTANCE.get();

    public DistWorkerCoProcFactory(IDistClient distClient,
                                   IEventCollector eventCollector,
                                   IResourceThrottler resourceThrottler,
                                   ISubBrokerManager subBrokerManager,
                                   Duration loadEstimateWindow) {
        this.distClient = distClient;
        this.eventCollector = eventCollector;
        this.resourceThrottler = resourceThrottler;
        this.loadEstWindow = loadEstimateWindow;
        deliverer = new MessageDeliverer(subBrokerManager);
        subscriptionChecker = new SubscriptionCleaner(subBrokerManager, distClient);

        matchExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ForkJoinPool(DistMatchParallelism.INSTANCE.get(), new ForkJoinPool.ForkJoinWorkerThreadFactory() {
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
                                                   Supplier<IKVCloseableReader> readerProvider) {
        return List.of(
            new FanoutSplitHinter(readerProvider, fanoutSplitThreshold,
                "clusterId", clusterId, "storeId", storeId, "rangeId", KVRangeIdUtil.toString(id)),
            new MutationKVLoadBasedSplitHinter(loadEstWindow, Optional::of,
                "clusterId", clusterId, "storeId", storeId, "rangeId", KVRangeIdUtil.toString(id)));
    }

    @Override
    public IKVRangeCoProc createCoProc(String clusterId, String storeId, KVRangeId id,
                                       Supplier<IKVCloseableReader> rangeReaderProvider) {
        ISubscriptionCache routeCache = new SubscriptionCache(id, rangeReaderProvider, matchExecutor);
        ITenantsState tenantsState = new TenantsState(rangeReaderProvider.get(),
            "clusterId", clusterId, "storeId", storeId, "rangeId", KVRangeIdUtil.toString(id));

        IDeliverExecutorGroup deliverExecutorGroup = new DeliverExecutorGroup(
            deliverer, eventCollector, resourceThrottler, distClient, DistFanOutParallelism.INSTANCE.get());
        return new DistWorkerCoProc(
            id, rangeReaderProvider, routeCache, tenantsState, deliverExecutorGroup, subscriptionChecker);
    }

    public void close() {
        deliverer.close();
        matchExecutor.shutdown();
    }
}
