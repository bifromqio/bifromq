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

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basehookloader.BaseHookLoader;
import com.baidu.bifromq.basekv.balance.KVStoreBalanceController;
import com.baidu.bifromq.basekv.server.IBaseKVStoreServer;
import com.baidu.bifromq.basekv.store.util.AsyncRunner;
import com.baidu.bifromq.dist.worker.spi.IDistWorkerBalancerFactory;
import com.google.common.util.concurrent.MoreExecutors;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DistWorker implements IDistWorker {
    private enum Status {
        INIT, STARTING, STARTED, STOPPING, STOPPED
    }

    private final String clusterId;
    private final IBaseKVStoreServer storeServer;
    private final AtomicReference<Status> status = new AtomicReference<>(Status.INIT);
    private final KVStoreBalanceController storeBalanceController;
    private final ScheduledExecutorService jobScheduler;
    private final AsyncRunner jobRunner;
    private final boolean jobExecutorOwner;
    private final List<IDistWorkerBalancerFactory> effectiveBalancerFactories = new LinkedList<>();
    protected final DistWorkerCoProcFactory coProcFactory;

    public DistWorker(DistWorkerBuilder builder) {
        this.clusterId = builder.clusterId;
        coProcFactory = new DistWorkerCoProcFactory(
            builder.distClient,
            builder.eventCollector,
            builder.resourceThrottler,
            builder.subBrokerManager,
            builder.loadEstimateWindow);
        Map<String, IDistWorkerBalancerFactory> loadedFactories = BaseHookLoader.load(IDistWorkerBalancerFactory.class);
        for (String factoryName : builder.balancerFactoryConfig.keySet()) {
            if (!loadedFactories.containsKey(factoryName)) {
                log.warn("DistWorkerBalancerFactory[{}] not found", factoryName);
                continue;
            }
            IDistWorkerBalancerFactory balancer = loadedFactories.get(factoryName);
            balancer.init(builder.balancerFactoryConfig.get(factoryName));
            log.info("DistWorkerBalancerFactory[{}] enabled", factoryName);
            effectiveBalancerFactories.add(balancer);
        }

        storeBalanceController = new KVStoreBalanceController(
            builder.metaService.metadataManager(clusterId),
            builder.storeClient,
            effectiveBalancerFactories,
            builder.balancerRetryDelay,
            builder.bgTaskExecutor);
        jobExecutorOwner = builder.bgTaskExecutor == null;
        if (jobExecutorOwner) {
            String threadName = String.format("dist-worker[%s]-job-executor", builder.clusterId);
            jobScheduler = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ScheduledThreadPoolExecutor(1, EnvProvider.INSTANCE.newThreadFactory(threadName)), threadName);
        } else {
            jobScheduler = builder.bgTaskExecutor;
        }
        jobRunner = new AsyncRunner("job.runner", jobScheduler, "type", "distworker");

        storeServer = IBaseKVStoreServer.builder()
            // attach to rpc server
            .rpcServerBuilder(builder.rpcServerBuilder)
            .metaService(builder.metaService)
            // build basekv store service
            .addService(builder.clusterId, builder.bootstrap)
            .coProcFactory(coProcFactory)
            .storeOptions(builder.storeOptions)
            .agentHost(builder.agentHost)
            .queryExecutor(builder.queryExecutor)
            .rpcExecutor(builder.rpcExecutor)
            .tickerThreads(builder.tickerThreads)
            .bgTaskExecutor(builder.bgTaskExecutor)
            .finish()
            .build();
        start();
    }


    public String id() {
        return storeServer.storeId(clusterId);
    }

    private void start() {
        if (status.compareAndSet(Status.INIT, Status.STARTING)) {
            log.info("Starting dist worker");
            storeServer.start();
            storeBalanceController.start(storeServer.storeId(clusterId));
            status.compareAndSet(Status.STARTING, Status.STARTED);
            log.info("Dist worker started");
        }
    }

    public void close() {
        if (status.compareAndSet(Status.STARTED, Status.STOPPING)) {
            log.info("Stopping DistWorker");
            jobRunner.awaitDone();
            storeBalanceController.stop();
            storeServer.stop();
            log.debug("Stopping CoProcFactory");
            coProcFactory.close();
            effectiveBalancerFactories.forEach(IDistWorkerBalancerFactory::close);
            if (jobExecutorOwner) {
                log.debug("Stopping Job Executor");
                MoreExecutors.shutdownAndAwaitTermination(jobScheduler, 5, TimeUnit.SECONDS);
            }
            log.info("DistWorker stopped");
            status.compareAndSet(Status.STOPPING, Status.STOPPED);
        }
    }
}
