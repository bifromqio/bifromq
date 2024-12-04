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

package com.baidu.bifromq.inbox.store;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basehookloader.BaseHookLoader;
import com.baidu.bifromq.basekv.balance.KVStoreBalanceController;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.server.IBaseKVStoreServer;
import com.baidu.bifromq.basekv.store.util.AsyncRunner;
import com.baidu.bifromq.baserpc.client.IConnectable;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.store.gc.IInboxStoreGCProcessor;
import com.baidu.bifromq.inbox.store.gc.InboxStoreGCProcessor;
import com.baidu.bifromq.inbox.store.spi.IInboxStoreBalancerFactory;
import com.google.common.util.concurrent.MoreExecutors;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class InboxStore implements IInboxStore {
    private enum Status {
        INIT, STARTING, STARTED, STOPPING, STOPPED
    }

    private final String clusterId;
    private final ExecutorService rpcExecutor;
    private final IBaseKVStoreServer storeServer;
    private final AtomicReference<Status> status = new AtomicReference<>(Status.INIT);
    private final IBaseKVStoreClient inboxStoreClient;
    private final IInboxClient inboxClient;
    private final KVStoreBalanceController balanceController;
    private final AsyncRunner jobRunner;
    private final ScheduledExecutorService jobScheduler;
    private final boolean jobExecutorOwner;
    private final Duration gcInterval;
    private final List<IInboxStoreBalancerFactory> effectiveBalancerFactories = new LinkedList<>();
    protected final InboxStoreCoProcFactory coProcFactory;
    private IInboxStoreGCProcessor inboxStoreGCProc;
    private volatile CompletableFuture<Void> gcJob;

    InboxStore(InboxStoreBuilder builder) {
        this.clusterId = builder.clusterId;
        this.inboxStoreClient = builder.inboxStoreClient;
        this.inboxClient = builder.inboxClient;
        this.gcInterval = builder.gcInterval;
        coProcFactory =
            new InboxStoreCoProcFactory(builder.settingProvider, builder.eventCollector, builder.loadEstimateWindow);
        Map<String, IInboxStoreBalancerFactory> loadedFactories = BaseHookLoader.load(IInboxStoreBalancerFactory.class);
        for (String factoryName : builder.balancerFactoryConfig.keySet()) {
            if (!loadedFactories.containsKey(factoryName)) {
                log.warn("InboxStoreBalancerFactory[{}] not found", factoryName);
                continue;
            }
            IInboxStoreBalancerFactory balancer = loadedFactories.get(factoryName);
            balancer.init(builder.balancerFactoryConfig.get(factoryName));
            log.info("InboxStoreBalancerFactory[{}] enabled", factoryName);
            effectiveBalancerFactories.add(balancer);
        }

        balanceController = new KVStoreBalanceController(
            builder.metaService.metadataManager(clusterId),
            inboxStoreClient,
            effectiveBalancerFactories,
            builder.balancerRetryDelay,
            builder.bgTaskExecutor);
        jobExecutorOwner = builder.bgTaskExecutor == null;
        if (jobExecutorOwner) {
            String threadName = String.format("inbox-store[%s]-job-executor", builder.clusterId);
            jobScheduler = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ScheduledThreadPoolExecutor(1, EnvProvider.INSTANCE.newThreadFactory(threadName)), threadName);
        } else {
            jobScheduler = builder.bgTaskExecutor;
        }
        jobRunner = new AsyncRunner("job.runner", jobScheduler, "type", "inboxstore");
        if (builder.workerThreads == 0) {
            rpcExecutor = MoreExecutors.newDirectExecutorService();
        } else {
            rpcExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ThreadPoolExecutor(builder.workerThreads,
                    builder.workerThreads, 0L,
                    TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                    EnvProvider.INSTANCE.newThreadFactory("inbox-store-executor")), "inbox-store-executor");

        }
        storeServer = IBaseKVStoreServer.builder()
            // attach to rpc server
            .rpcServerBuilder(builder.rpcServerBuilder)
            .metaService(builder.metaService)
            // build basekv store service
            .addService(builder.clusterId)
            .coProcFactory(coProcFactory)
            .storeOptions(builder.storeOptions)
            .agentHost(builder.agentHost)
            .queryExecutor(MoreExecutors.directExecutor())
            .rpcExecutor(rpcExecutor)
            .tickerThreads(builder.tickerThreads)
            .bgTaskExecutor(builder.bgTaskExecutor)
            .attributes(builder.attributes)
            .finish()
            .build();
        start();
    }


    public String id() {
        return storeServer.storeId(clusterId);
    }

    private void start() {
        if (status.compareAndSet(Status.INIT, Status.STARTING)) {
            log.info("Starting inbox store");
            storeServer.start();
            balanceController.start(storeServer.storeId(clusterId));
            status.compareAndSet(Status.STARTING, Status.STARTED);
            this.inboxStoreGCProc = new InboxStoreGCProcessor(inboxClient, inboxStoreClient, id());
            inboxStoreClient
                .connState()
                // observe the first READY state
                .filter(connState -> connState == IConnectable.ConnState.READY)
                .takeUntil(connState -> connState == IConnectable.ConnState.READY)
                .doOnComplete(() -> scheduleGC(Duration.ofSeconds(5)))
                .subscribe();
            log.debug("Inbox store started");
        }
    }

    public void close() {
        if (status.compareAndSet(Status.STARTED, Status.STOPPING)) {
            log.info("Stopping InboxStore");
//            if (gcJob != null && !gcJob.isDone()) {
//                gcJob.cancel(true);
//            }
            jobRunner.awaitDone().toCompletableFuture().join();
            balanceController.stop();
            storeServer.stop();
            log.debug("Stopping CoProcFactory");
            coProcFactory.close();
            effectiveBalancerFactories.forEach(IInboxStoreBalancerFactory::close);
            if (jobExecutorOwner) {
                log.debug("Shutting down job executor");
                MoreExecutors.shutdownAndAwaitTermination(jobScheduler, 5, TimeUnit.SECONDS);
            }
            MoreExecutors.shutdownAndAwaitTermination(rpcExecutor, 5, TimeUnit.SECONDS);
            log.debug("InboxStore stopped");
            status.compareAndSet(Status.STOPPING, Status.STOPPED);
        }
    }

    private void scheduleGC(Duration delay) {
        if (status.get() != Status.STARTED) {
            return;
        }
        jobScheduler.schedule(this::gc, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void gc() {
        jobRunner.add(() -> {
            if (status.get() != Status.STARTED) {
                return;
            }
            long reqId = HLC.INST.getPhysical();
            log.debug("Start GC job, reqId={}", reqId);
            gcJob = inboxStoreGCProc.gc(reqId, null, null, HLC.INST.getPhysical())
                .handle((v, e) -> {
                    scheduleGC(gcInterval);
                    return null;
                });
        });
    }
}