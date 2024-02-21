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
import com.baidu.bifromq.basekv.balance.KVRangeBalanceController;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.server.IBaseKVStoreServer;
import com.baidu.bifromq.basekv.store.util.AsyncRunner;
import com.baidu.bifromq.baserpc.IConnectable;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.store.gc.IInboxStoreGCProcessor;
import com.baidu.bifromq.inbox.store.gc.InboxStoreGCProcessor;
import com.google.common.util.concurrent.MoreExecutors;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class AbstractInboxStore<T extends AbstractInboxStoreBuilder<T>> implements IInboxStore {
    private enum Status {
        INIT, STARTING, STARTED, STOPPING, STOPPED
    }

    private final String clusterId;
    private final AtomicReference<Status> status = new AtomicReference<>(Status.INIT);
    private final IBaseKVStoreClient storeClient;
    private final IInboxClient inboxClient;
    private final KVRangeBalanceController balanceController;
    private final AsyncRunner jobRunner;
    private final ScheduledExecutorService jobScheduler;
    private final boolean jobExecutorOwner;
    private final Duration gcInterval;
    protected final InboxStoreCoProcFactory coProcFactory;
    private IInboxStoreGCProcessor inboxStoreGCProc;
    private volatile CompletableFuture<Void> gcJob;

    public AbstractInboxStore(T builder) {
        this.clusterId = builder.clusterId;
        this.storeClient = builder.storeClient;
        this.inboxClient = builder.inboxClient;
        this.gcInterval = builder.gcInterval;
        coProcFactory =
            new InboxStoreCoProcFactory(builder.settingProvider, builder.eventCollector, builder.loadEstimateWindow);
        balanceController =
            new KVRangeBalanceController(storeClient, builder.balanceControllerOptions, builder.bgTaskExecutor);
        jobExecutorOwner = builder.bgTaskExecutor == null;
        if (jobExecutorOwner) {
            String threadName = String.format("inbox-store[%s]-job-executor", builder.clusterId);
            jobScheduler = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ScheduledThreadPoolExecutor(1, EnvProvider.INSTANCE.newThreadFactory(threadName)), threadName);
        } else {
            jobScheduler = builder.bgTaskExecutor;
        }
        jobRunner = new AsyncRunner("job.runner", jobScheduler, "type", "inboxstore");
    }

    protected abstract IBaseKVStoreServer storeServer();

    public String id() {
        return storeServer().storeId(clusterId);
    }

    public void start() {
        if (status.compareAndSet(Status.INIT, Status.STARTING)) {
            log.info("Starting inbox store");
            storeServer().start();
            balanceController.start(storeServer().storeId(clusterId));
            status.compareAndSet(Status.STARTING, Status.STARTED);
            this.inboxStoreGCProc = new InboxStoreGCProcessor(inboxClient, storeClient, id());
            storeClient
                .connState()
                // observe the first READY state
                .filter(connState -> connState == IConnectable.ConnState.READY)
                .takeUntil(connState -> connState == IConnectable.ConnState.READY)
                .doOnComplete(() -> scheduleGC(Duration.ofSeconds(5)))
                .subscribe();
            log.info("Inbox store started");
        }
    }

    public void stop() {
        if (status.compareAndSet(Status.STARTED, Status.STOPPING)) {
            log.info("Shutting down inbox store");
            jobRunner.awaitDone();
            if (gcJob != null && !gcJob.isDone()) {
                gcJob.join();
            }
            balanceController.stop();
            storeServer().stop();
            log.debug("Stopping CoProcFactory");
            coProcFactory.close();
            if (jobExecutorOwner) {
                log.debug("Shutting down job executor");
                MoreExecutors.shutdownAndAwaitTermination(jobScheduler, 5, TimeUnit.SECONDS);
            }
            log.info("Inbox store shutdown");
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
            gcJob = inboxStoreGCProc.gc(reqId, null, null, HLC.INST.getPhysical())
                .handle((v, e) -> {
                    scheduleGC(gcInterval);
                    return null;
                });
        });
    }
}
