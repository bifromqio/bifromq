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

package com.baidu.bifromq.retain.store;

import static com.baidu.bifromq.basekv.Constants.FULL_RANGE;
import static com.baidu.bifromq.metrics.TenantMeter.gauging;
import static com.baidu.bifromq.metrics.TenantMeter.stopGauging;
import static com.baidu.bifromq.metrics.TenantMetric.RetainUsedSpaceGauge;
import static com.baidu.bifromq.retain.utils.MessageUtil.buildCollectMetricsRequest;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.balance.KVRangeBalanceController;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.server.IBaseKVStoreServer;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.util.AsyncRunner;
import com.baidu.bifromq.baserpc.IConnectable;
import com.baidu.bifromq.retain.rpc.proto.CollectMetricsReply;
import com.baidu.bifromq.retain.utils.MessageUtil;
import com.google.common.util.concurrent.MoreExecutors;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class AbstractRetainStore<T extends AbstractRetainStoreBuilder<T>> implements IRetainStore {
    private enum Status {
        INIT, STARTING, STARTED, STOPPING, STOPPED
    }

    private final String clusterId;
    private final AtomicReference<Status> status = new AtomicReference<>(Status.INIT);
    private final IBaseKVStoreClient storeClient;
    private final KVRangeBalanceController rangeBalanceController;
    private final AsyncRunner jobRunner;
    private final ScheduledExecutorService jobScheduler;
    private final boolean jobExecutorOwner;
    private final Duration statsInterval;
    private final Duration gcInterval;
    private final Map<String, Long> tenantRetainSpaceSize = new ConcurrentHashMap<>();
    private volatile CompletableFuture<Void> gcJob;
    private volatile CompletableFuture<Void> statsJob;
    protected final RetainStoreCoProcFactory coProcFactory;

    public AbstractRetainStore(T builder) {
        this.clusterId = builder.clusterId;
        this.storeClient = builder.storeClient;
        this.gcInterval = builder.gcInterval;
        this.statsInterval = builder.statsInterval;
        coProcFactory = new RetainStoreCoProcFactory(builder.settingProvider, builder.clock);
        rangeBalanceController =
            new KVRangeBalanceController(storeClient, builder.balanceControllerOptions, builder.bgTaskExecutor);
        jobExecutorOwner = builder.bgTaskExecutor == null;
        if (jobExecutorOwner) {
            String threadName = String.format("retain-store[%s]-job-executor", builder.clusterId);
            jobScheduler = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ScheduledThreadPoolExecutor(1, EnvProvider.INSTANCE.newThreadFactory(threadName)), threadName);
        } else {
            jobScheduler = builder.bgTaskExecutor;
        }
        jobRunner = new AsyncRunner(jobScheduler);
    }

    protected abstract IBaseKVStoreServer storeServer();

    public String id() {
        return storeServer().storeId(clusterId);
    }

    @Override
    public void start() {
        if (status.compareAndSet(Status.INIT, Status.STARTING)) {
            log.info("Starting retain store");
            storeServer().start();
            rangeBalanceController.start(id());
            status.compareAndSet(Status.STARTING, Status.STARTED);
            storeClient
                .connState()
                // observe the first READY state
                .filter(connState -> connState == IConnectable.ConnState.READY)
                .takeUntil(connState -> connState == IConnectable.ConnState.READY)
                .doOnComplete(() -> {
                    scheduleGC();
                    scheduleStats();
                })
                .subscribe();
            log.info("Retain store started");
        }
    }

    public void stop() {
        if (status.compareAndSet(Status.STARTED, Status.STOPPING)) {
            log.info("Stopping retain store");
            jobRunner.awaitDone();
            if (gcJob != null && !gcJob.isDone()) {
                gcJob.join();
            }
            if (statsJob != null && !statsJob.isDone()) {
                statsJob.join();
            }
            rangeBalanceController.stop();
            storeServer().stop();
            if (jobExecutorOwner) {
                log.debug("Shutting down job executor");
                MoreExecutors.shutdownAndAwaitTermination(jobScheduler, 5, TimeUnit.SECONDS);
            }
            log.info("Retain store shutdown");
            status.compareAndSet(Status.STOPPING, Status.STOPPED);
        }
    }

    private void scheduleGC() {
        jobScheduler.schedule(this::gc, gcInterval.toMinutes(), TimeUnit.MINUTES);
    }

    private void gc() {
        jobRunner.add(() -> {
            if (status.get() != Status.STARTED) {
                return;
            }
            Iterator<KVRangeSetting> itr = storeClient.findByRange(FULL_RANGE)
                .stream().filter(k -> k.leader.equals(id())).iterator();
            List<CompletableFuture<?>> gcFutures = new ArrayList<>();
            while (itr.hasNext()) {
                KVRangeSetting leaderReplica = itr.next();
                gcFutures.add(gcRange(leaderReplica));
            }
            gcJob = CompletableFuture.allOf(gcFutures.toArray(new CompletableFuture[0]))
                .whenComplete((v, e) -> scheduleGC());
        });
    }

    private CompletableFuture<Void> gcRange(KVRangeSetting leaderReplica) {
        long reqId = System.currentTimeMillis();
        return storeClient.execute(leaderReplica.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setKvRangeId(leaderReplica.id)
                .setVer(leaderReplica.ver)
                .setRwCoProc(RWCoProcInput.newBuilder().setRetainService(MessageUtil.buildGCRequest(reqId)).build())
                .build())
            .thenAccept(reply -> log.debug("Range gc succeed: serverId={}, rangeId={}, ver={}",
                leaderReplica.leader, leaderReplica.id, leaderReplica.ver))
            .exceptionally(e -> {
                log.error("Range gc failed: serverId={}, rangeId={}, ver={}",
                    leaderReplica.leader, leaderReplica.id, leaderReplica.ver);
                return null;
            });
    }

    private void scheduleStats() {
        jobScheduler.schedule(this::collectMetrics, statsInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void collectMetrics() {
        jobRunner.add(() -> {
            if (status.get() != Status.STARTED) {
                return;
            }
            List<KVRangeSetting> settings = storeClient.findByRange(FULL_RANGE);
            Iterator<KVRangeSetting> itr = settings.stream().filter(k -> k.leader.equals(id())).iterator();
            List<CompletableFuture<CollectMetricsReply>> statsFutures = new ArrayList<>();
            while (itr.hasNext()) {
                KVRangeSetting leaderReplica = itr.next();
                statsFutures.add(collectRangeMetrics(leaderReplica));
            }
            statsJob = CompletableFuture.allOf(statsFutures.toArray(new CompletableFuture[0]))
                .whenComplete((v, e) -> {
                    if (e == null) {
                        Map<String, Long> usedSpaceMap = statsFutures.stream().map(f -> f.join().getUsedSpacesMap())
                            .reduce(new HashMap<>(), (result, item) -> {
                                item.forEach((tenantId, usedSpace) -> result.compute(tenantId, (k, read) -> {
                                    if (read == null) {
                                        read = 0L;
                                    }
                                    read += usedSpace;
                                    return read;
                                }));
                                return result;
                            });
                        record(usedSpaceMap);
                    }
                    scheduleStats();
                });
        });
    }

    private void record(Map<String, Long> sizeMap) {
        for (String tenantId : sizeMap.keySet()) {
            boolean newGauging = !tenantRetainSpaceSize.containsKey(tenantId);
            tenantRetainSpaceSize.put(tenantId, sizeMap.get(tenantId));
            if (newGauging) {
                gauging(tenantId, RetainUsedSpaceGauge, () -> tenantRetainSpaceSize.getOrDefault(tenantId, 0L));
            }
        }
        for (String tenantId : tenantRetainSpaceSize.keySet()) {
            if (!sizeMap.containsKey(tenantId)) {
                stopGauging(tenantId, RetainUsedSpaceGauge);
                tenantRetainSpaceSize.remove(tenantId);
            }
        }
    }

    private CompletableFuture<CollectMetricsReply> collectRangeMetrics(KVRangeSetting leaderReplica) {
        long reqId = System.currentTimeMillis();
        return storeClient.query(leaderReplica.leader, KVRangeRORequest.newBuilder()
                .setReqId(reqId)
                .setKvRangeId(leaderReplica.id)
                .setVer(leaderReplica.ver)
                .setRoCoProcInput(ROCoProcInput.newBuilder()
                    .setRetainService(buildCollectMetricsRequest(reqId))
                    .build())
                .build())
            .thenApply(reply -> {
                log.debug("Range metrics collected: serverId={}, rangeId={}, ver={}",
                    leaderReplica.leader, leaderReplica.id, leaderReplica.ver);
                return reply.getRoCoProcResult().getRetainService().getCollectMetrics();
            })
            .exceptionally(e -> {
                log.error("Failed to collect range metrics: serverId={}, rangeId={}, ver={}",
                    leaderReplica.leader, leaderReplica.id, leaderReplica.ver);
                return CollectMetricsReply.newBuilder().setReqId(reqId).build();
            });
    }
}
