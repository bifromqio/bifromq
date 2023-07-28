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

import static com.baidu.bifromq.basekv.Constants.FULL_RANGE;
import static com.baidu.bifromq.dist.util.MessageUtil.buildCollectMetricsRequest;
import static com.baidu.bifromq.dist.util.MessageUtil.buildGCRequest;
import static com.baidu.bifromq.metrics.TenantMeter.gauging;
import static com.baidu.bifromq.metrics.TenantMeter.stopGauging;
import static com.baidu.bifromq.metrics.TenantMetric.DistSubInfoSizeGauge;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.balance.KVRangeBalanceController;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.server.IBaseKVStoreServer;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.util.AsyncRunner;
import com.baidu.bifromq.dist.rpc.proto.CollectMetricsReply;
import com.baidu.bifromq.dist.rpc.proto.DistServiceROCoProcOutput;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.InvalidProtocolBufferException;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class AbstractDistWorker<T extends AbstractDistWorkerBuilder<T>> implements IDistWorker {
    private enum Status {
        INIT, STARTING, STARTED, STOPPING, STOPPED
    }

    private final String clusterId;
    private final AtomicReference<Status> status = new AtomicReference<>(Status.INIT);
    private final IBaseKVStoreClient storeClient;
    private final KVRangeBalanceController rangeBalanceController;
    private final ScheduledExecutorService jobScheduler;
    private final AsyncRunner jobRunner;
    private final boolean jobExecutorOwner;
    private final Duration statsInterval;
    private final Duration gcInterval;
    private volatile ScheduledFuture<?> gcJob;
    private volatile ScheduledFuture<?> statsJob;
    private final Map<String, Long> tenantSubInfoSize = new ConcurrentHashMap<>();
    protected final DistWorkerCoProcFactory coProcFactory;

    public AbstractDistWorker(T builder) {
        this.clusterId = builder.clusterId;
        this.storeClient = builder.storeClient;
        this.gcInterval = builder.gcInterval;
        this.statsInterval = builder.statsInterval;
        coProcFactory = new DistWorkerCoProcFactory(
            builder.distClient,
            builder.settingProvider,
            builder.eventCollector,
            builder.subBrokerManager);
        rangeBalanceController =
            new KVRangeBalanceController(storeClient, builder.balanceControllerOptions, builder.bgTaskExecutor);
        jobExecutorOwner = builder.bgTaskExecutor == null;
        if (jobExecutorOwner) {
            String threadName = String.format("dist-worker[%s]-job-executor", builder.clusterId);
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
            log.info("Starting dist worker");
            storeServer().start();
            rangeBalanceController.start(storeServer().storeId(clusterId));
            status.compareAndSet(Status.STARTING, Status.STARTED);
            scheduleGC();
            scheduleStats();
            log.info("Dist worker started");
        }
    }

    public void stop() {
        if (status.compareAndSet(Status.STARTED, Status.STOPPING)) {
            log.info("Stopping dist worker");
            if (gcJob != null && !gcJob.isDone()) {
                gcJob.cancel(true);
                awaitIfNotCancelled(gcJob);
            }
            if (statsJob != null && !statsJob.isDone()) {
                statsJob.cancel(true);
                awaitIfNotCancelled(statsJob);
            }
            jobRunner.awaitDone();
            // FIXME
//            rangeBalanceController.stop();
            storeServer().stop();
            log.debug("Stopping CoProcFactory");
            coProcFactory.close();
            if (jobExecutorOwner) {
                log.debug("Stopping Job Executor");
                MoreExecutors.shutdownAndAwaitTermination(jobScheduler, 5, TimeUnit.SECONDS);
            }
            log.info("Dist worker stopped");
            status.compareAndSet(Status.STOPPING, Status.STOPPED);
        }
    }

    private void scheduleGC() {
        gcJob = jobScheduler.schedule(this::gc, gcInterval.toSeconds(), TimeUnit.SECONDS);
    }

    private void gc() {
        jobRunner.add(() -> {
            if (status.get() != Status.STARTED) {
                return;
            }
            List<KVRangeSetting> settings = storeClient.findByRange(FULL_RANGE);
            Iterator<KVRangeSetting> itr = settings.stream().filter(k -> k.leader.equals(id())).iterator();
            List<CompletableFuture<?>> gcFutures = new ArrayList<>();
            while (itr.hasNext()) {
                KVRangeSetting leaderReplica = itr.next();
                gcFutures.add(gcRange(leaderReplica));
            }
            CompletableFuture.allOf(gcFutures.toArray(new CompletableFuture[0])).whenComplete((v, e) -> scheduleGC());
        });
    }

    private CompletableFuture<Void> gcRange(KVRangeSetting leaderReplica) {
        long reqId = System.currentTimeMillis();
        return storeClient.query(leaderReplica.leader, KVRangeRORequest.newBuilder()
                .setReqId(reqId)
                .setKvRangeId(leaderReplica.id)
                .setVer(leaderReplica.ver)
                .setRoCoProcInput(buildGCRequest(reqId).toByteString())
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
        statsJob = jobScheduler.schedule(this::collectMetrics, statsInterval.toMillis(), TimeUnit.MILLISECONDS);
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
            CompletableFuture.allOf(statsFutures.toArray(new CompletableFuture[0]))
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
            boolean newGauging = !tenantSubInfoSize.containsKey(tenantId);
            tenantSubInfoSize.put(tenantId, sizeMap.get(tenantId));
            if (newGauging) {
                gauging(tenantId, DistSubInfoSizeGauge, () -> tenantSubInfoSize.getOrDefault(tenantId, 0L));
            }
        }
        for (String tenantId : tenantSubInfoSize.keySet()) {
            if (!sizeMap.containsKey(tenantId)) {
                stopGauging(tenantId, DistSubInfoSizeGauge);
                tenantSubInfoSize.remove(tenantId);
            }
        }
    }

    private CompletableFuture<CollectMetricsReply> collectRangeMetrics(KVRangeSetting leaderReplica) {
        long reqId = System.currentTimeMillis();
        return storeClient.query(leaderReplica.leader, KVRangeRORequest.newBuilder()
                .setReqId(reqId)
                .setKvRangeId(leaderReplica.id)
                .setVer(leaderReplica.ver)
                .setRoCoProcInput(buildCollectMetricsRequest(reqId).toByteString())
                .build())
            .thenApply(reply -> {
                log.debug("Range metrics collected: serverId={}, rangeId={}, ver={}",
                    leaderReplica.leader, leaderReplica.id, leaderReplica.ver);

                try {
                    return DistServiceROCoProcOutput.parseFrom(reply.getRoCoProcResult())
                        .getCollectMetricsReply();
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalStateException("Unable to parse CollectMetricReply", e);
                }
            })
            .exceptionally(e -> {
                log.error("Failed to collect range metrics: serverId={}, rangeId={}, ver={}",
                    leaderReplica.leader, leaderReplica.id, leaderReplica.ver);
                return CollectMetricsReply.newBuilder().setReqId(reqId).build();
            });
    }

    private <T> void awaitIfNotCancelled(ScheduledFuture<T> sf) {
        try {
            if (!sf.isCancelled()) {
                sf.get();
            }
        } catch (Throwable e) {
            log.error("Error during awaiting", e);
        }
    }
}
