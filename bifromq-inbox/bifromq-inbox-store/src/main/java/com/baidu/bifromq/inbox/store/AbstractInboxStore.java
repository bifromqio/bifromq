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

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static com.baidu.bifromq.inbox.util.MessageUtil.buildCollectMetricsRequest;
import static com.baidu.bifromq.metrics.ITenantMeter.gauging;
import static com.baidu.bifromq.metrics.ITenantMeter.stopGauging;
import static com.baidu.bifromq.metrics.TenantMetric.MqttPersistentSessionSpaceGauge;
import static com.baidu.bifromq.metrics.TenantMetric.MqttPersistentSubSpaceGauge;
import static com.baidu.bifromq.metrics.TenantMetric.MqttPersistentSubCountGauge;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.balance.KVRangeBalanceController;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.server.IBaseKVStoreServer;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.util.AsyncRunner;
import com.baidu.bifromq.baserpc.IConnectable;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.storage.proto.CollectMetricsReply;
import com.baidu.bifromq.inbox.store.gc.IInboxGCProcessor;
import com.baidu.bifromq.inbox.store.gc.InboxGCProcessor;
import com.baidu.bifromq.metrics.TenantMetric;
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
import java.util.stream.Stream;
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
    private final IInboxGCProcessor inboxStoreGCProc;
    private final AsyncRunner jobRunner;
    private final ScheduledExecutorService jobScheduler;
    private final boolean jobExecutorOwner;
    private final Duration statsInterval;
    private final Duration gcInterval;
    private final Map<String, Long> tenantInboxSpaceSize = new ConcurrentHashMap<>();
    private final Map<String, Long> tenantSubCounts = new ConcurrentHashMap<>();
    private final Map<String, Long> tenantSubUsedSpaces = new ConcurrentHashMap<>();
    private volatile CompletableFuture<Void> gcJob;
    private volatile CompletableFuture<Void> statsJob;
    protected final InboxStoreCoProcFactory coProcFactory;

    public AbstractInboxStore(T builder) {
        this.clusterId = builder.clusterId;
        this.storeClient = builder.storeClient;
        this.inboxClient = builder.inboxClient;
        this.gcInterval = builder.gcInterval;
        this.statsInterval = builder.statsInterval;
        coProcFactory = new InboxStoreCoProcFactory(builder.settingProvider,
            builder.eventCollector, builder.loadEstimateWindow,
            builder.purgeDelay);
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
        this.inboxStoreGCProc = new InboxGCProcessor(inboxClient, storeClient);
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
            storeClient
                .connState()
                // observe the first READY state
                .filter(connState -> connState == IConnectable.ConnState.READY)
                .takeUntil(connState -> connState == IConnectable.ConnState.READY)
                .doOnComplete(() -> {
                    scheduleGC(Duration.ofSeconds(5));
                    scheduleStats();
                })
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
            if (statsJob != null && !statsJob.isDone()) {
                statsJob.join();
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
            List<KVRangeSetting> settings = storeClient.findByBoundary(FULL_BOUNDARY);
            Iterator<KVRangeSetting> itr = settings.stream().filter(k -> k.leader.equals(id())).iterator();
            List<CompletableFuture<?>> gcFutures = new ArrayList<>();
            long reqId = HLC.INST.getPhysical();
            while (itr.hasNext()) {
                KVRangeSetting leaderReplica = itr.next();
                gcFutures.add(
                    inboxStoreGCProc.gcRange(reqId, leaderReplica.id, null, null,
                        HLC.INST.getPhysical(), Integer.MAX_VALUE));
            }
            gcJob = CompletableFuture.allOf(gcFutures.toArray(new CompletableFuture[0]))
                .whenComplete((v, e) -> scheduleGC(gcInterval));
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
            List<KVRangeSetting> settings = storeClient.findByBoundary(FULL_BOUNDARY);
            Iterator<KVRangeSetting> itr = settings.stream().filter(k -> k.leader.equals(id())).iterator();
            List<CompletableFuture<CollectMetricsReply>> statsFutures = new ArrayList<>();
            while (itr.hasNext()) {
                KVRangeSetting leaderReplica = itr.next();
                statsFutures.add(collectRangeMetrics(leaderReplica));
            }
            statsJob = CompletableFuture.allOf(statsFutures.toArray(new CompletableFuture[0]))
                .thenApply(v -> statsFutures.stream().map(CompletableFuture::join).toList())
                .thenAccept(v -> {
                    recordInboxUsedSpace(reduceMap(v.stream().map(CollectMetricsReply::getUsedSpacesMap)));
                    recordSubCounts(reduceMap(v.stream().map(CollectMetricsReply::getSubCountsMap)));
                    recordSubUsedSpaces(reduceMap(v.stream().map(CollectMetricsReply::getSubUsedSpacesMap)));
                })
                .whenComplete((v, e) -> scheduleStats());
        });
    }

    private Map<String, Long> reduceMap(Stream<Map<String, Long>> mapStream) {
        return mapStream.reduce(new HashMap<>(), (result, item) -> {
            item.forEach((tenantId, value) -> result.compute(tenantId, (k, read) -> {
                if (read == null) {
                    read = 0L;
                }
                read += value;
                return read;
            }));
            return result;
        });
    }

    private void recordInboxUsedSpace(Map<String, Long> sizeMap) {
        recordTenantMetricGauge(sizeMap, tenantInboxSpaceSize, MqttPersistentSessionSpaceGauge);
    }

    private void recordSubCounts(Map<String, Long> subCounts) {
        recordTenantMetricGauge(subCounts, tenantSubCounts, MqttPersistentSubCountGauge);
    }

    private void recordSubUsedSpaces(Map<String, Long> subUsedSpaces) {
        recordTenantMetricGauge(subUsedSpaces, tenantSubUsedSpaces, MqttPersistentSubSpaceGauge);
    }

    private void recordTenantMetricGauge(Map<String, Long> metrics, Map<String, Long> gaugeMap, TenantMetric metric) {
        for (String tenantId : metrics.keySet()) {
            boolean newGauging = !gaugeMap.containsKey(tenantId);
            gaugeMap.put(tenantId, metrics.get(tenantId));
            if (newGauging) {
                gauging(tenantId, metric, () -> gaugeMap.getOrDefault(tenantId, 0L));
            }
        }
        for (String tenantId : gaugeMap.keySet()) {
            if (!metrics.containsKey(tenantId)) {
                stopGauging(tenantId, metric);
                gaugeMap.remove(tenantId);
            }
        }
    }


    private CompletableFuture<CollectMetricsReply> collectRangeMetrics(KVRangeSetting leaderReplica) {
        long reqId = System.currentTimeMillis();
        return storeClient.query(leaderReplica.leader, KVRangeRORequest.newBuilder()
                .setReqId(reqId)
                .setKvRangeId(leaderReplica.id)
                .setVer(leaderReplica.ver)
                .setRoCoProc(ROCoProcInput.newBuilder()
                    .setInboxService(buildCollectMetricsRequest(reqId))
                    .build())
                .build())
            .thenApply(reply -> {
                log.debug("Range metrics collected: serverId={}, rangeId={}, ver={}",
                    leaderReplica.leader, leaderReplica.id, leaderReplica.ver);

                return reply.getRoCoProcResult().getInboxService().getCollectedMetrics();
            })
            .exceptionally(e -> {
                log.debug("Failed to collect range metrics: serverId={}, rangeId={}, ver={}",
                    leaderReplica.leader, leaderReplica.id, leaderReplica.ver, e);
                return CollectMetricsReply.newBuilder().setReqId(reqId).build();
            });
    }
}
