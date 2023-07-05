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
import static com.baidu.bifromq.metrics.TrafficMeter.gauging;
import static com.baidu.bifromq.metrics.TrafficMeter.stopGauging;
import static com.baidu.bifromq.metrics.TrafficMetric.DistSubInfoSizeGauge;

import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.balance.KVRangeBalanceController;
import com.baidu.bifromq.basekv.balance.option.KVRangeBalanceControllerOptions;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.server.IBaseKVStoreServer;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProcFactory;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.util.AsyncRunner;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.rpc.proto.CollectMetricsReply;
import com.baidu.bifromq.dist.rpc.proto.DistServiceROCoProcOutput;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
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
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class DistWorker implements IDistWorker {
    private enum Status {
        INIT, STARTING, STARTED, STOPPING, STOPPED
    }

    private final AtomicReference<Status> status = new AtomicReference(Status.INIT);
    private final IBaseKVStoreClient storeClient;
    private final DistWorkerCoProcFactory coProcFactory;
    private final IBaseKVStoreServer storeServer;
    private final KVRangeBalanceController rangeBalanceController;
    private final ScheduledExecutorService jobScheduler;
    private final AsyncRunner jobRunner;
    private final boolean jobExecutorOwner;
    private final Duration statsInterval;
    private final Duration gcInterval;
    private volatile ScheduledFuture<?> gcJob;
    private volatile ScheduledFuture<?> statsJob;
    private final Map<String, Long> trafficSubInfoSize = new ConcurrentHashMap<>();

    public DistWorker(IAgentHost agentHost,
                      ICRDTService crdtService,
                      ISettingProvider settingProvider,
                      IEventCollector eventCollector,
                      IDistClient distClient,
                      IBaseKVStoreClient storeClient,
                      ISubBrokerManager subBrokerManager,
                      Duration statsInterval,
                      Duration gcInterval,
                      KVRangeStoreOptions kvRangeStoreOptions,
                      KVRangeBalanceControllerOptions balanceOptions,
                      Executor ioExecutor,
                      Executor queryExecutor,
                      Executor mutationExecutor,
                      ScheduledExecutorService tickTaskExecutor,
                      ScheduledExecutorService bgTaskExecutor) {
        assert storeClient.clusterId().equals(CLUSTER_NAME);
        this.storeClient = storeClient;
        this.gcInterval = gcInterval;
        this.statsInterval = statsInterval;
        coProcFactory = new DistWorkerCoProcFactory(distClient, settingProvider, eventCollector, subBrokerManager);
        storeServer = buildKVStoreServer(CLUSTER_NAME,
            agentHost,
            crdtService,
            coProcFactory,
            kvRangeStoreOptions,
            ioExecutor,
            queryExecutor,
            mutationExecutor,
            tickTaskExecutor,
            bgTaskExecutor);
        rangeBalanceController = new KVRangeBalanceController(storeClient, balanceOptions, bgTaskExecutor);
        jobExecutorOwner = bgTaskExecutor == null;
        if (jobExecutorOwner) {
            jobScheduler = ExecutorServiceMetrics.monitor(Metrics.globalRegistry, new ScheduledThreadPoolExecutor(1,
                    EnvProvider.INSTANCE.newThreadFactory("dist-worker-job-executor")),
                CLUSTER_NAME + "-job-executor");
        } else {
            jobScheduler = bgTaskExecutor;
        }
        jobRunner = new AsyncRunner(jobScheduler);
    }

    protected abstract IBaseKVStoreServer buildKVStoreServer(String clusterId,
                                                             IAgentHost agentHost,
                                                             ICRDTService crdtService,
                                                             IKVRangeCoProcFactory coProcFactory,
                                                             KVRangeStoreOptions kvRangeStoreOptions,
                                                             Executor ioExecutor,
                                                             Executor queryExecutor,
                                                             Executor mutationExecutor,
                                                             ScheduledExecutorService tickTaskExecutor,
                                                             ScheduledExecutorService bgTaskExecutor);

    public String id() {
        return storeServer.id();
    }

    public void start(boolean bootstrap) {
        if (status.compareAndSet(Status.INIT, Status.STARTING)) {
            log.info("Starting dist worker");
            log.debug("Starting KVStore server: bootstrap={}", bootstrap);
            storeServer.start(bootstrap);
            rangeBalanceController.start(storeServer.id());
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
            log.debug("Stopping KVStore server");
//            rangeBalanceController.stop();
            storeServer.stop();
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
            List<CompletableFuture> gcFutures = new ArrayList<>();
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
                                item.forEach((trafficId, usedSpace) -> result.compute(trafficId, (k, read) -> {
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
        for (String trafficId : sizeMap.keySet()) {
            boolean newGauging = !trafficSubInfoSize.containsKey(trafficId);
            trafficSubInfoSize.put(trafficId, sizeMap.get(trafficId));
            if (newGauging) {
                gauging(trafficId, DistSubInfoSizeGauge, () -> trafficSubInfoSize.getOrDefault(trafficId, 0L));
            }
        }
        for (String trafficId : trafficSubInfoSize.keySet()) {
            if (!sizeMap.containsKey(trafficId)) {
                stopGauging(trafficId, DistSubInfoSizeGauge);
                trafficSubInfoSize.remove(trafficId);
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
