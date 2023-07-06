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

import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.server.IBaseKVStoreServer;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProcFactory;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.retain.utils.MessageUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class RetainStore implements IRetainStore {
    private enum Status {
        INIT, STARTING, STARTED, STOPPING, STOPPED
    }

    private final AtomicReference<Status> status = new AtomicReference(Status.INIT);
    private final IBaseKVStoreClient storeClient;
    private final RetainStoreCoProcFactory coProcFactory;
    private final IBaseKVStoreServer storeServer;
    private final ScheduledExecutorService jobExecutor;
    private final boolean jobExecutorOwner;
    private final Duration statsInterval;
    private final Duration gcInterval;

    private volatile ScheduledFuture gcJob;
    private volatile ScheduledFuture statsJob;

    public RetainStore(@NonNull IAgentHost agentHost,
                       @NonNull ICRDTService crdtService,
                       @NonNull IBaseKVStoreClient storeClient,
                       Duration statsInterval,
                       Duration gcInterval,
                       Clock clock,
                       KVRangeStoreOptions kvRangeStoreOptions,
                       Executor ioExecutor,
                       Executor queryExecutor,
                       Executor mutationExecutor,
                       ScheduledExecutorService tickTaskExecutor,
                       ScheduledExecutorService bgTaskExecutor) {
        this.storeClient = storeClient;
        this.gcInterval = gcInterval;
        this.statsInterval = statsInterval;
        coProcFactory = new RetainStoreCoProcFactory(clock);
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
        jobExecutorOwner = bgTaskExecutor == null;
        if (jobExecutorOwner) {
            jobExecutor = ExecutorServiceMetrics
                .monitor(Metrics.globalRegistry,
                    new ScheduledThreadPoolExecutor(1,
                        EnvProvider.INSTANCE.newThreadFactory("retain-store-job-executor")),
                    CLUSTER_NAME + "-job-executor");
        } else {
            jobExecutor = bgTaskExecutor;
        }
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
            log.info("Starting retain store");
            log.debug("Starting KVStore server: bootstrap={}", bootstrap);
            storeServer.start(bootstrap);
            status.compareAndSet(Status.STARTING, Status.STARTED);
            scheduleGC();
            scheduleStats();
            log.info("Retain store started");
        }
    }

    public void stop() {
        if (status.compareAndSet(Status.STARTED, Status.STOPPING)) {
            log.info("Stopping retain store");
            log.debug("Stopping KVStore server");
            storeServer.stop();
            if (gcJob != null && !gcJob.isDone()) {
                gcJob.cancel(true);
            }
            if (statsJob != null && !statsJob.isDone()) {
                statsJob.cancel(true);
            }
            if (jobExecutorOwner) {
                log.debug("Shutting down job executor");
                MoreExecutors.shutdownAndAwaitTermination(jobExecutor, 5, TimeUnit.SECONDS);
            }
            log.info("Retain store shutdown");
            status.compareAndSet(Status.STOPPING, Status.STOPPED);
        }
    }

    private void scheduleGC() {
        gcJob = jobExecutor.schedule(this::gc, gcInterval.toMinutes(), TimeUnit.MINUTES);
    }

    private void gc() {
        if (status.get() != Status.STARTED) {
            return;
        }
        Iterator<KVRangeSetting> itr = storeClient.findByRange(FULL_RANGE)
            .stream().filter(k -> k.equals(id())).iterator();
        List<CompletableFuture> gcFutures = new ArrayList<>();
        while (itr.hasNext()) {
            KVRangeSetting leaderReplica = itr.next();
            gcFutures.add(gcRange(leaderReplica));
        }
        CompletableFuture.allOf(gcFutures.toArray(new CompletableFuture[0])).whenComplete((v, e) -> scheduleGC());
    }

    @VisibleForTesting
    CompletableFuture<Void> gcRange(KVRangeSetting leaderReplica) {
        long reqId = System.currentTimeMillis();
        return storeClient.execute(leaderReplica.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setKvRangeId(leaderReplica.id)
                .setVer(leaderReplica.ver)
                .setRwCoProc(MessageUtil.buildGCRequest(reqId).toByteString())
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
        statsJob = jobExecutor.schedule(this::collectMetrics, statsInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void collectMetrics() {
        if (status.get() != Status.STARTED) {
            return;
        }
        // TODO: collect stats
//        Iterator<KVRangeSettings> itr = storeClient.findByRange(FULL_RANGE)
//                .stream().filter(k -> k.getLeader() == id()).iterator();
//        List<CompletableFuture<CollectMetricsReply>> statsFutures = new ArrayList<>();
//        while (itr.hasNext()) {
//            KVRangeSettings leaderReplica = itr.next();
//            statsFutures.add(collectRangeMetrics(leaderReplica));
//        }
//        CompletableFuture.allOf(statsFutures.toArray(new CompletableFuture[0]))
//                .whenComplete((v, e) -> {
//                    Map<String, Long> usedSpaceMap = statsFutures.stream().map(f -> f.join().getUsedSpacesMap())
//                            .reduce(new HashMap<>(), (result, item) -> {
//                                item.forEach((tenantId, usedSpace) -> result.compute(tenantId, (k, read) -> {
//                                    if (read == null) {
//                                        read = 0L;
//                                    }
//                                    read += usedSpace;
//                                    return read;
//                                }));
//                                return result;
//                            });
//                    meter.recordSpaceUsed(usedSpaceMap);
//                    scheduleStats();
//                });
    }

//    @VisibleForTesting
//    CompletableFuture<CollectMetricsReply> collectRangeMetrics(KVRangeSettings leaderReplica) {
//        long reqId = System.currentTimeMillis();
//        return storeClient.query(leaderReplica.getLeader(), KVRangeRORequest.newBuilder()
//                        .setReqId(reqId)
//                        .setKvRangeId(leaderReplica.getId())
//                        .setVer(leaderReplica.getVer())
//                        .setCmd(KVROCommand.newBuilder()
//                                .setRoCoProc(ROCoProc.newBuilder()
//                                        .setKey(leaderReplica.getRange().getStartKey())
//                                        .setProc(buildCollectMetricsRequest(reqId).toByteString())
//                                        .build())
//                                .build())
//                        .build())
//                .thenApply(reply -> {
//                    log.debug("Range gc succeed: serverId={}, rangeId={}, ver={}",
//                            leaderReplica.getLeader(), leaderReplica.getId(), leaderReplica.getVer());
//
//                    try {
//                        return InboxServiceROCoProcOutput.parseFrom(reply.getResult().getRoCoProcResult().getOutput())
//                                .getCollectMetricsReply();
//                    } catch (InvalidProtocolBufferException e) {
//                        throw new IllegalStateException("Unable to parse CollectMetricReply", e);
//                    }
//                })
//                .exceptionally(e -> {
//                    log.error("Range gc failed: serverId={}, rangeId={}, ver={}",
//                            leaderReplica.getLeader(), leaderReplica.getId(), leaderReplica.getVer());
//                    return CollectMetricsReply.newBuilder().setReqId(reqId).build();
//                });
//    }
}
