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

package com.baidu.bifromq.inbox.store;

import static com.baidu.bifromq.basekv.Constants.FULL_RANGE;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseInboxId;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseTenantId;
import static com.baidu.bifromq.inbox.util.MessageUtil.buildCollectMetricsRequest;
import static com.baidu.bifromq.metrics.TenantMeter.gauging;
import static com.baidu.bifromq.metrics.TenantMeter.stopGauging;
import static com.baidu.bifromq.metrics.TenantMetric.InboxSpaceSizeGauge;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.balance.KVRangeBalanceController;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.server.IBaseKVStoreServer;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basekv.store.util.AsyncRunner;
import com.baidu.bifromq.baserpc.IConnectable;
import com.baidu.bifromq.inbox.client.IInboxReaderClient;
import com.baidu.bifromq.inbox.storage.proto.CollectMetricsReply;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import com.baidu.bifromq.inbox.util.MessageUtil;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
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
abstract class AbstractInboxStore<T extends AbstractInboxStoreBuilder<T>> implements IInboxStore {
    private enum Status {
        INIT, STARTING, STARTED, STOPPING, STOPPED
    }

    private final String clusterId;
    private final AtomicReference<Status> status = new AtomicReference<>(Status.INIT);
    private final IInboxReaderClient inboxReaderClient;
    private final IBaseKVStoreClient storeClient;
    private final KVRangeBalanceController balanceController;
    private final AsyncRunner jobRunner;
    private final ScheduledExecutorService jobScheduler;
    private final boolean jobExecutorOwner;
    private final Duration statsInterval;
    private final Duration gcInterval;
    private final Map<String, Long> tenantInboxSpaceSize = new ConcurrentHashMap<>();
    private volatile CompletableFuture<Void> gcJob;
    private volatile CompletableFuture<Void> statsJob;
    protected final InboxStoreCoProcFactory coProcFactory;

    public AbstractInboxStore(T builder) {
        this.clusterId = builder.clusterId;
        this.inboxReaderClient = builder.inboxReaderClient;
        this.storeClient = builder.storeClient;
        this.gcInterval = builder.gcInterval;
        this.statsInterval = builder.statsInterval;
        coProcFactory = new InboxStoreCoProcFactory(builder.settingProvider,
            builder.eventCollector, builder.clock, builder.purgeDelay);
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
        jobRunner = new AsyncRunner(jobScheduler);
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
                    scheduleGC();
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

    private void scheduleGC() {
        if (status.get() != Status.STARTED) {
            return;
        }
        jobScheduler.schedule(this::gc, gcInterval.toSeconds(), TimeUnit.SECONDS);
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
                CompletableFuture<Void> onDone = new CompletableFuture<>();
                gcFutures.add(onDone);
                gcRange(leaderReplica, null, 100, onDone);
            }
            gcJob = CompletableFuture.allOf(gcFutures.toArray(new CompletableFuture[0]))
                .whenComplete((v, e) -> scheduleGC());
        });
    }

    private void gcRange(KVRangeSetting leaderReplica,
                         ByteString scopedInboxId,
                         int limit,
                         CompletableFuture<Void> onDone) {
        long reqId = System.currentTimeMillis();
        storeClient.query(leaderReplica.leader, KVRangeRORequest.newBuilder()
                .setReqId(reqId)
                .setKvRangeId(leaderReplica.id)
                .setVer(leaderReplica.ver)
                .setRoCoProcInput(MessageUtil.buildGCRequest(reqId, scopedInboxId, limit).toByteString())
                .build())
            .thenApply(v -> {
                try {
                    if (v.getCode() == ReplyCode.Ok) {
                        return InboxServiceROCoProcOutput.parseFrom(v.getRoCoProcResult()).getGc()
                            .getScopedInboxIdList();
                    }
                    throw new RuntimeException("BaseKV Query failed:" + v.getCode().name());
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            })
            .exceptionally(e -> {
                log.error("[InboxGC] scan failed: serverId={}, rangeId={}, ver={}",
                    leaderReplica.leader, leaderReplica.id, leaderReplica.ver, e);
                return Collections.emptyList();
            })
            .thenCompose(scopedInboxIdList -> {
                log.debug("[InboxGC] scan succeed: serverId={}, rangeId={}, ver={}",
                    leaderReplica.leader, leaderReplica.id, leaderReplica.ver);
                if (scopedInboxIdList.isEmpty()) {
                    return CompletableFuture.completedFuture(null);
                }
                return CompletableFuture.allOf(scopedInboxIdList.stream()
                        .map(scopedInboxIdToGc -> {
                            String tenantId = parseTenantId(scopedInboxIdToGc);
                            String inboxId = parseInboxId(scopedInboxIdToGc);
                            return inboxReaderClient.touch(reqId, tenantId, inboxId)
                                .handle((v, e) -> {
                                    if (e != null) {
                                        log.error("Failed to clean expired inbox", e);
                                    } else {
                                        log.debug("[InboxGC] clean success: tenantId={}, inboxId={}", tenantId, inboxId);
                                    }
                                    return null;
                                });
                        })
                        .toArray(CompletableFuture[]::new))
                    .thenApply(v -> scopedInboxIdList.get(scopedInboxIdList.size() - 1));
            })
            .thenAccept(startFromScopedInboxId -> {
                if (startFromScopedInboxId != null) {
                    jobRunner.add(() -> {
                        if (status.get() != Status.STARTED) {
                            return;
                        }
                        gcRange(leaderReplica, startFromScopedInboxId, limit, onDone);
                    });
                } else {
                    onDone.complete(null);
                }
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
            boolean newGauging = !tenantInboxSpaceSize.containsKey(tenantId);
            tenantInboxSpaceSize.put(tenantId, sizeMap.get(tenantId));
            if (newGauging) {
                gauging(tenantId, InboxSpaceSizeGauge, () -> tenantInboxSpaceSize.getOrDefault(tenantId, 0L));
            }
        }
        for (String tenantId : tenantInboxSpaceSize.keySet()) {
            if (!sizeMap.containsKey(tenantId)) {
                stopGauging(tenantId, InboxSpaceSizeGauge);
                tenantInboxSpaceSize.remove(tenantId);
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
                    return InboxServiceROCoProcOutput.parseFrom(reply.getRoCoProcResult()).getCollectedMetrics();
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
}
