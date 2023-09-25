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

package com.baidu.bifromq.basekv.store;

import static com.baidu.bifromq.basekv.Constants.EMPTY_RANGE;
import static com.baidu.bifromq.basekv.Constants.FULL_RANGE;
import static com.baidu.bifromq.basekv.KVRangeSetting.regInProcStore;
import static com.baidu.bifromq.basekv.localengine.IKVEngine.DEFAULT_NS;
import static com.baidu.bifromq.basekv.proto.State.StateType.Normal;
import static com.baidu.bifromq.basekv.store.exception.KVRangeStoreException.rangeNotFound;
import static com.baidu.bifromq.basekv.utils.KVRangeIdUtil.toShortString;
import static java.util.Collections.emptyList;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basekv.localengine.IKVEngine;
import com.baidu.bifromq.basekv.localengine.KVEngineFactory;
import com.baidu.bifromq.basekv.proto.EnsureRange;
import com.baidu.bifromq.basekv.proto.EnsureRangeReply;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeMessage;
import com.baidu.bifromq.basekv.proto.KVRangeSnapshot;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.proto.State;
import com.baidu.bifromq.basekv.proto.StoreMessage;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.raft.proto.Snapshot;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProcFactory;
import com.baidu.bifromq.basekv.store.exception.KVRangeStoreException;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ROCoProcOutput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.basekv.store.range.IKVRange;
import com.baidu.bifromq.basekv.store.range.KVRange;
import com.baidu.bifromq.basekv.store.stats.IStatsCollector;
import com.baidu.bifromq.basekv.store.util.AsyncRunner;
import com.baidu.bifromq.basekv.store.wal.KVRangeWALStorageEngine;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.basekv.utils.KeyRangeUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KVRangeStore implements IKVRangeStore {
    private enum Status {
        INIT, // store is created but cannot serve requests
        STARTING, // store is starting
        STARTED, // store can serve requests
        FATAL_FAILURE, // fatal failure happened during starting
        CLOSING, // store closing, no more outgoing messages
        CLOSED, // store closed, no tasks running
        TERMINATING, // releasing all resources
        TERMINATED // resource released
    }

    private final String clusterId;
    private final String id;
    private final AtomicReference<Status> status = new AtomicReference<>(Status.INIT);
    private final Map<KVRangeId, IKVRange> kvRangeMap = Maps.newConcurrentMap();
    private final Set<IKVRange> quitRanges = Sets.newConcurrentHashSet();
    private final Subject<List<Observable<KVRangeDescriptor>>> descriptorListSubject =
        BehaviorSubject.<List<Observable<KVRangeDescriptor>>>create().toSerialized();
    private final IKVRangeCoProcFactory coProcFactory;
    private final KVRangeWALStorageEngine walStorageEngine;
    private final IKVEngine kvRangeEngine;
    private final IStatsCollector storeStatsCollector;
    private final CompositeDisposable disposable = new CompositeDisposable();
    private final Executor queryExecutor;
    private final ScheduledExecutorService tickExecutor;
    private volatile ScheduledFuture<?> tickFuture;
    private final ScheduledExecutorService bgTaskExecutor;
    private final ScheduledExecutorService rangeMgmtTaskExecutor;
    private final AsyncRunner rangeMgmtTaskRunner;
    private final KVRangeStoreOptions opts;
    private final MetricsManager metricsManager;
    private IStoreMessenger messenger;

    public KVRangeStore(String clusterId,
                        KVRangeStoreOptions opts,
                        IKVRangeCoProcFactory coProcFactory,
                        @NonNull Executor queryExecutor,
                        @NonNull ScheduledExecutorService tickExecutor,
                        @NonNull ScheduledExecutorService bgTaskExecutor) {
        this.clusterId = clusterId;
        this.coProcFactory = coProcFactory;
        this.opts = opts.toBuilder().build();
        this.walStorageEngine =
            new KVRangeWALStorageEngine(clusterId, opts.getOverrideIdentity(), opts.getWalEngineConfigurator());
        id = walStorageEngine.id();
        if (opts.getOverrideIdentity() != null
            && !opts.getOverrideIdentity().trim().isEmpty()
            && !opts.getOverrideIdentity().equals(id)) {
            log.warn("KVRangeStore has been initialized with identity[{}], the override[{}] is ignored",
                id, opts.getOverrideIdentity());
        }
        kvRangeEngine = KVEngineFactory.create(null, List.of(DEFAULT_NS),
            this::check, opts.getDataEngineConfigurator());
        this.queryExecutor = queryExecutor;
        this.tickExecutor = tickExecutor;
        this.bgTaskExecutor = bgTaskExecutor;
        this.rangeMgmtTaskExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ScheduledThreadPoolExecutor(1, EnvProvider.INSTANCE.newThreadFactory("kvstore-mgmt-executor")),
            "kvstore[" + id + "]-mgmt-executor");
        this.rangeMgmtTaskRunner = new AsyncRunner(rangeMgmtTaskExecutor);
        this.metricsManager = new MetricsManager(clusterId, id);
        storeStatsCollector =
            new KVRangeStoreStatsCollector(opts, Duration.ofSeconds(opts.getStatsCollectIntervalSec()),
                this.bgTaskExecutor);
    }

    @Override
    public String clusterId() {
        return clusterId;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public boolean isStarted() {
        return status.get() == Status.STARTED;
    }

    @Override
    public void start(IStoreMessenger messenger) {
        if (status.compareAndSet(Status.INIT, Status.STARTING)) {
            try {
                this.messenger = messenger;
                walStorageEngine.start();
                kvRangeEngine.start("storeId", id, "type", "data");
                log.debug("KVRangeStore[{}] started", id);
                status.set(Status.STARTED);
                disposable.add(messenger.receive().subscribe(this::receive));
                loadExisting();
                scheduleTick(0);
                regInProcStore(clusterId, id);
            } catch (Throwable e) {
                status.set(Status.FATAL_FAILURE);
                throw new KVRangeStoreException("Failed to start kv range store", e);
            }
        } else {
            log.warn("KVRangeStore cannot start in {} status", status.get().name());
        }
    }

    private void loadExisting() {
        rangeMgmtTaskRunner.add(() -> {
            walStorageEngine.allKVRangeIds()
                .forEach(kvRangeId -> kvRangeMap.computeIfAbsent(kvRangeId, key -> initKVRange(key, null)));
            updateDescriptorList();
        }).toCompletableFuture().join();
    }

    @Override
    public void stop() {
        if (status.compareAndSet(Status.STARTED, Status.CLOSING)) {
            try {
                log.debug("Stop KVRange store[{}]", id);
                CompletableFuture.allOf(kvRangeMap.values().stream()
                        .map(IKVRange::close)
                        .toArray(CompletableFuture[]::new))
                    .join();
                disposable.dispose();
                storeStatsCollector.stop().toCompletableFuture().join();
                rangeMgmtTaskRunner.awaitDone().toCompletableFuture().join();
                log.debug("Stopping WAL Engine");
                walStorageEngine.stop();
                log.debug("Stopping KVRange Engine");
                kvRangeEngine.stop();
                descriptorListSubject.onComplete();
                status.set(Status.CLOSED);
                status.set(Status.TERMINATING);
                tickFuture.get();
            } catch (Throwable e) {
                log.error("Error occurred during stopping range store", e);
            } finally {
                messenger.close();
                rangeMgmtTaskExecutor.shutdown();
                status.set(Status.TERMINATED);
            }
        }
    }

    @Override
    public boolean bootstrap() {
        checkStarted();
        if (kvRangeMap.isEmpty()) {
            // build the genesis "full" KVRange in the cluster
            KVRangeId genesisId = KVRangeIdUtil.generate();
            log.debug("Creating the genesis KVRange[{}]", toShortString(genesisId));
            ensureKVRange(genesisId, Snapshot.newBuilder()
                .setClusterConfig(ClusterConfig.newBuilder()
                    .addVoters(id())
                    .build())
                .setTerm(0)
                .setIndex(5)
                .setData(KVRangeSnapshot.newBuilder()
                    .setId(genesisId)
                    .setVer(0)
                    .setLastAppliedIndex(5)
                    .setState(State.newBuilder().setType(Normal).build())
                    .setRange(FULL_RANGE)
                    .build().toByteString())
                .build()).toCompletableFuture().join();
            return true;
        }
        return false;
    }

    @Override
    public boolean isHosting(KVRangeId rangeId) {
        return kvRangeMap.containsKey(rangeId);
    }

    @Override
    public CompletionStage<Void> recover() {
        checkStarted();
        metricsManager.runningRecoverNum.increment();
        return rangeMgmtTaskRunner.add(() -> CompletableFuture.allOf(kvRangeMap.values().stream()
                .map(kvRange -> kvRange.recover().handle((v, e) -> {
                    if (e != null) {
                        log.warn("KVRange[{}] recover failed for some reason",
                            KVRangeIdUtil.toString(kvRange.id()), e);
                    }
                    return null;
                }).toCompletableFuture())
                .toArray(CompletableFuture[]::new)))
            .whenComplete((v, e) -> metricsManager.runningRecoverNum.decrement());
    }

    @Override
    public Observable<KVRangeStoreDescriptor> describe() {
        checkStarted();
        return descriptorListSubject
            .distinctUntilChanged()
            .switchMap(descriptorList -> {
                Observable<List<KVRangeDescriptor>> descListObservable = descriptorList.isEmpty() ?
                    BehaviorSubject.createDefault(emptyList()) :
                    Observable.combineLatest(descriptorList, descs ->
                        Arrays.stream(descs).map(desc -> (KVRangeDescriptor) desc).collect(Collectors.toList()));
                return Observable.combineLatest(storeStatsCollector.collect(), descListObservable,
                    (storeStats, descList) -> KVRangeStoreDescriptor.newBuilder()
                        .setId(id)
                        .putAllStatistics(storeStats)
                        .addAllRanges(descList)
                        .setHlc(HLC.INST.get())
                        .build());
            })
            .distinctUntilChanged();
    }

    private void receive(StoreMessage storeMessage) {
        if (status.get() == Status.STARTED) {
            KVRangeMessage payload = storeMessage.getPayload();
            if (payload.hasEnsureRange()) {
                EnsureRange request = storeMessage.getPayload().getEnsureRange();
                ensureKVRange(payload.getRangeId(), request.getInitSnapshot())
                    .whenComplete((v, e) -> messenger.send(StoreMessage.newBuilder()
                        .setFrom(id)
                        .setSrcRange(payload.getRangeId())
                        .setPayload(KVRangeMessage.newBuilder()
                            .setRangeId(storeMessage.getSrcRange())
                            .setHostStoreId(storeMessage.getFrom())
                            .setEnsureRangeReply(EnsureRangeReply.newBuilder()
                                .setResult(EnsureRangeReply.Result.OK)
                                .build())
                            .build())
                        .build()));
            }
        }
    }

    @Override
    public CompletionStage<Void> transferLeadership(long ver, KVRangeId rangeId, String newLeader) {
        checkStarted();
        IKVRange kvRange = kvRangeMap.get(rangeId);
        if (kvRange != null) {
            metricsManager.runningTransferLeaderNum.increment();
            return kvRange.transferLeadership(ver, newLeader)
                .whenComplete((v, e) -> metricsManager.runningTransferLeaderNum.decrement());
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<Void> changeReplicaConfig(long ver, KVRangeId rangeId,
                                                     Set<String> newVoters,
                                                     Set<String> newLearners) {
        checkStarted();
        IKVRange kvRange = kvRangeMap.get(rangeId);
        if (kvRange != null) {
            metricsManager.runningConfigChangeNum.increment();
            return kvRange.changeReplicaConfig(ver, newVoters, newLearners)
                .whenComplete((v, e) -> metricsManager.runningConfigChangeNum.decrement());
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<Void> split(long ver, KVRangeId rangeId, ByteString splitKey) {
        checkStarted();
        IKVRange kvRange = kvRangeMap.get(rangeId);
        if (kvRange != null) {
            metricsManager.runningSplitNum.increment();
            return kvRange.split(ver, splitKey).whenComplete((v, e) -> metricsManager.runningSplitNum.decrement());
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<Void> merge(long ver, KVRangeId mergerId, KVRangeId mergeeId) {
        checkStarted();
        IKVRange kvRange = kvRangeMap.get(mergerId);
        if (kvRange != null) {
            metricsManager.runningMergeNum.increment();
            return kvRange.merge(ver, mergeeId).whenComplete((v, e) -> metricsManager.runningMergeNum.decrement());
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<Boolean> exist(long ver, KVRangeId id, ByteString key, boolean linearized) {
        checkStarted();
        IKVRange kvRange = kvRangeMap.get(id);
        if (kvRange != null) {
            return kvRange.exist(ver, key, linearized);
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<Optional<ByteString>> get(long ver, KVRangeId id, ByteString key,
                                                     boolean linearized) {
        checkStarted();
        IKVRange kvRange = kvRangeMap.get(id);
        if (kvRange != null) {
            return kvRange.get(ver, key, linearized);
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<ROCoProcOutput> queryCoProc(long ver, KVRangeId id, ROCoProcInput query,
                                                       boolean linearized) {
        checkStarted();
        IKVRange kvRange = kvRangeMap.get(id);
        if (kvRange != null) {
            metricsManager.runningQueryNum.increment();
            return kvRange.queryCoProc(ver, query, linearized)
                .whenComplete((v, e) -> metricsManager.runningQueryNum.decrement());
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<ByteString> put(long ver, KVRangeId id, ByteString key, ByteString value) {
        checkStarted();
        IKVRange kvRange = kvRangeMap.get(id);
        if (kvRange != null) {
            return kvRange.put(ver, key, value);
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<ByteString> delete(long ver, KVRangeId id, ByteString key) {
        checkStarted();
        IKVRange kvRange = kvRangeMap.get(id);
        if (kvRange != null) {
            return kvRange.delete(ver, key);
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<RWCoProcOutput> mutateCoProc(long ver, KVRangeId id, RWCoProcInput mutate) {
        checkStarted();
        IKVRange kvRange = kvRangeMap.get(id);
        if (kvRange != null) {
            metricsManager.runningMutateNum.increment();
            return kvRange.mutateCoProc(ver, mutate)
                .whenComplete((v, e) -> metricsManager.runningMutateNum.decrement());
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    private void scheduleTick(long delayInMS) {
        tickFuture = tickExecutor.schedule(this::tick, delayInMS, TimeUnit.MILLISECONDS);
    }

    private void tick() {
        if (status.get() != Status.STARTED && status.get() != Status.CLOSING) {
            return;
        }
        kvRangeMap.forEach((v, r) -> {
            r.tick();
            if (r.readyToQuit()) {
                kvRangeMap.remove(v, r);
                quitKVRange(r);
            }
        });
        quitRanges.forEach(IKVRange::tick);
        storeStatsCollector.tick();
        scheduleTick(opts.getKvRangeOptions().getTickUnitInMS());
    }

    private CompletionStage<Void> ensureKVRange(KVRangeId rangeId, Snapshot snapshot) {
        return rangeMgmtTaskRunner.add(() -> addKVRange(rangeId, snapshot));
    }

    private void addKVRange(KVRangeId rangeId, Snapshot snapshot) {
        kvRangeMap.computeIfAbsent(rangeId, k -> {
            log.debug("Init range[{}] in store[{}]", toShortString(rangeId), id);
            return initKVRange(k, snapshot);
        });
        updateDescriptorList();
    }

    private void quitKVRange(IKVRange range) {
        quitRanges.add(range);
        rangeMgmtTaskRunner.add(() -> range.quit()
            .whenCompleteAsync((v, e) -> {
                if (e != null) {
                    quitKVRange(range);
                } else {
                    quitRanges.remove(range);
                    updateDescriptorList();
                }
            }, rangeMgmtTaskExecutor));
    }

    private KVRange initKVRange(KVRangeId kvRangeId, Snapshot initSnapshot) {
        checkStarted();
        log.debug("Create range: storeId={}, rangeId={}", id, toShortString(kvRangeId));
        KVRange kvRange = new KVRange(clusterId,
            id,
            kvRangeId,
            coProcFactory,
            this::checkSnapshot,
            kvRangeEngine,
            walStorageEngine,
            queryExecutor,
            rangeMgmtTaskExecutor,
            bgTaskExecutor,
            opts.getKvRangeOptions(),
            initSnapshot);
        log.debug("Open range: storeId={}, rangeId={}", id, toShortString(kvRange.id()));
        kvRange.open(new KVRangeMessenger(id, kvRange.id(), messenger));
        return kvRange;
    }

    private void updateDescriptorList() {
        descriptorListSubject.onNext(kvRangeMap.values().stream().map(IKVRange::describe).collect(Collectors.toList()));
    }

    private CompletionStage<Void> checkSnapshot(KVRangeSnapshot snapshot) {
        return rangeMgmtTaskRunner.add(() -> {
                CompletableFuture<List<KVRangeDescriptor>> onDone = new CompletableFuture<>();
                List<KVRangeDescriptor> overlapped = kvRangeMap.values().stream()
                    .map(r -> r.describe().blockingFirst())
                    .filter(r -> KeyRangeUtil.isOverlap(r.getRange(), snapshot.getRange())
                        && !r.getId().equals(snapshot.getId()))
                    .collect(Collectors.toList());
                if (overlapped.stream().anyMatch(r -> r.getVer() > snapshot.getVer())) {
                    onDone.completeExceptionally(new KVRangeStoreException("Staled snapshot"));
                } else {
                    onDone.complete(overlapped);
                    // destroy overlapped staled range
                }
                return onDone;
            })
            .thenCompose(overlapped -> rangeMgmtTaskRunner.add(() ->
                CompletableFuture.allOf(overlapped.stream()
                        .map(r -> kvRangeMap.remove(r.getId()).destroy())
                        .toArray(CompletableFuture[]::new))
                    .thenApply(v -> overlapped)))
            .thenCompose(overlapped -> {
                if (overlapped.isEmpty()) {
                    return CompletableFuture.completedFuture(null);
                }
                return rangeMgmtTaskRunner.add(() -> {
                    for (KVRangeDescriptor r : overlapped) {
                        addKVRange(r.getId(), Snapshot.newBuilder()
                            .setClusterConfig(ClusterConfig.getDefaultInstance())
                            .setTerm(0)
                            .setIndex(0)
                            .setData(KVRangeSnapshot.newBuilder()
                                .setVer(0)
                                .setId(r.getId())
                                .setLastAppliedIndex(0)
                                .setRange(EMPTY_RANGE)
                                .setState(State.newBuilder().setType(Normal).build())
                                .build()
                                .toByteString())
                            .build());
                    }
                });
            });
    }

    private boolean check(String checkpointId) {
        return kvRangeMap.values().stream().anyMatch(tb -> {
            try {
                return tb.isOccupying(checkpointId);
            } catch (Throwable e) {
                log.error("Failed to check checkpoint's[{}] occupation", checkpointId, e);
                return true;
            }
        });
    }

    private void checkStarted() {
        Preconditions.checkState(status.get() == Status.STARTED, "Store not running");
    }

    private static class MetricsManager {
        private final LongAdder runningConfigChangeNum;
        private final LongAdder runningTransferLeaderNum;
        private final LongAdder runningSplitNum;
        private final LongAdder runningMergeNum;
        private final LongAdder runningRecoverNum;
        private final LongAdder runningQueryNum;
        private final LongAdder runningMutateNum;

        MetricsManager(String clusterId, String storeId) {
            Tags tags = Tags.of("clusterId", clusterId).and("storeId", storeId);
            runningConfigChangeNum =
                Metrics.gauge("basekv.store.running", tags.and("cmd", "configchange"), new LongAdder());
            runningTransferLeaderNum =
                Metrics.gauge("basekv.store.running", tags.and("cmd", "transferleader"), new LongAdder());
            runningSplitNum =
                Metrics.gauge("basekv.store.running", tags.and("cmd", "split"), new LongAdder());
            runningMergeNum =
                Metrics.gauge("basekv.store.running", tags.and("cmd", "merge"), new LongAdder());
            runningRecoverNum =
                Metrics.gauge("basekv.store.running", tags.and("cmd", "recover"), new LongAdder());
            runningQueryNum =
                Metrics.gauge("basekv.store.running", tags.and("cmd", "query"), new LongAdder());
            runningMutateNum =
                Metrics.gauge("basekv.store.running", tags.and("cmd", "mutate"), new LongAdder());
        }
    }
}
