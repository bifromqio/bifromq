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

import static com.baidu.bifromq.basekv.KVRangeSetting.regInProcStore;
import static com.baidu.bifromq.basekv.proto.State.StateType.Normal;
import static com.baidu.bifromq.basekv.store.exception.KVRangeStoreException.rangeNotFound;
import static com.baidu.bifromq.basekv.store.util.ExecutorServiceUtil.awaitShutdown;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.EMPTY_BOUNDARY;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.isOverlap;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basekv.localengine.ICPableKVSpace;
import com.baidu.bifromq.basekv.localengine.IKVEngine;
import com.baidu.bifromq.basekv.localengine.IKVSpace;
import com.baidu.bifromq.basekv.localengine.KVEngineFactory;
import com.baidu.bifromq.basekv.proto.Boundary;
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
import com.baidu.bifromq.basekv.store.range.IKVRangeFSM;
import com.baidu.bifromq.basekv.store.range.KVRange;
import com.baidu.bifromq.basekv.store.range.KVRangeFSM;
import com.baidu.bifromq.basekv.store.stats.IStatsCollector;
import com.baidu.bifromq.basekv.store.util.AsyncRunner;
import com.baidu.bifromq.basekv.store.wal.IKVRangeWALStore;
import com.baidu.bifromq.basekv.store.wal.KVRangeWALStorageEngine;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.time.Duration;
import java.util.ArrayList;
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
    private final Map<KVRangeId, IKVRangeFSM> kvRangeMap = Maps.newConcurrentMap();
    private final Subject<List<Observable<KVRangeDescriptor>>> descriptorListSubject =
        BehaviorSubject.<List<Observable<KVRangeDescriptor>>>create().toSerialized();
    private final IKVRangeCoProcFactory coProcFactory;
    private final KVRangeWALStorageEngine walStorageEngine;
    private final IKVEngine<? extends ICPableKVSpace> kvRangeEngine;
    private final IStatsCollector storeStatsCollector;
    private final CompositeDisposable disposable = new CompositeDisposable();
    private final Executor queryExecutor;
    private final ScheduledExecutorService tickExecutor;
    private volatile ScheduledFuture<?> tickFuture;
    private final ScheduledExecutorService bgTaskExecutor;
    private final ScheduledExecutorService mgmtTaskExecutor;
    private final AsyncRunner mgmtTaskRunner;
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
        kvRangeEngine = KVEngineFactory.createCPable(null, opts.getDataEngineConfigurator());
        this.queryExecutor = queryExecutor;
        this.tickExecutor = tickExecutor;
        this.bgTaskExecutor = bgTaskExecutor;
        this.mgmtTaskExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ScheduledThreadPoolExecutor(1, EnvProvider.INSTANCE.newThreadFactory("kvstore-mgmt-executor")),
            "kvstore[" + id + "]-mgmt-executor");
        this.mgmtTaskRunner = new AsyncRunner(mgmtTaskExecutor);
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
        mgmtTaskRunner.add(() -> {
            kvRangeEngine.spaces().forEach((id, keyRange) ->
                putAndOpen(loadKVRangeFSM(KVRangeIdUtil.fromString(id), keyRange)));
            updateDescriptorList();
        }).toCompletableFuture().join();
    }

    @Override
    public void stop() {
        if (status.compareAndSet(Status.STARTED, Status.CLOSING)) {
            try {
                log.debug("Stop KVRange store[{}]", id);
                List<CompletableFuture<Void>> closeFutures = new ArrayList<>();
                try {
                    for (IKVRangeFSM rangeFSM : kvRangeMap.values()) {
                        closeFutures.add(rangeFSM.close());
                    }
                } catch (Throwable e) {
                    log.error("Failed to close range", e);
                }

                CompletableFuture.allOf(closeFutures.toArray(CompletableFuture[]::new)).join();
//                CompletableFuture.allOf(kvRangeMap.values().stream()
//                        .map(IKVRangeFSM::close)
//                        .toArray(CompletableFuture[]::new))
//                    .join();
                disposable.dispose();
                storeStatsCollector.stop().toCompletableFuture().join();
                mgmtTaskRunner.awaitDone().toCompletableFuture().join();
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
                awaitShutdown(mgmtTaskExecutor).join();
                status.set(Status.TERMINATED);
            }
        }
    }

    @Override
    public boolean bootstrap() {
        checkStarted();
        return mgmtTaskRunner.add(() -> {
            if (kvRangeMap.isEmpty()) {
                // build the genesis "full" KVRange in the cluster
                KVRangeId genesisId = KVRangeIdUtil.generate();
                log.debug("Creating the genesis KVRange[{}]", KVRangeIdUtil.toString(genesisId));
                assert !walStorageEngine.has(genesisId);
                KVRangeSnapshot rangeSnapshot = KVRangeSnapshot.newBuilder()
                    .setId(genesisId)
                    .setVer(0)
                    .setLastAppliedIndex(5)
                    .setState(State.newBuilder().setType(Normal).build())
                    .setBoundary(FULL_BOUNDARY)
                    .build();
                Snapshot snapshot = Snapshot.newBuilder()
                    .setClusterConfig(ClusterConfig.newBuilder()
                        .addVoters(id())
                        .build())
                    .setTerm(0)
                    .setIndex(5)
                    .setData(rangeSnapshot.toByteString())
                    .build();
                putAndOpen(createKVRangeFSM(genesisId, snapshot, rangeSnapshot));
                updateDescriptorList();
                return CompletableFuture.completedFuture(true);
            }
            return CompletableFuture.completedFuture(false);
        }).join();
    }

    @Override
    public boolean isHosting(KVRangeId rangeId) {
        return kvRangeMap.containsKey(rangeId);
    }

    @Override
    public CompletionStage<Void> recover() {
        checkStarted();
        metricsManager.runningRecoverNum.increment();
        return mgmtTaskRunner.add(() -> CompletableFuture.allOf(kvRangeMap.values().stream()
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
                mgmtTaskRunner.add(() -> {
                    KVRangeId rangeId = payload.getRangeId();
                    long ver = request.getVer();
                    Boundary boundary = request.getBoundary();
                    IKVRangeFSM range = kvRangeMap.get(rangeId);
                    try {
                        KVRangeSnapshot rangeSnapshot =
                            KVRangeSnapshot.parseFrom(request.getInitSnapshot().getData());
                        if (range != null) {
                            if (range.ver() >= ver) {
                                // just return OK
                                messenger.send(StoreMessage.newBuilder()
                                    .setFrom(id)
                                    .setSrcRange(payload.getRangeId())
                                    .setPayload(KVRangeMessage.newBuilder()
                                        .setRangeId(storeMessage.getSrcRange())
                                        .setHostStoreId(storeMessage.getFrom())
                                        .setEnsureRangeReply(EnsureRangeReply.newBuilder()
                                            .setResult(range.ver() == ver ?
                                                // ok for range already exists
                                                EnsureRangeReply.Result.OK :
                                                // error for staled ensure request
                                                EnsureRangeReply.Result.Error)
                                            .build())
                                        .build())
                                    .build());
                                return CompletableFuture.completedFuture(null);
                            } else {
                                // existing range is stale
                                // make sure boundary is compatible with others
                                return ensureCompatibilityInternal(rangeId, ver, boundary, emptySet())
                                    .thenCompose(v -> range.destroy())
                                    .thenAccept(v -> {
                                        putAndOpen(createKVRangeFSM(rangeId, request.getInitSnapshot(), rangeSnapshot));
                                        messenger.send(StoreMessage.newBuilder()
                                            .setFrom(id)
                                            .setSrcRange(payload.getRangeId())
                                            .setPayload(KVRangeMessage.newBuilder()
                                                .setRangeId(storeMessage.getSrcRange())
                                                .setHostStoreId(storeMessage.getFrom())
                                                .setEnsureRangeReply(EnsureRangeReply.newBuilder()
                                                    .setResult(EnsureRangeReply.Result.OK)
                                                    .build())
                                                .build())
                                            .build());
                                        updateDescriptorList();
                                    });
                            }
                        } else {
                            ICPableKVSpace keyRange = kvRangeEngine.spaces().get(KVRangeIdUtil.toString(rangeId));
                            if (keyRange == null) {
                                if (walStorageEngine.has(rangeId)) {
                                    log.warn("Staled WAL Store found, destroy it: rangeId={}, storeId={}",
                                        KVRangeIdUtil.toString(rangeId), id);
                                    walStorageEngine.get(rangeId).destroy();
                                }
                                putAndOpen(createKVRangeFSM(rangeId, request.getInitSnapshot(), rangeSnapshot));
                            } else {
                                // for split workflow, the keyspace is already created, just create walstore and load it
                                if (!walStorageEngine.has(rangeId)) {
                                    walStorageEngine.create(rangeId, request.getInitSnapshot());
                                }
                                putAndOpen(loadKVRangeFSM(rangeId, keyRange));
                            }
                            updateDescriptorList();
                            messenger.send(StoreMessage.newBuilder()
                                .setFrom(id)
                                .setSrcRange(payload.getRangeId())
                                .setPayload(KVRangeMessage.newBuilder()
                                    .setRangeId(storeMessage.getSrcRange())
                                    .setHostStoreId(storeMessage.getFrom())
                                    .setEnsureRangeReply(EnsureRangeReply.newBuilder()
                                        .setResult(EnsureRangeReply.Result.OK)
                                        .build())
                                    .build())
                                .build());
                            return CompletableFuture.completedFuture(null);
                        }
                    } catch (InvalidProtocolBufferException e) {
                        // should never happen
                        log.error("Unexpected error", e);
                        messenger.send(StoreMessage.newBuilder()
                            .setFrom(id)
                            .setSrcRange(payload.getRangeId())
                            .setPayload(KVRangeMessage.newBuilder()
                                .setRangeId(storeMessage.getSrcRange())
                                .setHostStoreId(storeMessage.getFrom())
                                .setEnsureRangeReply(EnsureRangeReply.newBuilder()
                                    .setResult(EnsureRangeReply.Result.Error)
                                    .build())
                                .build())
                            .build());
                        return CompletableFuture.completedFuture(null);
                    }
                });
            }
        }
    }

    @Override
    public CompletionStage<Void> transferLeadership(long ver, KVRangeId rangeId, String newLeader) {
        checkStarted();
        IKVRangeFSM kvRange = kvRangeMap.get(rangeId);
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
        IKVRangeFSM kvRange = kvRangeMap.get(rangeId);
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
        IKVRangeFSM kvRange = kvRangeMap.get(rangeId);
        if (kvRange != null) {
            metricsManager.runningSplitNum.increment();
            return kvRange.split(ver, splitKey).whenComplete((v, e) -> metricsManager.runningSplitNum.decrement());
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<Void> merge(long ver, KVRangeId mergerId, KVRangeId mergeeId) {
        checkStarted();
        IKVRangeFSM kvRange = kvRangeMap.get(mergerId);
        if (kvRange != null) {
            metricsManager.runningMergeNum.increment();
            return kvRange.merge(ver, mergeeId).whenComplete((v, e) -> metricsManager.runningMergeNum.decrement());
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<Boolean> exist(long ver, KVRangeId id, ByteString key, boolean linearized) {
        checkStarted();
        IKVRangeFSM kvRange = kvRangeMap.get(id);
        if (kvRange != null) {
            return kvRange.exist(ver, key, linearized);
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<Optional<ByteString>> get(long ver, KVRangeId id, ByteString key,
                                                     boolean linearized) {
        checkStarted();
        IKVRangeFSM kvRange = kvRangeMap.get(id);
        if (kvRange != null) {
            return kvRange.get(ver, key, linearized);
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<ROCoProcOutput> queryCoProc(long ver, KVRangeId id, ROCoProcInput query,
                                                       boolean linearized) {
        checkStarted();
        IKVRangeFSM kvRange = kvRangeMap.get(id);
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
        IKVRangeFSM kvRange = kvRangeMap.get(id);
        if (kvRange != null) {
            return kvRange.put(ver, key, value);
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<ByteString> delete(long ver, KVRangeId id, ByteString key) {
        checkStarted();
        IKVRangeFSM kvRange = kvRangeMap.get(id);
        if (kvRange != null) {
            return kvRange.delete(ver, key);
        }
        return CompletableFuture.failedFuture(rangeNotFound());
    }

    @Override
    public CompletionStage<RWCoProcOutput> mutateCoProc(long ver, KVRangeId id, RWCoProcInput mutate) {
        checkStarted();
        IKVRangeFSM kvRange = kvRangeMap.get(id);
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
        try {
            kvRangeMap.forEach((v, r) -> r.tick());
            storeStatsCollector.tick();
        } catch (Throwable e) {
            log.error("Unexpected error during tick", e);
        } finally {
            scheduleTick(opts.getKvRangeOptions().getTickUnitInMS());
        }
    }

    private void quitKVRange(IKVRangeFSM range) {
        mgmtTaskRunner.add(() -> {
            kvRangeMap.remove(range.id(), range);
            return range.destroy().thenAccept(v -> updateDescriptorList());
        });
    }

    private IKVRangeFSM loadKVRangeFSM(KVRangeId rangeId, ICPableKVSpace keyRange) {
        checkStarted();
        log.debug("Load existing kvrange: rangeId={},storeId={}", KVRangeIdUtil.toString(rangeId), id);
        assert walStorageEngine.has(rangeId);
        return new KVRangeFSM(clusterId,
            id,
            rangeId,
            coProcFactory,
            this::ensureCompatibility,
            new KVRange(keyRange),
            walStorageEngine.get(rangeId),
            queryExecutor,
            bgTaskExecutor,
            opts.getKvRangeOptions(),
            this::quitKVRange);
    }

    private IKVRangeFSM createKVRangeFSM(KVRangeId rangeId, Snapshot snapshot, KVRangeSnapshot rangeSnapshot) {
        log.debug("Creating new kvrange: rangeId={}, storeId={}", KVRangeIdUtil.toString(rangeId), id);
        IKVRangeWALStore walStore = walStorageEngine.create(rangeId, snapshot);
        return new KVRangeFSM(clusterId,
            id,
            rangeId,
            coProcFactory,
            this::ensureCompatibility,
            new KVRange(kvRangeEngine.createIfMissing(KVRangeIdUtil.toString(rangeId)), rangeSnapshot),
            walStore,
            queryExecutor,
            bgTaskExecutor,
            opts.getKvRangeOptions(),
            this::quitKVRange);
    }

    private void updateDescriptorList() {
        descriptorListSubject.onNext(
            kvRangeMap.values().stream().map(IKVRangeFSM::describe).collect(Collectors.toList()));
    }

    private CompletableFuture<Void> ensureCompatibility(KVRangeId rangeId,
                                                        long ver,
                                                        Boundary boundary,
                                                        Set<KVRangeId> ignoreRanges) {
        return mgmtTaskRunner.add(() -> ensureCompatibilityInternal(rangeId, ver, boundary, ignoreRanges));
    }

    private CompletableFuture<Void> ensureCompatibilityInternal(KVRangeId rangeId,
                                                                long ver,
                                                                Boundary boundary,
                                                                Set<KVRangeId> ignoreRanges) {
        log.debug("Ensuring snapshot's compatibility: rangeId={}, storeId={}", KVRangeIdUtil.toString(rangeId), id);
        List<IKVRangeFSM> overlapped = kvRangeMap.values().stream()
            .filter(r -> !ignoreRanges.contains(r.id()) &&
                isOverlap(r.boundary(), boundary) && !r.id().equals(rangeId))
            .toList();
        if (overlapped.stream().anyMatch(r -> r.ver() > ver)) {
            return CompletableFuture.failedFuture(new KVRangeStoreException("Incompatible range version"));
        } else {
            return CompletableFuture.allOf(overlapped.stream()
                    .map(d -> kvRangeMap.remove(d.id()))
                    .map(r -> {
                        KVRangeId overlappedRangeId = r.id();
                        log.debug("Destroying overlapped range[{}]: rangeId={}, storeId={}",
                            KVRangeIdUtil.toString(overlappedRangeId), KVRangeIdUtil.toString(rangeId), id);
                        return r.destroy()
                            .thenAccept(v -> {
                                log.debug("Overlapped range[{}] destroyed: rangeId={}, storeId={}",
                                    KVRangeIdUtil.toString(overlappedRangeId), KVRangeIdUtil.toString(rangeId), id);
                                KVRangeSnapshot rangeSnapshot = KVRangeSnapshot.newBuilder()
                                    .setVer(0)
                                    .setId(overlappedRangeId)
                                    .setLastAppliedIndex(0)
                                    .setBoundary(EMPTY_BOUNDARY)
                                    .setState(State.newBuilder().setType(Normal).build())
                                    .build();
                                Snapshot initSnapshot = Snapshot.newBuilder()
                                    .setClusterConfig(ClusterConfig.getDefaultInstance())
                                    .setTerm(0)
                                    .setIndex(0)
                                    .setData(rangeSnapshot.toByteString())
                                    .build();
                                log.debug("Init range using Snapshot: rangeId={}, storeId={}\n{}",
                                    KVRangeIdUtil.toString(overlappedRangeId), id, initSnapshot);
                                putAndOpen(createKVRangeFSM(overlappedRangeId, initSnapshot, rangeSnapshot));
                            });
                    })
                    .toArray(CompletableFuture[]::new))
                .thenAccept(v -> updateDescriptorList());
        }
    }

    private void putAndOpen(IKVRangeFSM kvRangeFSM) {
        kvRangeMap.put(kvRangeFSM.id(), kvRangeFSM);
        kvRangeFSM.open(new KVRangeMessenger(id, kvRangeFSM.id(), messenger));
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
