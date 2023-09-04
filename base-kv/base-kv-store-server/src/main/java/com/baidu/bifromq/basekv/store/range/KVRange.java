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

package com.baidu.bifromq.basekv.store.range;

import static com.baidu.bifromq.basekv.Constants.EMPTY_RANGE;
import static com.baidu.bifromq.basekv.proto.State.StateType.ConfigChanging;
import static com.baidu.bifromq.basekv.proto.State.StateType.Merged;
import static com.baidu.bifromq.basekv.proto.State.StateType.MergedQuiting;
import static com.baidu.bifromq.basekv.proto.State.StateType.Normal;
import static com.baidu.bifromq.basekv.proto.State.StateType.PreparedMerging;
import static com.baidu.bifromq.basekv.proto.State.StateType.Purged;
import static com.baidu.bifromq.basekv.proto.State.StateType.Removed;
import static com.baidu.bifromq.basekv.proto.State.StateType.WaitingForMerge;
import static com.baidu.bifromq.basekv.store.range.KVRange.Lifecycle.Closed;
import static com.baidu.bifromq.basekv.store.range.KVRange.Lifecycle.Destroyed;
import static com.baidu.bifromq.basekv.store.range.KVRange.Lifecycle.Destroying;
import static com.baidu.bifromq.basekv.store.range.KVRange.Lifecycle.Init;
import static com.baidu.bifromq.basekv.store.range.KVRange.Lifecycle.Open;
import static com.baidu.bifromq.basekv.utils.KVRangeIdUtil.toShortString;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.union;
import static java.util.Collections.singleton;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basekv.localengine.IKVEngine;
import com.baidu.bifromq.basekv.proto.CancelMerging;
import com.baidu.bifromq.basekv.proto.CancelMergingReply;
import com.baidu.bifromq.basekv.proto.CancelMergingRequest;
import com.baidu.bifromq.basekv.proto.ChangeConfig;
import com.baidu.bifromq.basekv.proto.Delete;
import com.baidu.bifromq.basekv.proto.EnsureRange;
import com.baidu.bifromq.basekv.proto.EnsureRangeReply;
import com.baidu.bifromq.basekv.proto.KVPair;
import com.baidu.bifromq.basekv.proto.KVRangeCommand;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeMessage;
import com.baidu.bifromq.basekv.proto.KVRangeSnapshot;
import com.baidu.bifromq.basekv.proto.LoadHint;
import com.baidu.bifromq.basekv.proto.Merge;
import com.baidu.bifromq.basekv.proto.MergeDone;
import com.baidu.bifromq.basekv.proto.MergeDoneReply;
import com.baidu.bifromq.basekv.proto.MergeDoneRequest;
import com.baidu.bifromq.basekv.proto.MergeReply;
import com.baidu.bifromq.basekv.proto.MergeRequest;
import com.baidu.bifromq.basekv.proto.PrepareMergeTo;
import com.baidu.bifromq.basekv.proto.PrepareMergeToReply;
import com.baidu.bifromq.basekv.proto.PrepareMergeToRequest;
import com.baidu.bifromq.basekv.proto.PrepareMergeWith;
import com.baidu.bifromq.basekv.proto.Put;
import com.baidu.bifromq.basekv.proto.Range;
import com.baidu.bifromq.basekv.proto.SaveSnapshotDataReply;
import com.baidu.bifromq.basekv.proto.SaveSnapshotDataRequest;
import com.baidu.bifromq.basekv.proto.SnapshotSyncRequest;
import com.baidu.bifromq.basekv.proto.SplitRange;
import com.baidu.bifromq.basekv.proto.State;
import com.baidu.bifromq.basekv.proto.TransferLeadership;
import com.baidu.bifromq.basekv.proto.WALRaftMessages;
import com.baidu.bifromq.basekv.raft.exception.ClusterConfigChangeException;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.raft.proto.LogEntry;
import com.baidu.bifromq.basekv.raft.proto.RaftMessage;
import com.baidu.bifromq.basekv.raft.proto.Snapshot;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProc;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProcFactory;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.exception.KVRangeException;
import com.baidu.bifromq.basekv.store.exception.KVRangeStoreException;
import com.baidu.bifromq.basekv.store.option.KVRangeOptions;
import com.baidu.bifromq.basekv.store.stats.IStatsCollector;
import com.baidu.bifromq.basekv.store.util.AsyncRunner;
import com.baidu.bifromq.basekv.store.util.VerUtil;
import com.baidu.bifromq.basekv.store.wal.IKVRangeWAL;
import com.baidu.bifromq.basekv.store.wal.IKVRangeWALStoreEngine;
import com.baidu.bifromq.basekv.store.wal.IKVRangeWALSubscriber;
import com.baidu.bifromq.basekv.store.wal.IKVRangeWALSubscription;
import com.baidu.bifromq.basekv.store.wal.KVRangeWAL;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.basekv.utils.KeyRangeUtil;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.observers.DisposableObserver;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KVRange implements IKVRange {
    enum Lifecycle {
        Init, // initialized but not open
        Opening,
        Open, // accepting operations, handling incoming messages, generate out-going messages
        Closing,
        Closed, // wait for tick activity stopped
        Destroying,
        Destroyed
    }

    private static final Runnable NOOP = () -> {
    };
    private final KVRangeId id;
    private final String hostStoreId;
    private final IKVRangeSnapshotChecker snapshotChecker;
    private final IKVRangeState accessor;
    private final IKVRangeWAL wal;
    private final IKVRangeWALSubscription commitLogSubscription;
    private final IStatsCollector statsCollector;
    private final Executor fsmExecutor;
    private final Executor bgExecutor;
    private final AsyncRunner mgmtTaskRunner;
    private final IKVRangeCoProc coProc;
    private final KVRangeQueryLinearizer linearizer;
    private final IKVRangeQueryRunner queryRunner;
    private final Map<String, CompletableFuture<?>> cmdFutures = new ConcurrentHashMap<>();
    private final Map<String, KVRangeDumpSession> dumpSessions = Maps.newConcurrentMap();
    private final AtomicInteger taskSeqNo = new AtomicInteger();
    private final Subject<ClusterConfig> clusterConfigSubject = BehaviorSubject.<ClusterConfig>create().toSerialized();
    private final Subject<KVRangeDescriptor> descriptorSubject =
        BehaviorSubject.<KVRangeDescriptor>create().toSerialized();
    private final KVRangeOptions opts;
    private final AtomicReference<Lifecycle> lifecycle = new AtomicReference<>(Lifecycle.Init);
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final CompletableFuture<Void> closeSignal = new CompletableFuture<>();
    private final CompletableFuture<State.StateType> quitReason = new CompletableFuture<>();
    private final CompletableFuture<Void> destroySignal = new CompletableFuture<>();
    private final KVRangeMetricManager metricManager;
    private final ILoadEstimator loadEstimator;
    private final long compactionLingerNanos;
    private IKVRangeMessenger messenger;
    private volatile CompletableFuture<Void> checkWalSizeTask;
    private long lastCompactionDoneAt;

    public KVRange(String clusterId,
                   String hostStoreId,
                   KVRangeId id,
                   IKVRangeCoProcFactory coProcFactory,
                   IKVRangeSnapshotChecker snapshotChecker,
                   IKVEngine rangeEngine,
                   IKVRangeWALStoreEngine walStateStorageEngine,
                   Executor queryExecutor,
                   Executor fsmExecutor,
                   Executor mgmtExecutor,
                   Executor bgExecutor,
                   KVRangeOptions opts,
                   @Nullable Snapshot initSnapshot) {
        try {
            this.opts = opts.toBuilder().build();
            this.id = id;
            this.hostStoreId = hostStoreId; // keep a local copy to decouple it from store's state
            this.compactionLingerNanos = Duration.ofSeconds(opts.getCompactLingerTimeSec()).toNanos();
            loadEstimator = new LoadEstimator(opts.getMaxRangeLoad(),
                opts.getSplitKeyThreshold(),
                opts.getLoadTrackingWindowSec(),
                coProcFactory::toSplitKey);
            if (initSnapshot != null) {
                walStateStorageEngine.destroy(id);
                walStateStorageEngine.newRaftStateStorage(id, initSnapshot);
                this.accessor =
                    new KVRangeState(KVRangeSnapshot.parseFrom(initSnapshot.getData()), rangeEngine, loadEstimator);
            } else {
                this.accessor = new KVRangeState(id, rangeEngine, loadEstimator);
            }
            this.snapshotChecker = snapshotChecker;
            this.wal = new KVRangeWAL(clusterId, id,
                walStateStorageEngine,
                opts.getWalRaftConfig(),
                opts.getMaxWALFatchBatchSize());
            this.fsmExecutor = fsmExecutor;
            this.bgExecutor = bgExecutor;
            this.mgmtTaskRunner = new AsyncRunner(mgmtExecutor);
            this.coProc = coProcFactory.create(id, () -> accessor.getReader(true), loadEstimator);
            this.linearizer = new KVRangeQueryLinearizer(wal::readIndex, queryExecutor);
            this.queryRunner = new KVRangeQueryRunner(accessor, coProc, queryExecutor, linearizer);
            this.commitLogSubscription = wal.subscribe(accessor.getReader(false).lastAppliedIndex(),
                new IKVRangeWALSubscriber() {
                    @Override
                    public CompletableFuture<Void> apply(LogEntry log) {
                        return KVRange.this.apply(log);
                    }

                    @Override
                    public CompletableFuture<Void> apply(KVRangeSnapshot snapshot) {
                        return KVRange.this.apply(snapshot);
                    }
                }, fsmExecutor);
            this.statsCollector = new KVRangeStatsCollector(accessor,
                wal, Duration.ofSeconds(opts.getStatsCollectIntervalSec()), bgExecutor);
            this.metricManager = new KVRangeMetricManager(clusterId, hostStoreId, id);
        } catch (InvalidProtocolBufferException e) {
            throw new KVRangeStoreException("Unable to restore from Snapshot", e);
        }
    }

    @Override
    public KVRangeId id() {
        return id;
    }

    @Override
    public boolean isOccupying(String checkpointId) {
        if (!isWorking()) {
            return false;
        }
        try {
            return checkpointId.equals(wal.latestSnapshot().getCheckpointId()) ||
                dumpSessions.values().stream().anyMatch(s -> s.checkpointId().equals(checkpointId));
        } catch (Throwable e) {
            return false;
        }
    }

    @Override
    public boolean readyToQuit() {
        return quitReason.isDone();
    }

    @Override
    public void open(IKVRangeMessenger messenger) {
        if (lifecycle.get() != Init) {
            return;
        }
        mgmtTaskRunner.add(() -> {
            if (lifecycle.compareAndSet(Init, Lifecycle.Opening)) {
                this.messenger = messenger;
                // start the wal
                wal.start();
                disposables.add(wal.peerMessages().observeOn(Schedulers.io())
                    .subscribe((messages) -> {
                        for (String peerId : messages.keySet()) {
                            messenger.send(KVRangeMessage.newBuilder()
                                .setRangeId(id)
                                .setHostStoreId(peerId)
                                .setWalRaftMessages(WALRaftMessages.newBuilder()
                                    .addAllWalMessages(messages.get(peerId))
                                    .build())
                                .build());
                        }
                    }));
                disposables.add(wal.snapshotRestoreEvent()
                    .subscribe(e -> clusterConfigSubject.onNext(e.snapshot.getClusterConfig())));
                disposables.add(descriptorSubject.subscribe(metricManager::report));
                disposables.add(Observable.combineLatest(
                        accessor.metadata().distinctUntilChanged(),
                        wal.state().distinctUntilChanged(),
                        wal.replicationStatus().distinctUntilChanged(),
                        clusterConfigSubject.distinctUntilChanged(),
                        statsCollector.collect().distinctUntilChanged(),
                        (meta, role, syncStats, clusterConfig, stats) -> {
                            LoadHint loadHint = loadEstimator.estimate();
                            assert 0 <= loadHint.getLoad() && loadHint.getLoad() < 1.0 : "load must be in [0.0, 1.0)";
                            assert !loadHint.hasSplitKey() || KeyRangeUtil.inRange(loadHint.getSplitKey(), meta.range) :
                                "splitKey must be in provided range";
                            return KVRangeDescriptor.newBuilder()
                                .setVer(meta.ver)
                                .setId(id)
                                .setRange(meta.range)
                                .setRole(role)
                                .setState(meta.state.getType())
                                .setConfig(clusterConfig)
                                .putAllSyncState(syncStats)
                                .putAllStatistics(stats)
                                .setLoadHint(loadHint)
                                .setHlc(HLC.INST.get())
                                .build();
                        })
                    .subscribe(descriptorSubject::onNext));
                disposables.add(messenger.receive().subscribe(this::handleMessage));
                // do compaction whenever there is a meta change
                disposables.add(accessor.metadata().subscribe(this::doCompact));
                clusterConfigSubject.onNext(wal.clusterConfig());
                lifecycle.set(Open);
                metricManager.reportLastAppliedIndex(accessor.getReader(false).lastAppliedIndex());
            }
        });
    }

    @Override
    public void tick() {
        if (!isWorking()) {
            return;
        }
        wal.tick();
        statsCollector.tick();
        dumpSessions.values().forEach(KVRangeDumpSession::tick);
        checkWalSize();
    }

    @Override
    public CompletableFuture<Void> close() {
        if (lifecycle.get() == Open) {
            mgmtTaskRunner.add(() -> {
                if (lifecycle.compareAndSet(Open, Lifecycle.Closing)) {
                    log.debug("Closing range[{}] from store[{}]", toShortString(id), hostStoreId);
                    clusterConfigSubject.onComplete();
                    descriptorSubject.onComplete();
                    disposables.dispose();
                    commitLogSubscription.stop();
                    queryRunner.close();
                    coProc.close();
                    cmdFutures.values()
                        .forEach(f -> f.completeExceptionally(new KVRangeException.TryLater("Range closed")));
                    dumpSessions.values().forEach(KVRangeDumpSession::cancel);
                    statsCollector.stop()
                        .thenComposeAsync(v -> mgmtTaskRunner.awaitDone(), bgExecutor)
                        .thenComposeAsync(v -> wal.close(), bgExecutor)
                        .whenCompleteAsync((v, e) -> {
                            metricManager.close();
                            lifecycle.set(Closed);
                            closeSignal.complete(null);
                        }, bgExecutor);
                }
            });
        }
        return closeSignal;
    }

    @Override
    public CompletionStage<Void> destroy() {
        return close().thenCompose(v -> mgmtTaskRunner.add(() -> {
            if (lifecycle.compareAndSet(Closed, Destroying)) {
                log.debug("Destroy range[{}] from store[{}]", toShortString(id), hostStoreId);
                accessor.destroy(true);
                wal.destroy();
                lifecycle.set(Destroyed);
                destroySignal.complete(null);
            }
            return destroySignal;
        }));
    }

    @Override
    public CompletableFuture<Void> quit() {
        if (!readyToQuit()) {
            return CompletableFuture.failedFuture(new KVRangeException.InternalException("Not ready to quit"));
        }
        return close().thenCompose(v -> mgmtTaskRunner.add(() -> {
            if (lifecycle.compareAndSet(Closed, Destroying)) {
                if (Objects.requireNonNull(quitReason.join()) == Purged) {
                    log.debug("Quit by purging range[{}] from store[{}]",
                        toShortString(id), hostStoreId);
                    accessor.destroy(true);
                    wal.destroy();
                } else {
                    log.debug("Quit by removing range[{}] from store[{}]",
                        toShortString(id), hostStoreId);
                    accessor.destroy(false);
                    wal.destroy();
                }
                lifecycle.set(Destroyed);
                destroySignal.complete(null);
            }
            return destroySignal;
        }));
    }

    @Override
    public CompletableFuture<Void> recover() {
        return wal.recover();
    }

    @Override
    public CompletionStage<Void> transferLeadership(long ver, String newLeader) {
        return metricManager.recordTransferLeader(() -> submitCommand(KVRangeCommand.newBuilder()
            .setTaskId(nextTaskId())
            .setVer(ver)
            .setTransferLeadership(TransferLeadership.newBuilder()
                .setNewLeader(newLeader)
                .build())
            .build()));
    }

    @Override
    public CompletionStage<Void> changeReplicaConfig(long ver, Set<String> newVoters, Set<String> newLearners) {
        return changeReplicaConfig(nextTaskId(), ver, newVoters, newLearners);
    }

    private CompletionStage<Void> changeReplicaConfig(String taskId, long ver,
                                                      Set<String> newVoters,
                                                      Set<String> newLearners) {
        return metricManager.recordConfigChange(() -> submitCommand(KVRangeCommand.newBuilder()
            .setTaskId(taskId)
            .setVer(ver)
            .setChangeConfig(ChangeConfig.newBuilder()
                .addAllVoters(newVoters)
                .addAllLearners(newLearners)
                .build())
            .build()));
    }

    @Override
    public CompletionStage<Void> split(long ver, ByteString splitKey) {
        return metricManager.recordSplit(() -> submitCommand(KVRangeCommand.newBuilder()
            .setTaskId(nextTaskId())
            .setVer(ver)
            .setSplitRange(SplitRange.newBuilder()
                .setSplitKey(splitKey)
                .setNewId(KVRangeIdUtil.next(id))
                .build())
            .build()));
    }

    @Override
    public CompletionStage<Void> merge(long ver, KVRangeId mergeeId) {
        return metricManager.recordMerge(() -> submitCommand(KVRangeCommand.newBuilder()
            .setTaskId(nextTaskId())
            .setVer(ver)
            .setPrepareMergeWith(PrepareMergeWith.newBuilder()
                .setMergeeId(mergeeId)
                .buildPartial())
            .build()));
    }

    @Override
    public CompletionStage<Boolean> exist(long ver, ByteString key, boolean linearized) {
        if (!isWorking()) {
            // treat un-opened range as not exist
            return CompletableFuture.failedFuture(
                new KVRangeException.InternalException("Range not open:" + KVRangeIdUtil.toString(id)));
        }
        return metricManager.recordExist(() -> queryRunner.exist(ver, key, linearized));
    }

    @Override
    public CompletionStage<Optional<ByteString>> get(long ver, ByteString key, boolean linearized) {
        if (!isWorking()) {
            // treat un-opened range as not exist
            return CompletableFuture.failedFuture(
                new KVRangeException.InternalException("Range not open:" + KVRangeIdUtil.toString(id)));
        }
        return metricManager.recordGet(() -> queryRunner.get(ver, key, linearized));
    }

    @Override
    public CompletionStage<ByteString> queryCoProc(long ver, ByteString query, boolean linearized) {
        if (!isWorking()) {
            // treat un-opened range as not exist
            return CompletableFuture.failedFuture(
                new KVRangeException.InternalException("Range not open:" + KVRangeIdUtil.toString(id)));
        }
        return metricManager.recordQueryCoProc(() -> queryRunner.queryCoProc(ver, query, linearized));
    }

    @Override
    public CompletionStage<ByteString> put(long ver, ByteString key, ByteString value) {
        return metricManager.recordPut(() -> submitCommand(KVRangeCommand.newBuilder()
            .setVer(ver)
            .setTaskId(nextTaskId())
            .setPut(Put.newBuilder().setKey(key).setValue(value).build())
            .build()));
    }

    @Override
    public CompletionStage<ByteString> delete(long ver, ByteString key) {
        return metricManager.recordDelete(() -> submitCommand(KVRangeCommand.newBuilder()
            .setVer(ver)
            .setTaskId(nextTaskId())
            .setDelete(Delete.newBuilder().setKey(key).build())
            .build()));
    }

    @Override
    public CompletionStage<ByteString> mutateCoProc(long ver, ByteString mutate) {
        return metricManager.recordMutateCoProc(() -> submitCommand(KVRangeCommand.newBuilder()
            .setVer(ver)
            .setTaskId(nextTaskId())
            .setRwCoProc(mutate)
            .build()));
    }

    @Override
    public Observable<KVRangeDescriptor> describe() {
        return descriptorSubject;
    }

    private String nextTaskId() {
        return hostStoreId + "-" + taskSeqNo.getAndIncrement();
    }

    @SuppressWarnings("unchecked")
    private <T> CompletableFuture<T> submitCommand(KVRangeCommand command) {
        if (!isWorking()) {
            return CompletableFuture.failedFuture(
                new KVRangeException.InternalException("Range not open:" + KVRangeIdUtil.toString(id)));
        }
        CompletableFuture<T> onDone = new CompletableFuture<>();

        // add to writeRequests must happen before proposing
        CompletableFuture<T> prev = (CompletableFuture<T>) cmdFutures.put(command.getTaskId(), onDone);
        assert prev == null;

        CompletableFuture<Long> proposeFuture = wal.propose(command);
        onDone.whenComplete((v, e) -> {
            cmdFutures.remove(command.getTaskId(), onDone);
            if (onDone.isCancelled()) {
                // canceled by caller, stop proposing as well
                proposeFuture.cancel(true);
            }
        });
        proposeFuture.whenCompleteAsync((r, e) -> {
            if (e != null) {
                onDone.completeExceptionally(e);
            }
        }, fsmExecutor);
        return onDone;
    }

    private void finishCommand(String taskId) {
        finishCommand(taskId, null);
    }

    @SuppressWarnings("unchecked")
    private <T> void finishCommand(String taskId, T result) {
        CompletableFuture<T> f = (CompletableFuture<T>) cmdFutures.get(taskId);
        if (f != null) {
            log.trace("Finish write request: taskId={}", taskId);
            f.complete(result);
        }
    }

    private void finishCommandWithError(String taskId, Throwable e) {
        CompletableFuture<?> f = cmdFutures.get(taskId);
        if (f != null) {
            log.trace("Finish write request with error: taskId={}", taskId, e);
            f.completeExceptionally(e);
        }
    }

    private CompletableFuture<Void> apply(LogEntry entry) {
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        IKVRangeWriter rangeWriter = accessor.getWriter(true);
        IKVRangeReader rangeReader = accessor.borrow();
        applyLog(entry, rangeReader, rangeWriter)
            .whenComplete((callback, e) -> {
                if (onDone.isCancelled()) {
                    rangeWriter.abort();
                } else {
                    try {
                        if (e != null) {
                            rangeWriter.abort();
                            onDone.completeExceptionally(e);
                        } else {
                            rangeWriter.setLastAppliedIndex(entry.getIndex());
                            rangeWriter.close();
                            callback.run();
                            linearizer.afterLogApplied(entry.getIndex());
                            metricManager.reportLastAppliedIndex(entry.getIndex());
                            onDone.complete(null);
                        }
                    } catch (Throwable t) {
                        log.error("Failed to apply log", t);
                        onDone.completeExceptionally(t);
                    }
                }
                accessor.returnBorrowed(rangeReader);
            });
        return onDone;
    }

    private CompletableFuture<Void> apply(KVRangeSnapshot ss) {
        if (!isWorking()) {
            return CompletableFuture.failedFuture(
                new KVRangeException.InternalException("Range not open:" + KVRangeIdUtil.toString(id)));
        }
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        snapshotChecker.check(ss)
            .toCompletableFuture()
            .whenCompleteAsync((v, e) -> {
                if (onDone.isCancelled()) {
                    return;
                }
                if (e != null) {
                    onDone.completeExceptionally(e);
                    return;
                }
                IKVRangeRestorer restorer = accessor.reset(ss);
                String sessionId = UUID.randomUUID().toString();
                DisposableObserver<KVRangeMessage> observer = messenger.receive()
                    .filter(m -> m.hasSaveSnapshotDataRequest() &&
                        m.getSaveSnapshotDataRequest().getSessionId().equals(sessionId))
                    .timeout(opts.getSnapshotSyncIdleTimeoutSec(), TimeUnit.SECONDS)
                    .subscribeWith(new DisposableObserver<KVRangeMessage>() {
                        @Override
                        public void onNext(@NonNull KVRangeMessage m) {
                            SaveSnapshotDataRequest request = m.getSaveSnapshotDataRequest();
                            try {
                                switch (request.getFlag()) {
                                    case More, End -> {
                                        int bytes = 0;
                                        for (KVPair kv : request.getKvList()) {
                                            bytes += kv.getKey().size();
                                            bytes += kv.getValue().size();
                                            restorer.add(kv);
                                        }
                                        metricManager.reportRestore(bytes);
                                        if (request.getFlag() == SaveSnapshotDataRequest.Flag.End) {
                                            restorer.close();
                                            linearizer.afterLogApplied(ss.getLastAppliedIndex());
                                            // finish all pending tasks
                                            cmdFutures.keySet().forEach(taskId -> finishCommandWithError(taskId,
                                                new KVRangeException.TryLater("Snapshot installed, try again")));
                                            dispose();
                                            onDone.complete(null);
                                            log.debug("Snapshot installed: rangeId={}, storeId={}, sessionId={}",
                                                toShortString(id), hostStoreId, sessionId);
                                        }
                                        messenger.send(KVRangeMessage.newBuilder()
                                            .setRangeId(id)
                                            .setHostStoreId(m.getHostStoreId())
                                            .setSaveSnapshotDataReply(SaveSnapshotDataReply.newBuilder()
                                                .setReqId(request.getReqId())
                                                .setSessionId(request.getSessionId())
                                                .setResult(SaveSnapshotDataReply.Result.OK)
                                                .build())
                                            .build());
                                    }
                                    case Error -> throw new KVRangeStoreException("Request Snapshot Failed");
                                }
                            } catch (Throwable t) {
                                log.error("Snapshot restored failed:sessionId={}", sessionId, t);
                                onError(t);
                                messenger.send(KVRangeMessage.newBuilder()
                                    .setRangeId(id)
                                    .setHostStoreId(m.getHostStoreId())
                                    .setSaveSnapshotDataReply(SaveSnapshotDataReply.newBuilder()
                                        .setReqId(request.getReqId())
                                        .setSessionId(request.getSessionId())
                                        .setResult(SaveSnapshotDataReply.Result.Error)
                                        .build())
                                    .build());
                            }
                        }

                        @Override
                        public void onError(@NonNull Throwable e) {
                            restorer.abort();
                            onDone.completeExceptionally(e);
                            dispose();
                        }

                        @Override
                        public void onComplete() {

                        }
                    });
                onDone.whenComplete((_v, _e) -> {
                    if (onDone.isCancelled()) {
                        observer.dispose();
                    }
                });
                if (!onDone.isDone()) {
                    messenger.send(KVRangeMessage.newBuilder()
                        .setRangeId(id)
                        .setHostStoreId(wal.currentLeader().get())
                        .setSnapshotSyncRequest(SnapshotSyncRequest.newBuilder()
                            .setSessionId(sessionId)
                            .setSnapshot(ss)
                            .build())
                        .build());
                }
            }, fsmExecutor);
        return onDone;
    }

    private CompletableFuture<Runnable> applyLog(LogEntry logEntry,
                                                 IKVRangeReader rangeReader,
                                                 IKVRangeWriter rangeWriter) {
        CompletableFuture<Runnable> onDone = new CompletableFuture<>();
        long ver = rangeReader.ver();
        State state = rangeReader.state();
        Range range = rangeReader.kvReader().range();
        switch (logEntry.getTypeCase()) {
            case DATA -> {
                try {
                    KVRangeCommand command = KVRangeCommand.parseFrom(logEntry.getData());
                    long reqVer = command.getVer();
                    String taskId = command.getTaskId();
                    log.trace(
                        "Execute KVRange Command[term={}, index={}]: rangeId={}, storeId={}, ver={}, state={}, \n{}",
                        logEntry.getTerm(), logEntry.getIndex(), toShortString(id), hostStoreId, ver,
                        state, command);
                    switch (command.getCommandTypeCase()) {
                        // admin commands
                        case CHANGECONFIG -> {
                            ChangeConfig request = command.getChangeConfig();
                            if (reqVer != ver) {
                                // version not match, hint the caller
                                onDone.complete(
                                    () -> finishCommandWithError(taskId,
                                        new KVRangeException.BadVersion("Version Mismatch")));
                                break;
                            }
                            if (state.getType() != Normal && state.getType() != Merged) {
                                onDone.complete(() -> finishCommandWithError(taskId, new KVRangeException.TryLater(
                                    "Transfer leader abort, range is in state:" + state.getType().name())));
                                break;
                            }
                            // notify new voters and learners host store to ensure the range exist
                            Set<String> newHostingStoreIds =
                                difference(
                                    difference(
                                        union(newHashSet(request.getVotersList()),
                                            newHashSet(request.getLearnersList())),
                                        union(newHashSet(wal.clusterConfig().getVotersList()),
                                            newHashSet(wal.clusterConfig().getLearnersList()))
                                    ),
                                    singleton(hostStoreId)
                                );
                            List<CompletableFuture<?>> onceFutures = newHostingStoreIds.stream()
                                .map(storeId -> messenger.once(m -> {
                                    if (m.hasEnsureRangeReply()) {
                                        EnsureRangeReply reply = m.getEnsureRangeReply();
                                        return reply.getResult() == EnsureRangeReply.Result.OK;
                                    }
                                    return false;
                                })).collect(Collectors.toList());
                            CompletableFuture.allOf(onceFutures.toArray(new CompletableFuture[] {}))
                                .orTimeout(5, TimeUnit.SECONDS)
                                .whenCompleteAsync((_v, _e) -> {
                                    if (_e != null) {
                                        onceFutures.forEach(f -> f.cancel(true));
                                    }
                                    wal.changeClusterConfig(taskId,
                                            newHashSet(request.getVotersList()),
                                            newHashSet(request.getLearnersList()))
                                        .whenCompleteAsync((v, e) -> {
                                            if (e != null) {
                                                String errorMessage =
                                                    String.format("Config change aborted[taskId=%s] due to %s", taskId,
                                                        e.getMessage());
                                                log.debug(errorMessage);
                                                if (e.getClass() !=
                                                    ClusterConfigChangeException.NotLeaderException.class &&
                                                    e.getCause().getClass() !=
                                                        ClusterConfigChangeException.NotLeaderException.class) {
                                                    finishCommandWithError(taskId,
                                                        new KVRangeException.TryLater(errorMessage));
                                                }
                                                wal.stepDown();
                                            }
                                            if (state.getType() == Normal) {
                                                // only transit to ConfigChanging from Normal
                                                rangeWriter.setState(State.newBuilder()
                                                    .setType(ConfigChanging)
                                                    .setTaskId(taskId)
                                                    .build());
                                            } else if (state.getType() == Merged) {
                                                rangeWriter.setState(State.newBuilder()
                                                    .setType(MergedQuiting)
                                                    .setTaskId(taskId)
                                                    .build());
                                            }
                                            rangeWriter.bumpVer(false);
                                            onDone.complete(NOOP);
                                        }, fsmExecutor);
                                });
                            newHostingStoreIds.forEach(storeId -> {
                                log.debug("Send range ensure request to store[{}]: rangeId={}",
                                    storeId, toShortString(id));
                                messenger.send(KVRangeMessage.newBuilder()
                                    .setRangeId(id)
                                    .setHostStoreId(storeId)
                                    .setEnsureRange(EnsureRange.newBuilder()
                                        .setInitSnapshot(Snapshot.newBuilder()
                                            .setClusterConfig(ClusterConfig.getDefaultInstance())
                                            .setTerm(0)
                                            .setIndex(0)
                                            .setData(KVRangeSnapshot.newBuilder()
                                                .setVer(0)
                                                .setId(id)
                                                .setLastAppliedIndex(0)
                                                .setRange(EMPTY_RANGE)
                                                .setState(State.newBuilder().setType(Normal).build())
                                                .build().toByteString())
                                            .build())
                                        .build())
                                    .build());
                            });
                        }
                        case TRANSFERLEADERSHIP -> {
                            TransferLeadership request = command.getTransferLeadership();
                            if (reqVer != ver) {
                                // version not match, hint the caller
                                onDone.complete(
                                    () -> finishCommandWithError(taskId,
                                        new KVRangeException.BadVersion("Version Mismatch")));
                                break;
                            }
                            if (state.getType() != Normal) {
                                onDone.complete(() -> finishCommandWithError(taskId,
                                    new KVRangeException.TryLater(
                                        "Transfer leader abort, range is in state:" + state.getType().name())));
                                break;
                            }
                            if (!wal.clusterConfig().getVotersList().contains(request.getNewLeader()) &&
                                !wal.clusterConfig().getNextVotersList().contains(request.getNewLeader())) {
                                onDone.complete(
                                    () -> finishCommandWithError(taskId,
                                        new KVRangeException.BadRequest("Invalid Leader")));
                                break;
                            }
                            wal.transferLeadership(request.getNewLeader())
                                .whenCompleteAsync((v, e) -> {
                                    if (e != null) {
                                        log.debug("Failed to transfer leadership[newLeader={}] due to {}",
                                            request.getNewLeader(), e.getMessage());
                                        onDone.complete(
                                            () -> finishCommandWithError(taskId, new KVRangeException.TryLater(
                                                "Failed to transfer leadership", e)));
                                    } else {
                                        onDone.complete(() -> finishCommand(taskId));
                                    }
                                }, fsmExecutor);
                        }
                        case SPLITRANGE -> {
                            SplitRange request = command.getSplitRange();
                            if (reqVer != ver) {
                                // version not match, hint the caller
                                onDone.complete(
                                    () -> finishCommandWithError(taskId,
                                        new KVRangeException.BadVersion("Version Mismatch")));
                                break;
                            }
                            if (state.getType() != Normal) {
                                onDone.complete(() -> finishCommandWithError(taskId,
                                    new KVRangeException.TryLater(
                                        "Split abort, range is in state:" + state.getType().name())));
                                break;
                            }
                            // under at-least-once semantic, we need to check if the splitKey is still valid to skip
                            // duplicated apply
                            if (KeyRangeUtil.inRange(request.getSplitKey(), range)) {
                                messenger.once(m -> {
                                    if (m.hasEnsureRangeReply()) {
                                        EnsureRangeReply reply = m.getEnsureRangeReply();
                                        return reply.getResult() == EnsureRangeReply.Result.OK;
                                    }
                                    return false;
                                }).whenCompleteAsync((v, e) -> {
                                    if (e != null) {
                                        onDone.completeExceptionally(e);
                                    } else {
                                        log.debug("Split KVRange[{}] created: rangeId={}, storeId={}",
                                            toShortString(request.getNewId()), toShortString(id), hostStoreId);
                                        rangeWriter.setRange(
                                            range.toBuilder().setEndKey(request.getSplitKey()).build());
                                        // bump to odd version to indicate there is a range change
                                        rangeWriter.bumpVer(true);
                                        onDone.complete(() -> finishCommand(taskId));
                                    }
                                }, fsmExecutor);
                                log.debug("Sending ensure request to create split KVRange[{}]: rangeId={}, storeId={}",
                                    toShortString(request.getNewId()), toShortString(id), hostStoreId);
                                // right-hand side KVRange's initial snapshot
                                KVRangeSnapshot rhsSS = KVRangeSnapshot.newBuilder()
                                    .setVer(ver + 1)
                                    .setId(request.getNewId())
                                    .setCheckpointId(accessor.checkpoint().getCheckpointId())
                                    .setLastAppliedIndex(5)
                                    .setRange(range.toBuilder()
                                        .setStartKey(request.getSplitKey())
                                        .build())
                                    .setState(State.newBuilder()
                                        .setType(Normal)
                                        .setTaskId(taskId)
                                        .build())
                                    .build();
                                messenger.send(
                                    KVRangeMessage.newBuilder()
                                        .setRangeId(rhsSS.getId())
                                        .setHostStoreId(hostStoreId)
                                        .setEnsureRange(EnsureRange.newBuilder()
                                            .setInitSnapshot(Snapshot.newBuilder()
                                                .setTerm(0)
                                                .setIndex(5)
                                                .setClusterConfig(wal.clusterConfig())
                                                .setData(rhsSS.toByteString())
                                                .build())
                                            .build()).build());
                            } else {
                                onDone.complete(
                                    () -> finishCommandWithError(taskId,
                                        new KVRangeException.BadRequest("Invalid split key")));
                            }
                        }
                        case PREPAREMERGEWITH -> {
                            PrepareMergeWith request = command.getPrepareMergeWith();
                            if (reqVer != ver) {
                                // version not match, hint the caller
                                onDone.complete(
                                    () -> finishCommandWithError(taskId,
                                        new KVRangeException.BadVersion("Version Mismatch")));
                                break;
                            }
                            if (state.getType() != Normal) {
                                onDone.complete(() -> finishCommandWithError(taskId, new KVRangeException.TryLater(
                                    "Merge abort, range is in state:" + state.getType().name())));
                                break;
                            }

                            CompletableFuture<KVRangeMessage> onceFuture =
                                messenger.once(m -> m.hasPrepareMergeToReply() &&
                                    m.getPrepareMergeToReply().getTaskId().equals(taskId) &&
                                    m.getPrepareMergeToReply().getAccept());
                            onDone.whenCompleteAsync((v, e) -> {
                                if (onDone.isCancelled()) {
                                    onceFuture.cancel(true);
                                }
                            }, fsmExecutor);
                            onceFuture.orTimeout(5, TimeUnit.SECONDS)
                                .whenCompleteAsync((v, e) -> {
                                    if (e != null) {
                                        onceFuture.cancel(true);
                                        onDone.completeExceptionally(e);
                                    } else {
                                        ClusterConfig config = wal.clusterConfig();
                                        Map<String, Boolean> waitingList = config.getVotersList().stream()
                                            .collect(Collectors.toMap(voter -> voter, voter -> false));
                                        rangeWriter.setState(State.newBuilder()
                                            .setType(PreparedMerging)
                                            .setTaskId(taskId)
                                            .putAllWaitingList(waitingList)
                                            .build());
                                        rangeWriter.bumpVer(false);
                                        onDone.complete(NOOP);
                                    }
                                }, fsmExecutor);
                            // broadcast
                            messenger.send(KVRangeMessage.newBuilder()
                                .setRangeId(request.getMergeeId())
                                .setPrepareMergeToRequest(PrepareMergeToRequest.newBuilder()
                                    .setTaskId(taskId)
                                    .setId(id)
                                    .setVer(VerUtil.bump(ver, false))
                                    .setRange(range)
                                    .setConfig(wal.clusterConfig())
                                    .build())
                                .build());
                        }
                        case CANCELMERGING -> {
                            if (reqVer != ver) {
                                onDone.complete(NOOP);
                                break;
                            }
                            assert state.getType() == PreparedMerging && state.hasTaskId() &&
                                taskId.equals(state.getTaskId());
                            rangeWriter.setState(State.newBuilder()
                                .setType(Normal)
                                .setTaskId(taskId)
                                .build());
                            rangeWriter.bumpVer(false);
                            onDone.complete(
                                () -> finishCommandWithError(taskId, new KVRangeException.TryLater("Merge canceled")));
                        }
                        case PREPAREMERGETO -> {
                            PrepareMergeTo request = command.getPrepareMergeTo();
                            if (reqVer != ver) {
                                onDone.complete(NOOP);
                                break;
                            }
                            // here is the formal mergeable condition check
                            if (state.getType() != Normal ||
                                !isCompatible(request.getConfig(), wal.clusterConfig()) ||
                                !KeyRangeUtil.canCombine(request.getRange(), range)) {
                                if (!taskId.equals(state.getTaskId())) {
                                    log.debug("Cancel the loser merger[{}]", toShortString(request.getMergerId()));
                                    // help the loser merger cancel its operation by broadcast CancelMerging
                                    // via store message and wait for at lease one response to make sure
                                    // the loser merger has got canceled
                                    log.debug("Loser merger[{}] not found in local store",
                                        toShortString(request.getMergerId()));
                                    CompletableFuture<KVRangeMessage> onceFuture = messenger.once(m ->
                                        m.hasCancelMergingReply() &&
                                            m.getCancelMergingReply().getTaskId().equals(taskId) &&
                                            m.getCancelMergingReply().getAccept());
                                    // cancel the future
                                    onDone.whenCompleteAsync((v, e) -> {
                                        if (onceFuture.isCancelled()) {
                                            onceFuture.cancel(true);
                                        }
                                    }, fsmExecutor);
                                    onceFuture.orTimeout(5, TimeUnit.SECONDS)
                                        .whenCompleteAsync((v, e) -> {
                                            if (e != null) {
                                                onDone.completeExceptionally(e);
                                            } else {
                                                onDone.complete(NOOP);
                                            }
                                        }, fsmExecutor);
                                    // broadcast
                                    messenger.send(KVRangeMessage.newBuilder()
                                        .setRangeId(request.getMergerId())
                                        .setCancelMergingRequest(CancelMergingRequest.newBuilder()
                                            .setTaskId(taskId)
                                            .setVer(request.getMergerVer())
                                            .setRequester(id)
                                            .build()).build());
                                } else {
                                    // ignore duplicated requests from same merger
                                    onDone.complete(NOOP);
                                }
                                break;
                            }
                            // tell merger replica to start merge by broadcast Merge command which may end up
                            // multiple Merge commands targeting to same merger replica
                            CompletableFuture<KVRangeMessage> onceFuture = messenger.once(m ->
                                m.hasMergeReply() &&
                                    m.getMergeReply().getTaskId().equals(taskId) &&
                                    m.getMergeReply().getAccept());
                            // cancel the future
                            onDone.whenCompleteAsync((v, e) -> {
                                if (onDone.isCancelled()) {
                                    onceFuture.cancel(true);
                                }
                            }, fsmExecutor);
                            onceFuture.orTimeout(5, TimeUnit.SECONDS)
                                .whenCompleteAsync((v, e) -> {
                                    if (e != null) {
                                        onceFuture.cancel(true);
                                        onDone.completeExceptionally(e);
                                    } else {
                                        rangeWriter.setState(State.newBuilder()
                                            .setType(WaitingForMerge)
                                            .setTaskId(taskId)
                                            .build());
                                        rangeWriter.bumpVer(false);
                                        onDone.complete(NOOP);
                                    }
                                }, fsmExecutor);
                            // broadcast
                            messenger.send(KVRangeMessage.newBuilder()
                                .setRangeId(request.getMergerId())
                                .setMergeRequest(MergeRequest.newBuilder()
                                    .setTaskId(taskId)
                                    .setVer(request.getMergerVer())
                                    .setMergeeId(id)
                                    .setMergeeVer(VerUtil.bump(ver, false))
                                    .setRange(range)
                                    .setStoreId(hostStoreId)
                                    .build())
                                .build());
                        }
                        case MERGE -> {
                            Merge request = command.getMerge();
                            if (reqVer != ver) {
                                onDone.complete(NOOP);
                                break;
                            }
                            assert state.getType() == PreparedMerging;
                            Map<String, Boolean> waitingList = Maps.newHashMap(state.getWaitingListMap());
                            waitingList.put(request.getStoreId(), true);
                            int quorumSize = waitingList.size() / 2 + 1;
                            int readyNum = Maps.filterValues(waitingList, v -> v).size();
                            if (readyNum < quorumSize) {
                                // not ready for merge
                                // update waiting list only
                                rangeWriter.setState(state.toBuilder()
                                    .clearWaitingList()
                                    .putAllWaitingList(waitingList)
                                    .build());
                                onDone.complete(NOOP);
                                break;
                            }
                            CompletableFuture<LogEntry> readyToMerge;
                            if (readyNum == quorumSize && waitingList.get(hostStoreId)) {
                                readyToMerge = CompletableFuture.completedFuture(logEntry);
                            } else {
                                // waiting for Merge command from local mergee committed in merger's WAL
                                readyToMerge = wal.once(logEntry.getIndex(), l -> {
                                    if (l.hasData()) {
                                        try {
                                            KVRangeCommand nextCmd = KVRangeCommand.parseFrom(l.getData());
                                            if (nextCmd.hasMerge() &&
                                                nextCmd.getMerge().getStoreId().equals(hostStoreId)) {
                                                return true;
                                            }
                                        } catch (InvalidProtocolBufferException ex) {
                                            throw new KVRangeException.InternalException("Unable to parse logEntry",
                                                ex);
                                        }
                                    }
                                    return false;
                                }, fsmExecutor);
                                // cancel the future
                                onDone.whenCompleteAsync((v, e) -> {
                                    if (onDone.isCancelled()) {
                                        readyToMerge.cancel(true);
                                    }
                                }, fsmExecutor);
                            }
                            readyToMerge.whenCompleteAsync((_v, _e) -> {
                                if (_e != null) {
                                    onDone.completeExceptionally(
                                        new KVRangeException.TryLater("Merge condition not met"));
                                    return;
                                }
                                CompletableFuture<KVRangeMessage> onceFuture = messenger.once(m ->
                                    m.hasMergeDoneReply() &&
                                        m.getMergeDoneReply().getTaskId().equals(taskId));
                                // cancel the future
                                onDone.whenCompleteAsync((v, e) -> {
                                    if (onDone.isCancelled()) {
                                        onceFuture.cancel(true);
                                    }
                                }, fsmExecutor);
                                onceFuture.orTimeout(5, TimeUnit.SECONDS)
                                    .whenCompleteAsync((v, e) -> {
                                        if (e != null || !v.getMergeDoneReply().getAccept()) {
                                            log.debug("Failed to send MergeDone request: rangeId={}, storeId={}",
                                                toShortString(id), hostStoreId);
                                            onceFuture.cancel(true);
                                            onDone.completeExceptionally(
                                                e != null ? e : new KVRangeException.InternalException(
                                                    "Failed to send MergeDone request"));
                                        } else {
                                            rangeWriter.setRange(KeyRangeUtil.combine(range, request.getRange()));
                                            rangeWriter.setState(State.newBuilder()
                                                .setType(Normal)
                                                .setTaskId(taskId)
                                                .build());
                                            long newVer = Math.max(ver, request.getMergeeVer()) + 2;
                                            // make sure the version is odd
                                            rangeWriter.resetVer(VerUtil.bump(newVer, true));
                                            onDone.complete(() -> finishCommand(taskId));
                                        }
                                    }, fsmExecutor);
                                // send to local mergee
                                log.debug("Send MergeDone request to Mergee[{}]: rangeId={}, storeId={}",
                                    toShortString(id), hostStoreId, toShortString(request.getMergeeId()));
                                messenger.send(KVRangeMessage.newBuilder()
                                    .setRangeId(request.getMergeeId())
                                    .setHostStoreId(hostStoreId)
                                    .setMergeDoneRequest(MergeDoneRequest.newBuilder()
                                        .setId(id)
                                        .setTaskId(taskId)
                                        .setMergeeVer(request.getMergeeVer())
                                        .setStoreId(hostStoreId)
                                        .build())
                                    .build());
                            }, fsmExecutor);
                        }
                        case MERGEDONE -> {
                            MergeDone request = command.getMergeDone();
                            if (reqVer != ver) {
                                onDone.complete(NOOP);
                                break;
                            }
                            if (request.getStoreId().equals(hostStoreId)) {
                                assert state.getType() == WaitingForMerge
                                    && state.hasTaskId()
                                    && taskId.equals(state.getTaskId());
                                rangeWriter.setRange(EMPTY_RANGE);
                                rangeWriter.bumpVer(true);
                                rangeWriter.setState(State.newBuilder()
                                    .setType(Merged)
                                    .setTaskId(taskId)
                                    .build());
                                onDone.complete(NOOP);
                            } else {
                                Map<String, Boolean> waitingList = Maps.newHashMap(state.getWaitingListMap());
                                waitingList.put(request.getStoreId(), true);
                                rangeWriter.setState(state.toBuilder()
                                    .clearWaitingList()
                                    .putAllWaitingList(waitingList)
                                    .build());
                                onDone.complete(NOOP);
                            }
                        }
                        case PUT, DELETE, RWCOPROC -> {
                            if (reqVer != ver) {
                                onDone.complete(
                                    () -> finishCommandWithError(taskId,
                                        new KVRangeException.BadVersion("Version Mismatch")));
                                break;
                            }
                            if (state.getType() == WaitingForMerge
                                || state.getType() == Merged
                                || state.getType() == Removed
                                || state.getType() == Purged) {
                                onDone.complete(() -> finishCommandWithError(taskId,
                                    new KVRangeException.BadRequest("Range is in state:" + state.getType().name())));
                                break;
                            }
                            try {
                                switch (command.getCommandTypeCase()) {
                                    // normal commands
                                    case DELETE -> {
                                        Delete delete = command.getDelete();
                                        Optional<ByteString> value = rangeReader.kvReader().get(delete.getKey());
                                        if (value.isPresent()) {
                                            rangeWriter.kvWriter().delete(delete.getKey());
                                        }
                                        onDone.complete(() -> finishCommand(taskId, value.orElse(ByteString.EMPTY)));
                                    }
                                    case PUT -> {
                                        Put put = command.getPut();
                                        Optional<ByteString> value = rangeReader.kvReader().get(put.getKey());
                                        rangeWriter.kvWriter().put(put.getKey(), put.getValue());
                                        onDone.complete(() -> finishCommand(taskId, value.orElse(ByteString.EMPTY)));
                                    }
                                    case RWCOPROC -> {
                                        Supplier<ByteString> outputSupplier = coProc.mutate(command.getRwCoProc(),
                                            rangeReader.kvReader(),
                                            rangeWriter.kvWriter());
                                        onDone.complete(() -> finishCommand(taskId, outputSupplier.get()));
                                    }
                                }
                            } catch (Throwable e) {
                                onDone.complete(
                                    () -> finishCommandWithError(taskId, new KVRangeException.InternalException(
                                        "Failed to execute " + command.getCommandTypeCase().name(), e)));
                            }
                        }
                        default -> {
                            log.error("Unknown KVRange Command[type={}]", command.getCommandTypeCase());
                            onDone.complete(NOOP);
                        }
                    }
                } catch (Throwable t) {
                    log.error("Unable to execute KVRange Command", t);
                    onDone.completeExceptionally(t);
                }
            }
            case CONFIG -> {
                ClusterConfig config = logEntry.getConfig();
                log.debug(
                    "Apply new config[term={}, index={}]: rangeId={}, storeId={}, state={}, leader={}\n{}",
                    logEntry.getTerm(), logEntry.getIndex(), toShortString(id), hostStoreId, state,
                    wal.isLeader(), config);
                if (config.getNextVotersCount() != 0 || config.getNextLearnersCount() != 0) {
                    // skip joint-config
                    onDone.complete(() -> clusterConfigSubject.onNext(config));
                    break;
                }
                Set<String> members = newHashSet();
                members.addAll(config.getVotersList());
                members.addAll(config.getLearnersList());
                switch (state.getType()) {
                    case ConfigChanging -> {
                        // reset back to normal
                        String taskId = state.getTaskId();
                        rangeWriter.setState(State.newBuilder()
                            .setType(Normal)
                            .setTaskId(taskId)
                            .build());
                        rangeWriter.bumpVer(false);
                        if (taskId.equals(config.getCorrelateId())) {
                            // if the config entry is resulted from user request, and removed from replica set,
                            // put it to the state of Removed
                            boolean remove = !members.contains(hostStoreId);
                            if (remove) {
                                log.debug("Replica removed[newConfig={}]", config);
                                rangeWriter.setState(State.newBuilder()
                                    .setType(Purged)
                                    .setTaskId(taskId)
                                    .build());
                            }
                            onDone.complete(() -> {
                                clusterConfigSubject.onNext(config);
                                if (remove) {
                                    quitReason.complete(Purged);
                                }
                                finishCommand(taskId);
                            });
                        } else {
                            onDone.complete(() -> {
                                clusterConfigSubject.onNext(config);
                                finishCommandWithError(taskId, new KVRangeException.TryLater("Retry config change"));
                            });
                        }
                    }
                    case MergedQuiting -> {
                        String taskId = state.getTaskId();
                        // has been removed from config or the only member in the config
                        boolean remove = !members.contains(hostStoreId) || singleton(hostStoreId).containsAll(members);
                        if (remove) {
                            rangeWriter.setState(State.newBuilder()
                                .setType(Removed)
                                .setTaskId(taskId)
                                .build());
                        } else {
                            rangeWriter.setState(State.newBuilder()
                                .setType(Merged)
                                .setTaskId(taskId)
                                .build());
                        }
                        rangeWriter.bumpVer(false);
                        onDone.complete(() -> {
                            clusterConfigSubject.onNext(config);
                            if (remove) {
                                quitReason.complete(Removed);
                            }
                            finishCommand(taskId);
                        });
                    }
                    default ->
                        // target config is applied
                        onDone.complete(() -> clusterConfigSubject.onNext(config));
                }
            }
            default -> {
            }
            // impossible to be here
        }
        return onDone;
    }

    private void checkWalSize() {
        if ((checkWalSizeTask == null || checkWalSizeTask.isDone()) &&
            System.nanoTime() - lastCompactionDoneAt > compactionLingerNanos &&
            wal.logDataSize() > opts.getCompactWALThresholdBytes()) {
            IKVRangeReader reader = accessor.borrow();
            long lastAppliedIndex = reader.lastAppliedIndex();
            accessor.returnBorrowed(reader);
            KVRangeSnapshot snapshot = wal.latestSnapshot();
            if (snapshot.getLastAppliedIndex() < lastAppliedIndex) {
                checkWalSizeTask = doCompact(accessor.metadata().blockingFirst())
                    .whenComplete((v, e) -> lastCompactionDoneAt = System.nanoTime());
            }
        }
    }

    private CompletableFuture<Void> doCompact(IKVRangeState.KVRangeMeta meta) {
        if (!isSafeToCompact(meta.state)) {
            return CompletableFuture.completedFuture(null);
        }
        return mgmtTaskRunner.add(() -> {
            if (!isWorking()) {
                return CompletableFuture.completedFuture(null);
            }
            KVRangeSnapshot snapshot = accessor.checkpoint();
            if (!isSafeToCompact(snapshot.getState())) {
                log.debug("Skip compaction using snapshot[{}]", snapshot);
                return CompletableFuture.completedFuture(null);
            }
            log.debug("Compact wal using snapshot: rangeId={}, storeId={}\n{}",
                toShortString(id), hostStoreId, snapshot);
            return wal.compact(snapshot);
        });
    }

    private boolean isWorking() {
        Lifecycle state = lifecycle.get();
        return state == Open;
    }

    private boolean isSafeToCompact(State state) {
        return switch (state.getType()) {
            case Normal, Merged, Removed, Purged -> true;
            default -> false;
        };
    }

    private boolean isCompatible(ClusterConfig config1, ClusterConfig config2) {
        return new HashSet<>(config1.getVotersList()).containsAll(config2.getVotersList()) &&
            new HashSet<>(config1.getLearnersList()).containsAll(config2.getLearnersList()) &&
            new HashSet<>(config2.getVotersList()).containsAll(config1.getVotersList()) &&
            new HashSet<>(config2.getLearnersList()).containsAll(config1.getLearnersList());
    }

    private void handleMessage(KVRangeMessage message) {
        switch (message.getPayloadTypeCase()) {
            case WALRAFTMESSAGES ->
                handleWALMessages(message.getHostStoreId(), message.getWalRaftMessages().getWalMessagesList());
            case SNAPSHOTSYNCREQUEST ->
                handleSnapshotSyncRequest(message.getHostStoreId(), message.getSnapshotSyncRequest());
            case PREPAREMERGETOREQUEST ->
                handlePrepareMergeToRequest(message.getHostStoreId(), message.getPrepareMergeToRequest());
            case MERGEREQUEST -> handleMergeRequest(message.getHostStoreId(), message.getMergeRequest());
            case CANCELMERGINGREQUEST ->
                handleCancelMergingRequest(message.getHostStoreId(), message.getCancelMergingRequest());
            case MERGEDONEREQUEST -> handleMergeDoneRequest(message.getHostStoreId(), message.getMergeDoneRequest());
        }
    }

    private void handleWALMessages(String peerId, List<RaftMessage> messages) {
        if (messages.stream().anyMatch(msg -> msg.hasInstallSnapshotReply()
            && msg.getInstallSnapshotReply().getRejected())) {
            log.debug("Snapshot rejected, compact now");
            // follower reject my snapshot, probably because it's too old, let's make a new one immediately
            doCompact(accessor.metadata().blockingFirst());
        }
        wal.receivePeerMessages(peerId, messages);
    }

    private void handleSnapshotSyncRequest(String follower, SnapshotSyncRequest request) {
        log.debug("Init snap-dump session for follower[{}]: rangeId={}, storeId={}, sessionId={}",
            follower, toShortString(id), hostStoreId, request.getSessionId());
        KVRangeDumpSession session = new KVRangeDumpSession(follower, request, accessor, messenger, fsmExecutor,
            Duration.ofSeconds(opts.getSnapshotSyncIdleTimeoutSec()),
            opts.getSnapshotSyncBytesPerSec(), metricManager::reportDump);
        dumpSessions.put(request.getSessionId(), session);
        session.awaitDone().whenComplete((v, e) -> dumpSessions.remove(request.getSessionId(), session));
    }

    private void handlePrepareMergeToRequest(String peer, PrepareMergeToRequest request) {
        log.debug("Handle PrepareMergeTo request: rangeId={}, storeId={}\n{}", toShortString(id), hostStoreId, request);
        // I'm the mergee
        descriptorSubject.firstElement()
            .timeout(5, TimeUnit.SECONDS)
            .subscribe(rangeDescriptor ->
                    wal.propose(KVRangeCommand.newBuilder()
                            .setTaskId(request.getTaskId())
                            .setVer(rangeDescriptor.getVer())
                            .setPrepareMergeTo(PrepareMergeTo.newBuilder()
                                .setMergerId(request.getId())
                                .setMergerVer(request.getVer())
                                .setRange(request.getRange())
                                .setConfig(request.getConfig())
                                .build())
                            .build())
                        .whenCompleteAsync((v, e) -> {
                            if (e != null) {
                                log.debug("Failed to propose command[PrepareMergeTo]: rangeId={}, storeId={}\n{}",
                                    toShortString(id), hostStoreId, request, e);
                            } else {
                                log.debug("Command[PrepareMergeTo] proposed: rangeId={}, storeId={}, index={}\n{}",
                                    toShortString(id), hostStoreId, v, request);
                            }
                            messenger.send(KVRangeMessage.newBuilder()
                                .setRangeId(request.getId())
                                .setHostStoreId(peer)
                                .setPrepareMergeToReply(PrepareMergeToReply.newBuilder()
                                    .setTaskId(request.getTaskId())
                                    .setAccept(e == null)
                                    .build())
                                .build());
                        }, fsmExecutor),
                e -> messenger.send(KVRangeMessage.newBuilder()
                    .setRangeId(request.getId())
                    .setHostStoreId(peer)
                    .setPrepareMergeToReply(PrepareMergeToReply.newBuilder()
                        .setTaskId(request.getTaskId())
                        .setAccept(false)
                        .build())
                    .build()));
    }

    private void handleMergeRequest(String peer, MergeRequest request) {
        log.debug("Handle Merge request: rangeId={}, storeId={}\n{}", toShortString(id), hostStoreId, request);
        wal.propose(KVRangeCommand.newBuilder()
                .setTaskId(request.getTaskId())
                .setVer(request.getVer())
                .setMerge(Merge.newBuilder()
                    .setMergeeId(request.getMergeeId())
                    .setMergeeVer(request.getMergeeVer())
                    .setRange(request.getRange())
                    .setStoreId(request.getStoreId())
                    .build())
                .build())
            .whenCompleteAsync((v, e) -> {
                if (e != null) {
                    log.debug("Failed to propose command[Merge]: rangeId={}, storeId={}\n{}",
                        toShortString(id), hostStoreId, request, e);
                } else {
                    log.debug("Command[Merge] proposed: rangeId={}, storeId={}, index={}\n{}",
                        toShortString(id), hostStoreId, v, request);
                }
                messenger.send(KVRangeMessage.newBuilder()
                    .setRangeId(request.getMergeeId())
                    .setHostStoreId(peer)
                    .setMergeReply(MergeReply.newBuilder()
                        .setTaskId(request.getTaskId())
                        .setAccept(e == null)
                        .build())
                    .build());
            }, fsmExecutor);
    }

    private void handleCancelMergingRequest(String peer, CancelMergingRequest request) {
        log.debug("Handle CancelMerging request: rangeId={}, storeId={}\n{}", toShortString(id), hostStoreId, request);
        wal.propose(KVRangeCommand.newBuilder()
                .setTaskId(request.getTaskId())
                .setVer(request.getVer())
                .setCancelMerging(CancelMerging.newBuilder()
                    .build())
                .build())
            .whenCompleteAsync((v, e) -> {
                if (e != null) {
                    log.debug("Failed to propose command[CancelMerging]: rangeId={}, storeId={}\n{}",
                        toShortString(id), hostStoreId, request, e);
                } else {
                    log.debug("Command[CancelMerging] proposed: rangeId={}, storeId={}, index={}\n{}",
                        toShortString(id), hostStoreId, v, request);
                }
                messenger.send(KVRangeMessage.newBuilder()
                    .setRangeId(request.getRequester())
                    .setHostStoreId(peer)
                    .setCancelMergingReply(CancelMergingReply.newBuilder()
                        .setTaskId(request.getTaskId())
                        .setAccept(e == null)
                        .build())
                    .build());
            }, fsmExecutor);
    }

    private void handleMergeDoneRequest(String peer, MergeDoneRequest request) {
        log.debug("Handle MergeDone request: rangeId={}, storeId={}\n{}",
            toShortString(id), hostStoreId, request);
        wal.propose(KVRangeCommand.newBuilder()
                .setTaskId(request.getTaskId())
                .setVer(request.getMergeeVer())
                .setMergeDone(MergeDone.newBuilder()
                    .setStoreId(request.getStoreId())
                    .build())
                .build())
            .whenCompleteAsync((v, e) -> {
                if (e != null) {
                    log.debug("Failed to propose command[MergeDone]: rangeId={}, storeId={}\n{}",
                        toShortString(id), hostStoreId, request, e);
                } else {
                    log.debug("Command[MergeDone] proposed: rangeId={}, storeId={}, index={}\n{}",
                        toShortString(id), hostStoreId, v, request);
                }
                messenger.send(KVRangeMessage.newBuilder()
                    .setRangeId(request.getId())
                    .setHostStoreId(peer)
                    .setMergeDoneReply(MergeDoneReply.newBuilder()
                        .setTaskId(request.getTaskId())
                        .setAccept(e == null)
                        .build())
                    .build());
            }, fsmExecutor);
    }
}
