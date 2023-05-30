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

package com.baidu.bifromq.basekv.raft;

import com.baidu.bifromq.basekv.raft.event.RaftEvent;
import com.baidu.bifromq.basekv.raft.event.RaftEventType;
import com.baidu.bifromq.basekv.raft.exception.InternalError;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.raft.proto.LogEntry;
import com.baidu.bifromq.basekv.raft.proto.RaftMessage;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.NonNull;
import org.slf4j.Logger;

public final class RaftNode implements IRaftNode {
    enum Status {
        INIT,
        STARTING,
        STARTED,
        STOPPING,
        STOPPED
    }

    private class MetricManager {
        final Timer tickTimer;
        final Timer proposeTimer;
        final Timer readIndexTimer;
        final Timer peerMsgHandlingTimer;
        final Timer compactTimer;
        final Timer transferLeadershipTimer;
        final Timer changeClusterConfigTimer;
        final Timer retrieveEntriesTimer;
        final Timer entryAtTimer;
        final Timer stableToTimer;
        final Timer messageSenderTimer;
        final Timer snapshotInstallTimer;
        final EnumMap<RaftEventType, Timer> eventTimers;
        final Gauge pendingProposalsGauge;
        final Gauge commitIndexGauge;
        final Gauge firstIndexGauge;
        final Gauge lastIndexGauge;
        final Gauge currentTermGauge;
        final Gauge currentStatusGauge;

        MetricManager(Tags tags) {
            // timer for tick operation latency
            tickTimer = Metrics.timer("raft.cmd.tick", tags);

            proposeTimer = Metrics.timer("raft.cmd.propose", tags);

            readIndexTimer = Metrics.timer("raft.cmd.readindex", tags);

            // timer for handling peer message operation
            peerMsgHandlingTimer = Metrics.timer("raft.cmd.recvmsg", tags);

            // timer for compact operation latency
            compactTimer = Metrics.timer("raft.cmd.compact", tags);

            // timer for transfer leadership operation latency
            transferLeadershipTimer = Metrics.timer("raft.cmd.trnsldr", tags);

            // timer for change cluster config operation latency
            changeClusterConfigTimer = Metrics.timer("raft.cmd.chgcfg", tags);

            // timer for retrieve entries
            retrieveEntriesTimer = Metrics.timer("raft.cmd.getentries", tags);

            // timer for get single entry
            entryAtTimer = Metrics.timer("raft.cmd.getentry", tags);

            // timer for stableTo operation
            stableToTimer = Metrics.timer("raft.cmd.markstable", tags);

            messageSenderTimer = Metrics.timer("raft.message.send", tags);
            snapshotInstallTimer = Metrics.timer("raft.snapshot.install", tags);
            eventTimers = new EnumMap<RaftEventType, Timer>(new HashMap<>() {{
                put(RaftEventType.ELECTION, Metrics.timer("raft.event.listener", tags.and("type", "election")));
                put(RaftEventType.COMMIT, Metrics.timer("raft.event.listener", tags.and("type", "commit")));
                put(RaftEventType.STATUS_CHANGED,
                    Metrics.timer("raft.event.listener", tags.and("type", "status_changed")));
                put(RaftEventType.SNAPSHOT_RESTORED,
                    Metrics.timer("raft.event.listener", tags.and("type", "snapshot_restored")));
                put(RaftEventType.SYNC_STATE_CHANGED,
                    Metrics.timer("raft.event.listener", tags.and("type", "sync_state_changed")));
            }});

            // gauge for pending proposal count
            pendingProposalsGauge = Gauge.builder("raft.cmd.propose.pending", RaftNode.this,
                    r -> r.stateRef.get().uncommittedProposals.size())
                .tags(tags)
                .register(Metrics.globalRegistry);

            // gauge for commit index
            commitIndexGauge = Gauge.builder("raft.log.commitindex", RaftNode.this,
                    r -> r.stateRef.get().commitIndex)
                .tags(tags)
                .register(Metrics.globalRegistry);
            // gauge for first index
            firstIndexGauge = Gauge.builder("raft.log.firstindex", RaftNode.this,
                    r -> r.stateStorage.firstIndex())
                .tags(tags)
                .register(Metrics.globalRegistry);
            // gauge for last index
            lastIndexGauge = Gauge.builder("raft.log.lastindex",
                    RaftNode.this, r -> r.stateStorage.lastIndex())
                .tags(tags)
                .register(Metrics.globalRegistry);
            // gauge for current term
            currentTermGauge = Gauge.builder("raft.log.term", RaftNode.this, r -> stateRef.get().currentTerm())
                .tags(tags)
                .register(Metrics.globalRegistry);
            // gauge for current status
            currentStatusGauge = Gauge.builder("raft.status", RaftNode.this,
                    r -> stateRef.get() instanceof RaftNodeStateLeader ?
                        3 : (stateRef.get() instanceof RaftNodeStateFollower ? 2 : 1))
                .tags(tags)
                .register(Metrics.globalRegistry);
        }

        void close() {
            Metrics.globalRegistry.removeByPreFilterId(tickTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(proposeTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(readIndexTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(peerMsgHandlingTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(compactTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(transferLeadershipTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(changeClusterConfigTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(retrieveEntriesTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(entryAtTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(stableToTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(messageSenderTimer.getId());
            Metrics.globalRegistry.removeByPreFilterId(snapshotInstallTimer.getId());

            eventTimers.forEach((e, t) -> Metrics.globalRegistry.removeByPreFilterId(t.getId()));

            Metrics.globalRegistry.removeByPreFilterId(pendingProposalsGauge.getId());

            Metrics.globalRegistry.removeByPreFilterId(commitIndexGauge.getId());

            Metrics.globalRegistry.removeByPreFilterId(firstIndexGauge.getId());

            Metrics.globalRegistry.removeByPreFilterId(lastIndexGauge.getId());

            Metrics.globalRegistry.removeByPreFilterId(currentTermGauge.getId());

            Metrics.globalRegistry.removeByPreFilterId(currentStatusGauge.getId());
        }
    }

    private class SampledRaftMessageListener implements IRaftMessageSender {
        final IRaftMessageSender delegate;

        SampledRaftMessageListener(IRaftMessageSender delegate) {
            this.delegate = delegate;
        }

        @Override
        public void send(Map<String, List<RaftMessage>> messages) {
            Timer.Sample sample = Timer.start();
            delegate.send(messages);
            sample.stop(metricMgr.messageSenderTimer);
        }
    }

    private class SampledRaftEventListener implements IRaftEventListener {
        final IRaftEventListener delegate;

        private SampledRaftEventListener(IRaftEventListener delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onEvent(RaftEvent event) {
            Timer.Sample sample = Timer.start();
            delegate.onEvent(event);
            sample.stop(metricMgr.eventTimers.get(event.type));
        }
    }

    private class SampledSnapshotInstaller implements ISnapshotInstaller {
        final ISnapshotInstaller delegate;

        private SampledSnapshotInstaller(ISnapshotInstaller delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<Void> install(ByteString fsmSnapshot) {
            return metricMgr.snapshotInstallTimer.record(() -> delegate.install(fsmSnapshot));
        }
    }

    private final String id;
    private final IRaftStateStore stateStorage;
    private final RaftConfig config;
    private final Logger log;
    private final ExecutorService raftExecutor;
    private final AtomicReference<RaftNodeState> stateRef = new AtomicReference<>();
    private final AtomicReference<Status> status = new AtomicReference<>(Status.INIT);
    private final AtomicReference<CompletableFuture<Void>> stopFuture = new AtomicReference<>();
    private final Tags metricTags;
    private MetricManager metricMgr;

    public RaftNode(RaftConfig config, IRaftStateStore stateStore, Logger logger, String... metricTags) {
        this(config, stateStore, logger, Executors.defaultThreadFactory(), metricTags);
    }

    public RaftNode(RaftConfig config, IRaftStateStore stateStore, Logger logger, ThreadFactory threadFactory,
                    String... metricTags) {
        verifyTags(metricTags);
        verifyConfig(config);
        verifyStateStore(stateStore);
        log = logger;
        this.metricTags = Tags.of(metricTags);
        this.stateStorage = new MetricMonitoredStateStore(stateStore, this.metricTags);
        this.id = stateStorage.local();
        this.config = config.toBuilder().build();
        this.raftExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new LinkedTransferQueue<>(), threadFactory);
        stateStore.addStableListener(this::onStabilized);
    }

    @Override
    public boolean isStarted() {
        return status.get() == Status.STARTED;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public RaftNodeStatus status() {
        checkStarted();
        return stateRef.get().getState();
    }

    @Override
    public void tick() {
        submit(() -> {
            RaftNodeState state = stateRef.get();
            Timer.Sample sample = Timer.start();
            String leader = state.currentLeader();
            RaftNodeState nextState = state.tick();
            sample.stop(metricMgr.tickTimer);
            if (nextState != state) {
                stateRef.set(nextState);
                // leader elected or candidate transit to follower
                nextState.notifyStateChanged();
            }
            if (nextState.currentLeader() != null && !nextState.currentLeader().equals(leader)) {
                nextState.notifyLeaderElected(nextState.currentLeader(), nextState.currentTerm());
            }
        });
    }

    @Override
    public CompletableFuture<Void> propose(ByteString appCommand) {
        return submit(onDone -> stateRef.get().propose(appCommand, sampleLatency(onDone, metricMgr.proposeTimer)));
    }

    private void onStabilized(long stableIndex) {
        submit(() -> {
            Timer.Sample sample = Timer.start();
            RaftNodeState state = stateRef.get();
            String leader = state.currentLeader();
            RaftNodeState nextState = state.stableTo(stableIndex);
            sample.stop(metricMgr.stableToTimer);
            if (nextState != state) {
                stateRef.set(nextState);
                // leader elected or candidate transit to follower when acknowledging there is a leader
                nextState.notifyStateChanged();
            }
            if (nextState.currentLeader() != null && !nextState.currentLeader().equals(leader)) {
                nextState.notifyLeaderElected(nextState.currentLeader(), nextState.currentTerm());
            }
        });
    }

    @Override
    public CompletableFuture<Long> readIndex() {
        return submit(onDone -> stateRef.get().readIndex(sampleLatency(onDone, metricMgr.readIndexTimer)));
    }

    @Override
    public void receive(String fromPeer, RaftMessage message) {
        submit(() -> {
            RaftNodeState state = stateRef.get();
            String leader = state.currentLeader();
            Timer.Sample sample = Timer.start();
            RaftNodeState nextState = state.receive(fromPeer, message);
            sample.stop(metricMgr.peerMsgHandlingTimer);
            if (nextState != state) {
                stateRef.set(nextState);
                // leader elected or candidate transit to follower when acknowledging there is a leader
                nextState.notifyStateChanged();
            }
            if (nextState.currentLeader() != null && !nextState.currentLeader().equals(leader)) {
                nextState.notifyLeaderElected(nextState.currentLeader(), nextState.currentTerm());
            }
        });
    }

    @Override
    public CompletableFuture<Void> compact(ByteString fsmSnapshot, long compactIndex) {
        return submit(onDone -> {
            Timer.Sample sample = Timer.start();
            stateRef.get()
                .compact(fsmSnapshot, compactIndex, sampleLatency(onDone, metricMgr.compactTimer));
            sample.stop(metricMgr.compactTimer);
        });
    }

    @Override
    public CompletableFuture<Void> transferLeadership(String newLeader) {
        return submit(onDone -> stateRef.get()
            .transferLeadership(newLeader, sampleLatency(onDone, metricMgr.transferLeadershipTimer)));
    }

    @Override
    public CompletableFuture<Void> recover() {
        return submit(onDone -> stateRef.get().recover(onDone));
    }

    @Override
    public ClusterConfig latestClusterConfig() {
        return unwrap(submit(onDone -> onDone.complete(stateRef.get().latestClusterConfig())));
    }

    @Override
    public Boolean stepDown() {
        return unwrap(submit(onDone -> {
            RaftNodeState state = stateRef.get();
            RaftNodeState nextState = state.stepDown(onDone);
            if (nextState != state) {
                stateRef.set(nextState);
            }
        }));
    }

    @Override
    public ByteString latestSnapshot() {
        return unwrap(submit(onDone -> onDone.complete(stateRef.get().latestSnapshot())));
    }

    @Override
    public CompletableFuture<Void> changeClusterConfig(String correlateId,
                                                       Set<String> nextVoters,
                                                       Set<String> nextLearners) {
        return submit(onDone -> stateRef.get().changeClusterConfig(correlateId, nextVoters, nextLearners,
            sampleLatency(onDone, metricMgr.changeClusterConfigTimer)));
    }

    @Override
    public CompletableFuture<Iterator<LogEntry>> retrieveCommitted(long fromIndex, long maxSize) {
        return submit(onDone -> stateRef.get().retrieveCommitted(fromIndex, maxSize,
            sampleLatency(onDone, metricMgr.retrieveEntriesTimer)));
    }

    @Override
    public void start(@NonNull IRaftMessageSender sender,
                      @NonNull IRaftEventListener listener,
                      @NonNull IRaftNode.ISnapshotInstaller installer) {
        if (status.compareAndSet(Status.INIT, Status.STARTING)) {
            long currentTerm = stateStorage.currentTerm();
            stateRef.set(new RaftNodeStateFollower(
                currentTerm,
                0, // commitIndex
                null,
                config,
                stateStorage,
                log,
                new SampledRaftMessageListener(sender),
                new SampledRaftEventListener(listener),
                new SampledSnapshotInstaller(installer),
                this::onSnapshotRestored
            ));
            metricMgr = new MetricManager(metricTags);
            status.set(Status.STARTED);
            log.debug("Raft node[{}] started: term={}", id(), currentTerm);
        }
    }

    @Override
    public CompletableFuture<Void> stop() {
        switch (status.get()) {
            case INIT:
                // fallthrough
            case STARTING:
                return CompletableFuture.failedFuture(new IllegalStateException("Raft node not started"));
            case STARTED:
                // fallthrough
            case STOPPING:
                // fallthrough
            case STOPPED:
            default:
                if (stopFuture.compareAndSet(null, new CompletableFuture<>())) {
                    log.debug("Stopping raft node[{}]", id());
                    CompletableFuture<Void> lastTask = new CompletableFuture<>();
                    lastTask.whenComplete((v, e) -> {
                        raftExecutor.shutdown();
                        stopFuture.get().complete(null);
                    });
                    Runnable stop = () -> {
                        assert status.get() == Status.STARTED;
                        status.set(Status.STOPPING);
                        stateStorage.stop().whenComplete((v, e) -> {
                            metricMgr.close();
                            status.set(Status.STOPPED);
                            log.debug("Raft node[{}] stopped", id());
                            lastTask.complete(null);
                        });
                    };
                    raftExecutor.execute(stop);
                }
                return stopFuture.get();
        }
    }

    void onSnapshotRestored(ByteString fsmSnapshot, Throwable ex) {
        if (isStarted()) {
            raftExecutor.execute(() -> {
                if (!isStarted()) {
                    return;
                }
                stateRef.get().onSnapshotRestored(fsmSnapshot, ex);
            });
        }
    }

    private void submit(Runnable task) {
        submit(onDone -> {
            task.run();
            onDone.complete(null);
        });
    }

    private <T> CompletableFuture<T> submit(Consumer<CompletableFuture<T>> task) {
        CompletableFuture<T> doneFuture = new CompletableFuture<>();
        try {
            raftExecutor.execute(() -> {
                switch (status.get()) {
                    case INIT:
                        // fallthrough
                    case STARTING:
                        doneFuture.completeExceptionally(new IllegalStateException("Raft node not started"));
                        break;
                    case STARTED:
                        try {
                            task.accept(doneFuture);
                        } catch (Throwable e) {
                            doneFuture.completeExceptionally(new InternalError(e));
                        }
                        break;
                    case STOPPING:
                        // fallthrough
                    case STOPPED:
                        doneFuture.completeExceptionally(new IllegalStateException("Raft node has stopped"));
                        break;
                }
            });
        } catch (RejectedExecutionException e) {
            doneFuture.completeExceptionally(new IllegalStateException(e));
        }
        return doneFuture;
    }

    private void verifyConfig(RaftConfig config) {
        if (config.getHeartbeatTimeoutTick() > config.getElectionTimeoutTick()) {
            throw new IllegalArgumentException("heartbeat timeout must be less than election timeout, normally 1/10");
        }
    }

    private void verifyStateStore(IRaftStateStore stateStorage) {
        if (stateStorage.local() == null) {
            throw new IllegalArgumentException("local id cannot be null");
        }
        if (stateStorage.lastIndex() < 0) {
            throw new IllegalArgumentException("last index must be non-negative");
        }
        if (stateStorage.firstIndex() <= 0) {
            throw new IllegalArgumentException("first index must be positive");
        }
        if (stateStorage.latestClusterConfig() == null) {
            throw new IllegalArgumentException("latest cluster config cannot be null");
        }
        if (stateStorage.latestSnapshot() == null) {
            throw new IllegalArgumentException("latest snapshot cannot be null");
        }
        ClusterConfig clusterConfig = stateStorage.latestSnapshot().getClusterConfig();
        if (ClusterConfigHelper.isIntersect(new HashSet<>(clusterConfig.getVotersList()),
            new HashSet<>(clusterConfig.getLearnersList()))) {
            throw new IllegalArgumentException("voters and learners mustn't intersect with each other");
        }
    }

    private void verifyTags(String[] tags) {
        if (tags.length % 2 != 0) {
            throw new IllegalArgumentException("Tags must be even number representing key/value pairs");
        }
    }

    private <T> T unwrap(CompletableFuture<T> future) {
        try {
            return future.get();
        } catch (Throwable e) {
            throw new IllegalStateException("Future cannot be unwrapped", e);
        }
    }

    private <T> CompletableFuture<T> sampleLatency(CompletableFuture<T> future, Timer timer) {
        Timer.Sample sample = Timer.start();
        future.whenComplete((v, e) -> sample.stop(timer));
        return future;
    }

    private void checkStarted() {
        if (status.get() != Status.STARTED) {
            throw new IllegalStateException("Raft node not started");
        }
    }
}
