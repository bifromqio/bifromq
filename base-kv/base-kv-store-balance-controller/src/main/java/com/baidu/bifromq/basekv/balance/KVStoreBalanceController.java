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

package com.baidu.bifromq.basekv.balance;

import com.baidu.bifromq.basekv.IBaseKVClusterMetadataManager;
import com.baidu.bifromq.basekv.LoadRulesProposalHandler;
import com.baidu.bifromq.basekv.balance.command.BalanceCommand;
import com.baidu.bifromq.basekv.balance.command.BootstrapCommand;
import com.baidu.bifromq.basekv.balance.command.ChangeConfigCommand;
import com.baidu.bifromq.basekv.balance.command.MergeCommand;
import com.baidu.bifromq.basekv.balance.command.RangeCommand;
import com.baidu.bifromq.basekv.balance.command.RecoveryCommand;
import com.baidu.bifromq.basekv.balance.command.SplitCommand;
import com.baidu.bifromq.basekv.balance.command.TransferLeadershipCommand;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.store.proto.BootstrapRequest;
import com.baidu.bifromq.basekv.store.proto.ChangeReplicaConfigReply;
import com.baidu.bifromq.basekv.store.proto.ChangeReplicaConfigRequest;
import com.baidu.bifromq.basekv.store.proto.KVRangeMergeReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeMergeRequest;
import com.baidu.bifromq.basekv.store.proto.KVRangeSplitReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeSplitRequest;
import com.baidu.bifromq.basekv.store.proto.RecoverRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basekv.store.proto.TransferLeadershipReply;
import com.baidu.bifromq.basekv.store.proto.TransferLeadershipRequest;
import com.baidu.bifromq.logger.SiftLogger;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.Struct;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Timer.Sample;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Builder;
import org.slf4j.Logger;

public class KVStoreBalanceController {
    private enum State {
        Init,
        Started,
        Closed
    }

    private final IBaseKVClusterMetadataManager metadataManager;
    private final IBaseKVStoreClient storeClient;
    private final Map<KVRangeId, Long> rangeCommandHistory = new ConcurrentHashMap<>();
    private final AtomicBoolean scheduling = new AtomicBoolean();
    private final AtomicReference<State> state = new AtomicReference<>(State.Init);
    private final ScheduledExecutorService executor;
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final List<? extends IStoreBalancerFactory> balancerFactories;
    private final Map<String, StoreBalancer> balancers;
    private final Duration retryDelay;
    private String localStoreId;
    private Logger log;
    private MetricManager metricsManager;
    private volatile Map<String, Struct> loadRules;
    private volatile Set<KVRangeStoreDescriptor> landscape;

    public KVStoreBalanceController(IBaseKVClusterMetadataManager metadataManager,
                                    IBaseKVStoreClient storeClient,
                                    List<? extends IStoreBalancerFactory> factories,
                                    Duration retryDelay,
                                    ScheduledExecutorService executor) {
        Preconditions.checkArgument(!factories.isEmpty(),
            "At least one balancer factory should be provided");
        this.metadataManager = metadataManager;
        this.storeClient = storeClient;
        this.balancerFactories = Lists.newArrayList(factories);
        this.balancers = new HashMap<>();
        this.retryDelay = retryDelay;
        this.executor = executor;
    }

    public void start(String localStoreId) {
        if (state.compareAndSet(State.Init, State.Started)) {
            this.localStoreId = localStoreId;
            log =
                SiftLogger.getLogger("balancer.logger", "clusterId", storeClient.clusterId(), "storeId", localStoreId);

            for (IStoreBalancerFactory factory : balancerFactories) {
                log.info("Create balancer from factory: {}", factory.getClass().getName());
                StoreBalancer balancer = factory.newBalancer(storeClient.clusterId(), localStoreId);
                balancers.put(balancer.getClass().getName(), balancer);
            }
            this.metricsManager = new MetricManager(localStoreId, storeClient.clusterId());
            log.info("BalancerController start");
            metadataManager.setLoadRulesProposalHandler(this::handleLoadRulesProposal);
            disposables.add(metadataManager.loadRules().subscribe(loadRules -> {
                this.loadRules = loadRules;
                trigger();
            }));
            disposables.add(storeClient.describe().subscribe(descriptors -> {
                this.landscape = descriptors;
                trimRangeHistory(descriptors);
                trigger();
            }));
        }
    }

    public void stop() {
        if (state.compareAndSet(State.Started, State.Closed)) {
            metadataManager.setLoadRulesProposalHandler(null);
            disposables.dispose();
            balancers.values().forEach(StoreBalancer::close);
        }
    }

    private LoadRulesProposalHandler.Result handleLoadRulesProposal(String balancerClassFQN, Struct loadRules) {
        StoreBalancer balancer = balancers.get(balancerClassFQN);
        if (balancer != null) {
            if (balancer.validate(loadRules)) {
                return LoadRulesProposalHandler.Result.ACCEPTED;
            }
            return LoadRulesProposalHandler.Result.REJECTED;
        } else {
            return LoadRulesProposalHandler.Result.NO_BALANCER;
        }
    }

    private void trigger() {
        if (state.get() == State.Started && scheduling.compareAndSet(false, true)) {
            long jitter = ThreadLocalRandom.current().nextLong(0, retryDelay.toMillis());
            executor.schedule(this::updateAndBalance, jitter, TimeUnit.MILLISECONDS);
        }
    }

    private void updateAndBalance() {
        Map<String, Struct> loadRules = this.loadRules;
        Set<KVRangeStoreDescriptor> landscape = this.landscape;
        if (landscape == null || landscape.isEmpty()) {
            scheduling.set(false);
            return;
        }
        for (StoreBalancer balancer : balancers.values()) {
            try {
                if (loadRules != null && loadRules.containsKey(balancer.getClass().getName())) {
                    balancer.update(loadRules.get(balancer.getClass().getName()));
                }
                balancer.update(landscape);
            } catch (Throwable e) {
                log.error("Balancer[{}] update failed", balancer.getClass().getSimpleName(), e);
            }
        }
        balance(loadRules, landscape);
    }

    private void scheduleRetry(Map<String, Struct> loadRules, Set<KVRangeStoreDescriptor> landscape, Duration delay) {
        executor.schedule(() -> {
            if (loadRules != this.loadRules || landscape != this.landscape) {
                // retry is preemptive
                return;
            }
            if (scheduling.compareAndSet(false, true)) {
                balance(loadRules, landscape);
            }
        }, delay.toNanos(), TimeUnit.NANOSECONDS);
    }

    private void balance(final Map<String, Struct> loadRules, final Set<KVRangeStoreDescriptor> landscape) {
        metricsManager.scheduleCount.increment();
        Duration delay = Duration.ZERO;
        for (StoreBalancer fromBalancer : balancers.values()) {
            try {
                BalanceResult result = fromBalancer.balance();
                switch (result.type()) {
                    case BalanceNow -> {
                        BalanceCommand commandToRun = ((BalanceNow<?>) result).command;
                        if (!isStaleCommand(commandToRun)) {
                            log.info("Balancer[{}] command run: {}", fromBalancer.getClass().getSimpleName(),
                                commandToRun);
                            String balancerName = fromBalancer.getClass().getSimpleName();
                            String cmdName = commandToRun.getClass().getSimpleName();
                            Sample start = Timer.start();
                            runCommand(commandToRun)
                                .whenCompleteAsync((success, e) -> {
                                    MetricManager.CommandMetrics
                                        metrics = metricsManager.getCommandMetrics(balancerName, cmdName);
                                    if (e != null) {
                                        log.error("Should not be here, error when run command", e);
                                        metrics.cmdFailedCounter.increment();
                                    } else {
                                        log.info("Balancer[{}] command run result[{}]: {}",
                                            fromBalancer.getClass().getSimpleName(), success, commandToRun);
                                        if (success) {
                                            metrics.cmdSucceedCounter.increment();
                                            start.stop(metrics.cmdRunTimer);
                                        } else {
                                            metrics.cmdFailedCounter.increment();
                                        }
                                    }
                                    scheduling.set(false);
                                    if (success) {
                                        if (this.landscape != landscape || this.loadRules != loadRules) {
                                            trigger();
                                        }
                                    } else {
                                        scheduleRetry(loadRules, landscape, retryDelay);
                                    }
                                }, executor);
                            return;
                        }
                    }
                    case AwaitBalance -> {
                        Duration await = ((AwaitBalance) result).await;
                        delay = await.toNanos() > delay.toNanos() ? await : delay;
                    }
                }
            } catch (Throwable e) {
                log.warn("Balancer[{}] unexpected error", fromBalancer.getClass().getSimpleName(), e);
            }
        }
        // no command to run
        scheduling.set(false);
        if (this.landscape != landscape || this.loadRules != loadRules) {
            trigger();
        } else if (!delay.isZero()) {
            // if some balancers are in the progress of generating balance command, wait for a while
            scheduleRetry(loadRules, landscape, delay);
        }
    }

    private boolean isStaleCommand(BalanceCommand command) {
        if (command instanceof RangeCommand rangeCommand) {
            if (rangeCommand.getExpectedVer() != null) {
                Long prevCMDVer = rangeCommandHistory.getOrDefault(rangeCommand.getKvRangeId(), null);
                if (prevCMDVer != null && prevCMDVer >= rangeCommand.getExpectedVer()) {
                    log.debug("Ignore staled command: {}", rangeCommand);
                    return true;
                }
            }
        }
        return false;
    }

    private void trimRangeHistory(Set<KVRangeStoreDescriptor> landscape) {
        for (KVRangeStoreDescriptor storeDescriptor : landscape) {
            if (storeDescriptor.getId().equals(localStoreId)) {
                Set<KVRangeId> localRangeIds = storeDescriptor.getRangesList().stream()
                    .map(KVRangeDescriptor::getId)
                    .collect(Collectors.toSet());
                rangeCommandHistory.keySet().retainAll(localRangeIds);
            }
        }
    }

    private CompletableFuture<Boolean> runCommand(BalanceCommand command) {
        return switch (command.type()) {
            case CHANGE_CONFIG -> {
                assert command instanceof ChangeConfigCommand;
                ChangeConfigCommand changeConfigCommand = (ChangeConfigCommand) command;
                ChangeReplicaConfigRequest changeConfigRequest = ChangeReplicaConfigRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setKvRangeId(changeConfigCommand.getKvRangeId())
                    .setVer(changeConfigCommand.getExpectedVer())
                    .addAllNewVoters(changeConfigCommand.getVoters())
                    .addAllNewLearners(changeConfigCommand.getLearners())
                    .build();
                yield handleStoreReplyCode(command,
                    storeClient.changeReplicaConfig(command.getToStore(), changeConfigRequest)
                        .thenApply(ChangeReplicaConfigReply::getCode)
                );
            }
            case MERGE -> {
                assert command instanceof MergeCommand;
                MergeCommand mergeCommand = (MergeCommand) command;
                KVRangeMergeRequest rangeMergeRequest = KVRangeMergeRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setVer(mergeCommand.getExpectedVer())
                    .setMergerId(mergeCommand.getKvRangeId())
                    .setMergeeId(mergeCommand.getMergeeId())
                    .build();
                yield handleStoreReplyCode(command,
                    storeClient.mergeRanges(command.getToStore(), rangeMergeRequest)
                        .thenApply(KVRangeMergeReply::getCode));
            }
            case SPLIT -> {
                assert command instanceof SplitCommand;
                SplitCommand splitCommand = (SplitCommand) command;
                KVRangeSplitRequest kvRangeSplitRequest = KVRangeSplitRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setKvRangeId(splitCommand.getKvRangeId())
                    .setVer(splitCommand.getExpectedVer())
                    .setSplitKey(splitCommand.getSplitKey())
                    .build();
                yield handleStoreReplyCode(command,
                    storeClient.splitRange(command.getToStore(), kvRangeSplitRequest)
                        .thenApply(KVRangeSplitReply::getCode));
            }
            case TRANSFER_LEADERSHIP -> {
                assert command instanceof TransferLeadershipCommand;
                TransferLeadershipCommand transferLeadershipCommand = (TransferLeadershipCommand) command;
                TransferLeadershipRequest transferLeadershipRequest = TransferLeadershipRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setKvRangeId(transferLeadershipCommand.getKvRangeId())
                    .setVer(transferLeadershipCommand.getExpectedVer())
                    .setNewLeaderStore(transferLeadershipCommand.getNewLeaderStore())
                    .build();
                yield handleStoreReplyCode(command,
                    storeClient.transferLeadership(command.getToStore(), transferLeadershipRequest)
                        .thenApply(TransferLeadershipReply::getCode));
            }
            case RECOVERY -> {
                assert command instanceof RecoveryCommand;
                RecoveryCommand recoveryCommand = (RecoveryCommand) command;
                RecoverRequest recoverRequest = RecoverRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setKvRangeId(recoveryCommand.getKvRangeId())
                    .build();
                yield storeClient.recover(command.getToStore(), recoverRequest)
                    .handle((r, e) -> {
                        if (e != null) {
                            log.error("Unexpected error when recover, req: {}", recoverRequest, e);
                        }
                        return true;
                    });
            }
            case BOOTSTRAP -> {
                assert command instanceof BootstrapCommand;
                BootstrapCommand bootstrapCommand = (BootstrapCommand) command;
                BootstrapRequest bootstrapRequest = BootstrapRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setKvRangeId(bootstrapCommand.getKvRangeId())
                    .setBoundary(bootstrapCommand.getBoundary())
                    .build();
                yield storeClient.bootstrap(command.getToStore(), bootstrapRequest)
                    .handle((r, e) -> {
                        if (e != null) {
                            log.error("Unexpected error when bootstrap: {}", command, e);
                        }
                        return true;
                    });
            }
        };
    }

    private CompletableFuture<Boolean> handleStoreReplyCode(BalanceCommand command,
                                                            CompletableFuture<ReplyCode> storeReply) {
        CompletableFuture<Boolean> onDone = new CompletableFuture<>();
        storeReply.whenComplete((code, e) -> {
            if (e != null) {
                log.error("Unexpected error when run command: {}", command, e);
                onDone.complete(false);
                return;
            }
            switch (code) {
                case Ok -> {
                    switch (command.type()) {
                        case SPLIT, MERGE, CHANGE_CONFIG -> {
                            RangeCommand rangeCommand = (RangeCommand) command;
                            rangeCommandHistory.compute(rangeCommand.getKvRangeId(), (k, v) -> {
                                if (v == null) {
                                    v = rangeCommand.getExpectedVer();
                                }
                                return Math.max(v, rangeCommand.getExpectedVer());
                            });
                        }
                        default -> {
                            // no nothing
                        }
                    }
                    onDone.complete(true);
                }
                case BadRequest, BadVersion, TryLater, InternalError -> {
                    log.warn("Failed with reply: {}, command: {}", code, command);
                    onDone.complete(false);
                }
                default -> onDone.complete(false);
            }
        });
        return onDone;
    }

    static class MetricManager {

        private final Tags tags;
        private final Counter scheduleCount;
        private final Map<MetricsKey, CommandMetrics> metricsMap = new HashMap<>();

        public MetricManager(String localStoreId, String clusterId) {
            tags = Tags.of("storeId", localStoreId).and("clusterId", clusterId);
            scheduleCount = Counter.builder("basekv.balance.scheduled")
                .tags(tags)
                .register(Metrics.globalRegistry);
        }

        public CommandMetrics getCommandMetrics(String fromBalancer, String command) {
            MetricsKey metricsKey = MetricsKey.builder()
                .balancer(fromBalancer)
                .cmdName(command)
                .build();
            return metricsMap.computeIfAbsent(metricsKey,
                k -> new CommandMetrics(tags.and("balancer", k.balancer).and("cmd", k.cmdName)));
        }

        public void close() {
            Metrics.globalRegistry.remove(scheduleCount);
            metricsMap.values().forEach(CommandMetrics::clear);
        }

        @Builder
        private static class MetricsKey {
            private String balancer;
            private String cmdName;
        }

        static class CommandMetrics {
            Counter cmdSucceedCounter;
            Counter cmdFailedCounter;
            Timer cmdRunTimer;

            private CommandMetrics(Tags tags) {
                cmdSucceedCounter = Counter.builder("basekv.balance.cmd.succeed")
                    .tags(tags)
                    .register(Metrics.globalRegistry);
                cmdFailedCounter = Counter.builder("basekv.balance.cmd.failed")
                    .tags(tags)
                    .register(Metrics.globalRegistry);
                cmdRunTimer = Timer.builder("basekv.balance.cmd.run")
                    .tags(tags)
                    .register(Metrics.globalRegistry);
            }

            private void clear() {
                Metrics.globalRegistry.remove(cmdSucceedCounter);
                Metrics.globalRegistry.remove(cmdFailedCounter);
                Metrics.globalRegistry.remove(cmdRunTimer);
            }
        }
    }
}
