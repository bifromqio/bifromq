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

package com.baidu.bifromq.basekv.balance;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basehookloader.BaseHookLoader;
import com.baidu.bifromq.basekv.balance.KVRangeBalanceController.MetricManager.CommandMetrics;
import com.baidu.bifromq.basekv.balance.command.BalanceCommand;
import com.baidu.bifromq.basekv.balance.command.CommandType;
import com.baidu.bifromq.basekv.balance.option.KVRangeBalanceControllerOptions;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.google.common.util.concurrent.MoreExecutors;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Timer.Sample;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.reactivex.rxjava3.disposables.Disposable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KVRangeBalanceController {

    private enum State {
        Init,
        Started,
        Closed
    }

    private final KVRangeBalanceControllerOptions options;
    private final IBaseKVStoreClient storeClient;
    private final CommandRunner commandRunner;
    private final List<StoreBalancer> balancers = new ArrayList<>();
    private final ScheduledExecutorService executor;
    private final AtomicBoolean scheduling = new AtomicBoolean();
    private final AtomicReference<State> state = new AtomicReference<>(State.Init);
    private final boolean executorOwner;

    private MetricManager metricsManager;
    private Disposable descriptorSub;
    private ScheduledFuture scheduledFuture;

    public KVRangeBalanceController(IBaseKVStoreClient storeClient,
                                    KVRangeBalanceControllerOptions balancerOptions,
                                    ScheduledExecutorService executor) {
        this.options = balancerOptions.toBuilder()
            .balancers(
                balancerOptions.getBalancers().stream()
                    .distinct()
                    .collect(Collectors.toList())
            )
            .build();
        this.storeClient = storeClient;
        this.commandRunner = new CommandRunner(storeClient);
        executorOwner = executor == null;
        if (executor == null) {
            this.executor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ScheduledThreadPoolExecutor(1,
                    EnvProvider.INSTANCE.newThreadFactory("balance-executor-" + storeClient.clusterId())),
                "balance-executor-" + storeClient.clusterId());
        } else {
            this.executor = executor;
        }
    }

    public void start(String localStoreId) {
        if (state.compareAndSet(State.Init, State.Started)) {
            Map<String, IStoreBalancerFactory> balancerFactoryMap = BaseHookLoader.load(IStoreBalancerFactory.class);
            for (String factoryName : options.getBalancers()) {
                if (!balancerFactoryMap.containsKey(factoryName)) {
                    log.warn("[{}]There is no balancer factory named: {}", storeClient.clusterId(), factoryName);
                    continue;
                }
                StoreBalancer balancer = balancerFactoryMap.get(factoryName).newBalancer(localStoreId);
                balancers.add(balancer);
            }
            this.metricsManager = new MetricManager(localStoreId, storeClient.clusterId());
            log.debug("[{}]KVRangeBalanceController start to balance in store: {}", storeClient.clusterId(),
                localStoreId);
            descriptorSub = this.storeClient.describe()
                .distinctUntilChanged()
                .subscribe(sds -> executor.execute(() -> updateStoreDescriptors(sds)));
            scheduleLater();
        }
    }

    public void stop() {
        if (state.compareAndSet(State.Started, State.Closed)) {
            descriptorSub.dispose();
            if (scheduledFuture != null) {
                scheduledFuture.cancel(true);
            }
            if (executorOwner) {
                MoreExecutors.shutdownAndAwaitTermination(executor, 5, TimeUnit.SECONDS);
            }
        }
    }

    private void updateStoreDescriptors(Set<KVRangeStoreDescriptor> descriptors) {
        for (StoreBalancer balancer : balancers) {
            balancer.update(descriptors);
        }
        scheduleLater();
    }

    private void scheduleLater() {
        if (state.get() == State.Started && scheduling.compareAndSet(false, true)) {
            scheduledFuture =
                executor.schedule(this::scheduleNow, randomDelay(), TimeUnit.MILLISECONDS);
        }
    }

    private long randomDelay() {
        return ThreadLocalRandom.current()
            .nextLong(options.getScheduleIntervalInMs(), options.getScheduleIntervalInMs() * 2);
    }

    private void scheduleNow() {
        metricsManager.scheduleCount.increment();
        for (StoreBalancer fromBalancer : balancers) {
            try {
                Optional<BalanceCommand> commandOpt = fromBalancer.balance();
                if (commandOpt.isPresent()) {
                    BalanceCommand commandToRun = commandOpt.get();
                    log.info("[{}]Run command: {}", storeClient.clusterId(), commandToRun);
                    String balancerName = fromBalancer.getClass().getSimpleName();
                    String cmdName = commandToRun.getClass().getSimpleName();
                    Sample start = Timer.start();
                    CommandType commandType = commandToRun.type();
                    commandRunner.run(commandToRun)
                        .whenCompleteAsync((r, e) -> {
                            CommandMetrics metrics = metricsManager.getCommandMetrics(balancerName, cmdName);
                            if (r == CommandRunner.Result.Succeed) {
                                metrics.cmdSucceedCounter.increment();
                                start.stop(metrics.cmdRunTimer);
                                // Always schedule later after recovery command
                                if (commandType == CommandType.RECOVERY) {
                                    scheduling.set(false);
                                    scheduleLater();
                                } else {
                                    scheduleNow();
                                }
                            } else {
                                scheduling.set(false);
                                if (e != null) {
                                    log.error("[{}]Should not be here, error when run command", storeClient.clusterId(),
                                        e);
                                }
                                metrics.cmdFailedCounter.increment();
                                scheduleLater();
                            }
                        }, executor);
                    return;
                }
            } catch (Throwable e) {
                log.warn("[{}]Run balancer[{}] failed", storeClient.clusterId(), fromBalancer.getClass().getName(), e);
            }
        }
        // no command to run
        scheduling.set(false);
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
                    .register(io.micrometer.core.instrument.Metrics.globalRegistry);
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
