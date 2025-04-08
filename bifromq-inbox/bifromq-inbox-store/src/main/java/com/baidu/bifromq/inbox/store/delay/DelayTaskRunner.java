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

package com.baidu.bifromq.inbox.store.delay;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.inbox.storage.proto.Replica;
import com.google.common.util.concurrent.RateLimiter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * DelayTaskRunner is a utility class to schedule tasks to be executed after a specified delay.
 *
 * @param <KeyT> the type of the key used to identify the task.
 */
public class DelayTaskRunner<KeyT extends Comparable<KeyT>> implements IDelayTaskRunner<KeyT> {
    private final Replica owner;
    // sorted by deadlineTS and then inboxId
    private final NavigableMap<SortKey<KeyT>, IDelayedTask<KeyT>> sortedDeadlines;
    // key: inboxId, value: deadlineTS
    private final Map<KeyT, Long> deadLines = new ConcurrentHashMap<>();
    private final Supplier<Long> currentMillisSupplier;
    private final ScheduledExecutorService executor;
    private final RateLimiter rateLimiter;
    private long nextTriggerTS;
    private ScheduledFuture<?> triggerTask;
    private volatile boolean isShutdown;

    /**
     * Constructor for DelayTaskRunner.
     *
     * @param rangeId               the range ID
     * @param storeId               the store ID
     * @param comparator            the comparator to be used for sorting the keys
     * @param currentMillisSupplier a supplier to get the current time in milliseconds
     * @param rateLimit             the rate limit for triggering the scheduled tasks
     */
    public DelayTaskRunner(KVRangeId rangeId,
                           String storeId,
                           Comparator<KeyT> comparator,
                           Supplier<Long> currentMillisSupplier,
                           int rateLimit) {
        this.owner = Replica.newBuilder().setRangeId(rangeId).setStoreId(storeId).build();
        this.currentMillisSupplier = currentMillisSupplier;
        this.sortedDeadlines = new TreeMap<>(
            Comparator.comparingLong((SortKey<KeyT> sk) -> sk.deadlineTS).thenComparing(sk -> sk.key, comparator));
        this.rateLimiter = RateLimiter.create(rateLimit);
        executor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            Executors.newSingleThreadScheduledExecutor(EnvProvider.INSTANCE.newThreadFactory("delay-task-runner")),
            "delay-task-runner");
    }

    @Override
    public Replica owner() {
        return owner;
    }

    /**
     * Check if there is a task registered with the specified key.
     *
     * @param key the key to check
     * @return true if there is a task registered with the specified key, false otherwise
     */
    public boolean hasTask(KeyT key) {
        return deadLines.containsKey(key);
    }

    @Override
    public <TaskT extends IDelayedTask<KeyT>> void reschedule(KeyT key,
                                                              Supplier<TaskT> supplier,
                                                              Class<TaskT> taskClass) {
        if (isShutdown) {
            return;
        }
        executor.submit(() -> {
            if (isShutdown) {
                return;
            }
            Long prevDeadlineTS = deadLines.remove(key);
            long now = currentMillisSupplier.get();
            if (prevDeadlineTS != null) {
                IDelayedTask<KeyT> prevTaskState = sortedDeadlines.remove(new SortKey<>(key, prevDeadlineTS));
                if (!prevTaskState.getClass().equals(taskClass)) {
                    TaskT task = supplier.get();
                    long deadlineTS = deadline(now, task.getDelay());
                    deadLines.put(key, deadlineTS);
                    // if prevTask is of another type
                    sortedDeadlines.put(new SortKey<>(key, deadlineTS), task);
                } else {
                    Duration delayInterval = prevTaskState.getDelay();
                    long deadlineTS = deadline(now, delayInterval);
                    deadLines.put(key, deadlineTS);
                    // reuse previous task object
                    sortedDeadlines.put(new SortKey<>(key, deadlineTS), prevTaskState);
                }
            } else {
                IDelayedTask<KeyT> delayedTask = supplier.get();
                Duration delayInterval = delayedTask.getDelay();
                long deadlineTS = deadline(now, delayInterval);
                deadLines.put(key, deadlineTS);
                sortedDeadlines.put(new SortKey<>(key, deadlineTS), delayedTask);
            }

            Map.Entry<SortKey<KeyT>, IDelayedTask<KeyT>> firstEntry = sortedDeadlines.firstEntry();
            long earliestDeadline = firstEntry.getKey().deadlineTS;
            if (nextTriggerTS == 0 || earliestDeadline < nextTriggerTS) {
                // postpone trigger task
                if (triggerTask != null) {
                    triggerTask.cancel(true);
                }
                nextTriggerTS = earliestDeadline;
                triggerTask = executor.schedule(this::trigger, earliestDeadline - now, TimeUnit.MILLISECONDS);
            }
        });
    }

    @Override
    public <TaskT extends IDelayedTask<KeyT>> void schedule(KeyT key, TaskT delayedTask) {
        if (isShutdown) {
            return;
        }
        executor.submit(() -> {
            if (isShutdown) {
                return;
            }
            Long prevDeadlineTS = deadLines.remove(key);
            long now = currentMillisSupplier.get();
            if (prevDeadlineTS != null) {
                sortedDeadlines.remove(new SortKey<>(key, prevDeadlineTS));
            }
            Duration delayInterval = delayedTask.getDelay();
            long deadlineTS = deadline(now, delayInterval);
            deadLines.put(key, deadlineTS);
            sortedDeadlines.put(new SortKey<>(key, deadlineTS), delayedTask);

            Map.Entry<SortKey<KeyT>, IDelayedTask<KeyT>> firstEntry = sortedDeadlines.firstEntry();
            long earliestDeadline = firstEntry.getKey().deadlineTS;
            if (nextTriggerTS == 0 || earliestDeadline < nextTriggerTS) {
                // postpone trigger task
                if (triggerTask != null) {
                    triggerTask.cancel(true);
                }
                nextTriggerTS = earliestDeadline;
                triggerTask = executor.schedule(this::trigger, earliestDeadline - now, TimeUnit.MILLISECONDS);
            }
        });
    }

    /**
     * Schedule a new task to be triggered after the specified delay.
     *
     * @param key      the key under monitoring
     * @param supplier the task to be triggered
     * @param <TaskT>  the type of the task
     */
    public <TaskT extends IDelayedTask<KeyT>> void scheduleIfAbsent(KeyT key, Supplier<TaskT> supplier) {
        executor.submit(() -> {
            if (isShutdown) {
                return;
            }
            Long prevDeadlineTS = deadLines.get(key);
            if (prevDeadlineTS == null) {
                long now = currentMillisSupplier.get();
                IDelayedTask<KeyT> delayedTask = supplier.get();
                Duration delayInterval = delayedTask.getDelay();
                if (delayInterval.isZero()) {
                    // triggered immediately
                    delayedTask.run(key, this);
                    return;
                }
                long deadlineTS = deadline(now, delayInterval);
                deadLines.put(key, deadlineTS);
                sortedDeadlines.put(new SortKey<>(key, deadlineTS), delayedTask);
                Map.Entry<SortKey<KeyT>, IDelayedTask<KeyT>> firstEntry = sortedDeadlines.firstEntry();
                long earliestDeadline = firstEntry.getKey().deadlineTS;
                if (nextTriggerTS == 0 || earliestDeadline < nextTriggerTS) {
                    // postpone trigger task
                    if (triggerTask != null) {
                        triggerTask.cancel(true);
                    }
                    nextTriggerTS = earliestDeadline;
                    triggerTask = executor.schedule(this::trigger, earliestDeadline - now, TimeUnit.MILLISECONDS);
                }
            }
        });
    }

    /**
     * Shutdown the DelayTaskRunner.
     */
    public void shutdown() {
        executor.submit(() -> {
            isShutdown = true;
            executor.shutdown();
        });
    }

    private void trigger() {
        long now = currentMillisSupplier.get();
        Map.Entry<SortKey<KeyT>, IDelayedTask<KeyT>> entry;
        while ((entry = sortedDeadlines.firstEntry()) != null && rateLimiter.tryAcquire()) {
            SortKey<KeyT> sortKey = entry.getKey();
            IDelayedTask<KeyT> delayedTask = entry.getValue();
            if (entry.getKey().deadlineTS <= now) {
                deadLines.remove(entry.getKey().key);
                sortedDeadlines.remove(entry.getKey());
                delayedTask.run(sortKey.key, this);
            } else {
                nextTriggerTS = entry.getKey().deadlineTS;
                triggerTask = executor.schedule(this::trigger, nextTriggerTS - now, TimeUnit.MILLISECONDS);
                return;
            }
        }
        if (entry != null) {
            nextTriggerTS = entry.getKey().deadlineTS;
            long delay = Math.max(nextTriggerTS - now, 1000);
            triggerTask = executor.schedule(this::trigger, delay, TimeUnit.MILLISECONDS);
        } else {
            // no deadlines
            nextTriggerTS = 0;
            triggerTask = null;
        }
    }

    private long deadline(long now, Duration delayInterval) {
        return delayInterval.plusMillis(now).toMillis();
    }

    private record SortKey<K extends Comparable<K>>(K key, long deadlineTS) {
    }
}
