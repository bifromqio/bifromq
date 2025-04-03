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

package com.baidu.bifromq.inbox.server;

import com.baidu.bifromq.baseenv.EnvProvider;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
class DelayTaskRunner<KeyT extends Comparable<KeyT>, TaskT extends Runnable> {
    // sorted by deadlineTS and then inboxId
    private final TreeMap<SortKey<KeyT>, DelayedTask<TaskT>> sortedDeadlines;
    // key: inboxId, value: deadlineTS
    private final HashMap<KeyT, Long> deadLines = new HashMap<>();
    private final Supplier<Long> currentMillisSupplier;
    private final ScheduledExecutorService executor;
    private long nextTriggerTS;
    private ScheduledFuture<?> triggerTask;
    private volatile boolean isShutdown;

    DelayTaskRunner(Comparator<KeyT> comparator, Supplier<Long> currentMillisSupplier) {
        this.currentMillisSupplier = currentMillisSupplier;
        this.sortedDeadlines = new TreeMap<>(Comparator.comparingLong((SortKey<KeyT> sk) -> sk.deadlineTS)
            .thenComparing(sk -> sk.key, comparator));
        executor = Executors.newSingleThreadScheduledExecutor(
            EnvProvider.INSTANCE.newThreadFactory("deadline-trigger"));
    }

    /**
     * Register a task to be triggered after the specified delay.
     *
     * @param key           the key under monitoring
     * @param delayInterval the delay interval
     * @param task          the task to be triggered
     */
    public void reg(KeyT key, Duration delayInterval, TaskT task) {
        assert !delayInterval.isNegative();
        executor.submit(() -> {
            if (isShutdown) {
                return;
            }
            Long prevDeadlineTS = deadLines.get(key);
            if (prevDeadlineTS != null) {
                deadLines.remove(key);
                sortedDeadlines.remove(new SortKey<>(key, prevDeadlineTS));
            }
            if (delayInterval.isZero()) {
                // triggered immediately
                task.run();
                return;
            }
            long now = currentMillisSupplier.get();
            long deadlineTS = deadline(now, delayInterval);
            deadLines.put(key, deadlineTS);
            sortedDeadlines.put(new SortKey<>(key, deadlineTS), new DelayedTask<>(task, delayInterval));

            Map.Entry<SortKey<KeyT>, DelayedTask<TaskT>> firstEntry = sortedDeadlines.firstEntry();
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
     * Update the monitored deadline for the provided key.
     *
     * @param key the key under monitoring
     */
    public void touch(KeyT key) {
        executor.submit(() -> {
            if (isShutdown) {
                return;
            }
            Long prevDeadlineTS = deadLines.get(key);
            if (prevDeadlineTS == null) {
                return;
            }
            deadLines.remove(key);
            DelayedTask<TaskT> delayedTask = sortedDeadlines.remove(new SortKey<>(key, prevDeadlineTS));
            assert delayedTask != null;

            long now = currentMillisSupplier.get();
            long deadlineTS = deadline(now, delayedTask.delayInterval);
            deadLines.put(key, deadlineTS);
            sortedDeadlines.put(new SortKey<>(key, deadlineTS), delayedTask);
            Map.Entry<SortKey<KeyT>, DelayedTask<TaskT>> firstEntry = sortedDeadlines.firstEntry();

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
     * Unregister the monitored deadline for the provided key.
     *
     * @param key the key under monitoring
     */
    public void unreg(KeyT key) {
        executor.submit(() -> {
            if (isShutdown) {
                return;
            }
            Long prevDeadlineTS = deadLines.get(key);
            if (prevDeadlineTS == null) {
                return;
            }
            deadLines.remove(key);
            sortedDeadlines.remove(new SortKey<>(key, prevDeadlineTS));

            Map.Entry<SortKey<KeyT>, DelayedTask<TaskT>> firstEntry = sortedDeadlines.firstEntry();
            if (firstEntry == null) {
                nextTriggerTS = 0;
                if (triggerTask != null) {
                    triggerTask.cancel(true);
                    triggerTask = null;
                }
                return;
            }
            long earliestDeadline = firstEntry.getKey().deadlineTS;
            if (nextTriggerTS == 0 || earliestDeadline > nextTriggerTS) {
                // postpone trigger task
                if (triggerTask != null) {
                    triggerTask.cancel(true);
                }
                nextTriggerTS = earliestDeadline;
                long now = currentMillisSupplier.get();
                triggerTask = executor.schedule(this::trigger, earliestDeadline - now, TimeUnit.MILLISECONDS);
            }
        });
    }

    public void shutdown() {
        executor.submit(() -> {
            isShutdown = true;
            executor.shutdown();
        });
    }

    private void trigger() {
        long now = currentMillisSupplier.get();
        Map.Entry<SortKey<KeyT>, DelayedTask<TaskT>> entry;
        while ((entry = sortedDeadlines.firstEntry()) != null) {
            if (entry.getKey().deadlineTS <= now) {
                deadLines.remove(entry.getKey().key);
                sortedDeadlines.remove(entry.getKey());
                entry.getValue().task.run();
            } else {
                nextTriggerTS = entry.getKey().deadlineTS;
                triggerTask = executor.schedule(this::trigger, nextTriggerTS - now, TimeUnit.MILLISECONDS);
                return;
            }
        }
        // no deadlines
        nextTriggerTS = 0;
        triggerTask = null;
    }

    private long deadline(long now, Duration delayInterval) {
        return delayInterval.plusMillis(now).toMillis();
    }

    private record SortKey<K extends Comparable<K>>(K key, long deadlineTS) {
    }

    private record DelayedTask<T extends Runnable>(T task, Duration delayInterval) {

    }
}
