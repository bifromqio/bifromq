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

package com.baidu.bifromq.basekv.store.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class AsyncRunner {
    private static final CompletableFuture<Void> DONE = CompletableFuture.completedFuture(null);
    private final ConcurrentLinkedDeque<Task<?>> taskQueue;
    private final Executor executor;
    private final AtomicReference<State> state = new AtomicReference<>(State.EMPTY_STOP);
    private volatile CompletableFuture<Void> whenDone = DONE;

    public AsyncRunner(Executor executor) {
        this.taskQueue = new ConcurrentLinkedDeque<>();
        this.executor = executor;
    }

    public CompletableFuture<Void> add(Runnable runnable) {
        return add(runnable, false);
    }

    public <T> CompletableFuture<T> add(Supplier<CompletableFuture<T>> taskSupplier) {
        return add(taskSupplier, false);
    }

    private CompletableFuture<Void> add(Runnable runnable, boolean first) {
        return add(() -> {
            CompletableFuture<Void> onDone = new CompletableFuture<>();
            if (onDone.isCancelled()) {
                return CompletableFuture.completedFuture(null);
            }
            try {
                runnable.run();
            } catch (Throwable e) {
                onDone.completeExceptionally(e);
            } finally {
                onDone.complete(null);
            }
            return onDone;
        }, first);
    }

    private <T> CompletableFuture<T> add(Supplier<CompletableFuture<T>> taskSupplier, boolean first) {
        CompletableFuture<T> f = new CompletableFuture<>();
        if (first) {
            taskQueue.addFirst(new Task<>(taskSupplier, f));
        } else {
            taskQueue.addLast(new Task<>(taskSupplier, f));
        }
        while (true) {
            if (state.compareAndSet(State.EMPTY_STOP, State.NONEMPTY_STOP)) {
                whenDone = new CompletableFuture<>();
                executor.execute(this::runTask);
                break;
            }
            if (state.get() == State.NONEMPTY_STOP || state.get() == State.NONEMPTY_RUNNING) {
                break;
            }
            if (state.compareAndSet(State.EMPTY_RUNNING, State.NONEMPTY_RUNNING)) {
                break;
            }
        }
        return f;
    }

    public CompletableFuture<Void> addFirst(Runnable runnable) {
        return add(runnable, true);
    }

    public <T> CompletableFuture<T> addFirst(Supplier<CompletableFuture<T>> taskSupplier) {
        return add(taskSupplier, true);
    }

    public void cancelAll() {
        taskQueue.descendingIterator().forEachRemaining(t -> t.future.cancel(true));
    }

    private void runTask() {
        while (true) {
            if (state.compareAndSet(State.NONEMPTY_STOP, State.NONEMPTY_RUNNING)
                || state.get() == State.NONEMPTY_RUNNING) {
                Task task = taskQueue.peek();
                if (task != null) {
                    Supplier<CompletableFuture<?>> taskSupplier = task.supplier;
                    executor.execute(() -> {
                        if (task.future.isDone()) {
                            taskQueue.removeFirstOccurrence(task);
                            runTask();
                            return;
                        }
                        try {
                            CompletableFuture<?> taskFuture = taskSupplier.get();
                            task.future.whenCompleteAsync((v, e) -> {
                                if (task.future.isCancelled()) {
                                    taskFuture.cancel(true);
                                }
                            }, executor);
                            taskFuture.whenCompleteAsync((v, e) -> {
                                if (!task.future.isDone()) {
                                    if (e != null) {
                                        task.future.completeExceptionally(e);
                                    } else {
                                        task.future.complete(v);
                                    }
                                }
                                taskQueue.removeFirstOccurrence(task);
                                runTask();
                            }, executor);
                        } catch (Throwable e) {
                            task.future.completeExceptionally(e);
                            taskQueue.removeFirstOccurrence(task);
                            runTask();
                        }
                    });
                    break;
                } else if (state.compareAndSet(State.NONEMPTY_RUNNING, State.EMPTY_RUNNING)) {
                    if (!taskQueue.isEmpty()) {
                        state.set(State.NONEMPTY_RUNNING);
                    }
                    executor.execute(this::runTask);
                    break;
                }
            }
            if (state.compareAndSet(State.EMPTY_RUNNING, State.EMPTY_STOP)) {
                whenDone.complete(null);
                whenDone = DONE;
                break;
            }
        }
    }

    public CompletionStage<Void> awaitDone() {
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        whenDone.whenComplete((v, e) -> onDone.complete(null));
        return onDone;
    }

    @AllArgsConstructor
    private static class Task<T> {
        final Supplier<CompletableFuture<T>> supplier;
        final CompletableFuture<T> future;
    }

    private enum State {
        EMPTY_STOP,
        NONEMPTY_STOP,
        NONEMPTY_RUNNING,
        EMPTY_RUNNING
    }
}
