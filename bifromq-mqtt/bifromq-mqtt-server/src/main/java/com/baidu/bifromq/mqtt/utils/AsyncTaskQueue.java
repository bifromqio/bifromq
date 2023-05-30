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

package com.baidu.bifromq.mqtt.utils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class AsyncTaskQueue {
    private static final CompletableFuture<Void> DONE = CompletableFuture.completedFuture(null);
    private final ConcurrentLinkedQueue<Supplier<CompletableFuture<Void>>> taskQueue;
    private final Executor executor;
    private final AtomicReference<State> state = new AtomicReference<>(State.EMPTY_STOP);
    private volatile CompletableFuture<Void> whenDone = DONE;

    public AsyncTaskQueue(Executor executor) {
        this.taskQueue = new ConcurrentLinkedQueue<>();
        this.executor = executor;
    }

    public void add(Supplier<CompletableFuture<Void>> taskSupplier) {
        while (true) {
            if (state.compareAndSet(State.EMPTY_STOP, State.NONEMPTY_STOP)) {
                taskQueue.add(taskSupplier);
                whenDone = new CompletableFuture<>();
                executor.execute(this::runTask);
                break;
            }
            if (state.get() == State.NONEMPTY_STOP || state.get() == State.NONEMPTY_RUNNING) {
                taskQueue.add(taskSupplier);
                break;
            }
            if (state.compareAndSet(State.EMPTY_RUNNING, State.NONEMPTY_RUNNING)) {
                taskQueue.add(taskSupplier);
                break;
            }
        }
    }

    private void runTask() {
        while (true) {
            if (state.compareAndSet(State.NONEMPTY_STOP, State.NONEMPTY_RUNNING)) {
                Supplier<CompletableFuture<Void>> taskSupplier = taskQueue.poll();
                executor.execute(() -> taskSupplier.get().whenCompleteAsync((v, e) -> this.runTask(), executor));
                break;
            }
            if (state.get() == State.NONEMPTY_RUNNING) {
                Supplier<CompletableFuture<Void>> taskSupplier = taskQueue.poll();
                if (taskSupplier != null) {
                    executor.execute(() -> taskSupplier.get().whenCompleteAsync((v, e) -> this.runTask(), executor));
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

    public void awaitDone() {
        whenDone.join();
    }

    private enum State {
        EMPTY_STOP,
        NONEMPTY_STOP,
        NONEMPTY_RUNNING,
        EMPTY_RUNNING
    }
}
