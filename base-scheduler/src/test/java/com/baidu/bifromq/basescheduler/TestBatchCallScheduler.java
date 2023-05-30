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

package com.baidu.bifromq.basescheduler;

import java.time.Duration;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestBatchCallScheduler extends BatchCallScheduler<Integer, Integer, Integer> {
    private final int queueNum;
    private final Duration callDelay;

    public TestBatchCallScheduler(int queues, Duration callDelay) {
        super("test_batch_call");
        this.queueNum = queues;
        this.callDelay = callDelay;
    }

    @Override
    protected BatchCallBuilder<Integer, Integer> newBuilder(String name, int maxInflights, Integer batchKey) {
        return new TestBatchCallerBuilder(name, maxInflights);
    }

    @Override
    protected Optional<Integer> find(Integer request) {
        return Optional.of(ThreadLocalRandom.current().nextInt(queueNum));
    }

    public class TestBatchCallerBuilder extends BatchCallBuilder<Integer, Integer> {
        @AllArgsConstructor
        public class Task {
            final int req;
            final CompletableFuture<Integer> onDone = new CompletableFuture<>();
        }

        public class TestBatchCall implements IBatchCall<Integer, Integer> {
            private final AtomicInteger count = new AtomicInteger();
            private final Queue<Task> batch = new ConcurrentLinkedQueue<>();
            private CompletableFuture<Void> onBatchDone = new CompletableFuture<>();

            @Override
            public boolean isEmpty() {
                return batch.isEmpty();
            }

            @Override
            public boolean isEnough() {
                return count.get() >= 10;
            }

            @Override
            public CompletableFuture<Integer> add(Integer request) {
                Task task = new Task(request);
                batch.add(task);
                count.incrementAndGet();
                return task.onDone;
            }

            @Override
            public void reset() {
                count.set(0);
                batch.clear();
                onBatchDone = new CompletableFuture<>();
            }

            @Override
            public CompletableFuture<Void> execute() {
                calls.add(this);
                exec();
                return onBatchDone;
            }
        }

        private final AtomicBoolean executing = new AtomicBoolean();
        private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        private final ConcurrentLinkedQueue<TestBatchCall> calls;


        public TestBatchCallerBuilder(String name, int maxInflights) {
            super(name, maxInflights);
            this.calls = new ConcurrentLinkedQueue<>();
        }

        @Override
        public IBatchCall newBatch() {
            return new TestBatchCall();
        }

        @Override
        public void close() {
            executor.shutdown();
        }

        private void exec() {
            if (executing.compareAndSet(false, true)) {
                executor.execute(this::run);
            }
        }

        private void run() {
            TestBatchCall call = calls.poll();
            if (call != null) {
                executor.schedule(() -> {
                    for (Task task : call.batch) {
                        task.onDone.complete(task.req);
                    }
                    call.onBatchDone.complete(null);
                    executing.set(false);
                    if (!calls.isEmpty()) {
                        exec();
                    }
                }, callDelay.toMillis(), TimeUnit.MILLISECONDS);
            }
        }
    }
}
