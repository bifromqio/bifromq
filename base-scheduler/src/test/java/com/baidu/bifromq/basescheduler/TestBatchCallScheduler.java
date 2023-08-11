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
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestBatchCallScheduler extends BatchCallScheduler<Integer, Integer, Integer> {
    private final int queueNum;
    private final Duration callDelay;

    public TestBatchCallScheduler(int queues, Duration callDelay) {
        this(queues, callDelay, callDelay, callDelay.multipliedBy(2));
    }

    public TestBatchCallScheduler(int queues,
                                  Duration callDelay,
                                  Duration tolerableLatency,
                                  Duration burstLatency) {
        super("test_batch_call", tolerableLatency, burstLatency);
        this.queueNum = queues;
        this.callDelay = callDelay;
    }

    @Override
    protected Batcher<Integer, Integer, Integer> newBatcher(String name,
                                                            long tolerableLatencyNanos,
                                                            long burstLatencyNanos,
                                                            Integer integer) {
        return new TestBatcher(integer, name, burstLatencyNanos);
    }

    @Override
    protected Optional<Integer> find(Integer request) {
        return Optional.of(ThreadLocalRandom.current().nextInt(queueNum));
    }

    public class TestBatcher extends Batcher<Integer, Integer, Integer> {
        public class TestBatchCall implements IBatchCall<Integer, Integer> {
            private final AtomicInteger count = new AtomicInteger();
            private final Queue<CallTask<Integer, Integer>> batch = new ConcurrentLinkedQueue<>();
            private CompletableFuture<Void> onBatchDone = new CompletableFuture<>();

            @Override
            public void reset() {
                count.set(0);
                batch.clear();
                onBatchDone = new CompletableFuture<>();
            }

            @Override
            public void add(CallTask<Integer, Integer> callTask) {
                batch.add(callTask);
                count.incrementAndGet();
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
        private final ScheduledExecutorService executor1 = Executors.newSingleThreadScheduledExecutor();
        private final ConcurrentLinkedQueue<TestBatchCall> calls;


        protected TestBatcher(Integer integer, String name, long burstLatencyNanos) {
            super(integer, name, burstLatencyNanos / 2, burstLatencyNanos);
            this.calls = new ConcurrentLinkedQueue<>();
        }

        @Override
        public IBatchCall<Integer, Integer> newBatch() {
            return new TestBatchCall();
        }

        @Override
        public void close() {
            super.close();
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
                    for (CallTask<Integer, Integer> task : call.batch) {
                        task.callResult.complete(task.call);
                    }
                    executor1.execute(() -> {
                        call.onBatchDone.complete(null);
                    });
                    executing.set(false);
                    if (!calls.isEmpty()) {
                        exec();
                    }
                }, callDelay.toMillis(), TimeUnit.MILLISECONDS);
            }
        }
    }
}
