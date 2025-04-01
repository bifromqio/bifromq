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

package com.baidu.bifromq.basescheduler;

import static com.baidu.bifromq.basescheduler.AsyncRetry.exec;

import com.baidu.bifromq.basescheduler.exception.BackPressureException;
import com.baidu.bifromq.basescheduler.exception.NeedRetryException;
import com.baidu.bifromq.basescheduler.exception.RetryTimeoutException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class Batcher<CallT, CallResultT, BatcherKeyT> {
    protected final String name;
    protected final BatcherKeyT batcherKey;
    private final Queue<IBatchCall<CallT, CallResultT, BatcherKeyT>> batchPool;
    private final Queue<ICallTask<CallT, CallResultT, BatcherKeyT>> callTaskBuffers = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean triggering = new AtomicBoolean();
    private final MovingAverage avgLatencyNanos;
    private final long tolerableLatencyNanos;
    private final long burstLatencyNanos;
    private final AtomicInteger pipelineDepth = new AtomicInteger();
    private final AtomicLong maxPipelineDepth;
    private final Counter dropCounter;
    private final Gauge batchSaturationGauge;
    private final Timer batchCallTimer;
    private final Timer batchExecTimer;
    private final Timer batchBuildTimer;
    private final DistributionSummary batchSizeSummary;
    private final DistributionSummary queueingTimeSummary;
    private final double alphaIncrease = 0.2;
    private final double alphaDecrease = 0.05;
    private final AtomicInteger emaMaxBatchSize = new AtomicInteger();
    private volatile int maxBatchSize = Integer.MAX_VALUE;

    protected Batcher(BatcherKeyT batcherKey, String name, long tolerableLatencyNanos, long burstLatencyNanos) {
        this(batcherKey, name, tolerableLatencyNanos, burstLatencyNanos, 1);
    }

    protected Batcher(BatcherKeyT batcherKey, String name, long tolerableLatencyNanos, long burstLatencyNanos,
                      int pipelineDepth) {
        this.name = name;
        this.batcherKey = batcherKey;
        this.tolerableLatencyNanos = tolerableLatencyNanos;
        this.burstLatencyNanos = burstLatencyNanos;
        this.maxPipelineDepth = new AtomicLong(pipelineDepth);
        avgLatencyNanos = new MovingAverage(100, Duration.ofSeconds(1));
        this.batchPool = new ArrayDeque<>(pipelineDepth);
        Tags tags = Tags.of("name", name, "key", Integer.toUnsignedString(System.identityHashCode(this)));
        dropCounter = Counter.builder("batcher.call.drop.count").tags(tags).register(Metrics.globalRegistry);
        batchSaturationGauge =
            Gauge.builder("batcher.saturation", () -> maxBatchSize).tags(tags).register(Metrics.globalRegistry);
        batchCallTimer = Timer.builder("batcher.call.time").tags(tags).register(Metrics.globalRegistry);
        batchExecTimer = Timer.builder("batcher.exec.time").tags(tags).register(Metrics.globalRegistry);
        batchBuildTimer = Timer.builder("batcher.build.time").tags(tags).register(Metrics.globalRegistry);
        batchSizeSummary =
            DistributionSummary.builder("batcher.batch.size").tags(tags).register(Metrics.globalRegistry);
        queueingTimeSummary =
            DistributionSummary.builder("batcher.queueing.time").tags(tags).register(Metrics.globalRegistry);
    }

    Batcher<CallT, CallResultT, BatcherKeyT> init() {
        for (int i = 0; i < maxPipelineDepth.get(); i++) {
            batchPool.offer(newBatch());
        }
        return this;
    }

    public final CompletableFuture<CallResultT> submit(BatcherKeyT batcherKey, CallT request) {
        if (avgLatencyNanos.estimate() < burstLatencyNanos) {
            ICallTask<CallT, CallResultT, BatcherKeyT> callTask = new CallTask<>(batcherKey, request);
            boolean offered = callTaskBuffers.offer(callTask);
            assert offered;
            trigger();
            return callTask.resultPromise();
        } else {
            dropCounter.increment();
            return CompletableFuture.failedFuture(new BackPressureException("Too high average latency"));
        }
    }

    public void close() {
        IBatchCall<CallT, CallResultT, BatcherKeyT> batchCall;
        while ((batchCall = batchPool.poll()) != null) {
            batchCall.destroy();
        }
        Metrics.globalRegistry.remove(dropCounter);
        Metrics.globalRegistry.remove(batchSaturationGauge);
        Metrics.globalRegistry.remove(batchCallTimer);
        Metrics.globalRegistry.remove(batchExecTimer);
        Metrics.globalRegistry.remove(batchBuildTimer);
        Metrics.globalRegistry.remove(batchSizeSummary);
        Metrics.globalRegistry.remove(queueingTimeSummary);
    }

    protected abstract IBatchCall<CallT, CallResultT, BatcherKeyT> newBatch();

    private void trigger() {
        if (triggering.compareAndSet(false, true)) {
            try {
                if (!callTaskBuffers.isEmpty() && pipelineDepth.get() < maxPipelineDepth.get()) {
                    batchAndEmit();
                }
            } catch (Throwable e) {
                log.error("Unexpected exception", e);
            } finally {
                triggering.set(false);
                if (!callTaskBuffers.isEmpty() && pipelineDepth.get() < maxPipelineDepth.get()) {
                    trigger();
                }
            }
        }
    }

    private void batchAndEmit() {
        pipelineDepth.incrementAndGet();
        long buildStart = System.nanoTime();
        IBatchCall<CallT, CallResultT, BatcherKeyT> batchCall = batchPool.poll();
        assert batchCall != null;
        int batchSize = 0;
        LinkedList<ICallTask<CallT, CallResultT, BatcherKeyT>> batchedTasks = new LinkedList<>();
        ICallTask<CallT, CallResultT, BatcherKeyT> callTask;
        while (batchSize < maxBatchSize && (callTask = callTaskBuffers.poll()) != null) {
            queueingTimeSummary.record(System.nanoTime() - callTask.ts());
            batchCall.add(callTask);
            batchedTasks.add(callTask);
            batchSize++;
        }
        batchSizeSummary.record(batchSize);
        long begin = System.nanoTime();
        batchBuildTimer.record(begin - buildStart, TimeUnit.NANOSECONDS);
        AtomicLong execStart = new AtomicLong();
        AtomicInteger execCounter = new AtomicInteger();
        final int finalBatchSize = batchSize;
        exec(() -> {
            execCounter.incrementAndGet();
            execStart.set(System.nanoTime());
            return batchCall.execute();
        }, (v, t) -> {
            if (t != null) {
                return !(t instanceof NeedRetryException || t.getCause() instanceof NeedRetryException);
            }
            return true;
        }, tolerableLatencyNanos >> 1, burstLatencyNanos << 1)
            .whenComplete((v, e) -> {
                long execEnd = System.nanoTime();
                if (e != null) {
                    if (e instanceof RetryTimeoutException || e.getCause() instanceof RetryTimeoutException) {
                        log.debug("Batchcall timeout", e);
                    } else {
                        log.error("Unexpected exception during handling batchcall result", e);
                    }
                    batchedTasks.forEach(t -> t.resultPromise().completeExceptionally(e));
                    // reset max batch size
                    maxBatchSize = 1;
                } else {
                    long thisLatency = execEnd - execStart.get();
                    if (thisLatency > 0) {
                        updateMaxBatchSize(finalBatchSize, thisLatency);
                    }
                    batchExecTimer.record(thisLatency, TimeUnit.NANOSECONDS);
                    if (execCounter.get() > 1) {
                        // do not observe retry latency
                        batchedTasks.forEach(t -> {
                            long callLatency = execEnd - t.ts();
                            batchCallTimer.record(callLatency, TimeUnit.NANOSECONDS);
                        });
                    } else {
                        batchedTasks.forEach(t -> {
                            long callLatency = execEnd - t.ts();
                            avgLatencyNanos.observe(callLatency);
                            batchCallTimer.record(callLatency, TimeUnit.NANOSECONDS);
                        });
                    }
                }
                batchCall.reset();
                batchPool.offer(batchCall);
                pipelineDepth.getAndDecrement();
                if (!callTaskBuffers.isEmpty()) {
                    trigger();
                }
            });
    }

    private void updateMaxBatchSize(int batchSize, long latency) {
        int calculatedMaxBatchSize = latency == tolerableLatencyNanos ? batchSize :
            Math.max(1, (int) (batchSize * tolerableLatencyNanos * 1.0 / latency));
        maxBatchSize = Math.max(1, emaMaxBatchSize.updateAndGet(old -> {
            if (old == 0) {
                return calculatedMaxBatchSize;
            }
            double alpha = calculatedMaxBatchSize > old ? alphaIncrease : alphaDecrease;
            return (int) (alpha * calculatedMaxBatchSize + (1 - alpha) * old);
        }));
    }
}
