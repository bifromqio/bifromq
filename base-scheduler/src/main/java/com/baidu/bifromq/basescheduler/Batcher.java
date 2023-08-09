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

import com.baidu.bifromq.basescheduler.exception.DropException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
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
public abstract class Batcher<Call, CallResult, BatcherKey> {
    private final Queue<CallTask<Call, CallResult>> callTaskBuffers = new ConcurrentLinkedQueue<>();
    protected final String name;
    protected final BatcherKey batcherKey;
    private final AtomicBoolean triggering = new AtomicBoolean();
    private final MovingAverage avgLatencyNanos;
    private final long maxTolerantLatencyNanos;
    private final AtomicInteger pipelineDepth = new AtomicInteger();
    private final AtomicInteger maxSaturation = new AtomicInteger(1);
    private final AtomicLong maxPipelineDepth;
    private final Counter dropCounter;
    private final Gauge avgLatencyGauge;
    private final Gauge pipelineDepthGauge;
    private final Gauge batchSaturationGauge;
    private final Timer batchExecTimer;
    private final DistributionSummary batchBuildTimeSummary;
    private final DistributionSummary batchSizeSummary;
    private final DistributionSummary queueingTimeSummary;

    protected Batcher(BatcherKey batcherKey, String name, long maxTolerantLatencyNanos) {
        this.name = name;
        this.batcherKey = batcherKey;
        this.maxTolerantLatencyNanos = maxTolerantLatencyNanos;
        this.maxPipelineDepth = new AtomicLong(1);
        avgLatencyNanos = new MovingAverage(100, Duration.ofSeconds(1));

        dropCounter = Counter.builder("batcher.call.drop.count")
            .tags("name", name)
            .register(Metrics.globalRegistry);
        avgLatencyGauge = Gauge.builder("batcher.call.avglatency", avgLatencyNanos::estimate)
            .tags("name", name)
            .register(Metrics.globalRegistry);
        pipelineDepthGauge = Gauge.builder("batcher.pipeline.depth", pipelineDepth::get)
            .tags("name", name)
            .register(Metrics.globalRegistry);
        batchSaturationGauge = Gauge.builder("batcher.saturation", maxSaturation::get)
            .tags("name", name)
            .register(Metrics.globalRegistry);
        batchExecTimer = Timer.builder("batcher.exec.time")
            .tags("name", name)
            .register(Metrics.globalRegistry);
        batchBuildTimeSummary = DistributionSummary.builder("batcher.build.time")
            .tags("name", name)
            .register(Metrics.globalRegistry);
        batchSizeSummary = DistributionSummary.builder("batcher.batch.size")
            .tags("name", name)
            .register(Metrics.globalRegistry);
        queueingTimeSummary = DistributionSummary.builder("batcher.queueing.time")
            .tags("name", name)
            .register(Metrics.globalRegistry);
    }


    public final CompletableFuture<CallResult> submit(Call request) {
        if (avgLatencyNanos.estimate() < maxTolerantLatencyNanos) {
            CallTask<Call, CallResult> callTask = new CallTask<>(request);
            boolean offered = callTaskBuffers.offer(callTask);
            assert offered;
            trigger();
            return callTask.callResult;
        } else {
            dropCounter.increment();
            return CompletableFuture.failedFuture(DropException.EXCEED_LIMIT);
        }
    }

    public void close() {
        Metrics.globalRegistry.remove(dropCounter);
        Metrics.globalRegistry.remove(avgLatencyGauge);
        Metrics.globalRegistry.remove(pipelineDepthGauge);
        Metrics.globalRegistry.remove(batchSaturationGauge);
        Metrics.globalRegistry.remove(batchExecTimer);
        Metrics.globalRegistry.remove(batchBuildTimeSummary);
        Metrics.globalRegistry.remove(batchSizeSummary);
        Metrics.globalRegistry.remove(queueingTimeSummary);
    }

    protected abstract BatchCall2<Call, CallResult> newBatch();

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
        long buildStart = System.nanoTime();
        BatchCall2<Call, CallResult> batchCall = newBatch();
        pipelineDepth.incrementAndGet();
        int batchSize = 0;
        LinkedList<CallTask<Call, CallResult>> batchedTasks = new LinkedList<>();
        CallTask<Call, CallResult> callTask;
        while ((callTask = callTaskBuffers.poll()) != null) {
            batchCall.add(callTask);
            batchedTasks.add(callTask);
            batchSize++;
            queueingTimeSummary.record(System.nanoTime() - callTask.ts);
        }
        batchSizeSummary.record(batchSize);
        long execStart = System.nanoTime();
        batchBuildTimeSummary.record((execStart - buildStart));
        batchCall.execute()
            .whenComplete((v, e) -> {
                long execEnd = System.nanoTime();
                batchedTasks.forEach(t -> avgLatencyNanos.observe(execEnd - t.ts));
                batchExecTimer.record(execEnd - execStart, TimeUnit.NANOSECONDS);
                pipelineDepth.getAndDecrement();
                if (!callTaskBuffers.isEmpty()) {
                    trigger();
                }
            });
    }
}
