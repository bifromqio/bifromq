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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class Batcher3<Call, CallResult, BatcherKey> {
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
    private final Gauge pipelineDepthGauge;
    private final Gauge batchSaturationGauge;
    private final Timer batchExecTimer;
    private final DistributionSummary batchBuildTimeSummary;
    private final DistributionSummary batchSizeSummary;
    private final DistributionSummary queueingTimeSummary;
    private final Counter subSaturateCounter;

    protected Batcher3(BatcherKey batcherKey, String name, long maxTolerantLatencyNanos) {
        this.name = name;
        this.batcherKey = batcherKey;
        this.maxTolerantLatencyNanos = maxTolerantLatencyNanos;
        this.maxPipelineDepth = new AtomicLong(1);
        avgLatencyNanos = new MovingAverage(100, Duration.ofSeconds(1));

        subSaturateCounter = Counter.builder("batcher.subsat")
            .tags("name", name)
            .register(Metrics.globalRegistry);

        dropCounter = Counter.builder("batcher.call.drop.count")
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
        CallTask<Call, CallResult> headTask = callTaskBuffers.peek();
//        if (headTask == null || (System.nanoTime() - headTask.ts) > 60_000_000_000L) {
        CallTask<Call, CallResult> callTask = new CallTask<>(request);
        boolean offered = callTaskBuffers.offer(callTask);
        assert offered;
        trigger(false);
        return callTask.callResult;
//        } else {
//            dropCounter.increment();
//            return CompletableFuture.failedFuture(EXCEED_LIMIT);
//        }
    }

    public void close() {
        Metrics.globalRegistry.remove(subSaturateCounter);
        Metrics.globalRegistry.remove(dropCounter);
        Metrics.globalRegistry.remove(pipelineDepthGauge);
        Metrics.globalRegistry.remove(batchSaturationGauge);
        Metrics.globalRegistry.remove(batchExecTimer);
        Metrics.globalRegistry.remove(batchBuildTimeSummary);
        Metrics.globalRegistry.remove(batchSizeSummary);
        Metrics.globalRegistry.remove(queueingTimeSummary);
    }

    protected abstract BatchCall2<Call, CallResult> newBatch();

    private void trigger(boolean checkLatency) {
        if (triggering.compareAndSet(false, true)) {
            try {
                if (!callTaskBuffers.isEmpty()) {
                    // no batch call inflight or avgLatency is below tolerant value
                    if (pipelineDepth.get() == 0 ||
                        (!checkLatency || avgLatencyNanos.estimate() < maxTolerantLatencyNanos) &&
                            pipelineDepth.get() < maxPipelineDepth.get()) {
                        buildBatchAndEmit();
                    }
                }
            } catch (Throwable e) {
                log.error("Unexpected exception", e);
            } finally {
                triggering.set(false);
                if (!callTaskBuffers.isEmpty() &&
                    (!checkLatency || avgLatencyNanos.estimate() < maxTolerantLatencyNanos) &&
                    pipelineDepth.get() < maxPipelineDepth.get()) {
                    trigger(checkLatency);
                }
            }
        }
    }

    private void buildBatchAndEmit() {
        long buildStart = System.nanoTime();
        BatchCall2<Call, CallResult> batchCall = newBatch();
        int depth = pipelineDepth.incrementAndGet();
        int sat = 0;
        int maxSat = maxSaturation.get();
        int batchSize = 0;
        int lastBatchedCallWeight = 0;
//        while (sat <= maxSat) {
        while (true) {
            if (batchSize == 0) {
                CallTask<Call, CallResult> callTask = callTaskBuffers.poll();
                assert callTask != null;
                // make sure at lease one user call batched
                lastBatchedCallWeight = batchCall.weight(callTask.call);
                sat += lastBatchedCallWeight;
                batchCall.add(callTask);
                batchSize++;
                queueingTimeSummary.record(System.nanoTime() - callTask.ts);
            } else {
                CallTask<Call, CallResult> callTask = callTaskBuffers.poll();
                if (callTask != null) {
                    lastBatchedCallWeight = batchCall.weight(callTask.call);
                    // batch it anyway even though it may be a little overweight
                    batchCall.add(callTask);
                    batchSize++;
                    queueingTimeSummary.record(System.nanoTime() - callTask.ts);
                    sat += lastBatchedCallWeight;
                } else {
                    break;
                }
            }
        }
        if (!callTaskBuffers.isEmpty()) {
            subSaturateCounter.increment();
        }
        batchSizeSummary.record(batchSize);
        int finalBatchSize = batchSize;
        int finalS = sat;
        int finalW = lastBatchedCallWeight;
        long execStart = System.nanoTime();
        batchBuildTimeSummary.record((execStart - buildStart));
        batchCall.execute()
            .whenComplete((v, e) -> {
                long lat = System.nanoTime() - execStart;
                avgLatencyNanos.observe(lat);
                batchExecTimer.record(lat, TimeUnit.NANOSECONDS);
                long avgLatency = avgLatencyNanos.estimate();
                long currentMaxDepth = maxPipelineDepth.get();
                int currentMaxSat = maxSaturation.get();
                int pplnDepth = pipelineDepth.getAndDecrement();
                if (avgLatency > maxTolerantLatencyNanos) {
                    // make sure pipeline depth >=1
//                    maxPipelineDepth.compareAndSet(currentMaxDepth, Math.max(1, Math.min(currentMaxDepth, depth - 1)));
                    // decrease saturation by last call weight
                    maxSaturation.compareAndSet(currentMaxSat, Math.max(1, maxSat - finalW));
                } else {
//                    maxPipelineDepth.compareAndSet(currentMaxDepth, Math.max(depth, pplnDepth) + 1);
                    // update saturation
                    if (finalS == maxSat) {
                        maxSaturation.compareAndSet(currentMaxSat, Math.max(depth, finalS + 1));
                    } else if (finalS > maxSat) {
                        maxSaturation.compareAndSet(currentMaxSat, Math.max(depth, finalS));
                    }
                }
//                log.info("Trigger[{}]: depth={}, maxDepth={}, sat={}, maxSat={}, size={}, lat={}",
//                    avgLatency > maxTolerantLatencyNanos, pipelineDepth, maxPipelineDepth, finalS, maxSaturation,
//                    finalBatchSize, avgLatency);
                if (!callTaskBuffers.isEmpty()) {
                    trigger(false);
                }
            });
    }
}
