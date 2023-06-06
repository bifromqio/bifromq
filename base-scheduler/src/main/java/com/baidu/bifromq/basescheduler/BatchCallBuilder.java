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
import com.netflix.concurrency.limits.limit.Gradient2Limit;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;
import lombok.SneakyThrows;
import org.jctools.queues.MpscArrayQueue;
import org.jctools.queues.atomic.MpscUnboundedAtomicArrayQueue;

public abstract class BatchCallBuilder<Req, Resp> {
    private static final int PREFLIGHT_BATCHES = 1024;
    private final StampedLock rwLock = new StampedLock();
    private final Gradient2Limit inflightLimiter;
    private final AtomicInteger inflightCalls = new AtomicInteger();
    private final AtomicBoolean calling = new AtomicBoolean();
    private final MetricsMeter meter;
    private final Queue<MonitoredBatchCall> preflightBatches = new ArrayDeque<>(PREFLIGHT_BATCHES);
    private final Queue<MonitoredBatchCall> reusableBatches = new MpscArrayQueue<>(64);
    private volatile MonitoredBatchCall currentBatchRef;

    protected BatchCallBuilder(String name, int maxInflights) {
        this.meter = new MetricsMeter(name);
        this.inflightLimiter = Gradient2Limit.newBuilder()
            .rttTolerance(2.0)
            .minLimit(1)
            .initialLimit(Math.max(1, maxInflights / 2))
            .maxConcurrency(maxInflights)
            .build();
        this.currentBatchRef = new MonitoredBatchCall();
    }

    @SneakyThrows
    CompletableFuture<Resp> submit(Req request) {
        long stamp = rwLock.readLock();
        try {
            return currentBatchRef.add(request);
        } finally {
            rwLock.unlockRead(stamp);
            if (currentBatchRef.isEnough()) {
                boolean offered = false;
                stamp = rwLock.writeLock();
                try {
                    if (currentBatchRef.isEnough()) {
                        offered = preflightBatches.offer(currentBatchRef);
                        assert offered;
                        MonitoredBatchCall newCall = reusableBatches.poll();
                        if (newCall != null) {
                            newCall.reset();
                        } else {
                            newCall = new MonitoredBatchCall();
                        }
                        currentBatchRef = newCall;
                    }
                } finally {
                    rwLock.unlockWrite(stamp);
                    if (offered) {
                        meter.queueDepthSummary.record(preflightBatches.size());
                    }
                }
            }
            trigger();
        }
    }

    @SneakyThrows
    private void trigger() {
        if (calling.compareAndSet(false, true)) {
            if (inflightCalls.get() < inflightLimiter.getLimit()) {
                MonitoredBatchCall batchCall = preflightBatches.poll();
                if (batchCall != null) {
                    inflightCalls.incrementAndGet();
                    meter.queueDepthSummary.record(preflightBatches.size());
                    Timer.Sample initTimeSample = Timer.start();
                    CompletableFuture<Void> task = batchCall.execute();
                    initTimeSample.stop(meter.batchInitTimer);
                    long start = System.nanoTime();
                    task.whenComplete((v, e) -> {
                        long processingTime = System.nanoTime() - start;
                        // never throws
                        inflightCalls.decrementAndGet();
                        inflightLimiter.onSample(0, processingTime, inflightCalls.get(), false);
                        meter.batchExecTimer.record(processingTime, TimeUnit.NANOSECONDS);
                        // try to reuse
                        boolean reused = reusableBatches.offer(batchCall);
                        if (!reused) {
                            meter.throwAwayCounter.increment(1);
                        }
                        trigger();
                    });
                } else if (!currentBatchRef.isEmpty()) {
                    // steal current batch and fire it right away
                    boolean offered = false;
                    long stamp = rwLock.writeLock();
                    try {
                        if (!currentBatchRef.isEmpty()) {
                            offered = preflightBatches.offer(currentBatchRef);
                            assert offered;
                            MonitoredBatchCall newCall = reusableBatches.poll();
                            if (newCall != null) {
                                newCall.reset();
                            } else {
                                newCall = new MonitoredBatchCall();
                            }
                            currentBatchRef = newCall;
                        }
                    } finally {
                        rwLock.unlockWrite(stamp);
                        if (offered) {
                            meter.queueDepthSummary.record(preflightBatches.size());
                        }
                    }
                }
            } else if (currentBatchRef.isEnough()) {
                boolean offered = false;
                long stamp = rwLock.writeLock();
                try {
                    if (currentBatchRef.isEnough()) {
                        offered = preflightBatches.offer(currentBatchRef);
                        assert offered;
                        MonitoredBatchCall newCall = reusableBatches.poll();
                        if (newCall != null) {
                            newCall.reset();
                        } else {
                            newCall = new MonitoredBatchCall();
                        }
                        currentBatchRef = newCall;
                    }
                } finally {
                    rwLock.unlockWrite(stamp);
                    if (offered) {
                        meter.queueDepthSummary.record(preflightBatches.size());
                    }
                }
            }
            calling.set(false);
            if (inflightCalls.get() < inflightLimiter.getLimit() &&
                (!preflightBatches.isEmpty() || !currentBatchRef.isEmpty())) {
                trigger();
            }
        }
    }

    /**
     * Return a new empty batch
     *
     * @return an empty batch
     */
    public abstract IBatchCall<Req, Resp> newBatch();

    public void close() {
        meter.close();
    }

    private class MonitoredBatchCall implements IBatchCall<Req, Resp> {
        private final AtomicInteger batchSize = new AtomicInteger();
        private final MpscUnboundedAtomicArrayQueue<Long> batchTime = new MpscUnboundedAtomicArrayQueue<>(512);
        private final IBatchCall<Req, Resp> delegate;

        private MonitoredBatchCall() {
            this.delegate = newBatch();
        }

        public boolean isEmpty() {
            return batchTime.isEmpty();
        }

        @Override
        public boolean isEnough() {
            return delegate.isEnough();
        }

        @Override
        public CompletableFuture<Resp> add(Req request) {
            if (preflightBatches.size() >= PREFLIGHT_BATCHES) {
                meter.dropCounter.increment();
                return CompletableFuture.failedFuture(DropException.EXCEED_LIMIT);

            }
            long now = System.nanoTime();
            batchTime.add(now);
            batchSize.incrementAndGet();
            return delegate.add(request)
                .whenComplete(
                    (v, e) -> meter.callLatencyTimer.record(System.nanoTime() - now, TimeUnit.NANOSECONDS));
        }

        @Override
        public void reset() {
            batchSize.set(0);
            delegate.reset();
            assert delegate.isEmpty();
            assert !delegate.isEnough();
        }

        @Override
        public CompletableFuture<Void> execute() {
            meter.batchSizeSummary.record(batchSize.get());
            Long start;
            while ((start = batchTime.poll()) != null) {
                meter.callQueuingTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            }
            return delegate.execute();
        }
    }

    private static class MetricsMeter {
        final DistributionSummary queueDepthSummary;
        final Counter throwAwayCounter;
        final Counter dropCounter;
        final DistributionSummary batchSizeSummary;
        final Timer callLatencyTimer;
        final Timer batchInitTimer;
        final Timer batchExecTimer;
        final Timer callQueuingTimer;

        MetricsMeter(String name) {
            queueDepthSummary = DistributionSummary.builder("batch.queue.depth")
                .tags("name", name)
                .register(Metrics.globalRegistry);
            throwAwayCounter = Counter.builder("batch.task.throwaway.count")
                .tags("name", name)
                .register(Metrics.globalRegistry);
            dropCounter = Counter.builder("batch.task.drop.count")
                .tags("name", name)
                .register(Metrics.globalRegistry);
            batchSizeSummary = DistributionSummary.builder("batch.task.summary")
                .tags("name", name)
                .register(Metrics.globalRegistry);
            callLatencyTimer = Timer.builder("batch.task.call.time")
                .tags("name", name)
                .register(Metrics.globalRegistry);
            batchInitTimer = Timer.builder("batch.task.init.time")
                .tags("name", name)
                .register(Metrics.globalRegistry);
            batchExecTimer = Timer.builder("batch.task.exec.time")
                .tags("name", name)
                .register(Metrics.globalRegistry);
            callQueuingTimer = Timer.builder("batch.task.queuing.time")
                .tags("name", name)
                .register(Metrics.globalRegistry);
        }

        void close() {
            Metrics.globalRegistry.remove(queueDepthSummary);
            Metrics.globalRegistry.remove(throwAwayCounter);
            Metrics.globalRegistry.remove(dropCounter);
            Metrics.globalRegistry.remove(batchSizeSummary);
            Metrics.globalRegistry.remove(callLatencyTimer);
            Metrics.globalRegistry.remove(batchInitTimer);
            Metrics.globalRegistry.remove(batchExecTimer);
            Metrics.globalRegistry.remove(callQueuingTimer);
        }
    }
}
