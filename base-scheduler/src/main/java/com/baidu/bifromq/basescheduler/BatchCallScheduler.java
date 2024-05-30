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

import com.baidu.bifromq.basescheduler.exception.AbortException;
import com.baidu.bifromq.basescheduler.exception.BatcherUnavailableException;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.netflix.concurrency.limits.internal.Preconditions;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BatchCallScheduler<Call, CallResult, BatcherKey>
    implements IBatchCallScheduler<Call, CallResult> {
    private static final int BATCHER_EXPIRY_SECONDS = 600;
    private final ICallScheduler<Call> callScheduler;
    private final long tolerableLatencyNanos;
    private final long burstLatencyNanos;
    private final LoadingCache<BatcherKey, Batcher<Call, CallResult, BatcherKey>> batchers;
    private final LongAdder runningCalls = new LongAdder();
    private final Gauge runningCallsGauge;
    private final Gauge batcherNumGauge;
    private final Counter callSchedCounter;
    private final Counter callSubmitCounter;

    public BatchCallScheduler(String name, Duration tolerableLatency, Duration burstLatency) {
        this(name, new ICallScheduler<>() {
        }, tolerableLatency, burstLatency, Duration.ofSeconds(BATCHER_EXPIRY_SECONDS));
    }

    public BatchCallScheduler(String name, Duration tolerableLatency, Duration burstLatency, Duration batcherExpiry) {
        this(name, new ICallScheduler<>() {
        }, tolerableLatency, burstLatency, batcherExpiry);
    }

    public BatchCallScheduler(String name,
                              ICallScheduler<Call> reqScheduler,
                              Duration tolerableLatency,
                              Duration burstLatency) {
        this(name, reqScheduler, tolerableLatency, burstLatency, Duration.ofSeconds(BATCHER_EXPIRY_SECONDS));
    }

    public BatchCallScheduler(String name,
                              ICallScheduler<Call> reqScheduler,
                              Duration tolerableLatency,
                              Duration burstLatency,
                              Duration batcherExpiry) {
        Preconditions.checkArgument(!tolerableLatency.isNegative() && !tolerableLatency.isZero(),
            "latency must be positive");
        Preconditions.checkArgument(!burstLatency.isNegative() && !burstLatency.isZero(),
            "latency must be positive");
        Preconditions.checkArgument(tolerableLatency.compareTo(burstLatency) <= 0,
            "tolerant latency must be shorter than burst latency");
        this.callScheduler = reqScheduler;
        this.tolerableLatencyNanos = tolerableLatency.toNanos();
        this.burstLatencyNanos = burstLatency.toNanos();
        batchers = Caffeine.newBuilder()
            .scheduler(Scheduler.systemScheduler())
            .expireAfterAccess(batcherExpiry)
            .evictionListener((RemovalListener<BatcherKey, Batcher<Call, CallResult, BatcherKey>>)
                (key, value, removalCause) -> {
                    if (value != null) {
                        value.close();
                    }
                })
            .build(k -> newBatcher(name, this.tolerableLatencyNanos, burstLatencyNanos, k).init());
        runningCallsGauge = Gauge.builder("batcher.call.running.gauge", runningCalls::sum)
            .tags("name", name)
            .register(Metrics.globalRegistry);
        batcherNumGauge = Gauge.builder("batcher.num", batchers::estimatedSize)
            .tags("name", name)
            .register(Metrics.globalRegistry);
        callSchedCounter = Counter.builder("batcher.call.sched.count")
            .tags("name", name)
            .register(Metrics.globalRegistry);
        callSubmitCounter = Counter.builder("batcher.call.submit.count")
            .tags("name", name)
            .register(Metrics.globalRegistry);
    }

    protected abstract Batcher<Call, CallResult, BatcherKey> newBatcher(String name,
                                                                        long tolerableLatencyNanos,
                                                                        long burstLatencyNanos,
                                                                        BatcherKey key);

    protected abstract Optional<BatcherKey> find(Call call);

    @Override
    public CompletableFuture<CallResult> schedule(Call request) {
        callSchedCounter.increment();
        runningCalls.increment();
        return callScheduler.submit(request)
            .thenCompose(req -> {
                try {
                    Optional<BatcherKey> batcherKey = find(req);
                    if (batcherKey.isPresent()) {
                        Batcher<Call, CallResult, BatcherKey> batcher = batchers.get(batcherKey.get());
                        callSubmitCounter.increment();
                        return batcher.submit(batcherKey.get(), req);
                    } else {
                        return CompletableFuture.failedFuture(new BatcherUnavailableException("Batcher not found"));
                    }
                } catch (Throwable e) {
                    log.error("Error", e);
                    return CompletableFuture.failedFuture(new AbortException("Failed to submit request", e));
                }
            })
            .whenComplete((v, e) -> runningCalls.decrement());
    }

    @Override
    public void close() {
        batchers.asMap().forEach((k, v) -> v.close());
        batchers.invalidateAll();
        Metrics.globalRegistry.remove(runningCallsGauge);
        Metrics.globalRegistry.remove(batcherNumGauge);
        Metrics.globalRegistry.remove(callSubmitCounter);
        Metrics.globalRegistry.remove(callSchedCounter);
    }
}
