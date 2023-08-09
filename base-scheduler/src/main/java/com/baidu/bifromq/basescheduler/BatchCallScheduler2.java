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
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BatchCallScheduler2<Call, CallResult, BatcherKey>
    implements IBatchCallScheduler<Call, CallResult> {
    private static final int BATCHER_EXPIRY_SECONDS = 3000;
    private final ICallScheduler<Call> callScheduler;
    private final long maxTolerantLatencyNanos;
    private final LoadingCache<BatcherKey, Batcher<Call, CallResult, BatcherKey>> batchers;
    private final Gauge batcherNumGauge;
    private final Counter callSchedCounter;
    private final Counter callSubmitCounter;

    public BatchCallScheduler2(String name, Duration maxTolerantLatency) {
        this(name, new ICallScheduler<>() {
        }, maxTolerantLatency);
    }

    public BatchCallScheduler2(String name, ICallScheduler<Call> reqScheduler, Duration maxTolerantLatency) {
        this.callScheduler = reqScheduler;
        this.maxTolerantLatencyNanos = maxTolerantLatency.toNanos();
        batchers = Caffeine.newBuilder()
            .scheduler(Scheduler.systemScheduler())
            .expireAfterAccess(Duration.ofSeconds(BATCHER_EXPIRY_SECONDS))
            .removalListener((RemovalListener<BatcherKey, Batcher<Call, CallResult, BatcherKey>>)
                (key, value, removalCause) -> {
                    if (value != null) {
                        value.close();
                    }
                })
            .build(k -> newBatcher(name, maxTolerantLatencyNanos, k));
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
                                                                        long maxTolerantLatencyNanos,
                                                                        BatcherKey key);

    protected abstract Optional<BatcherKey> find(Call call);

    @Override
    public CompletableFuture<CallResult> schedule(Call request) {
        callSchedCounter.increment();
        return callScheduler.submit(request)
            .thenCompose(req -> {
                try {
                    Optional<Batcher<Call, CallResult, BatcherKey>> builderOpt = find(req).map(batchers::get);
                    if (builderOpt.isPresent()) {
                        callSubmitCounter.increment();
                        return builderOpt.get().submit(req);
                    } else {
                        return CompletableFuture.failedFuture(DropException.BATCH_NOT_AVAILABLE);
                    }
                } catch (Throwable e) {
                    log.error("Error", e);
                    return CompletableFuture.failedFuture(DropException.BATCH_NOT_AVAILABLE);
                }
            });
    }

    @Override
    public void close() {
        batchers.invalidateAll();
        Metrics.globalRegistry.remove(batcherNumGauge);
        Metrics.globalRegistry.remove(callSubmitCounter);
        Metrics.globalRegistry.remove(callSchedCounter);
    }
}
