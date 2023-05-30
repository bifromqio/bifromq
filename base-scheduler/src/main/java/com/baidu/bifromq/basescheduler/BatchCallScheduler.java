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

public abstract class BatchCallScheduler<Req, Resp, BatchCallBuilderKey> implements IBatchCallScheduler<Req, Resp> {
    private static final int BUILDER_EXPIRY_SECONDS = 30;
    private final LoadingCache<BatchCallBuilderKey, BatchCallBuilder<Req, Resp>> callQueues;
    private final ICallScheduler<Req> reqScheduler;
    private final Gauge queueNumGauge;
    private final Counter callSchedCounter;
    private final Counter callEmitCounter;
    private final Counter dropCounter;


    public BatchCallScheduler(String name) {
        this(name, 1);
    }

    public BatchCallScheduler(String name, int maxInflights) {
        this(name, maxInflights, new ICallScheduler<>() {
        });
    }

    public BatchCallScheduler(String name, int maxInflights, ICallScheduler<Req> reqScheduler) {
        this.reqScheduler = reqScheduler;
        callQueues = Caffeine.newBuilder()
            .scheduler(Scheduler.systemScheduler())
            .expireAfterAccess(Duration.ofSeconds(BUILDER_EXPIRY_SECONDS))
            .removalListener((RemovalListener<BatchCallBuilderKey, BatchCallBuilder<Req, Resp>>)
                (key, value, removalCause) -> {
                    if (value != null) {
                        value.close();
                    }
                })
            .build(k -> newBuilder(name, maxInflights, k));
        queueNumGauge = Gauge.builder("batch.queue.num", callQueues::estimatedSize)
            .tags("name", name)
            .register(Metrics.globalRegistry);
        callSchedCounter = Counter.builder("batch.task.sched.count")
            .tags("name", name)
            .register(Metrics.globalRegistry);
        callEmitCounter = Counter.builder("batch.task.emit.count")
            .tags("name", name)
            .register(Metrics.globalRegistry);
        dropCounter = Counter.builder("batch.task.drop.count")
            .tags("name", name)
            .register(Metrics.globalRegistry);
    }

    protected abstract BatchCallBuilder<Req, Resp> newBuilder(String name, int maxInflights, BatchCallBuilderKey key);

    protected abstract Optional<BatchCallBuilderKey> find(Req request);

    @Override
    public CompletableFuture<Resp> schedule(Req request) {
        callSchedCounter.increment();
        return reqScheduler.submit(request)
            .thenCompose(req -> {
                Optional<BatchCallBuilder<Req, Resp>> builderOpt = find(req).map(callQueues::get);
                if (builderOpt.isPresent()) {
                    callEmitCounter.increment();
                    return builderOpt.get().submit(req);
                } else {
                    dropCounter.increment();
                    return CompletableFuture.failedFuture(DropException.BATCH_NOT_AVAILABLE);
                }
            });
    }

    @Override
    public void close() {
        callQueues.invalidateAll();
        Metrics.globalRegistry.remove(queueNumGauge);
        Metrics.globalRegistry.remove(callEmitCounter);
        Metrics.globalRegistry.remove(callSchedCounter);
        Metrics.globalRegistry.remove(dropCounter);
    }
}
