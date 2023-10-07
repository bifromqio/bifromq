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

package com.baidu.bifromq.basekv.localengine;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.google.common.util.concurrent.MoreExecutors;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractKVEngine implements IKVEngine {
    protected enum State {
        INIT, STARTING, STARTED, FATAL_FAILURE, STOPPING, STOPPED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);

    private final Duration gcInterval;
    private ScheduledExecutorService gcExecutor;
    private volatile CompletableFuture<Void> gcJob;
    protected final String overrideIdentity;
    private Gauge gauge;

    public AbstractKVEngine(String overrideIdentity, Duration gcInterval) {
        this.overrideIdentity = overrideIdentity;
        this.gcInterval = gcInterval;
    }

    protected State state() {
        return state.get();
    }

    @Override
    public void start(String... metricTags) {
        if (state.compareAndSet(State.INIT, State.STARTING)) {
            try {
                this.gcExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                    new ScheduledThreadPoolExecutor(1, EnvProvider.INSTANCE.newThreadFactory("kvengine-gc-executor")),
                    "kvengine-gc-executor-" + Tags.of(metricTags));
                doStart(metricTags);
                state.set(State.STARTED);
                gauge = Gauge.builder("basekv.le.ranges", this.ranges()::size)
                    .tags(metricTags)
                    .register(Metrics.globalRegistry);
                scheduleNextGC();
                afterStart();
            } catch (Throwable e) {
                state.set(State.FATAL_FAILURE);
                throw e;
            }
        }
    }

    protected abstract void doStart(String... metricTags);

    protected void afterStart() {

    }

    @Override
    public void stop() {
        assertStarted();
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            try {
                if (gcJob != null && !gcJob.isDone()) {
                    gcJob.join();
                }
                MoreExecutors.shutdownAndAwaitTermination(gcExecutor, 5, TimeUnit.SECONDS);
                doStop();
                Metrics.globalRegistry.remove(gauge);
            } finally {
                state.set(State.STOPPED);
            }
        }
    }

    protected abstract void doStop();

    protected void assertStarted() {
        assert state.get() == State.STARTED : "Not started";
    }

    private void scheduleNextGC() {
        if (state.get() != State.STARTED) {
            return;
        }
        log.debug("KVEngine gc scheduled[identity={}]", id());
        gcExecutor.schedule(this::gc, gcInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void gc() {
        gcJob = new CompletableFuture<>();
        try {
            doGC();
        } catch (Throwable e) {
            log.error("KVEngine GC error", e);
        } finally {
            gcJob.complete(null);
            scheduleNextGC();
        }
    }

    protected abstract void doGC();

}
