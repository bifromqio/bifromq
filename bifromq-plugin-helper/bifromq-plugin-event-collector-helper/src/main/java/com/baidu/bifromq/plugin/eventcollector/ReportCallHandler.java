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

package com.baidu.bifromq.plugin.eventcollector;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.CountDownLatch;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ReportCallHandler implements EventHandler<EventHolder>, LifecycleAware {
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final String id;
    private final IEventCollector collector;
    private final Counter reportFailCounter;
    private final Timer reportCallTimer;

    ReportCallHandler(IEventCollector collector) {
        this.id = collector.getClass().getName();
        this.collector = collector;
        reportCallTimer = Timer.builder("call.exec.timer")
            .tag("method", "EventCollector/report")
            .tag("type", id)
            .register(Metrics.globalRegistry);
        reportFailCounter = Counter.builder("call.exec.fail.count")
            .tag("method", "EventCollector/report")
            .tag("type", id)
            .register(Metrics.globalRegistry);
    }

    @Override
    public void onEvent(final EventHolder holder, final long sequence, final boolean endOfBatch) {
        Timer.Sample sample = Timer.start();
        try {
            collector.report(holder.event);
        } catch (Throwable e) {
            reportFailCounter.increment();
        } finally {
            sample.stop(reportCallTimer);
            holder.reset();
        }
    }

    @Override
    public void onStart() {
        log.debug("Event collector: {} start", id);
    }

    @Override
    public void onShutdown() {
        log.debug("Event collector: {} shutdown", id);
        shutdownLatch.countDown();
        collector.close();
        Metrics.globalRegistry.remove(reportCallTimer);
        Metrics.globalRegistry.remove(reportCallTimer);
    }

    @SneakyThrows
    public void awaitShutdown() {
        shutdownLatch.await();
    }
}
