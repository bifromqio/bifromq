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


import static com.baidu.bifromq.baseutils.ThreadUtil.threadFactory;

import com.baidu.bifromq.basehlc.HLC;
import com.google.common.annotations.VisibleForTesting;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.PluginManager;

@Slf4j
public class EventCollectorManager implements IEventCollector {

    private static final EventPool ZERO_OUT_HOLDERS = new EventPool();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final Disruptor<EventHolder> disruptor;
    private final RingBuffer<EventHolder> ringBuffer;
    private final Map<String, IEventCollector> eventCollectors;
    private final Map<String, ReportCallHandler> eventCollectorHandlers = new HashMap<>();
    private final Map<String, EventProcessor> eventProcessors = new HashMap<>();
    private final ExecutorService collectExecutor;
    private final Counter callInvokeCounter;
    private final Counter callDropCounter;
    private final Gauge bufferCapacityGauge;

    public EventCollectorManager(PluginManager pluginMgr) {
        this(1024, pluginMgr);
    }

    public EventCollectorManager(int bufferSize, PluginManager pluginMgr) {
        assert bufferSize > 0 && ((bufferSize & (bufferSize - 1)) == 0);
        disruptor = new Disruptor<>(EventHolder::new, bufferSize,
            threadFactory("event-collector-worker", true),
            ProducerType.MULTI, new BlockingWaitStrategy());
        ringBuffer = disruptor.start();
        eventCollectors = pluginMgr.getExtensions(IEventCollector.class).stream()
            .collect(Collectors.toMap(e -> e.getClass().getName(), e -> e));

        collectExecutor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
            60L, TimeUnit.SECONDS, new SynchronousQueue<>(),
            threadFactory("reportevent-call-executor-%d", true));
        eventCollectors.keySet().forEach(this::resume);

        callInvokeCounter = Counter.builder("event.collector.report.invoke.count")
            .register(Metrics.globalRegistry);
        callDropCounter = Counter.builder("event.collector.report.drop.count")
            .register(Metrics.globalRegistry);
        bufferCapacityGauge = Gauge.builder("event.collector.report.buffer.capacity.gauge",
                () -> ringBuffer.remainingCapacity())
            .register(Metrics.globalRegistry);
    }

    @Override
    public <T extends Event> void report(T event) {
        callInvokeCounter.increment();
        event.hlc(HLC.INST.get());
        if (!ringBuffer.tryPublishEvent((holder, seq, e) -> holder.hold(e), event)) {
            callDropCounter.increment();
            log.warn("Event dropped: {}", event);
            event.clone(ZERO_OUT_HOLDERS.get(event.type()));
        }
    }

    public Set<String> allEventCollectors() {
        return Collections.unmodifiableSet(eventCollectors.keySet());
    }

    public synchronized boolean isPaused(String collectorName) {
        return eventCollectors.containsKey(collectorName) && !eventProcessors.containsKey(collectorName);
    }

    public synchronized void resume(String collectorName) {
        if (isPaused(collectorName)) {
            eventCollectorHandlers.put(collectorName, new ReportCallHandler(eventCollectors.get(collectorName)));
            eventProcessors.put(collectorName, new BatchEventProcessor<>(ringBuffer, ringBuffer.newBarrier(),
                eventCollectorHandlers.get(collectorName)));
            ringBuffer.addGatingSequences(eventProcessors.get(collectorName).getSequence());
            collectExecutor.execute(eventProcessors.get(collectorName));
        }
    }

    public synchronized void pause(String collectorName) {
        if (eventProcessors.containsKey(collectorName) && eventCollectorHandlers.containsKey(collectorName)) {
            EventProcessor processor = eventProcessors.remove(collectorName);
            processor.halt();
            ringBuffer.removeGatingSequence(processor.getSequence());
            eventCollectorHandlers.remove(collectorName).awaitShutdown();
        }
    }

    @Override
    public void close() {
        if (stopped.compareAndSet(false, true)) {
            log.debug("Closing event collector manager");
            disruptor.shutdown();
            eventCollectors.keySet().forEach(this::pause);
            Metrics.globalRegistry.remove(callInvokeCounter);
            Metrics.globalRegistry.remove(callDropCounter);
            Metrics.globalRegistry.remove(bufferCapacityGauge);
            log.debug("Event collector manager closed");
        }
    }

    @VisibleForTesting
    IEventCollector get(String collectorName) {
        return eventCollectors.get(collectorName);
    }
}
