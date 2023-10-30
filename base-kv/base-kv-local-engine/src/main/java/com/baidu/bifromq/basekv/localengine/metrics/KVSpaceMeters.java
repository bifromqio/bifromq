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

package com.baidu.bifromq.basekv.localengine.metrics;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import java.lang.ref.Cleaner;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KVSpaceMeters {
    private static final Cleaner CLEANER = Cleaner.create();

    private record MeterKey(String id, KVSpaceMetric metric, Tags tags) {

    }

    private static final Cache<MeterKey, Meter> METERS = Caffeine.newBuilder()
        .evictionListener((RemovalListener<MeterKey, Meter>) (key, value, cause) -> {
            if (value != null) {
                Metrics.globalRegistry.remove(value);
            }
        })
        .weakValues()
        .build();

    public static Timer getTimer(String id, KVSpaceMetric metric, Tags tags) {
        assert metric.meterType == Meter.Type.TIMER;
        return (Timer) METERS.get(new MeterKey(id, metric, tags),
            k -> new TimerWrapper(Timer.builder(metric.metricName)
                .tags(tags)
                .tags("kvspace", id)
                .register(Metrics.globalRegistry)));
    }

    public static Counter getCounter(String id, KVSpaceMetric metric, Tags tags) {
        assert metric.meterType == Meter.Type.COUNTER;
        return (Counter) METERS.get(new MeterKey(id, metric, tags),
            k -> new CounterWrapper(Counter.builder(metric.metricName)
                .tags(tags)
                .tags("kvspace", id)
                .register(Metrics.globalRegistry)));
    }

    public static Gauge getGauge(String id, KVSpaceMetric metric, Supplier<Number> numProvider, Tags tags) {
        assert metric.meterType == Meter.Type.GAUGE;
        return (Gauge) METERS.get(new MeterKey(id, metric, tags),
            k -> new GaugeWrapper(Gauge.builder(metric.metricName, numProvider)
                .tags(tags)
                .tags("kvspace", id)
                .register(Metrics.globalRegistry)));
    }

    private record State(Meter meter) implements Runnable {
        @Override
        public void run() {
            Metrics.globalRegistry.remove(meter);
        }
    }

    private static final class TimerWrapper implements Timer {
        private final Timer delegate;

        private TimerWrapper(Timer delegate) {
            this.delegate = delegate;
            CLEANER.register(this, new State(delegate));
        }

        @Override
        public void record(long amount, TimeUnit unit) {
            delegate.record(amount, unit);
        }

        @Override
        public <T> T record(Supplier<T> f) {
            return delegate.record(f);
        }

        @Override
        public <T> T recordCallable(Callable<T> f) throws Exception {
            return delegate.recordCallable(f);
        }

        @Override
        public void record(Runnable f) {
            delegate.record(f);
        }

        @Override
        public long count() {
            return delegate.count();
        }

        @Override
        public double totalTime(TimeUnit unit) {
            return delegate.totalTime(unit);
        }

        @Override
        public double max(TimeUnit unit) {
            return delegate.max(unit);
        }

        @Override
        public TimeUnit baseTimeUnit() {
            return delegate.baseTimeUnit();
        }

        @Override
        public HistogramSnapshot takeSnapshot() {
            return delegate.takeSnapshot();
        }

        @Override
        public Id getId() {
            return delegate.getId();
        }
    }

    private static final class CounterWrapper implements Counter {
        private final Counter delegate;

        private CounterWrapper(Counter delegate) {
            this.delegate = delegate;
            CLEANER.register(this, new State(delegate));
        }

        @Override
        public Id getId() {
            return delegate.getId();
        }

        @Override
        public void increment(double amount) {
            delegate.increment(amount);
        }

        @Override
        public double count() {
            return delegate.count();
        }
    }

    private static final class GaugeWrapper implements Gauge {
        private final Gauge delegate;

        private GaugeWrapper(Gauge delegate) {
            this.delegate = delegate;
            CLEANER.register(this, new State(delegate));
        }

        @Override
        public Id getId() {
            return delegate.getId();
        }

        @Override
        public double value() {
            return delegate.value();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null || obj.getClass() != this.getClass()) {
                return false;
            }
            var that = (GaugeWrapper) obj;
            return Objects.equals(this.delegate, that.delegate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(delegate);
        }

        @Override
        public String toString() {
            return "GaugeWrapper[" +
                "delegate=" + delegate + ']';
        }
    }
}
