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

package com.baidu.bifromq.metrics;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.lang.ref.Cleaner;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TrafficMeter {
    private static final Cleaner CLEANER = Cleaner.create();

    static class State implements Runnable {
        final Map<TrafficMetric, Meter> meters = new HashMap<>();

        State(Tags tags) {
            for (TrafficMetric metric : TrafficMetric.values()) {
                switch (metric.meterType) {
                    case COUNTER:
                        meters.put(metric, Metrics.counter(metric.metricName, tags));
                        break;
                    case TIMER:
                        meters.put(metric, Metrics.timer(metric.metricName, tags));
                        break;
                    case DISTRIBUTION_SUMMARY:
                        meters.put(metric, Metrics.summary(metric.metricName, tags));
                        break;
                    case GAUGE:
                        // ignore gauge
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported traffic meter type");
                }
            }
        }

        @Override
        public void run() {
            meters.values().forEach(Metrics.globalRegistry::remove);
            meters.clear();
        }
    }

    public static final String TAG_TRAFFIC_ID = "trafficId";
    private static final ConcurrentMap<String, Map<TrafficMetric, Gauge>> TRAFFIC_GAUGES = new ConcurrentHashMap<>();
    private static final LoadingCache<String, TrafficMeter> TRAFFIC_METER_CACHE = Caffeine.newBuilder()
        .weakValues()
        .build(TrafficMeter::new);

    public static TrafficMeter get(String trafficId) {
        return TRAFFIC_METER_CACHE.get(trafficId);
    }

    public static void cleanUp() {
        TRAFFIC_METER_CACHE.cleanUp();
    }

    private final State state;
    private final Tags tags;
    private final Cleaner.Cleanable cleanable;

    public TrafficMeter(String trafficId) {
        this.tags = Tags.of(TAG_TRAFFIC_ID, trafficId);
        this.state = new State(tags);
        this.cleanable = CLEANER.register(this, state);
    }

    public void recordCount(TrafficMetric metric) {
        recordCount(metric, 1);
    }

    public void recordCount(TrafficMetric metric, double inc) {
        assert metric.meterType == Meter.Type.COUNTER;
        ((Counter) state.meters.get(metric)).increment(inc);
    }

    public Timer timer(TrafficMetric metric) {
        assert metric.meterType == Meter.Type.TIMER;
        return (Timer) state.meters.get(metric);
    }

    public void recordSummary(TrafficMetric metric, double value) {
        assert metric.meterType == Meter.Type.DISTRIBUTION_SUMMARY;
        ((DistributionSummary) state.meters.get(metric)).record(value);
    }

    public static void gauging(String trafficId, TrafficMetric gaugeMetric, Supplier<Number> supplier) {
        assert gaugeMetric.meterType == Meter.Type.GAUGE;
        TRAFFIC_GAUGES.compute(trafficId, (k, v) -> {
            if (v == null) {
                v = new HashMap<>();
            }
            v.put(gaugeMetric, Gauge.builder(gaugeMetric.metricName, supplier)
                .tags(Tags.of(TAG_TRAFFIC_ID, trafficId))
                .register(Metrics.globalRegistry));
            return v;
        });
    }

    public static void stopGauging(String trafficId, TrafficMetric gaugeMetric) {
        assert gaugeMetric.meterType == Meter.Type.GAUGE;
        TRAFFIC_GAUGES.computeIfPresent(trafficId, (k, gaugeMap) -> {
            Gauge gauge = gaugeMap.remove(gaugeMetric);
            if (gauge != null) {
                Metrics.globalRegistry.remove(gauge);
            }
            if (gaugeMap.isEmpty()) {
                return null;
            }
            return gaugeMap;
        });
    }

    public void destroy() {
        cleanable.clean();
    }
}
