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

package com.baidu.bifromq.baserpc.metrics;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.TimeUnit;
import lombok.Builder;
import lombok.EqualsAndHashCode;

public class RPCMeters {
    @Builder
    @EqualsAndHashCode
    public static class MeterKey {
        final String service;
        final String method;
        final String tenantId;
    }

    private static final LoadingCache<MeterKey, LoadingCache<RPCMetric, Meter>> TRAFFIC_METERS = Caffeine
        .newBuilder()
        .scheduler(Scheduler.systemScheduler())
        .expireAfterAccess(30, TimeUnit.SECONDS)
        .removalListener((RemovalListener<MeterKey, LoadingCache<RPCMetric, Meter>>)
            (key, value, removalCause) -> {
                if (value != null) {
                    value.invalidateAll();
                }
            })
        .build(key -> Caffeine.newBuilder()
            .expireAfterAccess(30, TimeUnit.SECONDS)
            .removalListener((RemovalListener<RPCMetric, Meter>) (key1, value, removalCause) -> {
                if (value != null) {
                    Metrics.globalRegistry.remove(value);
                }
            })
            .build(metric -> {
                Tags tags = Tags.of(MetricTag.SERVICE, key.service)
                    .and(MetricTag.METHOD, key.method)
                    .and(MetricTag.TENANT_ID, key.tenantId);

                switch (metric.meterType) {
                    case TIMER:
                        return Timer.builder(metric.metricName)
                            .tags(tags)
                            .register(Metrics.globalRegistry);
                    case COUNTER:
                        return Metrics.counter(metric.metricName, tags);
                    case DISTRIBUTION_SUMMARY:
                        return DistributionSummary.builder(metric.metricName)
                            .tags(tags)
                            .register(Metrics.globalRegistry);
                    default:
                        throw new IllegalStateException("Unsupported Meter Type: " + metric.meterType);
                }
            }));

    public static void recordCount(MeterKey key, RPCMetric metric) {
        assert metric.meterType == Meter.Type.COUNTER;
        ((Counter) TRAFFIC_METERS.get(key).get(metric)).increment();
    }

    public static void recordCount(MeterKey key, RPCMetric metric, double inc) {
        assert metric.meterType == Meter.Type.COUNTER;
        ((Counter) TRAFFIC_METERS.get(key).get(metric)).increment(inc);
    }

    public static Timer timer(MeterKey key, RPCMetric metric) {
        assert metric.meterType == Meter.Type.TIMER;
        return (Timer) TRAFFIC_METERS.get(key).get(metric);
    }

    public static void recordSummary(MeterKey key, RPCMetric metric, int depth) {
        assert metric.meterType == Meter.Type.DISTRIBUTION_SUMMARY;
        ((DistributionSummary) TRAFFIC_METERS.get(key).get(metric)).record(depth);
    }
}
