/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

import static com.baidu.bifromq.metrics.ITenantMeter.TAG_TENANT_ID;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

public class TenantGauges {
    private static final ConcurrentMap<String, Map<TenantMetric, Gauge>> TENANT_GAUGES = new ConcurrentHashMap<>();

    static void gauging(String tenantId, TenantMetric gaugeMetric, Supplier<Number> supplier) {
        assert gaugeMetric.meterType == Meter.Type.GAUGE;
        TENANT_GAUGES.compute(tenantId, (k, v) -> {
            if (v == null) {
                v = new HashMap<>();
            }
            v.put(gaugeMetric, Gauge.builder(gaugeMetric.metricName, supplier)
                .tags(Tags.of(TAG_TENANT_ID, tenantId))
                .register(Metrics.globalRegistry));
            return v;
        });
    }

    static void stopGauging(String tenantId, TenantMetric gaugeMetric) {
        assert gaugeMetric.meterType == Meter.Type.GAUGE;
        TENANT_GAUGES.computeIfPresent(tenantId, (k, gaugeMap) -> {
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
}
