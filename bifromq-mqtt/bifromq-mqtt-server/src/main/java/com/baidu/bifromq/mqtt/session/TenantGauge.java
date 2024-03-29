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

package com.baidu.bifromq.mqtt.session;

import com.baidu.bifromq.metrics.ITenantMeter;
import com.baidu.bifromq.metrics.TenantMetric;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.micrometer.core.instrument.Meter;
import io.netty.util.concurrent.FastThreadLocal;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class TenantGauge {
    private final TenantMetric metric;
    private final FastThreadLocal<LoadingCache<String, AtomicLong>> threadLocalTenantMemGauge;

    private final Map<String, WeakHashMap<AtomicLong, Object>> tenantMemGauge = new ConcurrentHashMap<>();

    public TenantGauge(TenantMetric gaugeMetric) {
        assert gaugeMetric.meterType == Meter.Type.GAUGE;
        this.metric = gaugeMetric;
        this.threadLocalTenantMemGauge = new FastThreadLocal<>() {
            @Override
            protected LoadingCache<String, AtomicLong> initialValue() {
                return Caffeine.newBuilder().weakValues()
                    .build(key -> {
                        AtomicLong gauge = new AtomicLong();
                        register(key, gauge);
                        return gauge;
                    });
            }
        };
    }

    public AtomicLong get(String tenant) {
        return threadLocalTenantMemGauge.get().get(tenant);
    }

    private void register(String tenantId, AtomicLong gauge) {
        tenantMemGauge.compute(tenantId, (k, v) -> {
            if (v == null) {
                WeakHashMap<AtomicLong, Object> threadLocalGauges = new WeakHashMap<>();
                v = threadLocalGauges;
                v.put(gauge, new Object());
                ITenantMeter.gauging(k, metric, () -> {
                    if (threadLocalGauges.isEmpty()) {
                        tenantMemGauge.remove(k, threadLocalGauges);
                        ITenantMeter.stopGauging(tenantId, metric);
                        return 0;
                    } else {
                        return threadLocalGauges.keySet().stream().mapToLong(AtomicLong::get).sum();
                    }
                });
            } else {
                v.put(gauge, new Object());
            }
            return v;
        });
    }
}
