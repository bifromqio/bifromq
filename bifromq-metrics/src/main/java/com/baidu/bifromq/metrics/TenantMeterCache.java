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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.micrometer.core.instrument.Gauge;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class TenantMeterCache {
    private static final ConcurrentMap<String, Map<TenantMetric, Gauge>> TENANT_GAUGES = new ConcurrentHashMap<>();
    private static final LoadingCache<String, TenantMeter> TENANT_METER_CACHE = Caffeine.newBuilder()
        .weakValues()
        .build(TenantMeter::new);

    static ITenantMeter get(String tenantId) {
        return TENANT_METER_CACHE.get(tenantId);
    }

    static void cleanUp() {
        TENANT_METER_CACHE.cleanUp();
    }
}
