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

import io.micrometer.core.instrument.Timer;
import java.util.function.Supplier;

public interface ITenantMeter {
    String TAG_TENANT_ID = "tenantId";

    static ITenantMeter get(String tenantId) {
        return TenantMeterCache.get(tenantId);
    }

    static void gauging(String tenantId, TenantMetric gaugeMetric, Supplier<Number> supplier) {
        TenantGauges.gauging(tenantId, gaugeMetric, supplier);
    }

    static void stopGauging(String tenantId, TenantMetric gaugeMetric) {
        TenantGauges.stopGauging(tenantId, gaugeMetric);
    }

    void recordCount(TenantMetric metric);

    void recordCount(TenantMetric metric, double inc);

    Timer timer(TenantMetric metric);

    void recordSummary(TenantMetric metric, double value);
}
