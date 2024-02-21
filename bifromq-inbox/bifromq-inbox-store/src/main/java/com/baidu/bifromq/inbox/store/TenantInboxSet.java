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

package com.baidu.bifromq.inbox.store;

import static com.baidu.bifromq.metrics.TenantMetric.MqttPersistentSessionNumGauge;
import static com.baidu.bifromq.metrics.TenantMetric.MqttPersistentSessionSpaceGauge;
import static com.baidu.bifromq.metrics.TenantMetric.MqttPersistentSubCountGauge;

import com.baidu.bifromq.inbox.storage.proto.InboxMetadata;
import com.baidu.bifromq.metrics.ITenantMeter;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * TenantInboxSet is used to hold all persistent session metadata in memory belonging to a tenant.
 */
class TenantInboxSet {
    private final LongAdder totalSubCount = new LongAdder();
    // inboxId -> incarnation -> inboxMetadata
    private final Map<String, Map<Long, InboxMetadata>> inboxMetadataMap = new ConcurrentHashMap<>();
    private final String tenantId;
    private final String[] tags;

    TenantInboxSet(String tenantId, Supplier<Number> usedSpaceGetter, String... tagValuePair) {
        this.tenantId = tenantId;
        this.tags = tagValuePair;
        ITenantMeter.gauging(tenantId, MqttPersistentSubCountGauge, totalSubCount::sum, tags);
        ITenantMeter.gauging(tenantId, MqttPersistentSessionNumGauge, inboxMetadataMap::size, tags);
        ITenantMeter.gauging(tenantId, MqttPersistentSessionSpaceGauge, usedSpaceGetter, tags);
    }

    void upsert(InboxMetadata metadata) {
        inboxMetadataMap.compute(metadata.getInboxId(), (k, v) -> {
            if (v == null) {
                v = new ConcurrentHashMap<>();
            }
            v.compute(metadata.getIncarnation(), (k1, v1) -> {
                if (v1 == null) {
                    totalSubCount.add(metadata.getTopicFiltersCount());
                } else {
                    // update the total sub count and used space with delta
                    totalSubCount.add(metadata.getTopicFiltersCount() - v1.getTopicFiltersCount());
                }
                return metadata;
            });
            return v;
        });
    }

    void remove(String inboxId, long incarnation) {
        inboxMetadataMap.computeIfPresent(inboxId, (v, k) -> {
            k.computeIfPresent(incarnation, (k1, v1) -> {
                // update the total sub count and used space with delta
                totalSubCount.add(-v1.getTopicFiltersCount());
                return null;
            });
            return k.isEmpty() ? null : k;
        });
    }

    boolean isEmpty() {
        return inboxMetadataMap.isEmpty();
    }

    Optional<InboxMetadata> get(String inboxId, long incarnation) {
        return Optional.ofNullable(inboxMetadataMap.getOrDefault(inboxId, Collections.emptyMap()).get(incarnation));
    }

    Collection<InboxMetadata> getAll() {
        return inboxMetadataMap.values().stream().flatMap(map -> map.values().stream()).collect(Collectors.toList());
    }

    Collection<InboxMetadata> getAll(String inboxId) {
        return inboxMetadataMap.getOrDefault(inboxId, Collections.emptyMap()).values();
    }

    void destroy() {
        ITenantMeter.stopGauging(tenantId, MqttPersistentSubCountGauge, tags);
        ITenantMeter.stopGauging(tenantId, MqttPersistentSessionNumGauge, tags);
        ITenantMeter.stopGauging(tenantId, MqttPersistentSessionSpaceGauge, tags);
    }
}
