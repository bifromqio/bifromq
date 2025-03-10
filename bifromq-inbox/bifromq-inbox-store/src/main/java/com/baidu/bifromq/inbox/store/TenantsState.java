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

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.intersect;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.isNULLRange;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static com.baidu.bifromq.inbox.store.schema.KVSchemaUtil.tenantBeginKeyPrefix;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.store.api.IKVCloseableReader;
import com.baidu.bifromq.inbox.storage.proto.InboxMetadata;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class TenantsState {
    private final Map<String, TenantInboxSet> tenantStates = new ConcurrentHashMap<>();
    private final IEventCollector eventCollector;
    private final IKVCloseableReader reader;
    private final String[] tags;
    private transient Boundary boundary;

    TenantsState(IEventCollector eventCollector, IKVCloseableReader reader, String... tags) {
        this.eventCollector = eventCollector;
        this.reader = reader;
        this.tags = tags;
        boundary = reader.boundary();
    }

    Collection<InboxMetadata> getAll(String tenantId) {
        TenantInboxSet inboxSet = tenantStates.get(tenantId);
        if (inboxSet == null) {
            return Collections.emptyList();
        }
        return inboxSet.getAll();
    }

    Collection<InboxMetadata> getAll(String tenantId, String inboxId) {
        TenantInboxSet inboxSet = tenantStates.get(tenantId);
        if (inboxSet == null) {
            return Collections.emptyList();
        }
        return inboxSet.getAll(inboxId);
    }

    Optional<InboxMetadata> get(String tenantId, String inboxId, long incarnation) {
        TenantInboxSet inboxSet = tenantStates.get(tenantId);
        if (inboxSet == null) {
            return Optional.empty();
        }
        return inboxSet.get(inboxId, incarnation);
    }

    Collection<String> getAllTenantIds() {
        return tenantStates.keySet();
    }

    void upsert(String tenantId, InboxMetadata metadata) {
        tenantStates.computeIfAbsent(tenantId, k ->
            new TenantInboxSet(eventCollector, tenantId, getTenantUsedSpace(tenantId), tags)).upsert(metadata);
    }

    void remove(String tenantId, String inboxId, long incarnation) {
        tenantStates.computeIfPresent(tenantId, (k, v) -> {
            v.remove(inboxId, incarnation);
            if (v.isEmpty()) {
                v.destroy();
                return null;
            }
            return v;
        });
    }

    void reset() {
        tenantStates.values().forEach(TenantInboxSet::removeAll);
        tenantStates.values().forEach(TenantInboxSet::destroy);
        tenantStates.clear();
        boundary = reader.boundary();
    }

    void close() {
        reset();
        reader.close();
    }

    private Supplier<Number> getTenantUsedSpace(String tenantId) {
        return () -> {
            try {
                ByteString startKey = tenantBeginKeyPrefix(tenantId);
                ByteString endKey = upperBound(startKey);
                Boundary tenantBoundary = intersect(boundary, toBoundary(startKey, endKey));
                if (isNULLRange(tenantBoundary)) {
                    return 0;
                }
                return reader.size(tenantBoundary);
            } catch (Exception e) {
                log.error("Failed to get used space for tenant:{}", tenantId, e);
                return 0;
            }
        };
    }
}
