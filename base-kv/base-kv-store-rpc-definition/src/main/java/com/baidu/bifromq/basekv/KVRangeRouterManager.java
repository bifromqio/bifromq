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

package com.baidu.bifromq.basekv;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class KVRangeRouterManager implements IKVRangeRouter {
    private final String clusterId;
    private final NavigableMap<Long, KVRangeRouter> routerByEpoch;
    private final IKVRangeRouterSelector routerSelector;

    public KVRangeRouterManager(String clusterId) {
        this(clusterId, routerByEpoch -> {
            // select min epoch router
            Map.Entry<Long, ? extends IKVRangeRouter> firstEntry = routerByEpoch.firstEntry();
            return firstEntry != null ? firstEntry.getValue() : null;
        });
    }

    public KVRangeRouterManager(String clusterId, IKVRangeRouterSelector routerSelector) {
        this.clusterId = clusterId;
        this.routerByEpoch = new ConcurrentSkipListMap<>();
        this.routerSelector = routerSelector;
    }

    public boolean upsert(KVRangeStoreDescriptor storeDescriptor) {
        AtomicBoolean changed = new AtomicBoolean();
        String storeId = storeDescriptor.getId();
        for (KVRangeDescriptor rangeDesc : storeDescriptor.getRangesList()) {
            KVRangeId rangeId = rangeDesc.getId();
            routerByEpoch.compute(rangeId.getEpoch(), (epoch, router) -> {
                if (router == null) {
                    router = new KVRangeRouter(clusterId);
                }
                boolean updated = router.upsert(storeId, rangeDesc);
                if (updated) {
                    changed.set(true);
                }
                // If the router is empty after the operation, remove it
                return router.isEmpty() ? null : router;
            });
        }
        return changed.get();
    }

    public boolean isFullRangeCovered() {
        KVRangeRouter router = (KVRangeRouter) routerSelector.select(routerByEpoch);
        if (router != null) {
            return router.isFullRangeCovered();
        }
        return false;
    }

    @Override
    public Optional<KVRangeSetting> findById(KVRangeId id) {
        IKVRangeRouter router = routerSelector.select(routerByEpoch);
        if (router != null) {
            return router.findById(id);
        }
        return Optional.empty();
    }

    @Override
    public Optional<KVRangeSetting> findByKey(ByteString key) {
        IKVRangeRouter router = routerSelector.select(routerByEpoch);
        if (router != null) {
            return router.findByKey(key);
        }
        return Optional.empty();
    }

    @Override
    public List<KVRangeSetting> findByBoundary(Boundary boundary) {
        IKVRangeRouter router = routerSelector.select(routerByEpoch);
        if (router != null) {
            return router.findByBoundary(boundary);
        }
        return Collections.emptyList();
    }
}
