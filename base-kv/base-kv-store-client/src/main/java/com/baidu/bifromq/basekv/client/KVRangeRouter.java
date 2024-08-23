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

package com.baidu.bifromq.basekv.client;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.compareEndKeys;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.endKey;
import static com.baidu.bifromq.basekv.utils.DescriptorUtil.filterLeaderRanges;
import static com.baidu.bifromq.basekv.utils.DescriptorUtil.getEffectiveEpoch;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.utils.BoundaryUtil;
import com.baidu.bifromq.basekv.utils.DescriptorUtil;
import com.baidu.bifromq.basekv.utils.KeySpaceDAG;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

public class KVRangeRouter implements IKVRangeRouter {
    private final String clusterId;
    private volatile NavigableMap<Boundary, KVRangeSetting> router = new TreeMap<>(BoundaryUtil::compare);


    public KVRangeRouter(String clusterId) {
        this.clusterId = clusterId;
    }

    /**
     * Reset the router with the latest store descriptors if it contains qualified keyspace.
     *
     * @param storeDescriptors the latest store descriptors
     * @return true if the router is changed, otherwise false
     */
    public boolean reset(Set<KVRangeStoreDescriptor> storeDescriptors) {
        Optional<DescriptorUtil.EffectiveEpoch> effectiveEpoch = getEffectiveEpoch(storeDescriptors);
        if (effectiveEpoch.isEmpty()) {
            return false;
        }
        Map<String, Set<KVRangeDescriptor>> leaderRanges = filterLeaderRanges(effectiveEpoch.get().storeDescriptors());
        KeySpaceDAG dag = new KeySpaceDAG(leaderRanges);
        NavigableMap<Boundary, KVRangeSetting> router = Maps.transformValues(dag.getEffectiveFullCoveredRoute(),
            leaderRange -> new KVRangeSetting(clusterId, leaderRange.storeId(), leaderRange.descriptor()));
        if (router.isEmpty()) {
            return false;
        }
        boolean changed = !this.router.equals(router);
        if (changed) {
            this.router = router;
        }
        return changed;
    }

    public boolean isFullRangeCovered() {
        return !router.isEmpty();
    }

    @Override
    public Optional<KVRangeSetting> findByKey(ByteString key) {
        Map.Entry<Boundary, KVRangeSetting> entry = router.floorEntry(Boundary.newBuilder().setStartKey(key).build());
        if (entry != null) {
            KVRangeSetting setting = entry.getValue();
            if (BoundaryUtil.inRange(key, entry.getKey())) {
                return Optional.of(setting);
            }
        }
        return Optional.empty();
    }

    @Override
    public List<KVRangeSetting> findByBoundary(Boundary boundary) {
        if (!boundary.hasStartKey() && !boundary.hasEndKey()) {
            return Lists.newArrayList(router.values());
        }
        if (!boundary.hasStartKey()) {
            Boundary boundaryEnd = Boundary.newBuilder()
                .setStartKey(boundary.getEndKey())
                .setEndKey(boundary.getEndKey()).build();
            return Lists.newArrayList(router.headMap(boundaryEnd, false).values());
        }
        if (!boundary.hasEndKey()) {
            Boundary boundaryStart = Boundary.newBuilder()
                .setStartKey(boundary.getStartKey())
                .setEndKey(boundary.getStartKey()).build();
            Boundary floorBoundary = router.floorKey(boundaryStart);
            return Lists.newArrayList(
                router.tailMap(floorBoundary, compareEndKeys(endKey(floorBoundary), boundary.getStartKey()) > 0)
                    .values());
        }
        Boundary boundaryStart = Boundary.newBuilder()
            .setStartKey(boundary.getStartKey())
            .setEndKey(boundary.getStartKey()).build();
        Boundary boundaryEnd = Boundary.newBuilder()
            .setStartKey(boundary.getEndKey())
            .setEndKey(boundary.getEndKey()).build();
        Boundary floorBoundary = router.floorKey(boundaryStart);

        return Lists.newArrayList(
            router.subMap(floorBoundary, compareEndKeys(endKey(floorBoundary), boundary.getStartKey()) > 0,
                boundaryEnd, false).values());
    }
}
