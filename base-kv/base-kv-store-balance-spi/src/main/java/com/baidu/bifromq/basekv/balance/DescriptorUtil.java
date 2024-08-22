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

package com.baidu.bifromq.basekv.balance;

import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Utilities for processing descriptor.
 */
public class DescriptorUtil {
    /**
     * Organize storeDescriptors by epoch.
     *
     * @param storeDescriptors storeDescriptors
     * @return storeDescriptors organized by epoch
     */
    public static NavigableMap<Long, Set<KVRangeStoreDescriptor>> organizeByEpoch(
        Set<KVRangeStoreDescriptor> storeDescriptors) {
        NavigableMap<Long, Set<KVRangeStoreDescriptor>> epochMap = new TreeMap<>();
        Map<Long, Map<KVRangeStoreDescriptor, KVRangeStoreDescriptor.Builder>> storeDescBuilderByEpoch =
            new HashMap<>();

        for (KVRangeStoreDescriptor storeDescriptor : storeDescriptors) {
            for (KVRangeDescriptor rangeDescriptor : storeDescriptor.getRangesList()) {
                long epoch = rangeDescriptor.getId().getEpoch();
                storeDescBuilderByEpoch.computeIfAbsent(epoch, e -> storeDescriptors.stream()
                        .collect(Collectors.toMap(k -> k, k -> k.toBuilder().clearRanges())))
                    .get(storeDescriptor)
                    .addRanges(rangeDescriptor);
            }
        }
        storeDescBuilderByEpoch.forEach((epoch, storeDescBuilderMap) -> {
            Set<KVRangeStoreDescriptor> storeDescSet = storeDescBuilderMap.values().stream()
                .map(KVRangeStoreDescriptor.Builder::build)
                .collect(Collectors.toSet());
            epochMap.put(epoch, storeDescSet);
        });
        return epochMap;
    }

    /**
     * Get the storeDescriptors with the least epoch.
     *
     * @param storeDescriptors storeDescriptors
     * @return storeDescriptors with the least epoch
     */
    public static Set<KVRangeStoreDescriptor> getLeastEpoch(Set<KVRangeStoreDescriptor> storeDescriptors) {
        NavigableMap<Long, Set<KVRangeStoreDescriptor>> storeDescriptorsByEpoch = organizeByEpoch(storeDescriptors);
        if (storeDescriptorsByEpoch.isEmpty()) {
            return new HashSet<>();
        }
        return storeDescriptorsByEpoch.firstEntry().getValue();
    }
}
