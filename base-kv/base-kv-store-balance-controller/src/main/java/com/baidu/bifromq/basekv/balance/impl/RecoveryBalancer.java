/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.basekv.balance.impl;

import com.baidu.bifromq.basekv.balance.StoreBalancer;
import com.baidu.bifromq.basekv.balance.command.RecoveryCommand;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Sets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RecoveryBalancer extends StoreBalancer {

    private final Cache<String, KVRangeStoreDescriptor> deadStoreCache;

    private volatile Set<KVRangeStoreDescriptor> latestStoreDescriptors = new HashSet<>();

    public RecoveryBalancer(String clusterId, String localStoreId, long deadStoreTimeoutMillis) {
        super(clusterId, localStoreId);
        deadStoreCache = Caffeine.newBuilder()
            .expireAfterWrite(deadStoreTimeoutMillis, TimeUnit.MILLISECONDS)
            .build();
    }

    @Override
    public void update(Set<KVRangeStoreDescriptor> storeDescriptors) {
        Map<String, KVRangeStoreDescriptor> prevAliveStores = this.latestStoreDescriptors.stream()
            .collect(Collectors.toMap(
                KVRangeStoreDescriptor::getId,
                sd -> sd
            ));
        Set<String> currAliveStores =
            storeDescriptors.stream().map(KVRangeStoreDescriptor::getId).collect(Collectors.toSet());
        // Put disappeared store into cache to avoid frequently scheduling
        for (String deadStore : Sets.difference(prevAliveStores.keySet(), currAliveStores)) {
            deadStoreCache.put(deadStore, prevAliveStores.get(deadStore));
        }
        for (String aliveStore : currAliveStores) {
            deadStoreCache.invalidate(aliveStore);
        }
        this.latestStoreDescriptors = storeDescriptors;
    }

    @Override
    public Optional<RecoveryCommand> balance() {
        return recoverNoLeaderRange();
    }

    private Optional<RecoveryCommand> recoverNoLeaderRange() {
        Set<KVRangeStoreDescriptor> allSds = Sets.newHashSet(latestStoreDescriptors);
        Map<String, KVRangeStoreDescriptor> deadSds = deadStoreCache.asMap();
        allSds.addAll(deadSds.values());
        Map<KVRangeId, Map<String, KVRangeDescriptor>> rangeDescriptorMap = new HashMap<>();
        Set<KVRangeId> rangeWithLeader = new HashSet<>();
        for (KVRangeStoreDescriptor sd : allSds) {
            for (KVRangeDescriptor rd : sd.getRangesList()) {
                rangeDescriptorMap.compute(rd.getId(), (rid, map) -> {
                    if (map == null) {
                        map = new HashMap<>();
                    }
                    map.put(sd.getId(), rd);
                    return map;
                });
                if (rd.getRole() == RaftNodeStatus.Leader) {
                    rangeWithLeader.add(rd.getId());
                }
            }
        }
        for (Entry<KVRangeId, Map<String, KVRangeDescriptor>> entry : rangeDescriptorMap.entrySet()) {
            if (rangeWithLeader.contains(entry.getKey())) {
                continue;
            }
            // if KVRange has no leader, check if range could self-healing:
            // current voters count > votersInConfig / 2
            KVRangeDescriptor maxVerRd =
                entry.getValue().values().stream().max(Comparator.comparingLong(KVRangeDescriptor::getVer)).get();
            Set<String> votersInConfig = Sets.newHashSet(maxVerRd.getConfig().getVotersList());
            Set<String> currentVoters = entry.getValue().keySet().stream()
                .filter(votersInConfig::contains)
                .collect(Collectors.toSet());
            if (currentVoters.size() > votersInConfig.size() / 2) {
                continue;
            }
            // choose optimal voter to recover
            TreeMap<String, KVRangeDescriptor> sortedRange = new TreeMap<>();
            for (String storeId : currentVoters) {
                if (deadSds.containsKey(storeId)) {
                    continue;
                }
                if (entry.getValue().get(storeId).getVer() != maxVerRd.getVer()) {
                    continue;
                }
                sortedRange.put(storeId, entry.getValue().get(storeId));
            }
            if (!sortedRange.isEmpty()) {
                return Optional.of(
                    RecoveryCommand.builder()
                        .toStore(sortedRange.firstKey())
                        .kvRangeId(entry.getKey())
                        .build()
                );
            }
        }
        return Optional.empty();
    }

}
