/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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
import com.baidu.bifromq.basekv.balance.command.BalanceCommand;
import com.baidu.bifromq.basekv.balance.command.ChangeConfigCommand;
import com.baidu.bifromq.basekv.balance.command.RecoveryCommand;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.proto.State.StateType;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RecoveryBalancer extends StoreBalancer {

    private final Cache<String, KVRangeStoreDescriptor> deadStoreCache;

    private Set<KVRangeStoreDescriptor> latestStoreDescriptors = new HashSet<>();

    public RecoveryBalancer(String localStoreId, long deadStoreTimeoutMillis) {
        super(localStoreId);
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
    public Optional<BalanceCommand> balance() {
        Optional<BalanceCommand> commandOptional = recoverNoLeaderRange();
        if (commandOptional.isPresent()) {
            return commandOptional;
        }
        return recoverRangeWithDeadStore();
    }

    private Optional<BalanceCommand> recoverNoLeaderRange() {
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

    private Optional<BalanceCommand> recoverRangeWithDeadStore() {
        KVRangeStoreDescriptor localStoreDesc = null;
        for (KVRangeStoreDescriptor d : latestStoreDescriptors) {
            if (d.getId().equals(localStoreId)) {
                localStoreDesc = d;
                break;
            }
        }
        if (localStoreDesc == null) {
            log.warn("There is no storeDescriptor for local store: {}", localStoreId);
            return Optional.empty();
        }
        List<KVRangeDescriptor> localLeaderRangeDescriptors = localStoreDesc.getRangesList()
            .stream()
            .filter(d -> d.getRole() == RaftNodeStatus.Leader)
            .filter(d -> d.getState() == StateType.Normal)
            .collect(Collectors.toList());
        // No range in localStore
        if (localLeaderRangeDescriptors.isEmpty()) {
            return Optional.empty();
        }
        Collections.shuffle(localLeaderRangeDescriptors);
        // Sort store list by storeLoad for adding or removing replica
        List<String> sortedAliveStores = latestStoreDescriptors.stream()
            .sorted(Comparator.comparingDouble(this::calStoreLoad))
            .map(KVRangeStoreDescriptor::getId).toList();
        Set<String> allStores = Sets.newHashSet(deadStoreCache.asMap().keySet());
        allStores.addAll(sortedAliveStores);
        for (KVRangeDescriptor rangeDescriptor : localLeaderRangeDescriptors) {
            Set<String> votersInConfig = Sets.newHashSet(rangeDescriptor.getConfig().getVotersList());
            Set<String> learnersInConfig = Sets.newHashSet(rangeDescriptor.getConfig().getLearnersList());
            List<String> sortedSpareStores = sortedAliveStores.stream()
                .filter(s -> !votersInConfig.contains(s))
                .filter(s -> !learnersInConfig.contains(s))
                .collect(Collectors.toList());
            Set<String> newVoters = tryReplace(votersInConfig, sortedSpareStores, s -> !allStores.contains(s));
            Set<String> newLearners = tryReplace(learnersInConfig, sortedSpareStores, s -> !allStores.contains(s));
            if (!votersInConfig.equals(newVoters) || !learnersInConfig.equals(newLearners)) {
                ChangeConfigCommand changeConfigCommand = ChangeConfigCommand.builder()
                    .toStore(localStoreId)
                    .expectedVer(rangeDescriptor.getVer())
                    .kvRangeId(rangeDescriptor.getId())
                    .voters(newVoters)
                    .learners(newLearners)
                    .build();
                return Optional.of(changeConfigCommand);
            }
        }
        return Optional.empty();
    }

    private Set<String> tryReplace(Set<String> origin, List<String> spareStores, Predicate<String> replaceCondition) {
        Set<String> result = new HashSet<>();
        for (String s : origin) {
            if (replaceCondition.test(s)) {
                // try to replace store not exist
                if (!spareStores.isEmpty()) {
                    result.add(spareStores.remove(0));
                }
            } else {
                result.add(s);
            }
        }
        return result;
    }


    private double calStoreLoad(KVRangeStoreDescriptor descriptor) {
        return descriptor.getRangesList()
            .stream()
            .map(kvRangeDescriptor -> kvRangeDescriptor.getLoadHint().getLoad())
            .reduce(Double::sum).orElse(0.0D);
    }
}
