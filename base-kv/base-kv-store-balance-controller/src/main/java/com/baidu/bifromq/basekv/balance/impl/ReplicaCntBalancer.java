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
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.proto.State.StateType;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReplicaCntBalancer extends StoreBalancer {

    private final int voterCount;
    private final Cache<String, Long> deadStoreCache;

    private Set<KVRangeStoreDescriptor> latestStoreDescriptors = new HashSet<>();

    public ReplicaCntBalancer(String localStoreId, int voterCount, long deadStoreTimeoutMillis) {
        super(localStoreId);
        this.voterCount = voterCount;
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
            deadStoreCache.put(deadStore, System.nanoTime());
        }
        for (String aliveStore : currAliveStores) {
            deadStoreCache.invalidate(aliveStore);
        }
        this.latestStoreDescriptors = storeDescriptors;
    }

    @Override
    public Optional<BalanceCommand> balance() {
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
        // No leader range in localStore
        if (localLeaderRangeDescriptors.isEmpty()) {
            return Optional.empty();
        }
        Collections.shuffle(localLeaderRangeDescriptors);
        // Sort store list by storeLoad for adding or removing replica
        List<String> sortedAliveStore = latestStoreDescriptors.stream()
            .sorted(Comparator.comparingDouble(this::calStoreLoad))
            .map(KVRangeStoreDescriptor::getId)
            .collect(Collectors.toList());

        Set<String> storesShouldNotRemove = Sets.newHashSet(deadStoreCache.asMap().keySet());
        storesShouldNotRemove.addAll(sortedAliveStore);
        for (KVRangeDescriptor rangeDescriptor : localLeaderRangeDescriptors) {
            Set<String> votersInConfig = Sets.newHashSet(rangeDescriptor.getConfig().getVotersList());
            Set<String> learnersInConfig = Sets.newHashSet(rangeDescriptor.getConfig().getLearnersList());
            Set<String> newVoters = Sets.newHashSet(votersInConfig).stream()
                .filter(storesShouldNotRemove::contains)
                .collect(Collectors.toSet());
            checkVotersCount(sortedAliveStore, newVoters);
            Set<String> newLearners = storesShouldNotRemove.stream()
                .filter(l -> !newVoters.contains(l))
                .collect(Collectors.toSet());
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

    private void checkVotersCount(List<String> sortedAliveStores, Set<String> voters) {
        if (voters.size() < voterCount) {
            // Add voters from less hot store
            for (String s : sortedAliveStores) {
                if (voters.size() == voterCount) {
                    break;
                }
                voters.add(s);
            }
        }
        if (voters.size() > voterCount) {
            // Try to remove redundant voters from dead store cache firstly
            for (String s : deadStoreCache.asMap().keySet()) {
                if (voters.size() == voterCount) {
                    return;
                }
                voters.remove(s);
            }
            // Remove redundant voters from sortedAliveStores
            for (int i = sortedAliveStores.size() - 1; i >= 0; i--) {
                if (voters.size() == voterCount) {
                    return;
                }
                String s = sortedAliveStores.get(i);
                if (s.equals(localStoreId)) {
                    continue;
                }
                voters.remove(s);
            }
        }
    }


    private double calStoreLoad(KVRangeStoreDescriptor descriptor) {
        return descriptor.getRangesList()
            .stream()
            .map(kvRangeDescriptor -> kvRangeDescriptor.getLoadHint().getLoad())
            .reduce(Double::sum)
            .orElse(0.0D);
    }
}
