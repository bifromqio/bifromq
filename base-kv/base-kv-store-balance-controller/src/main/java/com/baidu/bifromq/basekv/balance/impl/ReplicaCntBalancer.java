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

import static com.baidu.bifromq.basekv.utils.DescriptorUtil.getEffectiveEpoch;
import static com.baidu.bifromq.basekv.utils.DescriptorUtil.toLeaderRanges;
import static java.util.Collections.emptySet;

import com.baidu.bifromq.basekv.balance.StoreBalancer;
import com.baidu.bifromq.basekv.balance.command.ChangeConfigCommand;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.utils.DescriptorUtil;
import com.baidu.bifromq.basekv.utils.KeySpaceDAG;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ReplicaCntBalancer is a load balancer designed to manage the number of replicas across distributed storage nodes. Its
 * primary responsibility is to ensure that each storage node (Store) maintains the expected number of replicas of
 * effective ranges, including both Voter and Learner replicas.
 *
 * <p>Main functionalities:</p>
 *
 * <ul>
 *   <li><b>voterCount</b>: The expected number of Voter replicas on each store.</li>
 *   <li><b>learnerCount</b>: The expected number of Learner replicas. If set to -1, there is no limit on Learner replicas.</li>
 * </ul>
 */
public class ReplicaCntBalancer extends StoreBalancer {
    private final int voterCount;
    private final int learnerCount; // -1 represent no limit
    private volatile Set<KVRangeStoreDescriptor> effectiveEpoch = emptySet();

    public ReplicaCntBalancer(String clusterId, String localStoreId, int voterCount, int learnerCount) {
        super(clusterId, localStoreId);
        this.voterCount = voterCount;
        this.learnerCount = learnerCount;
    }

    @Override
    public void update(Set<KVRangeStoreDescriptor> storeDescriptors) {
        Optional<DescriptorUtil.EffectiveEpoch> effectiveEpoch = getEffectiveEpoch(storeDescriptors);
        if (effectiveEpoch.isEmpty()) {
            return;
        }
        this.effectiveEpoch = effectiveEpoch.get().storeDescriptors();
    }

    @Override
    public Optional<ChangeConfigCommand> balance() {
        Set<KVRangeStoreDescriptor> current = effectiveEpoch;
        Map<String, Map<KVRangeId, KVRangeDescriptor>> allLeaderRangesByStoreId = toLeaderRanges(current);
        KeySpaceDAG keySpaceDAG = new KeySpaceDAG(allLeaderRangesByStoreId);
        NavigableMap<Boundary, KeySpaceDAG.LeaderRange> effectiveRoute = keySpaceDAG.getEffectiveFullCoveredRoute();
        if (effectiveRoute.isEmpty()) {
            // only operate on the leader range in effectiveRoute
            return Optional.empty();
        }
        Map<String, Set<KVRangeId>> effectiveLeaderRangesByStoreId = new HashMap<>();
        for (KeySpaceDAG.LeaderRange leaderRange : effectiveRoute.values()) {
            effectiveLeaderRangesByStoreId.computeIfAbsent(leaderRange.storeId(), k -> new HashSet<>())
                .add(leaderRange.descriptor().getId());
        }
        Set<KVRangeId> localEffectiveLeaderRangeIds =
            effectiveLeaderRangesByStoreId.getOrDefault(localStoreId, Collections.emptySet());

        // No leader range in localStore
        if (localEffectiveLeaderRangeIds.isEmpty()) {
            return Optional.empty();
        }
        // Sort store list by storeLoad for adding or removing replica
        List<String> sortedAliveStore = current.stream()
            .sorted(Comparator.comparingDouble(this::calStoreLoad))
            .map(KVRangeStoreDescriptor::getId)
            .collect(Collectors.toList());
        Map<KVRangeId, KVRangeDescriptor> allLeaderRanges = allLeaderRangesByStoreId.get(localStoreId);
        for (KVRangeId rangeId : localEffectiveLeaderRangeIds) {
            KVRangeDescriptor rangeDescriptor = allLeaderRanges.get(rangeId);
            Set<String> votersInConfig = Sets.newHashSet(rangeDescriptor.getConfig().getVotersList());
            Set<String> learnersInConfig = Sets.newHashSet(rangeDescriptor.getConfig().getLearnersList());
            Set<String> newVoters = getNewVoters(sortedAliveStore, votersInConfig);
            Set<String> newLearners = addLearners(sortedAliveStore, newVoters, learnersInConfig);
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

    private Set<String> getNewVoters(List<String> sortedCandidateStores, Set<String> oldVoters) {
        Set<String> newVoters = oldVoters.stream()
            .filter(sortedCandidateStores::contains)
            .collect(Collectors.toSet());
        // Add voter from less hot store
        for (String s : sortedCandidateStores) {
            if (newVoters.size() == voterCount) {
                break;
            }
            newVoters.add(s);
        }
        return newVoters;
    }

    private Set<String> addLearners(List<String> sortedCandidateStores, Set<String> voters, Set<String> oldLearners) {
        if (learnerCount < 0) {
            // if learnerCount < 0, no limit
            return sortedCandidateStores.stream()
                .filter(s -> !voters.contains(s))
                .collect(Collectors.toSet());
        } else if (learnerCount > 0) {
            Set<String> newLearners = oldLearners.stream()
                .filter(l -> !voters.contains(l))
                .filter(sortedCandidateStores::contains)
                .collect(Collectors.toSet());
            // Add some learners from less hot store
            for (String storeId : sortedCandidateStores) {
                if (newLearners.size() == learnerCount) {
                    break;
                }
                if (!voters.contains(storeId)) {
                    newLearners.add(storeId);
                }
            }
            return newLearners;
        } else {
            return emptySet();
        }
    }

    private double calStoreLoad(KVRangeStoreDescriptor descriptor) {
        return descriptor.getStatisticsMap().get("cpu.usage");
    }
}
