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
import com.baidu.bifromq.basekv.balance.command.BalanceCommand;
import com.baidu.bifromq.basekv.balance.command.ChangeConfigCommand;
import com.baidu.bifromq.basekv.balance.command.TransferLeadershipCommand;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.utils.DescriptorUtil;
import com.baidu.bifromq.basekv.utils.KeySpaceDAG;
import com.google.common.collect.Sets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * RangeLeaderBalancer is a load balancer that manages the distribution of leader replicas across storage nodes (Stores)
 * in a distributed key-value storage system. The goal of this balancer is to evenly distribute leader replicas of
 * effective ranges among available stores to prevent any single store from becoming overloaded with too many leader
 * roles.
 */
public class RangeLeaderBalancer extends StoreBalancer {
    private volatile Set<KVRangeStoreDescriptor> effectiveEpoch = emptySet();

    public RangeLeaderBalancer(String clusterId, String localStoreId) {
        super(clusterId, localStoreId);
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
    public Optional<BalanceCommand> balance() {
        Set<KVRangeStoreDescriptor> current = effectiveEpoch;
        // leader ranges including non-effective ranges
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
        return balanceStoreLeaders(allLeaderRangesByStoreId, effectiveLeaderRangesByStoreId, effectiveRoute.size());
    }

    private Optional<BalanceCommand> balanceStoreLeaders(
        Map<String, Map<KVRangeId, KVRangeDescriptor>> allLeaderRangesByStoreId,
        Map<String, Set<KVRangeId>> effectiveLeaderRangesByStoreId,
        int effectiveLeaderRangeCount) {
        double storeCount = allLeaderRangesByStoreId.size();
        if (storeCount == 1) {
            return Optional.empty();
        }
        int atMost = (int) Math.ceil(effectiveLeaderRangeCount / storeCount);
        int atLeast = (int) Math.floor(effectiveLeaderRangeCount / storeCount);
        Set<KVRangeId> localEffectiveLeaderRangeIds =
            effectiveLeaderRangesByStoreId.getOrDefault(localStoreId, emptySet());

        // Case when local store has more leaders than the maximum allowed
        if (localEffectiveLeaderRangeIds.size() > atMost) {
            // Existing logic to redistribute leaders from overloaded store
            return handleOverloadedStore(allLeaderRangesByStoreId, effectiveLeaderRangesByStoreId, atLeast,
                localEffectiveLeaderRangeIds);
            // Case when local store has exactly 'atMost' leaders
        } else if (localEffectiveLeaderRangeIds.size() == atMost) {
            // Handle the scenario where redistribution is necessary to balance other stores
            return handleExactBalance(allLeaderRangesByStoreId, effectiveLeaderRangesByStoreId, atLeast,
                localEffectiveLeaderRangeIds);
        }

        return Optional.empty();
    }

    private Optional<BalanceCommand> handleOverloadedStore(
        Map<String, Map<KVRangeId, KVRangeDescriptor>> allLeaderRangesByStoreId,
        Map<String, Set<KVRangeId>> effectiveLeaderRangesByStoreId,
        int atLeast,
        Set<KVRangeId> localEffectiveLeaderRangeIds) {
        NavigableSet<StoreLeaderCount> storeLeaders =
            new TreeSet<>(Comparator.comparingInt(StoreLeaderCount::leaderCount));
        effectiveLeaderRangesByStoreId.forEach(
            (storeId, leaderRangeIds) -> storeLeaders.add(new StoreLeaderCount(storeId, leaderRangeIds.size())));
        Sets.difference(allLeaderRangesByStoreId.keySet(), effectiveLeaderRangesByStoreId.keySet())
            .forEach(storeId -> storeLeaders.add(new StoreLeaderCount(storeId, 0)));
        BalanceTask balanceTask = findBestRangeToBalance(localEffectiveLeaderRangeIds,
            allLeaderRangesByStoreId.get(localStoreId),
            storeLeaders, atLeast);
        KVRangeDescriptor rangeToBalance = balanceTask.range;
        String targetStoreId = balanceTask.targetStoreId;
        return generateBalanceCommand(rangeToBalance, targetStoreId);
    }

    private Optional<BalanceCommand> handleExactBalance(
        Map<String, Map<KVRangeId, KVRangeDescriptor>> allLeaderRangesByStoreId,
        Map<String, Set<KVRangeId>> effectiveLeaderRangesByStoreId,
        int atLeast,
        Set<KVRangeId> localEffectiveLeaderRangeIds) {
        // If there are stores with fewer than 'atLeast' leaders, we should consider balancing
        NavigableSet<StoreLeaderCount> storeOrderByLeaders =
            new TreeSet<>(Comparator.comparingInt(StoreLeaderCount::leaderCount)
                .thenComparing(StoreLeaderCount::storeId));
        effectiveLeaderRangesByStoreId.forEach(
            (storeId, leaderRangeIds) -> storeOrderByLeaders.add(new StoreLeaderCount(storeId, leaderRangeIds.size())));

        if (!storeOrderByLeaders.last().storeId.equals(localStoreId)) {
            return Optional.empty();
        }
        for (StoreLeaderCount storeLeaderCount : storeOrderByLeaders) {
            if (storeLeaderCount.leaderCount() < atLeast) {
                // Transfer a leader from the local store to the store with fewer leaders
                BalanceTask balanceTask = findBestRangeToBalance(localEffectiveLeaderRangeIds,
                    allLeaderRangesByStoreId.get(localStoreId), storeOrderByLeaders, atLeast);
                KVRangeDescriptor rangeToBalance = balanceTask.range;
                String targetStoreId = balanceTask.targetStoreId;
                return generateBalanceCommand(rangeToBalance, targetStoreId);
            } else {
                break;
            }
        }
        return Optional.empty();
    }

    private Optional<BalanceCommand> generateBalanceCommand(KVRangeDescriptor rangeToBalance, String targetStoreId) {
        if (rangeToBalance.getConfig().getVotersList().contains(targetStoreId)) {
            return Optional.of(TransferLeadershipCommand.builder()
                .toStore(localStoreId)
                .kvRangeId(rangeToBalance.getId())
                .expectedVer(rangeToBalance.getVer())
                .newLeaderStore(targetStoreId)
                .build());
        } else if (rangeToBalance.getConfig().getLearnersList().contains(targetStoreId)) {
            Set<String> voters = Sets.newHashSet(rangeToBalance.getConfig().getVotersList());
            Set<String> learners = Sets.newHashSet(rangeToBalance.getConfig().getLearnersList());
            voters.remove(localStoreId);
            voters.add(targetStoreId);
            learners.add(localStoreId);
            learners.remove(targetStoreId);
            return Optional.of(ChangeConfigCommand.builder()
                .toStore(localStoreId)
                .kvRangeId(rangeToBalance.getId())
                .expectedVer(rangeToBalance.getVer())
                .voters(voters)
                .learners(learners)
                .build());
        } else {
            Set<String> voters = Sets.newHashSet(rangeToBalance.getConfig().getVotersList());
            Set<String> learners = Sets.newHashSet(rangeToBalance.getConfig().getLearnersList());
            voters.remove(localStoreId);
            voters.add(targetStoreId);
            return Optional.of(ChangeConfigCommand.builder()
                .toStore(localStoreId)
                .kvRangeId(rangeToBalance.getId())
                .expectedVer(rangeToBalance.getVer())
                .voters(voters)
                .learners(learners)
                .build());
        }
    }

    private record BalanceTask(KVRangeDescriptor range, String targetStoreId) {

    }

    private BalanceTask findBestRangeToBalance(Set<KVRangeId> localEffectiveLeaderRangeIds,
                                               Map<KVRangeId, KVRangeDescriptor> localLeaderRanges,
                                               NavigableSet<StoreLeaderCount> storeLeaderCounts,
                                               int atLeastLeaders) {
        for (StoreLeaderCount storeLeaderCount : storeLeaderCounts) {
            if (storeLeaderCount.leaderCount() <= atLeastLeaders) {
                for (KVRangeId rangeId : localEffectiveLeaderRangeIds) {
                    KVRangeDescriptor range = localLeaderRanges.get(rangeId);
                    if (range.getConfig().getVotersList().contains(storeLeaderCount.storeId())) {
                        return new BalanceTask(range, storeLeaderCount.storeId());
                    }
                }
                for (KVRangeId rangeId : localEffectiveLeaderRangeIds) {
                    KVRangeDescriptor range = localLeaderRanges.get(rangeId);
                    if (range.getConfig().getLearnersList().contains(storeLeaderCount.storeId())) {
                        return new BalanceTask(range, storeLeaderCount.storeId());
                    }
                }
            }
        }
        return new BalanceTask(localLeaderRanges.get(localEffectiveLeaderRangeIds.iterator().next()),
            storeLeaderCounts.first().storeId());
    }

    private record StoreLeaderCount(String storeId, int leaderCount) {
    }
}
