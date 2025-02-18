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

import com.baidu.bifromq.basekv.balance.BalanceNow;
import com.baidu.bifromq.basekv.balance.BalanceResult;
import com.baidu.bifromq.basekv.balance.NoNeedBalance;
import com.baidu.bifromq.basekv.balance.StoreBalancer;
import com.baidu.bifromq.basekv.balance.command.TransferLeadershipCommand;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.utils.DescriptorUtil;
import com.baidu.bifromq.basekv.utils.KeySpaceDAG;
import com.google.common.collect.Sets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The goal of the balancer is to balance the leader count of each store by emitting TransferLeadership command.
 */
public class RangeLeaderBalancer extends StoreBalancer {
    private volatile Set<KVRangeStoreDescriptor> effectiveEpoch = emptySet();

    public RangeLeaderBalancer(String clusterId, String localStoreId) {
        super(clusterId, localStoreId);
    }

    @Override
    public void update(Set<KVRangeStoreDescriptor> landscape) {
        Optional<DescriptorUtil.EffectiveEpoch> effectiveEpoch = getEffectiveEpoch(landscape);
        if (effectiveEpoch.isEmpty()) {
            return;
        }
        this.effectiveEpoch = effectiveEpoch.get().storeDescriptors();
    }

    @Override
    public BalanceResult balance() {
        Set<KVRangeStoreDescriptor> current = effectiveEpoch;
        // leader ranges including non-effective ranges
        Map<String, Map<KVRangeId, KVRangeDescriptor>> allLeaderRangesByStoreId = toLeaderRanges(current);
        KeySpaceDAG keySpaceDAG = new KeySpaceDAG(allLeaderRangesByStoreId);
        NavigableMap<Boundary, KeySpaceDAG.LeaderRange> effectiveRoute = keySpaceDAG.getEffectiveFullCoveredRoute();
        if (effectiveRoute.isEmpty()) {
            // only operate on the leader range in effectiveRoute
            return NoNeedBalance.INSTANCE;
        }
        Map<String, KVRangeStoreDescriptor> landscape = current.stream()
            .collect(Collectors.toMap(KVRangeStoreDescriptor::getId, store -> store));
        return balanceLeaderCount(landscape, effectiveRoute);
    }

    private BalanceResult balanceLeaderCount(Map<String, KVRangeStoreDescriptor> landscape,
                                             NavigableMap<Boundary, KeySpaceDAG.LeaderRange> effectiveRoute) {
        Map<String, Integer> storeLeaderCount = new HashMap<>();
        for (Map.Entry<Boundary, KeySpaceDAG.LeaderRange> entry : effectiveRoute.entrySet()) {
            String localStoreId = entry.getValue().storeId();
            KVRangeDescriptor rangeDescriptor = entry.getValue().descriptor();
            ClusterConfig clusterConfig = rangeDescriptor.getConfig();
            clusterConfig.getVotersList().forEach(voter -> storeLeaderCount.computeIfAbsent(voter, k -> 0));
            storeLeaderCount.compute(localStoreId, (k, v) -> v == null ? 1 : v + 1);
        }
        List<String> storeSortedByLeaderCount = storeLeaderCount.entrySet().stream()
            .sorted(Comparator.comparingInt(Map.Entry::getValue))
            .map(Map.Entry::getKey)
            .toList();
        String mostLeadersStore = storeSortedByLeaderCount.get(storeSortedByLeaderCount.size() - 1);
        String leastLeadersStore = storeSortedByLeaderCount.get(0);
        int mostLeadersCount = storeLeaderCount.get(mostLeadersStore);
        int leastLeadersCount = storeLeaderCount.get(leastLeadersStore);

        double totalLeaders = storeLeaderCount.values().stream().mapToInt(Integer::intValue).sum();
        double targetLeadersPerStore = totalLeaders / storeLeaderCount.size();
        int atMostLeadersPerStore = (int) Math.ceil(targetLeadersPerStore);
        int atLeastLeadersPerStore = (int) Math.floor(targetLeadersPerStore);

        // no need to balance if the leader count is within the range
        if (mostLeadersCount <= atMostLeadersPerStore && leastLeadersCount >= atLeastLeadersPerStore) {
            return NoNeedBalance.INSTANCE;
        }
        // scan the effective route to find the range to balance
        for (Map.Entry<Boundary, KeySpaceDAG.LeaderRange> entry : effectiveRoute.entrySet()) {
            KeySpaceDAG.LeaderRange leaderRange = entry.getValue();
            String leaderStoreId = leaderRange.storeId();
            KVRangeDescriptor rangeDescriptor = leaderRange.descriptor();
            ClusterConfig clusterConfig = rangeDescriptor.getConfig();
            Set<String> voters = Sets.newHashSet(clusterConfig.getVotersList());

            // check if current range leader store is most overloaded
            if (leaderStoreId.equals(mostLeadersStore)) {
                // leader store has overloaded leaders replicas
                for (String underloadedStore : storeSortedByLeaderCount) {
                    // move to one underloaded store which is current follower
                    int leaderCount = storeLeaderCount.get(underloadedStore);
                    if (leaderCount + 1 <= atMostLeadersPerStore
                        && voters.contains(underloadedStore)
                        && !underloadedStore.equals(leaderStoreId)) {
                        if (leaderStoreId.equals(localStoreId)) {
                            return BalanceNow.of(TransferLeadershipCommand.builder()
                                .toStore(leaderStoreId)
                                .kvRangeId(rangeDescriptor.getId())
                                .expectedVer(rangeDescriptor.getVer())
                                .newLeaderStore(underloadedStore)
                                .build());
                        }
                        return NoNeedBalance.INSTANCE;
                    }
                }
            } else {
                // check if least overloaded store holds a voter replica of current range and safe to transfer leadership to it
                int leaderCount = storeLeaderCount.get(leaderStoreId);
                if (voters.contains(leastLeadersStore) && leaderCount - 1 >= atLeastLeadersPerStore) {
                    if (leaderStoreId.equals(localStoreId)) {
                        return BalanceNow.of(TransferLeadershipCommand.builder()
                            .toStore(leaderStoreId)
                            .kvRangeId(rangeDescriptor.getId())
                            .expectedVer(rangeDescriptor.getVer())
                            .newLeaderStore(leastLeadersStore)
                            .build());
                    }
                    return NoNeedBalance.INSTANCE;
                }
            }
        }
        return NoNeedBalance.INSTANCE;
    }
}
