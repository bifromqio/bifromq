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

import com.baidu.bifromq.basekv.balance.StoreBalancer;
import com.baidu.bifromq.basekv.balance.command.BalanceCommand;
import com.baidu.bifromq.basekv.balance.command.ChangeConfigCommand;
import com.baidu.bifromq.basekv.balance.command.TransferLeadershipCommand;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.proto.State.StateType;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.baidu.bifromq.basekv.utils.DescriptorUtil;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class RangeLeaderBalancer extends StoreBalancer {
    private volatile Set<KVRangeStoreDescriptor> latestStoreDescriptors = Collections.emptySet();

    public RangeLeaderBalancer(String clusterId, String localStoreId) {
        super(clusterId, localStoreId);
    }

    @Override
    public void update(Set<KVRangeStoreDescriptor> storeDescriptors) {
        Optional<DescriptorUtil.EffectiveEpoch> effectiveEpoch = getEffectiveEpoch(storeDescriptors);
        if (effectiveEpoch.isEmpty()) {
            return;
        }
        latestStoreDescriptors = effectiveEpoch.get().storeDescriptors();
    }

    @Override
    public Optional<BalanceCommand> balance() {
        List<KVRangeDescriptor> localLeaderRangeDescriptors = localLeaderRangeDescriptors();
        if (localLeaderRangeDescriptors.isEmpty()) {
            return Optional.empty();
        }
        // Sort store list by leader count and storeId
        TreeMap<StoreLeader, Set<KVRangeId>> storeLeaders = new TreeMap<>((s1, s2) -> {
            if (s1.leaderCount == s2.leaderCount) {
                return s1.storeId.compareTo(s2.storeId);
            }
            return s2.leaderCount - s1.leaderCount;
        });
        Set<KVRangeId> allLeaderIds = new HashSet<>();
        for (KVRangeStoreDescriptor storeDescriptor : latestStoreDescriptors) {
            Set<KVRangeId> rangeIds = storeDescriptor.getRangesList().stream()
                .filter(rd -> RaftNodeStatus.Leader.equals(rd.getRole()))
                .filter(rd -> StateType.Normal.equals(rd.getState()))
                .map(KVRangeDescriptor::getId)
                .collect(Collectors.toSet());
            allLeaderIds.addAll(rangeIds);
            storeLeaders.put(new StoreLeader(storeDescriptor.getId(), rangeIds.size()), rangeIds);
        }
        // check if there is KVRange with more than one leader
        double leaderCount = storeLeaders.keySet().stream().map(sl -> sl.leaderCount).reduce(Integer::sum).orElse(0);
        if (leaderCount > allLeaderIds.size()) {
            log.debug("Failed to balance rangeLeaders for there is KVRange with more than one leader!");
            return Optional.empty();
        }
        return balanceStoreLeaders(localLeaderRangeDescriptors, storeLeaders, leaderCount);
    }

    private Optional<BalanceCommand> balanceStoreLeaders(List<KVRangeDescriptor> localLeaderRangeDescriptors,
                                                         TreeMap<StoreLeader, Set<KVRangeId>> storeLeaders,
                                                         double leaderCount) {
        // check if local store should be firstly balanced
        if (!localStoreId.equals(storeLeaders.firstKey().storeId)) {
            return Optional.empty();
        }
        double storeCount = latestStoreDescriptors.size();
        int cMax = (int) Math.ceil(leaderCount / storeCount);
        int cMin = (int) Math.floor(leaderCount / storeCount);
        log.debug("prepare to balance with rangeCount={}, storeCount={}, cMax={}, cMin={}", leaderCount, storeCount,
            cMax, cMin);
        if (localLeaderRangeDescriptors.size() > cMax ||
            (localLeaderRangeDescriptors.size() == cMax && storeLeaders.lastKey().leaderCount < cMin)) {
            KVRangeDescriptor rangeToBalance = localLeaderRangeDescriptors.get(0);
            String storeToTransfer = storeLeaders.lastKey().storeId;
            if (rangeToBalance.getConfig().getVotersList().contains(storeToTransfer)) {
                // destination store is in voter list, just transfer
                return Optional.of(TransferLeadershipCommand.builder()
                    .kvRangeId(rangeToBalance.getId())
                    .expectedVer(rangeToBalance.getVer())
                    .toStore(localStoreId)
                    .newLeaderStore(storeToTransfer)
                    .build());
            } else {
                // destination store is not in voter list, changeConfig first
                Set<String> voters = new HashSet<>(rangeToBalance.getConfig().getVotersList());
                Set<String> learners = new HashSet<>(rangeToBalance.getConfig().getLearnersList());
                String voterToRemove = null;
                if (voters.size() == 1) {
                    voterToRemove = localStoreId;
                    voters = Sets.newHashSet(storeToTransfer);
                } else {
                    Iterator<String> iterator = voters.iterator();
                    while (iterator.hasNext()) {
                        String voter = iterator.next();
                        if (!localStoreId.equals(voter)) {
                            voterToRemove = voter;
                            iterator.remove();
                            break;
                        }
                    }
                    voters.add(storeToTransfer);
                }
                if (learners.remove(storeToTransfer)) {
                    learners.add(voterToRemove);
                }
                return Optional.of(ChangeConfigCommand.builder()
                    .kvRangeId(rangeToBalance.getId())
                    .expectedVer(rangeToBalance.getVer())
                    .toStore(localStoreId)
                    .voters(voters)
                    .learners(learners)
                    .build());
            }
        }
        return Optional.empty();
    }

    private List<KVRangeDescriptor> localLeaderRangeDescriptors() {
        KVRangeStoreDescriptor localStoreDesc = null;
        for (KVRangeStoreDescriptor d : latestStoreDescriptors) {
            if (d.getId().equals(localStoreId)) {
                localStoreDesc = d;
                break;
            }
        }
        if (localStoreDesc == null) {
            log.debug("There is no storeDescriptor for local store: {}", localStoreId);
            return Collections.emptyList();
        }
        return localStoreDesc.getRangesList()
            .stream()
            .filter(d -> d.getRole() == RaftNodeStatus.Leader)
            .filter(d -> d.getState() == StateType.Normal)
            .sorted(Comparator.comparingLong(o -> o.getId().getId()))
            .toList();
    }

    private record StoreLeader(String storeId, int leaderCount) {
    }
}
