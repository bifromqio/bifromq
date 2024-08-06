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
import com.baidu.bifromq.basekv.balance.command.BalanceCommand;
import com.baidu.bifromq.basekv.balance.command.ChangeConfigCommand;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.proto.State.StateType;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

public class ReplicaCntBalancer extends StoreBalancer {
    private final int voterCount;
    private final int learnerCount; // -1 represent no limit
    private volatile Set<KVRangeStoreDescriptor> latestStoreDescriptors = new HashSet<>();

    public ReplicaCntBalancer(String clusterId, String localStoreId, int voterCount, int learnerCount) {
        super(clusterId, localStoreId);
        this.voterCount = voterCount;
        this.learnerCount = learnerCount;
    }

    @Override
    public void update(Set<KVRangeStoreDescriptor> storeDescriptors) {
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
            log.debug("There is no storeDescriptor for local store: {}", localStoreId);
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

        for (KVRangeDescriptor rangeDescriptor : localLeaderRangeDescriptors) {
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
            return Sets.newHashSet();
        }
    }

    private double calStoreLoad(KVRangeStoreDescriptor descriptor) {
        return descriptor.getStatisticsMap().get("cpu.usage");
    }
}
