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

package com.baidu.bifromq.inbox.store.balance;

import com.baidu.bifromq.basekv.balance.StoreBalancer;
import com.baidu.bifromq.basekv.balance.command.BalanceCommand;
import com.baidu.bifromq.basekv.balance.command.SplitCommand;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.proto.State;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class RangeSplitBalancer extends StoreBalancer {
    private volatile Set<KVRangeStoreDescriptor> latestStoreDescriptors = Collections.emptySet();

    public RangeSplitBalancer(String localStoreId) {
        super(localStoreId);
    }

    @Override
    public void update(Set<KVRangeStoreDescriptor> storeDescriptors) {
        latestStoreDescriptors = storeDescriptors;
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
            .filter(d -> d.getState() == State.StateType.Normal)
            .toList();
        // No leader range in localStore
        if (localLeaderRangeDescriptors.isEmpty()) {
            return Optional.empty();
        }
        for (KVRangeDescriptor leaderRangeDescriptor : localLeaderRangeDescriptors) {
            if (leaderRangeDescriptor.getLoadHint().hasSplitKey()) {
                return Optional.of(SplitCommand.builder()
                    .toStore(localStoreId)
                    .expectedVer(leaderRangeDescriptor.getVer())
                    .kvRangeId(leaderRangeDescriptor.getId())
                    .splitKey(leaderRangeDescriptor.getLoadHint().getSplitKey())
                    .build());
            }
        }
        return Optional.empty();
    }
}
