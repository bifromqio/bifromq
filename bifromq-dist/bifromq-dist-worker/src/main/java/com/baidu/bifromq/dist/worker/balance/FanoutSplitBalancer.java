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

package com.baidu.bifromq.dist.worker.balance;

import com.baidu.bifromq.basekv.balance.StoreBalancer;
import com.baidu.bifromq.basekv.balance.command.BalanceCommand;
import com.baidu.bifromq.basekv.balance.command.SplitCommand;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.proto.SplitHint;
import com.baidu.bifromq.basekv.proto.State;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.baidu.bifromq.dist.worker.hinter.FanoutSplitHinter;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class FanoutSplitBalancer extends StoreBalancer {
    private static final double DEFAULT_CPU_USAGE_LIMIT = 0.8;
    private final double cpuUsageLimit;
    private volatile Set<KVRangeStoreDescriptor> latestStoreDescriptors = Collections.emptySet();

    public FanoutSplitBalancer(String localStoreId) {
        this(localStoreId, DEFAULT_CPU_USAGE_LIMIT);
    }

    public FanoutSplitBalancer(String localStoreId, double cpuUsageLimit) {
        super(localStoreId);
        Preconditions.checkArgument(0 < cpuUsageLimit && cpuUsageLimit < 1.0, "Invalid cpu usage limit");
        this.cpuUsageLimit = cpuUsageLimit;
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
            log.warn("There is no storeDescriptor for local store[{}]", localStoreId);
            return Optional.empty();
        }
        double cpuUsage = localStoreDesc.getStatisticsMap().get("cpu.usage");
        if (cpuUsage > cpuUsageLimit) {
            log.warn("High CPU usage[{}], temporarily disable FanoutSplitBalancer for local store[{}]",
                cpuUsage, localStoreId);
            return Optional.empty();
        }
        List<KVRangeDescriptor> localLeaderRangeDescriptors = localStoreDesc.getRangesList()
            .stream()
            .filter(d -> d.getRole() == RaftNodeStatus.Leader)
            .filter(d -> d.getState() == State.StateType.Normal)
            .filter(d -> d.getHintsList().stream()
                .anyMatch(hint -> hint.getType().equals(FanoutSplitHinter.TYPE)))
            // split range with highest io density
            .sorted((o1, o2) -> {
                int c1 = Double.compare(
                    o2.getHints(0).getLoadOrDefault(FanoutSplitHinter.LOAD_TYPE_FANOUT_TOPIC_FILTERS, 0),
                    o1.getHints(0).getLoadOrDefault(FanoutSplitHinter.LOAD_TYPE_FANOUT_TOPIC_FILTERS, 0));
                if (c1 != 0) {
                    return c1;
                }
                return Double.compare(o2.getHints(0).getLoadOrDefault(FanoutSplitHinter.LOAD_TYPE_FANOUT_SCALE, 0),
                    o1.getHints(0).getLoadOrDefault(FanoutSplitHinter.LOAD_TYPE_FANOUT_SCALE, 0));
            })
            .toList();
        // No leader range in localStore
        if (localLeaderRangeDescriptors.isEmpty()) {
            return Optional.empty();
        }
        for (KVRangeDescriptor leaderRangeDescriptor : localLeaderRangeDescriptors) {
            SplitHint splitHint = leaderRangeDescriptor.getHints(0);
            assert splitHint.getType().equals(FanoutSplitHinter.TYPE);
            if (splitHint.hasSplitKey()) {
                return Optional.of(SplitCommand.builder()
                    .toStore(localStoreId)
                    .expectedVer(leaderRangeDescriptor.getVer())
                    .kvRangeId(leaderRangeDescriptor.getId())
                    .splitKey(splitHint.getSplitKey())
                    .build());
            }
        }
        return Optional.empty();
    }
}
