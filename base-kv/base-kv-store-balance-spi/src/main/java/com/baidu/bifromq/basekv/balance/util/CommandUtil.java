/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.basekv.balance.util;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.compareStartKey;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.inRange;

import com.baidu.bifromq.basekv.balance.BalanceNow;
import com.baidu.bifromq.basekv.balance.BalanceResult;
import com.baidu.bifromq.basekv.balance.command.ChangeConfigCommand;
import com.baidu.bifromq.basekv.balance.command.MergeCommand;
import com.baidu.bifromq.basekv.balance.command.RangeCommand;
import com.baidu.bifromq.basekv.balance.command.SplitCommand;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.utils.BoundaryUtil;
import com.baidu.bifromq.basekv.utils.KeySpaceDAG;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Set;

/**
 * Utility class for generating balance commands.
 */
public class CommandUtil {
    /**
     * Generate correct ChangeConfigCommand for quit.
     *
     * @param localStoreId    the store id of the store which the balancer is responsible for
     * @param rangeDescriptor the range descriptor of the range which the balancer is responsible for
     * @return the generated ChangeConfigCommand
     */
    public static BalanceResult quit(String localStoreId, KVRangeDescriptor rangeDescriptor) {
        ClusterConfig config = rangeDescriptor.getConfig();
        if (config.getVotersCount() > 1 || config.getLearnersCount() > 0) {
            return BalanceNow.of(ChangeConfigCommand.builder()
                .toStore(localStoreId)
                .kvRangeId(rangeDescriptor.getId())
                .expectedVer(rangeDescriptor.getVer())
                .voters(Set.of(localStoreId))
                .learners(Collections.emptySet())
                .build());
        } else {
            return BalanceNow.of(ChangeConfigCommand.builder()
                .toStore(localStoreId)
                .kvRangeId(rangeDescriptor.getId())
                .expectedVer(rangeDescriptor.getVer())
                .voters(Collections.emptySet())
                .learners(Collections.emptySet())
                .build());
        }
    }

    /**
     * Calculate the range layout difference between expected and current layout, and returns the first command to apply
     * so that the current layout can be transformed to the expected layout.
     *
     * @param expectedRangeLayout the expected range layout.
     * @param currentRangeLayout  the current range layout.
     * @return the first command to apply so that the current layout can be transformed to the expected layout.
     */
    public static RangeCommand diffBy(NavigableMap<Boundary, ClusterConfig> expectedRangeLayout,
                                      NavigableMap<Boundary, KeySpaceDAG.LeaderRange> currentRangeLayout) {
        assert BoundaryUtil.isValidSplitSet(expectedRangeLayout.keySet());
        assert BoundaryUtil.isValidSplitSet(currentRangeLayout.keySet());
        Iterator<Boundary> expectedRangeItr = expectedRangeLayout.keySet().iterator();
        Iterator<Boundary> currentRangeItr = currentRangeLayout.keySet().iterator();
        while (expectedRangeItr.hasNext() && currentRangeItr.hasNext()) {
            Boundary expectedRange = expectedRangeItr.next();
            ClusterConfig expectedClusterConfig = expectedRangeLayout.get(expectedRange);

            Boundary currentRange = currentRangeItr.next();
            KeySpaceDAG.LeaderRange currentLeaderRange = currentRangeLayout.get(currentRange);
            ClusterConfig currentClusterConfig = currentLeaderRange.descriptor().getConfig();
            if (!currentClusterConfig.getNextVotersList().isEmpty()
                || !currentClusterConfig.getNextLearnersList().isEmpty()) {
                // abort generation if there is running config change process
                return null;
            }
            if (expectedRange.equals(currentRange)) {
                if (!isSame(expectedClusterConfig, currentClusterConfig)) {
                    // generate config change
                    return ChangeConfigCommand.builder()
                        .toStore(currentLeaderRange.storeId())
                        .kvRangeId(currentLeaderRange.descriptor().getId())
                        .expectedVer(currentLeaderRange.descriptor().getVer())
                        .voters(setOf(expectedClusterConfig.getVotersList()))
                        .learners(setOf(expectedClusterConfig.getLearnersList()))
                        .build();
                }
                // continue to next range
            } else if (inRange(expectedRange, currentRange)) {
                assert compareStartKey(expectedRange.getStartKey(), currentRange.getStartKey()) == 0;
                // generate split
                return SplitCommand.builder()
                    .toStore(currentLeaderRange.storeId())
                    .kvRangeId(currentLeaderRange.descriptor().getId())
                    .expectedVer(currentLeaderRange.descriptor().getVer())
                    .splitKey(expectedRange.getEndKey())
                    .build();
            } else {
                // handling merging
                assert inRange(currentRange, expectedRange);
                assert currentRangeItr.hasNext();
                Boundary nextCurrentBoundary = currentRangeItr.next();
                KeySpaceDAG.LeaderRange nextCurrentLeaderRange = currentRangeLayout.get(nextCurrentBoundary);
                ClusterConfig nextCurrentRangeClusterConfig = nextCurrentLeaderRange.descriptor().getConfig();
                if (!nextCurrentRangeClusterConfig.getNextVotersList().isEmpty()
                    || !nextCurrentRangeClusterConfig.getNextLearnersList().isEmpty()) {
                    // abort generation if there is running config change process in mergee range
                    return null;
                } else if (isSame(expectedClusterConfig, nextCurrentRangeClusterConfig)) {
                    return MergeCommand.builder()
                        .toStore(currentLeaderRange.storeId())
                        .kvRangeId(currentLeaderRange.descriptor().getId())
                        .expectedVer(currentLeaderRange.descriptor().getVer())
                        .mergeeId(nextCurrentLeaderRange.descriptor().getId())
                        .build();
                } else {
                    return ChangeConfigCommand.builder()
                        .toStore(nextCurrentLeaderRange.storeId())
                        .kvRangeId(nextCurrentLeaderRange.descriptor().getId())
                        .expectedVer(nextCurrentLeaderRange.descriptor().getVer())
                        .voters(setOf(currentClusterConfig.getVotersList()))
                        .learners(setOf(currentClusterConfig.getLearnersList()))
                        .build();
                }
            }
        }
        return null;
    }

    /**
     * Check if two ClusterConfigs are the same.
     *
     * @param config1 the first ClusterConfig
     * @param config2 the second ClusterConfig
     * @return true if the two ClusterConfigs are the same, otherwise false
     */
    public static boolean isSame(ClusterConfig config1, ClusterConfig config2) {
        return setOf(config1.getVotersList()).equals(setOf(config2.getVotersList()))
            && setOf(config1.getLearnersList()).equals(setOf(config2.getLearnersList()))
            && setOf(config1.getNextVotersList()).equals(setOf(config2.getNextVotersList()))
            && setOf(config1.getNextLearnersList()).equals(setOf(config2.getNextLearnersList()));
    }

    private static Set<String> setOf(Collection<String> iterable) {
        return new HashSet<>(iterable);
    }
}
