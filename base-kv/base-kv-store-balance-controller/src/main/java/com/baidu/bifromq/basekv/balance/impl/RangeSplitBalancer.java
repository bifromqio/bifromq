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

import static com.baidu.bifromq.basekv.balance.DescriptorUtil.getLeastEpoch;

import com.baidu.bifromq.basekv.balance.StoreBalancer;
import com.baidu.bifromq.basekv.balance.command.SplitCommand;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.proto.SplitHint;
import com.baidu.bifromq.basekv.proto.State;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class RangeSplitBalancer extends StoreBalancer {
    public static final String LOAD_TYPE_IO_DENSITY = "ioDensity";
    public static final String LOAD_TYPE_IO_LATENCY_NANOS = "ioLatencyNanos";
    public static final String LOAD_TYPE_AVG_LATENCY_NANOS = "avgLatencyNanos";


    private static final int DEFAULT_MAX_RANGES_PER_STORE = 30;
    private static final double DEFAULT_CPU_USAGE_LIMIT = 0.8;
    private static final int DEFAULT_MAX_IO_DENSITY_PER_RANGE = 30;
    private static final long DEFAULT_IO_NANOS_LIMIT_PER_RANGE = 30_000;
    private final int maxRanges;
    private final double cpuUsageLimit;
    private final int maxIODensityPerRange;
    private final long ioNanosLimitPerRange;
    private final String hintType;
    private volatile Set<KVRangeStoreDescriptor> latestStoreDescriptors = Collections.emptySet();

    public RangeSplitBalancer(String clusterId, String localStoreId, String hintType) {
        this(clusterId,
            localStoreId,
            hintType,
            DEFAULT_MAX_RANGES_PER_STORE,
            DEFAULT_CPU_USAGE_LIMIT,
            DEFAULT_MAX_IO_DENSITY_PER_RANGE,
            DEFAULT_IO_NANOS_LIMIT_PER_RANGE);
    }

    public RangeSplitBalancer(String clusterId,
                              String localStoreId,
                              String hintType,
                              int maxRanges,
                              double cpuUsageLimit,
                              int maxIoDensityPerRange,
                              long ioNanoLimitPerRange) {
        super(clusterId, localStoreId);
        this.hintType = hintType;
        this.maxRanges = maxRanges;
        this.cpuUsageLimit = cpuUsageLimit;
        this.maxIODensityPerRange = maxIoDensityPerRange;
        this.ioNanosLimitPerRange = ioNanoLimitPerRange;
    }

    @Override
    public void update(Set<KVRangeStoreDescriptor> storeDescriptors) {
        latestStoreDescriptors = getLeastEpoch(storeDescriptors);
    }

    @Override
    public Optional<SplitCommand> balance() {
        KVRangeStoreDescriptor localStoreDesc = null;
        for (KVRangeStoreDescriptor d : latestStoreDescriptors) {
            if (d.getId().equals(localStoreId)) {
                localStoreDesc = d;
                break;
            }
        }
        if (localStoreDesc == null) {
            log.debug("There is no storeDescriptor for local store[{}]", localStoreId);
            return Optional.empty();
        }
        double cpuUsage = localStoreDesc.getStatisticsMap().get("cpu.usage");
        if (cpuUsage > cpuUsageLimit) {
            log.debug("High CPU usage[{}], temporarily disable RangeSplitBalancer for local store[{}]",
                cpuUsage, localStoreId);
            return Optional.empty();
        }
        if (localStoreDesc.getRangesList().size() >= maxRanges) {
            log.debug("Max {} ranges allowed for local store[{}]", maxRanges, localStoreId);
            return Optional.empty();
        }
        List<KVRangeDescriptor> localLeaderRangeDescriptors = localStoreDesc.getRangesList()
            .stream()
            .filter(d -> d.getRole() == RaftNodeStatus.Leader)
            .filter(d -> d.getState() == State.StateType.Normal)
            .filter(d -> d.getHintsList().stream()
                .anyMatch(hint -> hint.getType().equals(hintType)))
            // split range with highest io density
            .sorted((o1, o2) -> Double.compare(o2.getHints(0).getLoadOrDefault(LOAD_TYPE_IO_DENSITY, 0),
                o1.getHints(0).getLoadOrDefault(LOAD_TYPE_IO_DENSITY, 0)))
            .toList();
        // No leader range in localStore
        if (localLeaderRangeDescriptors.isEmpty()) {
            return Optional.empty();
        }
        for (KVRangeDescriptor leaderRangeDescriptor : localLeaderRangeDescriptors) {
            Optional<SplitHint> splitHintOpt = leaderRangeDescriptor
                .getHintsList()
                .stream()
                .filter(h -> h.getType().equals(hintType))
                .findFirst();
            assert splitHintOpt.isPresent();
            SplitHint splitHint = splitHintOpt.get();
            if (splitHint.getLoadOrDefault(LOAD_TYPE_IO_LATENCY_NANOS, 0) < ioNanosLimitPerRange
                &&
                splitHint.getLoadOrDefault(LOAD_TYPE_IO_DENSITY, 0) > maxIODensityPerRange && splitHint.hasSplitKey()) {
                log.debug("Split range[{}] in store[{}]: key={}",
                    KVRangeIdUtil.toString(leaderRangeDescriptor.getId()),
                    localStoreId, splitHint.getSplitKey());
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
