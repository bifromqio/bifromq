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

package com.baidu.bifromq.basekv.balance.impl;

import com.baidu.bifromq.basekv.balance.BalanceResult;
import com.baidu.bifromq.basekv.balance.NoNeedBalance;
import com.baidu.bifromq.basekv.balance.StoreBalancer;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;

import java.util.Collections;
import java.util.NavigableMap;
import java.util.Set;

import static com.baidu.bifromq.basekv.balance.util.CommandUtil.quit;
import static com.baidu.bifromq.basekv.utils.DescriptorUtil.organizeByEpoch;

/**
 * The RedundantEpochRemovalBalancer is a specialized StoreBalancer designed to manage and remove redundant replicas
 * associated with higher epochs in a distributed key-value store. This balancer is primarily focused on ensuring that
 * only the necessary replicas from the OLDEST epoch remain active, while redundant replicas generated during bootstrap
 * or startup are removed.
 *
 * <p><b>WARNING:</b> This balancer treats the oldest epoch as the valid epoch. If a node with an older epoch is
 * introduced into a working cluster, it may lead to the removal of replicas from nodes that were previously functioning
 * correctly. This behavior can potentially disrupt the stability of the cluster and should be handled with
 * caution.</p>
 */
public class RedundantEpochRemovalBalancer extends StoreBalancer {
    private volatile NavigableMap<Long, Set<KVRangeStoreDescriptor>> latest = Collections.emptyNavigableMap();

    /**
     * Constructor of StoreBalancer.
     *
     * @param clusterId    the id of the BaseKV cluster which the store belongs to
     * @param localStoreId the id of the store which the balancer is responsible for
     */
    public RedundantEpochRemovalBalancer(String clusterId, String localStoreId) {
        super(clusterId, localStoreId);
    }

    @Override
    public void update(Set<KVRangeStoreDescriptor> landscape) {
        latest = organizeByEpoch(landscape);
    }

    @Override
    public BalanceResult balance() {
        if (latest.isEmpty()) {
            return NoNeedBalance.INSTANCE;
        }
        if (latest.size() > 1) {
            // deal with higher epoch redundant replicas generated during bootstrap at startup time
            Set<KVRangeStoreDescriptor> storeDescriptors = latest.lastEntry().getValue();
            for (KVRangeStoreDescriptor storeDescriptor : storeDescriptors) {
                if (!storeDescriptor.getId().equals(localStoreId)) {
                    continue;
                }
                for (KVRangeDescriptor rangeDescriptor : storeDescriptor.getRangesList()) {
                    if (rangeDescriptor.getRole() != RaftNodeStatus.Leader) {
                        continue;
                    }
                    return quit(localStoreId, rangeDescriptor);
                }
            }
        }
        return NoNeedBalance.INSTANCE;
    }
}
