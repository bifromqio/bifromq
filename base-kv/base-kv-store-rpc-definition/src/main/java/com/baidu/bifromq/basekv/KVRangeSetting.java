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

package com.baidu.bifromq.basekv;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeSyncState;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class KVRangeSetting {
    private static final Map<String, Set<String>> IN_PROC_STORES = new ConcurrentHashMap<>();

    public static void regInProcStore(String clusterId, String storeId) {
        IN_PROC_STORES.compute(clusterId, (k, v) -> {
            if (v == null) {
                v = new HashSet<>();
            }
            v.add(storeId);
            return v;
        });
    }

    private final String clusterId;
    public final KVRangeId id;
    public final long ver;
    public final Boundary boundary;
    public final String leader;
    public final List<String> voters;
    private final List<String> inProcVoters;
    public final List<String> followers;
    private final List<String> inProcFollowers;
    public final List<String> allReplicas;
    private final List<String> inProcReplicas;

    public KVRangeSetting(String clusterId, String leaderStoreId, KVRangeDescriptor desc) {
        this.clusterId = clusterId;
        id = desc.getId();
        ver = desc.getVer();
        boundary = desc.getBoundary();
        leader = leaderStoreId;
        Set<String> voters = new HashSet<>();
        Set<String> inProcVoters = new HashSet<>();
        Set<String> followers = new HashSet<>();
        Set<String> inProcFollowers = new HashSet<>();
        Set<String> allReplicas = new HashSet<>();
        Set<String> inProcReplicas = new HashSet<>();

        Set<String> allVoters =
            Sets.newHashSet(Iterables.concat(desc.getConfig().getVotersList(), desc.getConfig().getNextVotersList()));
        for (String v : allVoters) {
            if (desc.getSyncStateMap().get(v) == RaftNodeSyncState.Replicating) {
                voters.add(v);
                if (IN_PROC_STORES.getOrDefault(clusterId, Collections.emptySet()).contains(v)) {
                    inProcVoters.add(v);
                }
                if (!v.equals(leaderStoreId)) {
                    followers.add(v);
                    if (IN_PROC_STORES.getOrDefault(clusterId, Collections.emptySet()).contains(v)) {
                        inProcFollowers.add(v);
                    }
                }
                allReplicas.add(v);
                if (IN_PROC_STORES.getOrDefault(clusterId, Collections.emptySet()).contains(v)) {
                    inProcReplicas.add(v);
                }
            }
        }
        Set<String> allLearners = Sets.union(Sets.newHashSet(
            Iterables.concat(desc.getConfig().getLearnersList(), desc.getConfig().getNextLearnersList())), allVoters);

        for (String v : allLearners) {
            if (desc.getSyncStateMap().get(v) == RaftNodeSyncState.Replicating) {
                allReplicas.add(v);
                if (IN_PROC_STORES.getOrDefault(clusterId, Collections.emptySet()).contains(v)) {
                    inProcReplicas.add(v);
                }
            }
        }
        this.voters = Collections.unmodifiableList(Lists.newArrayList(voters));
        this.inProcVoters = Collections.unmodifiableList(Lists.newArrayList(inProcVoters));
        this.followers = Collections.unmodifiableList(Lists.newArrayList(followers));
        this.inProcFollowers = Collections.unmodifiableList(Lists.newArrayList(inProcFollowers));
        this.allReplicas = Collections.unmodifiableList(Lists.newArrayList(allReplicas));
        this.inProcReplicas = Collections.unmodifiableList(Lists.newArrayList(inProcReplicas));
    }

    public String randomReplica() {
        if (!inProcReplicas.isEmpty()) {
            return inProcReplicas.get(ThreadLocalRandom.current().nextInt(inProcReplicas.size()));
        }
        return allReplicas.get(ThreadLocalRandom.current().nextInt(allReplicas.size()));
    }

    public String randomVoters() {
        if (IN_PROC_STORES.getOrDefault(clusterId, Collections.emptySet()).contains(leader)) {
            return leader;
        } else if (!inProcVoters.isEmpty()) {
            return inProcVoters.get(ThreadLocalRandom.current().nextInt(inProcVoters.size()));
        }
        return voters.get(ThreadLocalRandom.current().nextInt(voters.size()));
    }
}
