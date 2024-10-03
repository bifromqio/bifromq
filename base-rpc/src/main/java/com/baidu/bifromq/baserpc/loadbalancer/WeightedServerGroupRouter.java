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

package com.baidu.bifromq.baserpc.loadbalancer;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

class WeightedServerGroupRouter implements IServerGroupRouter {
    private final List<String> weightedServerLists;
    private final WCHRouter<String> chRouter;
    private final AtomicInteger rrIndex = new AtomicInteger(0);
    private final Optional<String> inProcServerId;

    WeightedServerGroupRouter(Map<String, Integer> groupWeights,
                              Map<String, Set<String>> serverGroups,
                              String inProcServerId) {
        Map<String, Integer> weightedServers = Maps.newHashMap();
        for (String group : groupWeights.keySet()) {
            int weight = Math.abs(groupWeights.get(group)) % 11; // weight range: 0-10
            serverGroups.getOrDefault(group, Collections.emptySet()).forEach(serverId ->
                weightedServers.compute(serverId, (k, w) -> {
                    if (w == null) {
                        w = 0;
                    }
                    w = Math.max(w, weight);
                    return w;
                }));
        }
        weightedServerLists = LBUtils.toWeightedRRSequence(weightedServers);
        chRouter = new WCHRouter<>(weightedServers.keySet(), serverId -> serverId, weightedServers::get, 100);
        // if inproc server is not in the weightedServers, it will be ignored
        if (weightedServers.containsKey(inProcServerId)) {
            this.inProcServerId = Optional.of(inProcServerId);
        } else {
            this.inProcServerId = Optional.empty();
        }
    }

    @Override
    public boolean exists(String serverId) {
        return weightedServerLists.contains(serverId);
    }

    @Override
    public Optional<String> random() {
        if (weightedServerLists.isEmpty()) {
            return Optional.empty();
        }
        // prefer in-proc server
        if (inProcServerId.isPresent()) {
            return inProcServerId;
        }
        return Optional.of(weightedServerLists.get(ThreadLocalRandom.current().nextInt(0, weightedServerLists.size())));
    }

    @Override
    public Optional<String> roundRobin() {
        int size = weightedServerLists.size();
        if (size == 0) {
            return Optional.empty();
        }
        // prefer in-proc server
        if (inProcServerId.isPresent()) {
            return inProcServerId;
        } else {
            int i = rrIndex.incrementAndGet();
            if (i >= size) {
                int oldi = i;
                i %= size;
                rrIndex.compareAndSet(oldi, i);
            }
            return Optional.of(weightedServerLists.get(i));
        }
    }

    @Override
    public Optional<String> hashing(String key) {
        return Optional.ofNullable(chRouter.routeNode(key));
    }
}
