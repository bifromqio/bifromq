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
    private final String inProcServerId;

    WeightedServerGroupRouter(Map<String, Integer> trafficAssignment,
                              Map<String, Set<String>> groupAssignment,
                              String inProcServerId) {
        Map<String, Integer> weightedServers = Maps.newHashMap();
        for (String group : trafficAssignment.keySet()) {
            int weight = Math.abs(trafficAssignment.get(group)) % 11; // weight range: 0-10
            groupAssignment.get(group).forEach(serverId ->
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
        this.inProcServerId = inProcServerId;
    }

    @Override
    public Optional<String> random() {
        if (weightedServerLists.isEmpty()) {
            return Optional.empty();
        }
        // prefer in-proc server
        if (inProcServerId != null) {
            return Optional.of(inProcServerId);
        }
        return Optional.of(weightedServerLists.get(ThreadLocalRandom.current().nextInt(0, weightedServerLists.size())));
    }

    @Override
    public Optional<String> roundRobin() {
        int size = weightedServerLists.size();
        if (size == 0) {
            return Optional.empty();
        }
        String selected;
        // prefer in-proc server
        if (inProcServerId != null) {
            selected = inProcServerId;
        } else {
            int i = rrIndex.incrementAndGet();
            if (i >= size) {
                int oldi = i;
                i %= size;
                rrIndex.compareAndSet(oldi, i);
            }
            selected = weightedServerLists.get(i);
        }
        return Optional.of(selected);
    }

    @Override
    public Optional<String> hashing(String key) {
        return Optional.ofNullable(chRouter.routeNode(key));
    }
}
