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

import static io.grpc.ConnectivityState.READY;
import static java.util.Collections.singletonMap;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.grpc.LoadBalancer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

class TrafficRouter implements ITrafficRouter {
    private final TrieMap<WeightedServerGroupRouter> matcher = new TrieMap<>();
    private final Map<String, List<LoadBalancer.Subchannel>> subchannelMap;

    // trafficDirective: tenantIdPrefix -> groupTag -> weight
    // serverGroups: serverId -> groupTags
    TrafficRouter(Map<String, Map<String, Integer>> trafficDirective,
                  Map<TrafficDirectiveLoadBalancer.ServerKey, List<LoadBalancer.Subchannel>> subchannelMap,
                  Map<String, Set<String>> serverGroups) {
        this.subchannelMap = new HashMap<>();
        String inProcServerId = null;
        for (TrafficDirectiveLoadBalancer.ServerKey serverKey : subchannelMap.keySet()) {
            this.subchannelMap.put(serverKey.serverId(), subchannelMap.get(serverKey));
            if (serverKey.inProc()) {
                inProcServerId = serverKey.serverId();
            }
        }

        Set<String> defaultLBGroup = Sets.newHashSet();
        Map<String, Set<String>> lbGroups = Maps.newHashMap();
        for (String serverId : serverGroups.keySet()) {
            Set<String> lbGroupTags = serverGroups.get(serverId);
            if (lbGroupTags.isEmpty()) {
                // no group tag assigned, add it to default group
                defaultLBGroup.add(serverId);
            } else {
                lbGroupTags.forEach(lbGroupTag ->
                    lbGroups.computeIfAbsent(lbGroupTag, l -> Sets.newHashSet()).add(serverId));
            }
        }
        // default group is used as fallback assignment
        // make sure there is always a matcher for any tenantId
        prepareMatcher("", singletonMap("", 1), singletonMap("", defaultLBGroup), inProcServerId);

        for (String tenantIdPrefix : trafficDirective.keySet()) {
            prepareMatcher(tenantIdPrefix, trafficDirective.get(tenantIdPrefix), lbGroups, inProcServerId);
        }
    }

    private void prepareMatcher(String tenantIdPrefix,
                                Map<String, Integer> serverWeights,
                                Map<String, Set<String>> serverGroups,
                                String inProcServerId) {
        matcher.put(tenantIdPrefix, new WeightedServerGroupRouter(serverWeights, serverGroups, inProcServerId));
    }

    @Override
    public boolean exists(String serverId) {
        return subchannelMap.containsKey(serverId);
    }

    public IServerGroupRouter get(String tenantId) {
        return matcher.bestMatch(tenantId);
    }

    public Optional<LoadBalancer.Subchannel> getSubchannel(String serverId) {
        List<LoadBalancer.Subchannel> subChannels = subchannelMap.get(serverId).stream()
            .filter(sc -> sc.getAttributes().get(Constants.STATE_INFO).get().getState() == READY)
            .toList();
        if (subChannels.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(subChannels.get(ThreadLocalRandom.current().nextInt(subChannels.size())));
    }
}
