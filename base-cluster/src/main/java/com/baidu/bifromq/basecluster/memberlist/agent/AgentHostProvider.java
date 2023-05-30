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

package com.baidu.bifromq.basecluster.memberlist.agent;

import com.baidu.bifromq.basecluster.membership.proto.HostEndpoint;
import com.baidu.bifromq.basecluster.membership.proto.HostMember;
import io.reactivex.rxjava3.core.Observable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AgentHostProvider implements IAgentHostProvider {
    private final String agentId;
    private final Observable<Map<HostEndpoint, HostMember>> aliveHosts;

    public AgentHostProvider(String agentId, Observable<Map<HostEndpoint, HostMember>> aliveHosts) {
        this.agentId = agentId;
        this.aliveHosts = aliveHosts;
    }

    @Override
    public Observable<Set<HostEndpoint>> getHostEndpoints() {
        return aliveHosts
            .map(aliveHostList -> {
                Set<HostEndpoint> agentHosts = new HashSet<>();
                for (HostMember record : aliveHostList.values()) {
                    if (record.getAgentIdList().contains(agentId)) {
                        agentHosts.add(record.getEndpoint());
                    }
                }
                return agentHosts;
            })
            .distinctUntilChanged();
    }
}
