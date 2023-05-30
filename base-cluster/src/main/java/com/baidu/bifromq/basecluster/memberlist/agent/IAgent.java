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

import com.baidu.bifromq.basecluster.agent.proto.AgentMemberAddr;
import com.baidu.bifromq.basecluster.agent.proto.AgentMemberMetadata;
import com.baidu.bifromq.basecluster.membership.proto.HostEndpoint;
import io.reactivex.rxjava3.core.Observable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface IAgent {
    String id();

    HostEndpoint endpoint();

    /**
     * A hot observable of agent membership
     *
     * @return
     */
    Observable<Map<AgentMemberAddr, AgentMemberMetadata>> membership();

    /**
     * Register a local agent member.
     * It's allowed to register same member name in same logical agent from different agent hosts
     *
     * @param memberName
     */
    IAgentMember register(String memberName);

    /**
     * Deregister a member instance, the caller should never hold the reference to the instance after deregistered
     *
     * @param member
     */
    CompletableFuture<Void> deregister(IAgentMember member);
}
