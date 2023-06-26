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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basecluster.membership.proto.HostEndpoint;
import com.baidu.bifromq.basecluster.membership.proto.HostMember;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class AgentHostProviderTest {
    private String agentId1 = "agentA";
    private String agentId2 = "agentB";

    private InetSocketAddress hostAddr1 = new InetSocketAddress("localhost", 1111);
    private InetSocketAddress hostAddr2 = new InetSocketAddress("localhost", 2222);
    private HostEndpoint endpoint1 = HostEndpoint.newBuilder()
        .setId(ByteString.copyFromUtf8("ep1"))
        .setAddress(hostAddr1.getHostName())
        .setPort(hostAddr1.getPort())
        .build();
    private HostEndpoint endpoint2 = HostEndpoint.newBuilder()
        .setId(ByteString.copyFromUtf8("ep2"))
        .setAddress(hostAddr2.getHostName())
        .setPort(hostAddr2.getPort())
        .build();

    private HostMember hostMember1 = HostMember.newBuilder()
        .setEndpoint(endpoint1)
        .addAgentId(agentId1)
        .addAgentId(agentId2)
        .build();

    private HostMember hostMember2 = HostMember.newBuilder()
        .setEndpoint(endpoint2)
        .addAgentId(agentId2)
        .build();

    @Test
    public void getHostEndpoints() {
        BehaviorSubject<Map<HostEndpoint, HostMember>> aliveHostsSubject =
            BehaviorSubject.createDefault(Collections.emptyMap());
        AgentHostProvider hostProvider1 = new AgentHostProvider(agentId1, aliveHostsSubject);
        AgentHostProvider hostProvider2 = new AgentHostProvider(agentId2, aliveHostsSubject);

        assertTrue(hostProvider1.getHostEndpoints().blockingFirst().isEmpty());
        assertTrue(hostProvider2.getHostEndpoints().blockingFirst().isEmpty());
        aliveHostsSubject.onNext(new HashMap<>() {{
            put(endpoint1, hostMember1);
            put(endpoint2, hostMember2);
        }});

        assertEquals(hostProvider1.getHostEndpoints().blockingFirst().size(), 1);
        assertTrue(hostProvider1.getHostEndpoints().blockingFirst().contains(endpoint1));

        assertEquals(hostProvider2.getHostEndpoints().blockingFirst().size(), 2);
        assertTrue(hostProvider2.getHostEndpoints().blockingFirst().containsAll(Sets.newHashSet(endpoint1, endpoint2)));
    }
}
