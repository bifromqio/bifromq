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

import static io.grpc.ConnectivityState.IDLE;
import static io.grpc.ConnectivityState.READY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import io.grpc.Attributes;
import io.grpc.ConnectivityStateInfo;
import io.grpc.LoadBalancer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;

public class TrafficRouterTest {
    @Test
    void init() {
        Map<String, Map<String, Integer>> trafficDirective = Map.of(
            "tenantA", Map.of("group1", 5)
        );
        Map<TrafficDirectiveLoadBalancer.ServerKey, List<LoadBalancer.Subchannel>> subchannelMap = new HashMap<>();
        subchannelMap.put(new TrafficDirectiveLoadBalancer.ServerKey("server1", false), List.of());

        Map<String, Set<String>> serverGroups = Map.of(
            "server1", Set.of("group1")
        );

        TrafficRouter router = new TrafficRouter(trafficDirective, subchannelMap, serverGroups);

        assertTrue(router.exists("server1"));
        assertFalse(router.exists("serverX"));
    }

    @Test
    void getRouterForTenant() {
        Map<String, Map<String, Integer>> trafficDirective = Map.of(
            "tenantA", Map.of("group1", 5),
            "tenantB", Map.of("group2", 3)
        );
        Map<TrafficDirectiveLoadBalancer.ServerKey, List<LoadBalancer.Subchannel>> subchannelMap = new HashMap<>();
        Map<String, Set<String>> serverGroups = Map.of(
            "server1", Set.of("group1"),
            "server2", Set.of("group2")
        );

        TrafficRouter router = new TrafficRouter(trafficDirective, subchannelMap, serverGroups);

        IServerGroupRouter tenantARouter = router.get("tenantA");
        assertNotNull(tenantARouter);
        assertEquals("server1", tenantARouter.random().get());

        IServerGroupRouter tenantBRouter = router.get("tenantB");
        assertNotNull(tenantBRouter);
        assertEquals("server2", tenantBRouter.random().get());

        IServerGroupRouter unknownTenantRouter = router.get("unknownTenant");
        assertNotNull(unknownTenantRouter);
        assertFalse(unknownTenantRouter.random().isPresent());
    }

    @Test
    void getSubChannel() {
        LoadBalancer.Subchannel subchannel1 = mock(LoadBalancer.Subchannel.class);
        LoadBalancer.Subchannel subchannel2 = mock(LoadBalancer.Subchannel.class);
        Attributes ready = Attributes.newBuilder()
            .set(Constants.STATE_INFO, new AtomicReference<>(ConnectivityStateInfo.forNonError(READY)))
            .build();
        Attributes notready = Attributes.newBuilder()
            .set(Constants.STATE_INFO, new AtomicReference<>(ConnectivityStateInfo.forNonError(IDLE)))
            .build();
        when(subchannel1.getAttributes()).thenReturn(ready);
        when(subchannel2.getAttributes()).thenReturn(notready);

        Map<TrafficDirectiveLoadBalancer.ServerKey, List<LoadBalancer.Subchannel>> subchannelMap = Map.of(
            new TrafficDirectiveLoadBalancer.ServerKey("server1", false), List.of(subchannel1, subchannel2)
        );

        TrafficRouter router = new TrafficRouter(new HashMap<>(), subchannelMap, new HashMap<>());

        Optional<LoadBalancer.Subchannel> subchannel = router.getSubchannel("server1");
        assertTrue(subchannel.isPresent());
        assertEquals(subchannel.get(), subchannel1);
    }
}
