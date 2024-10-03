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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.Test;

public class WeightedServerGroupRouterTest {
    @Test
    void exists() {
        Map<String, Integer> trafficAssignment = Map.of("group1", 5);
        Map<String, Set<String>> groupAssignment = Map.of(
            "group1", Set.of("server1", "server2", "server3")
        );

        WeightedServerGroupRouter router = new WeightedServerGroupRouter(trafficAssignment, groupAssignment, null);
        assertTrue(router.exists("server1"));
        assertTrue(router.exists("server2"));
        assertTrue(router.exists("server3"));
    }

    @Test
    void randomRouting() {
        Map<String, Integer> trafficAssignment = Map.of("group1", 5);
        Map<String, Set<String>> groupAssignment = Map.of(
            "group1", Set.of("server1", "server2", "server3")
        );

        WeightedServerGroupRouter router = new WeightedServerGroupRouter(trafficAssignment, groupAssignment, null);

        Optional<String> server = router.random();
        assertTrue(server.isPresent());
        assertTrue(Set.of("server1", "server2", "server3").contains(server.get()));
    }

    @Test
    void randomRoutingPreferInProc() {
        Map<String, Integer> trafficAssignment = Map.of("group1", 5);
        Map<String, Set<String>> groupAssignment = Map.of(
            "group1", Set.of("server1", "server2", "server3")
        );

        WeightedServerGroupRouter router = new WeightedServerGroupRouter(trafficAssignment, groupAssignment, "server1");

        Optional<String> server = router.random();
        assertTrue(server.isPresent());
        assertEquals("server1", server.get());
    }

    @Test
    void inProcIgnored() {
        Map<String, Integer> trafficAssignment = Map.of("group1", 5);
        Map<String, Set<String>> groupAssignment = Map.of(
            "group1", Set.of("server1", "server2", "server3")
        );

        WeightedServerGroupRouter router = new WeightedServerGroupRouter(trafficAssignment, groupAssignment, "server4");

        Optional<String> server = router.random();
        assertTrue(server.isPresent());
        assertNotEquals("server4", server.get());

        server = router.roundRobin();
        assertTrue(server.isPresent());
        assertNotEquals("server4", server.get());
    }


    @Test
    void roundRobinRouting() {
        Map<String, Integer> trafficAssignment = Map.of("group1", 5);
        Map<String, Set<String>> groupAssignment = Map.of(
            "group1", Set.of("server1", "server2")
        );

        WeightedServerGroupRouter router = new WeightedServerGroupRouter(trafficAssignment, groupAssignment, null);

        // First round-robin call should return server1, then server2
        assertEquals("server1", router.roundRobin().get());
        assertEquals("server2", router.roundRobin().get());
        assertEquals("server1", router.roundRobin().get());
    }

    @Test
    void roundRobinPreferInProc() {
        Map<String, Integer> trafficAssignment = Map.of("group1", 5);
        Map<String, Set<String>> groupAssignment = Map.of(
            "group1", Set.of("server1", "server2")
        );

        WeightedServerGroupRouter router = new WeightedServerGroupRouter(trafficAssignment, groupAssignment, "server1");

        assertEquals("server1", router.roundRobin().get());
        assertEquals("server1", router.roundRobin().get());
        assertEquals("server1", router.roundRobin().get());
    }

    @Test
    void hashingRouting() {
        Map<String, Integer> trafficAssignment = Map.of("group1", 5);
        Map<String, Set<String>> groupAssignment = Map.of(
            "group1", Set.of("server1", "server2", "server3")
        );

        WeightedServerGroupRouter router = new WeightedServerGroupRouter(trafficAssignment, groupAssignment, null);

        String key = "myKey";
        Optional<String> server = router.hashing(key);
        assertTrue(server.isPresent());
        assertTrue(Set.of("server1", "server2", "server3").contains(server.get()));
    }
}
