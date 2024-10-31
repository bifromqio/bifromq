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

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basekv.balance.command.BootstrapCommand;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RangeBootstrapBalancerTest {

    private RangeBootstrapBalancer balancer;
    private String clusterId = "testCluster";
    private String localStoreId = "localStore";
    private AtomicLong mockTime;

    @BeforeMethod
    public void setUp() {
        mockTime = new AtomicLong(0L); // Start time at 0
        Supplier<Long> mockMillisSource = mockTime::get;
        balancer = new RangeBootstrapBalancer(clusterId, localStoreId, Duration.ofSeconds(1), mockMillisSource);
    }

    @Test
    public void updateWithoutStoreDescriptors() {
        // Test when there are no store descriptors
        balancer.update("{}", Collections.emptySet());
        mockTime.addAndGet(2000L); // Advance time by 2 seconds

        Optional<BootstrapCommand> command = balancer.balance();
        assertTrue(command.isPresent());
        assertEquals(FULL_BOUNDARY, command.get().getBoundary());
    }


    @Test
    public void balanceWithTrigger() {
        // Test when balance should trigger a bootstrap command
        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .build();

        balancer.update("{}", Set.of(storeDescriptor));
        mockTime.addAndGet(2000L); // Advance time by 2 seconds

        Optional<BootstrapCommand> command = balancer.balance();
        assertTrue(command.isPresent());
        assertEquals(FULL_BOUNDARY, command.get().getBoundary());
    }
}