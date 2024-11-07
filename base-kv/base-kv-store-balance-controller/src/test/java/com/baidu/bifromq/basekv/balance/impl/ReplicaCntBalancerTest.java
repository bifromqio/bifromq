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

package com.baidu.bifromq.basekv.balance.impl;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basekv.balance.BalanceNow;
import com.baidu.bifromq.basekv.balance.BalanceResult;
import com.baidu.bifromq.basekv.balance.BalanceResultType;
import com.baidu.bifromq.basekv.balance.command.ChangeConfigCommand;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import java.util.HashSet;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ReplicaCntBalancerTest {
    private ReplicaCntBalancer balancer;

    @BeforeMethod
    public void setUp() {
        balancer = new ReplicaCntBalancer("testCluster", "localStore", 3, 2);
    }

    @Test
    public void balanceWithNoLeaderRange() {
        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        balancer.update(storeDescriptors);

        assertSame(balancer.balance().type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void balanceToAddVoter() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("localStore")
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("localStore")
            .addRanges(kvRangeDescriptor)
            .putStatistics("cpu.usage", 0.5)
            .build();
        KVRangeStoreDescriptor remoteStoreDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("remoteStore")
            .addRanges(kvRangeDescriptor)
            .putStatistics("cpu.usage", 0.5)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor);
        storeDescriptors.add(remoteStoreDescriptor);

        balancer.update(storeDescriptors);

        BalanceResult result = balancer.balance();
        ChangeConfigCommand command = (ChangeConfigCommand) ((BalanceNow<?>) result).command;
        assertTrue(command.getVoters().contains("localStore"));
        assertTrue(command.getVoters().contains("remoteStore"));
        assertTrue(command.getLearners().isEmpty());
    }

    @Test
    public void balanceToAddLearner() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("localStore")
                .addVoters("voterStore2")
                .addVoters("voterStore3")
                .build())
            .build();

        KVRangeStoreDescriptor voterStore1Descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("localStore")
            .addRanges(kvRangeDescriptor)
            .putStatistics("cpu.usage", 0.5)
            .build();
        KVRangeStoreDescriptor voterStore2Descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("voterStore2")
            .putStatistics("cpu.usage", 0.5)
            .build();
        KVRangeStoreDescriptor voterStore3Descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("voterStore3")
            .putStatistics("cpu.usage", 0.5)
            .build();
        KVRangeStoreDescriptor learnerStoreDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("learnerStore")
            .putStatistics("cpu.usage", 0.5)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(voterStore1Descriptor);
        storeDescriptors.add(voterStore2Descriptor);
        storeDescriptors.add(voterStore3Descriptor);
        storeDescriptors.add(learnerStoreDescriptor);

        balancer.update(storeDescriptors);

        BalanceResult result = balancer.balance();
        ChangeConfigCommand command = (ChangeConfigCommand) ((BalanceNow<?>) result).command;
        assertTrue(command.getLearners().contains("learnerStore"));
    }

    @Test
    public void balanceToRemoveVoter() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("localStore")
                .addVoters("remoteStore")
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("localStore")
            .addRanges(kvRangeDescriptor)
            .putStatistics("cpu.usage", 0.5)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor);

        balancer.update(storeDescriptors);

        BalanceResult result = balancer.balance();
        ChangeConfigCommand command = (ChangeConfigCommand) ((BalanceNow<?>) result).command;
        assertTrue(command.getVoters().contains("localStore"));
        assertFalse(command.getVoters().contains("remoteStore"));
        assertTrue(command.getLearners().isEmpty());
    }

    @Test
    public void balanceToRemoveLearner() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("localStore")
                .addLearners("learnerStore")
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("localStore")
            .addRanges(kvRangeDescriptor)
            .putStatistics("cpu.usage", 0.5)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor);

        balancer.update(storeDescriptors);

        BalanceResult result = balancer.balance();
        ChangeConfigCommand command = (ChangeConfigCommand) ((BalanceNow<?>) result).command;
        assertTrue(command.getVoters().contains("localStore"));
        assertTrue(command.getLearners().isEmpty());
    }

    @Test
    public void nothingChanged() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("localStore")
                .addLearners("learnerStore")
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("localStore")
            .addRanges(kvRangeDescriptor)
            .putStatistics("cpu.usage", 0.5)
            .build();
        KVRangeStoreDescriptor learnerStoreDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("learnerStore")
            .addRanges(kvRangeDescriptor)
            .putStatistics("cpu.usage", 0.5)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor);
        storeDescriptors.add(learnerStoreDescriptor);

        balancer.update(storeDescriptors);

        assertSame(balancer.balance().type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void balanceToAddAllRestLearners() {
        balancer = new ReplicaCntBalancer("testCluster", "localStore", 3, -1);
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("localStore")
                .addVoters("voterStore2")
                .addVoters("voterStore3")
                .build())
            .build();

        KVRangeStoreDescriptor voterStore1Descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("localStore")
            .addRanges(kvRangeDescriptor)
            .putStatistics("cpu.usage", 0.5)
            .build();
        KVRangeStoreDescriptor voterStore2Descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("voterStore2")
            .putStatistics("cpu.usage", 0.5)
            .build();
        KVRangeStoreDescriptor voterStore3Descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("voterStore3")
            .putStatistics("cpu.usage", 0.5)
            .build();
        KVRangeStoreDescriptor learnerStoreDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("learnerStore")
            .putStatistics("cpu.usage", 0.5)
            .build();
        KVRangeStoreDescriptor learnerStore1Descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("learnerStore1")
            .putStatistics("cpu.usage", 0.5)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(voterStore1Descriptor);
        storeDescriptors.add(voterStore2Descriptor);
        storeDescriptors.add(voterStore3Descriptor);
        storeDescriptors.add(learnerStoreDescriptor);
        storeDescriptors.add(learnerStore1Descriptor);

        balancer.update(storeDescriptors);

        BalanceResult result = balancer.balance();
        ChangeConfigCommand command = (ChangeConfigCommand) ((BalanceNow<?>) result).command;
        assertTrue(command.getLearners().contains("learnerStore"));
        assertTrue(command.getLearners().contains("learnerStore1"));
    }
}
