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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basekv.balance.command.BalanceCommand;
import com.baidu.bifromq.basekv.balance.command.ChangeConfigCommand;
import com.baidu.bifromq.basekv.balance.command.TransferLeadershipCommand;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.google.protobuf.ByteString;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RangeLeaderBalancerTest {

    private RangeLeaderBalancer balancer;
    private String clusterId = "testCluster";
    private String localStoreId = "localStore";

    @BeforeMethod
    public void setUp() {
        balancer = new RangeLeaderBalancer(clusterId, localStoreId);
    }

    @Test
    public void noEffectiveRouteNoBalanceNeeded() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(
                Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("a")).setEndKey(ByteString.copyFromUtf8("z"))
                    .build())
            .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).build())
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(kvRangeDescriptor)
            .build();

        balancer.update("{}", Set.of(storeDescriptor));

        Optional<BalanceCommand> command = balancer.balance();
        assertFalse(command.isPresent());
    }

    @Test
    public void hasEffectiveRouteSingleStoreNoBalanceNeeded() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(FULL_BOUNDARY)
            .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).build())
            .build();
        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(kvRangeDescriptor)
            .build();
        balancer.update("{}", Set.of(storeDescriptor));
        Optional<BalanceCommand> command = balancer.balance();
        assertFalse(command.isPresent());
    }

    @Test
    public void hasEffectiveRouteMultiStoreNoBalanceNeeded() {
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();
        KVRangeId kvRangeId3 = KVRangeId.newBuilder().setEpoch(1).setId(3).build();
        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(KVRangeDescriptor.newBuilder()
                .setId(kvRangeId1)
                .setRole(RaftNodeStatus.Leader)
                .setBoundary(FULL_BOUNDARY)
                .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).build())
                .build())
            .build();
        KVRangeStoreDescriptor storeDescriptor2 = KVRangeStoreDescriptor.newBuilder()
            .setId("remoteStore1")
            .addRanges(KVRangeDescriptor.newBuilder()
                .setId(kvRangeId2)
                .setRole(RaftNodeStatus.Leader)
                .setBoundary(FULL_BOUNDARY)
                .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).build())
                .build())
            .build();
        KVRangeStoreDescriptor storeDescriptor3 = KVRangeStoreDescriptor.newBuilder()
            .setId("remoteStore2")
            .addRanges(KVRangeDescriptor.newBuilder()
                .setId(kvRangeId3)
                .setRole(RaftNodeStatus.Leader)
                .setBoundary(FULL_BOUNDARY)
                .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).build())
                .build())
            .build();
        balancer.update("{}", Set.of(storeDescriptor1, storeDescriptor2, storeDescriptor3));
        Optional<BalanceCommand> command = balancer.balance();
        assertFalse(command.isPresent());
    }

    @Test
    public void balanceToOtherNoLeaderStore() {
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).addLearners("otherStore").build())
            .build();
        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();
        KVRangeDescriptor kvRangeDescriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).addVoters("otherStore").build())
            .build();
        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(kvRangeDescriptor1)
            .addRanges(kvRangeDescriptor2)
            .build();
        KVRangeId kvRangeId3 = KVRangeId.newBuilder().setEpoch(1).setId(10).build();
        KVRangeDescriptor kvRangeDescriptor3 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId3)
            .setRole(RaftNodeStatus.Follower)
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters("otherStore").build())
            .build();
        KVRangeStoreDescriptor storeDescriptor2 = KVRangeStoreDescriptor.newBuilder()
            .setId("otherStore")
            .addRanges(kvRangeDescriptor3)
            .build();
        balancer.update("{}", Set.of(storeDescriptor1, storeDescriptor2));

        Optional<BalanceCommand> command = balancer.balance();
        assertTrue(command.isPresent());
        assertTrue(command.get() instanceof TransferLeadershipCommand);
        assertEquals(command.get().getKvRangeId(), kvRangeId2);
        assertEquals(((TransferLeadershipCommand) command.get()).getNewLeaderStore(), "otherStore");
        assertEquals(((TransferLeadershipCommand) command.get()).getExpectedVer(), 1);
    }

    @Test
    public void transferLeadership() {
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).addLearners("otherStore").build())
            .build();
        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();
        KVRangeDescriptor kvRangeDescriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).addVoters("otherStore").build())
            .build();
        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(kvRangeDescriptor1)
            .addRanges(kvRangeDescriptor2)
            .build();
        KVRangeId kvRangeId3 = KVRangeId.newBuilder().setEpoch(1).setId(10).build();
        KVRangeDescriptor kvRangeDescriptor3 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId3)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters("otherStore").build())
            .build();
        KVRangeStoreDescriptor storeDescriptor2 = KVRangeStoreDescriptor.newBuilder()
            .setId("otherStore")
            .addRanges(kvRangeDescriptor3)
            .build();
        balancer.update("{}", Set.of(storeDescriptor1, storeDescriptor2));

        Optional<BalanceCommand> command = balancer.balance();
        assertTrue(command.isPresent());
        assertTrue(command.get() instanceof TransferLeadershipCommand);
        assertEquals(command.get().getKvRangeId(), kvRangeId2);
        assertEquals(((TransferLeadershipCommand) command.get()).getNewLeaderStore(), "otherStore");
        assertEquals(((TransferLeadershipCommand) command.get()).getExpectedVer(), 1);
    }

    @Test
    public void balanceToLearner() {
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).addLearners("otherStore").build())
            .build();
        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();
        KVRangeDescriptor kvRangeDescriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).build())
            .build();
        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(kvRangeDescriptor1)
            .addRanges(kvRangeDescriptor2)
            .build();
        KVRangeId kvRangeId3 = KVRangeId.newBuilder().setEpoch(1).setId(10).build();
        KVRangeDescriptor kvRangeDescriptor3 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId3)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters("otherStore").build())
            .build();
        KVRangeStoreDescriptor storeDescriptor2 = KVRangeStoreDescriptor.newBuilder()
            .setId("otherStore")
            .addRanges(kvRangeDescriptor3)
            .build();
        balancer.update("{}", Set.of(storeDescriptor1, storeDescriptor2));

        Optional<BalanceCommand> command = balancer.balance();
        assertTrue(command.isPresent());
        assertTrue(command.get() instanceof ChangeConfigCommand);
        assertEquals(command.get().getKvRangeId(), kvRangeId1);
        assertEquals(((ChangeConfigCommand) command.get()).getExpectedVer(), 1);
        assertTrue(((ChangeConfigCommand) command.get()).getVoters().contains("otherStore"));
        assertTrue(((ChangeConfigCommand) command.get()).getLearners().contains("localStore"));
        assertFalse(((ChangeConfigCommand) command.get()).getLearners().contains("otherStore"));
    }

    @Test
    public void balanceToOther() {
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).build())
            .build();
        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();
        KVRangeDescriptor kvRangeDescriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters(localStoreId).build())
            .build();
        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(kvRangeDescriptor1)
            .addRanges(kvRangeDescriptor2)
            .build();
        KVRangeId kvRangeId3 = KVRangeId.newBuilder().setEpoch(1).setId(10).build();
        KVRangeDescriptor kvRangeDescriptor3 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId3)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("z")).build())
            .setConfig(ClusterConfig.newBuilder().addVoters("otherStore").build())
            .build();
        KVRangeStoreDescriptor storeDescriptor2 = KVRangeStoreDescriptor.newBuilder()
            .setId("otherStore")
            .addRanges(kvRangeDescriptor3)
            .build();
        balancer.update("{}", Set.of(storeDescriptor1, storeDescriptor2));

        Optional<BalanceCommand> command = balancer.balance();
        assertTrue(command.isPresent());
        assertTrue(command.get() instanceof ChangeConfigCommand);
        assertTrue(command.get().getKvRangeId().equals(kvRangeId1) || command.get().getKvRangeId().equals(kvRangeId2));
        assertEquals(((ChangeConfigCommand) command.get()).getExpectedVer(), 1);
        assertTrue(((ChangeConfigCommand) command.get()).getVoters().contains("otherStore"));
        assertFalse(((ChangeConfigCommand) command.get()).getLearners().contains("localStore"));
        assertFalse(((ChangeConfigCommand) command.get()).getLearners().contains("otherStore"));
    }

    @Test
    public void exactLeaderBalanceScenario() {
        // Setup KVRangeIds and descriptors
        KVRangeId rangeId1 = KVRangeId.newBuilder().setId(1).setEpoch(1).build();
        KVRangeId rangeId2 = KVRangeId.newBuilder().setId(2).setEpoch(1).build();
        KVRangeId rangeId3 = KVRangeId.newBuilder().setId(3).setEpoch(1).build();

        // Create a cluster config
        ClusterConfig config = ClusterConfig.newBuilder().addVoters(localStoreId).build();

        // Create KVRangeDescriptors
        KVRangeDescriptor descriptor1 = KVRangeDescriptor.newBuilder()
            .setId(rangeId1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("m")).build())
            .setConfig(config)
            .build();

        KVRangeDescriptor descriptor2 = KVRangeDescriptor.newBuilder()
            .setId(rangeId2)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(
                Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("m")).setEndKey(ByteString.copyFromUtf8("r"))
                    .build())
            .setConfig(config)
            .build();

        KVRangeDescriptor descriptor3 = KVRangeDescriptor.newBuilder()
            .setId(rangeId3)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("r")).build())
            .setConfig(config)
            .build();

        // Create KVRangeStoreDescriptors
        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(descriptor1)
            .addRanges(descriptor2)
            .addRanges(descriptor3)
            .build();

        KVRangeStoreDescriptor otherStoreDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("otherStore")
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor);
        storeDescriptors.add(otherStoreDescriptor);

        // Update balancer with current store descriptors
        balancer.update("{}", storeDescriptors);

        // Simulate the scenario where localStore has exactly 'atMost' leaders
        Optional<BalanceCommand> balanceCommand = balancer.balance();

        // Assert that the balancer does not produce any balance commands since the store has exactly atMost leaders
        assertTrue(balanceCommand.isPresent());

        // Check if the balance command involves transferring a leader or changing the configuration
        BalanceCommand command = balanceCommand.get();
        assertTrue(command instanceof ChangeConfigCommand || command instanceof TransferLeadershipCommand);
    }
}