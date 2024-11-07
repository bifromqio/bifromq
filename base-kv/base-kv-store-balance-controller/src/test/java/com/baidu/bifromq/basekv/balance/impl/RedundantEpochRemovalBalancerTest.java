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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import com.baidu.bifromq.basekv.balance.BalanceNow;
import com.baidu.bifromq.basekv.balance.BalanceResult;
import com.baidu.bifromq.basekv.balance.BalanceResultType;
import com.baidu.bifromq.basekv.balance.command.ChangeConfigCommand;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RedundantEpochRemovalBalancerTest {

    private RedundantEpochRemovalBalancer balancer;
    private String clusterId = "testCluster";
    private String localStoreId = "localStore";

    @BeforeMethod
    public void setUp() {
        balancer = new RedundantEpochRemovalBalancer(clusterId, localStoreId);
    }

    @Test
    public void noRedundantEpoch() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(kvRangeDescriptor)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        storeDescriptors.add(storeDescriptor);

        balancer.update(storeDescriptors);

        assertSame(balancer.balance().type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void removeRangeInRedundantEpoch() {
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("m"))
                .build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .build())
            .build();

        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(2).setId(2).build();
        KVRangeDescriptor kvRangeDescriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("n"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("otherStore")
                .addVoters(localStoreId)
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(kvRangeDescriptor1)
            .build();

        KVRangeStoreDescriptor storeDescriptor2 = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(kvRangeDescriptor2)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = Set.of(storeDescriptor1, storeDescriptor2);

        balancer.update(storeDescriptors);

        BalanceResult command = balancer.balance();
        assertEquals(command.type(), BalanceResultType.BalanceNow);
        ChangeConfigCommand changeConfigCommand = (ChangeConfigCommand) ((BalanceNow<?>) command).command;

        assertEquals(changeConfigCommand.getToStore(), localStoreId);
        assertEquals(changeConfigCommand.getKvRangeId(), kvRangeId2);
        assertEquals(changeConfigCommand.getVoters(), Set.of(localStoreId));
        assertEquals(changeConfigCommand.getLearners(), Collections.emptySet());
    }

    @Test
    public void noLocalLeaderRangeInRedundantEpoch() {
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        KVRangeDescriptor kvRangeDescriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("m"))
                .build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .build())
            .build();

        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(2).setId(2).build();
        KVRangeDescriptor kvRangeDescriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setRole(RaftNodeStatus.Follower)
            .setVer(1)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("n"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .build())
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .build())
            .build();

        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(kvRangeDescriptor1)
            .build();

        KVRangeStoreDescriptor storeDescriptor2 = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .addRanges(kvRangeDescriptor2)
            .build();

        Set<KVRangeStoreDescriptor> storeDescriptors = Set.of(storeDescriptor1, storeDescriptor2);

        balancer.update(storeDescriptors);

        assertSame(balancer.balance().type(), BalanceResultType.NoNeedBalance);
    }
}