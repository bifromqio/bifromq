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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import com.baidu.bifromq.basekv.balance.BalanceNow;
import com.baidu.bifromq.basekv.balance.BalanceResult;
import com.baidu.bifromq.basekv.balance.BalanceResultType;
import com.baidu.bifromq.basekv.balance.command.RecoveryCommand;
import com.baidu.bifromq.basekv.balance.utils.DescriptorUtils;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RecoveryBalancerTest {

    private static final String LOCAL_STORE_ID = "localStoreId";

    private RecoveryBalancer balancer;

    @BeforeMethod
    public void setup() {
        balancer = new RecoveryBalancer("clusterId", LOCAL_STORE_ID, 200L);
    }

    @Test
    public void balanceWithoutUpdate() {
        assertEquals(balancer.balance().type(), BalanceResultType.NoNeedBalance);
    }

    @Test
    public void rangeNoLeader() {
        KVRangeId id = KVRangeIdUtil.generate();
        List<String> voters = Lists.newArrayList(LOCAL_STORE_ID, "store2", "store3", "store4", "store5");
        List<KVRangeDescriptor> rangeDescriptors =
            DescriptorUtils.generateRangeDesc(id, Sets.newHashSet(voters), Sets.newHashSet());
        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId(LOCAL_STORE_ID)
            .addRanges(rangeDescriptors.get(0).toBuilder().setRole(RaftNodeStatus.Candidate).setVer(2).build())
            .build();
        KVRangeStoreDescriptor storeDescriptor2 = KVRangeStoreDescriptor.newBuilder()
            .setId("store2")
            .addRanges(rangeDescriptors.get(0).toBuilder().setRole(RaftNodeStatus.Candidate).setVer(3).build())
            .build();
        balancer.update(Sets.newHashSet(storeDescriptor1, storeDescriptor2));
        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.BalanceNow);
        RecoveryCommand recoveryCommand = (RecoveryCommand) ((BalanceNow<?>) result).command;
        assertEquals("store2", recoveryCommand.getToStore());
    }

    @Test
    public void rangeNoLeaderAndOneVTemporaryDead() {
        KVRangeId id = KVRangeIdUtil.generate();
        List<String> voters = Lists.newArrayList(LOCAL_STORE_ID, "aaaaa", "store3", "store4", "store5");
        List<KVRangeDescriptor> rangeDescriptors =
            DescriptorUtils.generateRangeDesc(id, Sets.newHashSet(voters), Sets.newHashSet());
        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId(LOCAL_STORE_ID)
            .addRanges(rangeDescriptors.get(0).toBuilder().setRole(RaftNodeStatus.Candidate).setVer(3).build())
            .build();
        KVRangeStoreDescriptor storeDescriptor2 = KVRangeStoreDescriptor.newBuilder()
            .setId("aaaaa")
            .addRanges(rangeDescriptors.get(0).toBuilder().setRole(RaftNodeStatus.Candidate).setVer(3).build())
            .build();
        balancer.update(Sets.newHashSet(storeDescriptor1, storeDescriptor2));
        // store2 dead temporarily
        balancer.update(Sets.newHashSet(storeDescriptor1));

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.BalanceNow);
        RecoveryCommand recoveryCommand = (RecoveryCommand) ((BalanceNow<?>) result).command;
        assertEquals(LOCAL_STORE_ID, recoveryCommand.getToStore());
    }

    @Test
    public void rangeNoLeaderAndSelfHealing() {
        KVRangeId id = KVRangeIdUtil.generate();
        List<String> voters = Lists.newArrayList(LOCAL_STORE_ID, "store2", "store3", "store4", "store5");
        List<KVRangeDescriptor> rangeDescriptors =
            DescriptorUtils.generateRangeDesc(id, Sets.newHashSet(voters), Sets.newHashSet());
        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId(LOCAL_STORE_ID)
            .addRanges(rangeDescriptors.get(0).toBuilder().setRole(RaftNodeStatus.Candidate).setVer(2).build())
            .build();
        KVRangeStoreDescriptor storeDescriptor2 = KVRangeStoreDescriptor.newBuilder()
            .setId("store2")
            .addRanges(rangeDescriptors.get(0).toBuilder().setRole(RaftNodeStatus.Candidate).setVer(3).build())
            .build();
        KVRangeStoreDescriptor storeDescriptor3 = KVRangeStoreDescriptor.newBuilder()
            .setId("store3")
            .addRanges(rangeDescriptors.get(0).toBuilder().setRole(RaftNodeStatus.Candidate).setVer(3).build())
            .build();

        balancer.update(Sets.newHashSet(storeDescriptor1, storeDescriptor2, storeDescriptor3));
        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.NoNeedBalance);
    }
}
