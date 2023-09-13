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

package com.baidu.bifromq.basekv.balance.impl;

import com.baidu.bifromq.basekv.balance.command.BalanceCommand;
import com.baidu.bifromq.basekv.balance.command.ChangeConfigCommand;
import com.baidu.bifromq.basekv.balance.command.CommandType;
import com.baidu.bifromq.basekv.balance.utils.DescriptorUtils;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor.Builder;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RangeLeaderBalancerTest {

    private static final String LOCAL_STORE_ID = "localStoreId";

    private RangeLeaderBalancer balancer;

    @BeforeMethod
    public void setup() {
        balancer = new RangeLeaderBalancer(LOCAL_STORE_ID);
    }

    @Test
    public void balanceWithoutUpdate() {
        Optional<BalanceCommand> balance = balancer.balance();
        Assert.assertTrue(balance.isEmpty());
    }

    @Test
    public void balanceWith3StoreAnd5Leader() {
        List<String> voters = Lists.newArrayList(LOCAL_STORE_ID, "store1", "store2");
        List<String> learners = Lists.newArrayList();
        List<List<KVRangeDescriptor>> allRangeDescriptors = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            KVRangeId id = KVRangeIdUtil.generate();
            List<KVRangeDescriptor> rangeDescriptors =
                DescriptorUtils.generateRangeDesc(id, Sets.newHashSet(voters), Sets.newHashSet(learners));
            allRangeDescriptors.add(rangeDescriptors);
        }
        List<KVRangeStoreDescriptor> storeDescriptors = new ArrayList<>();
        for (int i = 0; i < voters.size(); i++) {
            Builder builder = KVRangeStoreDescriptor.newBuilder();
            builder.setId(voters.get(i));
            for (int j = 0; j < 5; j++) {
                builder.addRanges(allRangeDescriptors.get(j).get(i));
            }
            storeDescriptors.add(builder.build());
        }
        balancer.update(Sets.newHashSet(storeDescriptors));
        Optional<BalanceCommand> commandOptional = balancer.balance();
        Assert.assertTrue(commandOptional.isPresent());
        Assert.assertEquals(CommandType.TRANSFER_LEADERSHIP, commandOptional.get().type());
    }

    @Test
    public void balanceWith4StoreAnd5Leader() {
        List<String> voters = Lists.newArrayList(LOCAL_STORE_ID, "store1", "store2");
        List<String> learners = Lists.newArrayList();
        List<List<KVRangeDescriptor>> allRangeDescriptors = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            KVRangeId id = KVRangeIdUtil.generate();
            List<KVRangeDescriptor> rangeDescriptors =
                DescriptorUtils.generateRangeDesc(id, Sets.newHashSet(voters), Sets.newHashSet(learners));
            allRangeDescriptors.add(rangeDescriptors);
        }
        List<KVRangeStoreDescriptor> storeDescriptors = new ArrayList<>();
        for (int i = 0; i < voters.size(); i++) {
            Builder builder = KVRangeStoreDescriptor.newBuilder();
            builder.setId(voters.get(i));
            for (int j = 0; j < 5; j++) {
                builder.addRanges(allRangeDescriptors.get(j).get(i));
            }
            storeDescriptors.add(builder.build());
        }
        storeDescriptors.add(KVRangeStoreDescriptor.newBuilder()
            .setId("store_spare")
            .build());
        balancer.update(Sets.newHashSet(storeDescriptors));
        Optional<BalanceCommand> commandOptional = balancer.balance();
        Assert.assertTrue(commandOptional.isPresent());
        Assert.assertEquals(CommandType.CHANGE_CONFIG, commandOptional.get().type());
    }

    @Test
    public void balanceWithOneVoterLeader() {
        List<String> voters = Lists.newArrayList(LOCAL_STORE_ID);
        List<String> learners = Lists.newArrayList();
        List<List<KVRangeDescriptor>> allRangeDescriptors = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            KVRangeId id = KVRangeIdUtil.generate();
            List<KVRangeDescriptor> rangeDescriptors =
                DescriptorUtils.generateRangeDesc(id, Sets.newHashSet(voters), Sets.newHashSet(learners));
            allRangeDescriptors.add(rangeDescriptors);
        }
        List<KVRangeStoreDescriptor> storeDescriptors = new ArrayList<>();
        for (int i = 0; i < voters.size(); i++) {
            Builder builder = KVRangeStoreDescriptor.newBuilder();
            builder.setId(voters.get(i));
            for (int j = 0; j < 2; j++) {
                builder.addRanges(allRangeDescriptors.get(j).get(i));
            }
            storeDescriptors.add(builder.build());
        }
        storeDescriptors.add(KVRangeStoreDescriptor.newBuilder()
            .setId("store_spare")
            .build());
        balancer.update(Sets.newHashSet(storeDescriptors));
        Optional<BalanceCommand> commandOptional = balancer.balance();
        Assert.assertTrue(commandOptional.isPresent());
        Assert.assertEquals(CommandType.CHANGE_CONFIG, commandOptional.get().type());
        Assert.assertEquals(1, ((ChangeConfigCommand) commandOptional.get()).getVoters().size());
    }

}
