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
import com.baidu.bifromq.basekv.balance.utils.DescriptorUtils;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.proto.LoadHint;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ReplicaCntBalancerTest {

    private static final String LOCAL_STORE_ID = "localStoreId";

    private ReplicaCntBalancer balancer;

    @BeforeMethod
    public void setup() {
        balancer = new ReplicaCntBalancer(LOCAL_STORE_ID, 3, 200L);
    }

    @Test
    public void balanceWithoutUpdate() {
        Optional<BalanceCommand> balance = balancer.balance();
        Assert.assertTrue(balance.isEmpty());
    }

    @Test
    public void balanceWithOneStore() {
        KVRangeId id = KVRangeIdUtil.generate();
        List<KVRangeDescriptor> rangeDescriptors =
            DescriptorUtils.generateRangeDesc(id, Sets.newHashSet(LOCAL_STORE_ID), Sets.newHashSet());
        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId(LOCAL_STORE_ID)
            .addRanges(rangeDescriptors.get(0))
            .build();
        balancer.update(Sets.newHashSet(storeDescriptor1));
        Optional<BalanceCommand> commandOptional = balancer.balance();
        Assert.assertTrue(commandOptional.isEmpty());
    }

    @Test
    public void balanceWithTwoStore() {
        KVRangeId id = KVRangeIdUtil.generate();
        List<KVRangeDescriptor> rangeDescriptors =
            DescriptorUtils.generateRangeDesc(id, Sets.newHashSet(LOCAL_STORE_ID), Sets.newHashSet());
        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId(LOCAL_STORE_ID)
            .addRanges(rangeDescriptors.get(0))
            .build();
        KVRangeStoreDescriptor storeDescriptor2 = KVRangeStoreDescriptor.newBuilder()
            .setId("store2")
            .build();
        // two store with one voter
        balancer.update(Sets.newHashSet(storeDescriptor1, storeDescriptor2));
        Optional<BalanceCommand> commandOptional = balancer.balance();
        Assert.assertTrue(commandOptional.isPresent());
        ChangeConfigCommand changeConfigCommand = (ChangeConfigCommand) commandOptional.get();
        Assert.assertEquals(2, changeConfigCommand.getVoters().size());
        Assert.assertEquals(0, changeConfigCommand.getLearners().size());

        Set<String> expectedVoters = Sets.newHashSet(LOCAL_STORE_ID, "store2");
        Assert.assertEquals(expectedVoters, changeConfigCommand.getVoters());
    }

    @Test
    public void balanceWithThreeStore() {
        KVRangeId id = KVRangeIdUtil.generate();
        List<String> voters = Lists.newArrayList(LOCAL_STORE_ID, "store1");
        List<String> learners = Lists.newArrayList();
        List<KVRangeDescriptor> rangeDescriptors =
            DescriptorUtils.generateRangeDesc(id, Sets.newHashSet(voters), Sets.newHashSet(learners));
        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        for (int i = 0; i < voters.size(); i++) {
            storeDescriptors.add(KVRangeStoreDescriptor.newBuilder()
                .setId(voters.get(i))
                .addRanges(rangeDescriptors.get(i))
                .build());
        }
        storeDescriptors.add(KVRangeStoreDescriptor.newBuilder()
            .setId("store2")
            .build());
        // three store with one voter
        balancer.update(storeDescriptors);
        Optional<BalanceCommand> commandOptional = balancer.balance();
        Assert.assertTrue(commandOptional.isPresent());
        ChangeConfigCommand changeConfigCommand = (ChangeConfigCommand) commandOptional.get();
        Assert.assertEquals(3, changeConfigCommand.getVoters().size());
        Assert.assertEquals(0, changeConfigCommand.getLearners().size());

        Set<String> expectedVoters = Sets.newHashSet(voters);
        expectedVoters.add("store2");
        Assert.assertEquals(expectedVoters, changeConfigCommand.getVoters());
    }

    @Test
    public void balanceWithFourStore() {
        KVRangeId id = KVRangeIdUtil.generate();
        List<String> voters = Lists.newArrayList(LOCAL_STORE_ID, "store1", "store2");
        List<String> learners = Lists.newArrayList();
        List<KVRangeDescriptor> rangeDescriptors =
            DescriptorUtils.generateRangeDesc(id, Sets.newHashSet(voters), Sets.newHashSet(learners));
        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        for (int i = 0; i < voters.size(); i++) {
            storeDescriptors.add(KVRangeStoreDescriptor.newBuilder()
                .setId(voters.get(i))
                .addRanges(rangeDescriptors.get(i))
                .build());
        }
        storeDescriptors.add(KVRangeStoreDescriptor.newBuilder()
            .setId("store3")
            .build());
        // four store, config with three voters and no learner
        balancer.update(storeDescriptors);
        Optional<BalanceCommand> commandOptional = balancer.balance();
        Assert.assertTrue(commandOptional.isPresent());
        ChangeConfigCommand changeConfigCommand = (ChangeConfigCommand) commandOptional.get();
        Assert.assertEquals(3, changeConfigCommand.getVoters().size());
        Assert.assertEquals(1, changeConfigCommand.getLearners().size());

        Set<String> expectedVoters = Sets.newHashSet(voters);
        Set<String> expectedLearners = Sets.newHashSet("store3");
        Assert.assertEquals(expectedVoters, changeConfigCommand.getVoters());
        Assert.assertEquals(expectedLearners, changeConfigCommand.getLearners());
    }

    @Test
    public void balanceWithFourVoters() {
        KVRangeId id = KVRangeIdUtil.generate();
        List<String> voters = Lists.newArrayList(LOCAL_STORE_ID, "store1", "store2", "store3");
        List<KVRangeDescriptor> rangeDescriptors = DescriptorUtils.generateRangeDesc(id, Sets.newHashSet(voters), Sets.newHashSet());
        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        for (int i = 0; i < 4; i++) {
            storeDescriptors.add(KVRangeStoreDescriptor.newBuilder()
                .setId(voters.get(i))
                .addRanges(rangeDescriptors.get(i))
                .build());
        }
        // four store with four voters
        balancer.update(storeDescriptors);
        Optional<BalanceCommand> commandOptional = balancer.balance();
        Assert.assertTrue(commandOptional.isPresent());
        ChangeConfigCommand changeConfigCommand = (ChangeConfigCommand) commandOptional.get();
        Assert.assertEquals(3, changeConfigCommand.getVoters().size());

        String maxLoadStore = storeDescriptors.stream()
            .filter(sd -> !sd.getId().equals(LOCAL_STORE_ID))
            .max(Comparator.comparingDouble(sd -> sd.getRanges(0).getLoadHint().getLoad()))
            .get()
            .getId();
        Set<String> expectedVoters = Sets.newHashSet(voters);
        expectedVoters.remove(maxLoadStore);
        Assert.assertEquals(expectedVoters, changeConfigCommand.getVoters());
    }

    @Test
    public void balanceWithFourLearners() {
        KVRangeId id = KVRangeIdUtil.generate();
        List<String> voters = Lists.newArrayList(LOCAL_STORE_ID, "store2", "store3");
        List<String> learners = Lists.newArrayList("store4", "store5", "store6", "store7");
        List<KVRangeDescriptor> rangeDescriptors =
            DescriptorUtils.generateRangeDesc(id, Sets.newHashSet(voters), Sets.newHashSet(learners));
        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        for (int i = 0; i < voters.size() + learners.size(); i++) {
            storeDescriptors.add(KVRangeStoreDescriptor.newBuilder()
                .setId(i < voters.size() ? voters.get(i) : learners.get(i - voters.size()))
                .addRanges(rangeDescriptors.get(i))
                .build());
        }
        // 7 store with 4 voters
        balancer.update(storeDescriptors);
        Optional<BalanceCommand> commandOptional = balancer.balance();
        Assert.assertFalse(commandOptional.isPresent());
    }

    @Test
    public void balanceWithTwoVotersAndThreeLearners() {
        KVRangeId id = KVRangeIdUtil.generate();
        List<String> voters = Lists.newArrayList(LOCAL_STORE_ID, "store2");
        List<String> learners = Lists.newArrayList("store3", "store4", "store5");
        List<KVRangeDescriptor> rangeDescriptors =
            DescriptorUtils.generateRangeDesc(id, Sets.newHashSet(voters), Sets.newHashSet(learners));
        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        for (int i = 0; i < voters.size() + learners.size(); i++) {
            storeDescriptors.add(KVRangeStoreDescriptor.newBuilder()
                .setId(i < voters.size() ? voters.get(i) : learners.get(i - voters.size()))
                .addRanges(rangeDescriptors.get(i))
                .build());
        }
        // 5 store with 2 voters and 3 learners
        balancer.update(storeDescriptors);
        Optional<BalanceCommand> commandOptional = balancer.balance();
        Assert.assertTrue(commandOptional.isPresent());
        ChangeConfigCommand changeConfigCommand = (ChangeConfigCommand) commandOptional.get();
        Assert.assertEquals(3, changeConfigCommand.getVoters().size());
        Assert.assertEquals(2, changeConfigCommand.getLearners().size());
    }


    @Test
    public void rangeWithThreeVAndTwoAlive() throws InterruptedException {
        KVRangeId id = KVRangeIdUtil.generate();
        List<String> voters = Lists.newArrayList(LOCAL_STORE_ID, "store1", "store2");
        List<String> learners = Lists.newArrayList();
        List<KVRangeDescriptor> rangeDescriptors =
            DescriptorUtils.generateRangeDesc(id, Sets.newHashSet(voters), Sets.newHashSet(learners));
        List<KVRangeStoreDescriptor> storeDescriptors = new ArrayList<>();
        for (int i = 0; i < voters.size(); i++) {
            storeDescriptors.add(KVRangeStoreDescriptor.newBuilder()
                .setId(voters.get(i))
                .addRanges(rangeDescriptors.get(i))
                .build());
        }
        balancer.update(Sets.newHashSet(storeDescriptors));
        // store2 dead
        storeDescriptors.remove(storeDescriptors.size() - 1);
        balancer.update(Sets.newHashSet(storeDescriptors));
        Optional<BalanceCommand> commandOptional = balancer.balance();
        Assert.assertTrue(commandOptional.isEmpty());
        Thread.sleep(250);
        commandOptional = balancer.balance();
        Assert.assertTrue(commandOptional.isPresent());
        ChangeConfigCommand changeConfigCommand = (ChangeConfigCommand) commandOptional.get();
        Assert.assertEquals(2, changeConfigCommand.getVoters().size());
        Assert.assertEquals(0, changeConfigCommand.getLearners().size());
        Set<String> expectedVoters = Sets.newHashSet(voters);
        expectedVoters.remove("store2");
        Assert.assertEquals(expectedVoters, changeConfigCommand.getVoters());
    }

    @Test
    public void rangeWithThreeVAndTwoAliveAndOneStoreSpare() throws InterruptedException {
        KVRangeId id = KVRangeIdUtil.generate();
        List<String> voters = Lists.newArrayList(LOCAL_STORE_ID, "store1", "store2");
        List<String> learners = Lists.newArrayList();
        List<KVRangeDescriptor> rangeDescriptors =
            DescriptorUtils.generateRangeDesc(id, Sets.newHashSet(voters), Sets.newHashSet(learners));
        List<KVRangeStoreDescriptor> storeDescriptors = new ArrayList<>();
        storeDescriptors.add(KVRangeStoreDescriptor.newBuilder()
            .setId("store_spare")
            .build());
        for (int i = 0; i < voters.size(); i++) {
            storeDescriptors.add(KVRangeStoreDescriptor.newBuilder()
                .setId(voters.get(i))
                .addRanges(rangeDescriptors.get(i))
                .build());
        }
        balancer.update(Sets.newHashSet(storeDescriptors));
        storeDescriptors.remove(storeDescriptors.size() - 1);
        balancer.update(Sets.newHashSet(storeDescriptors));
        Thread.sleep(250);
        Optional<BalanceCommand> commandOptional = balancer.balance();
        Assert.assertTrue(commandOptional.isPresent());
        ChangeConfigCommand changeConfigCommand = (ChangeConfigCommand) commandOptional.get();
        Assert.assertEquals(3, changeConfigCommand.getVoters().size());
        Assert.assertEquals(0, changeConfigCommand.getLearners().size());

        Set<String> expectedVoters = Sets.newHashSet(voters);
        expectedVoters.remove("store2");
        expectedVoters.add("store_spare");
        Assert.assertEquals(expectedVoters, changeConfigCommand.getVoters());
    }

    @Test
    public void rangeWithThreeVAndThreeL() {
        KVRangeId id = KVRangeIdUtil.generate();
        List<String> voters = Lists.newArrayList(LOCAL_STORE_ID, "store1", "store2");
        List<String> learners = Lists.newArrayList("store3", "store4", "store5");
        List<KVRangeDescriptor> rangeDescriptors =
            DescriptorUtils.generateRangeDesc(id, Sets.newHashSet(voters), Sets.newHashSet(learners));
        Map<String, KVRangeStoreDescriptor> storeDescriptors = new HashMap<>();
        storeDescriptors.put("store_spare_1", KVRangeStoreDescriptor.newBuilder()
            .setId("store_spare_1")
            .build());
        storeDescriptors.put("store_spare_2", KVRangeStoreDescriptor.newBuilder()
            .setId("store_spare_2")
            .addRanges(KVRangeDescriptor.newBuilder()
                .setId(KVRangeIdUtil.generate())
                .setLoadHint(LoadHint.newBuilder().setLoad(1D).build())
            )
            .build());
        for (int i = 0; i < voters.size() + learners.size(); i++) {
            String storeId = i < voters.size() ? voters.get(i) : learners.get(i - voters.size());
            storeDescriptors.put(storeId, KVRangeStoreDescriptor.newBuilder()
                .setId(storeId)
                .addRanges(rangeDescriptors.get(i))
                .build());
        }
        storeDescriptors.remove("store2");
        storeDescriptors.remove("store5");
        balancer.update(Sets.newHashSet(storeDescriptors.values()));
        Optional<BalanceCommand> commandOptional = balancer.balance();
        Assert.assertTrue(commandOptional.isPresent());
        ChangeConfigCommand changeConfigCommand = (ChangeConfigCommand) commandOptional.get();
        Assert.assertEquals(3, changeConfigCommand.getVoters().size());
        Assert.assertEquals(3, changeConfigCommand.getLearners().size());

        Set<String> expectedVoters = Sets.newHashSet(LOCAL_STORE_ID, "store1", "store_spare_1");
        Set<String> expectedLearners = Sets.newHashSet("store3", "store4", "store_spare_2");
        Assert.assertEquals(expectedVoters, changeConfigCommand.getVoters());
        Assert.assertEquals(expectedLearners, changeConfigCommand.getLearners());
    }
}
