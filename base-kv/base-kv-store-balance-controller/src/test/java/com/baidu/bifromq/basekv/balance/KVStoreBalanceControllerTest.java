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

package com.baidu.bifromq.basekv.balance;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basekv.balance.command.BootstrapCommand;
import com.baidu.bifromq.basekv.balance.command.ChangeConfigCommand;
import com.baidu.bifromq.basekv.balance.command.MergeCommand;
import com.baidu.bifromq.basekv.balance.command.RecoveryCommand;
import com.baidu.bifromq.basekv.balance.command.SplitCommand;
import com.baidu.bifromq.basekv.balance.command.TransferLeadershipCommand;
import com.baidu.bifromq.basekv.balance.utils.DescriptorUtils;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.metaservice.IBaseKVClusterMetadataManager;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.store.proto.KVRangeSplitReply;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class KVStoreBalanceControllerTest {

    private static final String CLUSTER_ID = "test_cluster";
    private static final String LOCAL_STORE_ID = "localStoreId";

    @Mock
    private IBaseKVClusterMetadataManager metadataManager;
    @Mock
    private IBaseKVStoreClient storeClient;

    @Mock
    private IStoreBalancerFactory balancerFactory;

    @Mock
    private StoreBalancer storeBalancer;

    private final PublishSubject<Map<String, Struct>> loadRuleSubject = PublishSubject.create();
    private final PublishSubject<Set<KVRangeStoreDescriptor>> storeDescSubject = PublishSubject.create();

    private KVStoreBalanceController balanceController;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() throws IOException {
        closeable = MockitoAnnotations.openMocks(this);
        when(storeClient.clusterId()).thenReturn(CLUSTER_ID);
        when(balancerFactory.newBalancer(eq(CLUSTER_ID), eq(LOCAL_STORE_ID))).thenReturn(storeBalancer);
        balanceController = new KVStoreBalanceController(
            metadataManager,
            storeClient,
            List.of(balancerFactory),
            Duration.ofMillis(100),
            Executors.newScheduledThreadPool(1)
        );
        when(metadataManager.loadRules()).thenReturn(loadRuleSubject);
        when(storeClient.describe()).thenReturn(storeDescSubject);
        balanceController.start(LOCAL_STORE_ID);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        balanceController.stop();
        closeable.close();
    }

    @Test
    public void testNoInput() {
        verify(storeBalancer, timeout(1200).times(0)).update(any(Struct.class));
        verify(storeBalancer, timeout(1200).times(0)).update(any(Set.class));
    }

    @Test
    public void testInput() {
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        when(storeBalancer.balance()).thenReturn(NoNeedBalance.INSTANCE);
        storeDescSubject.onNext(storeDescriptors);
        verify(storeBalancer, timeout(1200).times(1)).update(eq(storeDescriptors));
        verify(storeBalancer, timeout(1200).times(1)).balance();

        log.info("Test input done");
        loadRuleSubject.onNext(Map.of(storeBalancer.getClass().getName(), Struct.getDefaultInstance()));
        verify(storeBalancer, timeout(1200).times(1)).update(eq(Struct.getDefaultInstance()));
        verify(storeBalancer, timeout(1200).times(2)).balance();
    }

    @Test
    public void testBootStrapCommand() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        when(storeBalancer.balance()).thenReturn(
            BalanceNow.of(
                BootstrapCommand.builder().kvRangeId(id).toStore(LOCAL_STORE_ID).boundary(FULL_BOUNDARY).build()));
        when(storeClient.bootstrap(eq(LOCAL_STORE_ID), any())).thenReturn(new CompletableFuture<>());
        storeDescSubject.onNext(storeDescriptors);
        verify(storeClient, timeout(1200).times(1)).bootstrap(eq(LOCAL_STORE_ID),
            argThat(c -> c.getKvRangeId().equals(id) && c.getBoundary().equals(FULL_BOUNDARY)));
    }

    @Test
    public void testChangeConfig() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        ChangeConfigCommand command = ChangeConfigCommand.builder()
            .kvRangeId(id)
            .expectedVer(2L)
            .toStore(LOCAL_STORE_ID)
            .voters(Sets.newHashSet(LOCAL_STORE_ID, "store1"))
            .learners(Sets.newHashSet("learner1"))
            .build();
        when(storeBalancer.balance()).thenReturn(BalanceNow.of(command));
        when(storeClient.changeReplicaConfig(eq(LOCAL_STORE_ID), any())).thenReturn(new CompletableFuture<>());
        storeDescSubject.onNext(storeDescriptors);
        verify(storeClient, timeout(1200).times(1)).changeReplicaConfig(eq(LOCAL_STORE_ID),
            argThat(r -> r.getKvRangeId().equals(id)
                && r.getVer() == command.getExpectedVer()
                && r.getNewVotersList().containsAll(command.getVoters())
                && r.getNewLearnersList().containsAll(command.getLearners())));
    }

    @Test
    public void testMerge() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        MergeCommand command = MergeCommand.builder()
            .kvRangeId(id)
            .mergeeId(KVRangeIdUtil.generate())
            .toStore(LOCAL_STORE_ID)
            .expectedVer(2L)
            .build();
        when(storeBalancer.balance()).thenReturn(BalanceNow.of(command));
        when(storeClient.mergeRanges(eq(LOCAL_STORE_ID), any())).thenReturn(new CompletableFuture<>());
        storeDescSubject.onNext(storeDescriptors);
        verify(storeClient, timeout(1200).times(1)).mergeRanges(eq(LOCAL_STORE_ID),
            argThat(r -> r.getMergerId().equals(id)
                && r.getVer() == command.getExpectedVer()
                && r.getMergeeId().equals(command.getMergeeId())));
    }

    @Test
    public void testSplit() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        SplitCommand command = SplitCommand.builder()
            .toStore(LOCAL_STORE_ID)
            .kvRangeId(id)
            .splitKey(ByteString.copyFromUtf8("splitKey"))
            .expectedVer(2L)
            .build();
        when(storeBalancer.balance()).thenReturn(BalanceNow.of(command));
        when(storeClient.splitRange(eq(LOCAL_STORE_ID), any())).thenReturn(new CompletableFuture<>());
        storeDescSubject.onNext(storeDescriptors);
        verify(storeClient, timeout(1200).times(1)).splitRange(eq(LOCAL_STORE_ID),
            argThat(r -> r.getKvRangeId().equals(id)
                && r.getVer() == command.getExpectedVer()
                && r.getSplitKey().equals(command.getSplitKey())));
    }

    @Test
    public void testTransferLeadership() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        TransferLeadershipCommand command = TransferLeadershipCommand.builder()
            .toStore(LOCAL_STORE_ID)
            .newLeaderStore("store1")
            .kvRangeId(id)
            .expectedVer(2L)
            .build();
        when(storeBalancer.balance()).thenReturn(BalanceNow.of(command));
        when(storeClient.transferLeadership(eq(LOCAL_STORE_ID), any())).thenReturn(new CompletableFuture<>());
        storeDescSubject.onNext(storeDescriptors);
        verify(storeClient, timeout(1200).times(1)).transferLeadership(eq(LOCAL_STORE_ID),
            argThat(r -> r.getKvRangeId().equals(id)
                && r.getVer() == command.getExpectedVer()
                && r.getNewLeaderStore().equals(command.getNewLeaderStore())));
    }

    @Test
    public void testRecover() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        RecoveryCommand command = RecoveryCommand.builder()
            .toStore(LOCAL_STORE_ID)
            .kvRangeId(id)
            .build();
        when(storeBalancer.balance()).thenReturn(BalanceNow.of(command));
        when(storeClient.recover(eq(LOCAL_STORE_ID), any())).thenReturn(new CompletableFuture<>());
        storeDescSubject.onNext(storeDescriptors);
        verify(storeClient, timeout(1200).times(1)).recover(eq(LOCAL_STORE_ID),
            argThat(r -> r.getKvRangeId().equals(id)));
    }

    @Test
    public void testRangeCommandRunHistoryKept() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        SplitCommand command = SplitCommand.builder()
            .toStore(LOCAL_STORE_ID)
            .kvRangeId(id)
            .splitKey(ByteString.copyFromUtf8("splitKey"))
            .expectedVer(2L)
            .build();
        when(storeBalancer.balance()).thenReturn(BalanceNow.of(command));
        when(storeClient.splitRange(eq(LOCAL_STORE_ID), any()))
            .thenReturn(CompletableFuture.completedFuture(KVRangeSplitReply.newBuilder()
                .setCode(ReplyCode.Ok).build()
            ));
        storeDescSubject.onNext(storeDescriptors);
        verify(storeClient, timeout(1200).times(1)).splitRange(eq(LOCAL_STORE_ID),
            argThat(r -> r.getKvRangeId().equals(id)
                && r.getVer() == command.getExpectedVer()
                && r.getSplitKey().equals(command.getSplitKey())));

        loadRuleSubject.onNext(Map.of(storeBalancer.getClass().getName(), Struct.getDefaultInstance()));

        verify(storeClient, timeout(1200).times(1)).splitRange(eq(LOCAL_STORE_ID),
            argThat(r -> r.getKvRangeId().equals(id)
                && r.getVer() == command.getExpectedVer()
                && r.getSplitKey().equals(command.getSplitKey())));
    }

    @Test
    public void testRangeCommandRunHistoryCleared() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        SplitCommand command = SplitCommand.builder()
            .toStore(LOCAL_STORE_ID)
            .kvRangeId(id)
            .splitKey(ByteString.copyFromUtf8("splitKey"))
            .expectedVer(2L)
            .build();
        when(storeBalancer.balance()).thenReturn(BalanceNow.of(command));
        when(storeClient.splitRange(eq(LOCAL_STORE_ID), any()))
            .thenReturn(CompletableFuture.completedFuture(KVRangeSplitReply.newBuilder()
                .setCode(ReplyCode.Ok).build()));
        storeDescSubject.onNext(storeDescriptors);
        verify(storeClient, timeout(1200).times(1)).splitRange(eq(LOCAL_STORE_ID),
            argThat(r -> r.getKvRangeId().equals(id)
                && r.getVer() == command.getExpectedVer()
                && r.getSplitKey().equals(command.getSplitKey())));

        storeDescriptors = generateDescriptor();
        storeDescSubject.onNext(storeDescriptors);
        verify(storeClient, timeout(1200).times(2)).splitRange(eq(LOCAL_STORE_ID),
            argThat(r -> r.getKvRangeId().equals(id)
                && r.getVer() == command.getExpectedVer()
                && r.getSplitKey().equals(command.getSplitKey())));
    }

    @Test
    public void testRetryWhenCommandRunFailed() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        SplitCommand command = SplitCommand.builder()
            .toStore(LOCAL_STORE_ID)
            .kvRangeId(id)
            .splitKey(ByteString.copyFromUtf8("splitKey"))
            .expectedVer(2L)
            .build();
        when(storeBalancer.balance()).thenReturn(BalanceNow.of(command));
        when(storeClient.splitRange(eq(LOCAL_STORE_ID), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Mock Exception")),
                CompletableFuture.completedFuture(KVRangeSplitReply.newBuilder().setCode(ReplyCode.Ok).build()));
        storeDescSubject.onNext(storeDescriptors);
        verify(storeClient, timeout(1200).times(2)).splitRange(eq(LOCAL_STORE_ID),
            argThat(r -> r.getKvRangeId().equals(id)
                && r.getVer() == command.getExpectedVer()
                && r.getSplitKey().equals(command.getSplitKey())));
    }

    @Test
    public void testRetryWhenCommandNotReady() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        SplitCommand command = SplitCommand.builder()
            .toStore(LOCAL_STORE_ID)
            .kvRangeId(id)
            .splitKey(ByteString.copyFromUtf8("splitKey"))
            .expectedVer(2L)
            .build();
        when(storeBalancer.balance()).thenReturn(AwaitBalance.of(Duration.ofMillis(200)), BalanceNow.of(command));
        when(storeClient.splitRange(eq(LOCAL_STORE_ID), any()))
            .thenReturn(
                CompletableFuture.completedFuture(KVRangeSplitReply.newBuilder().setCode(ReplyCode.Ok).build()));
        storeDescSubject.onNext(storeDescriptors);
        verify(storeClient, timeout(1200).times(1)).splitRange(eq(LOCAL_STORE_ID),
            argThat(r -> r.getKvRangeId().equals(id)
                && r.getVer() == command.getExpectedVer()
                && r.getSplitKey().equals(command.getSplitKey())));
    }

    @Test
    public void testRetryBeingPreempted() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        SplitCommand command = SplitCommand.builder()
            .toStore(LOCAL_STORE_ID)
            .kvRangeId(id)
            .splitKey(ByteString.copyFromUtf8("splitKey"))
            .expectedVer(2L)
            .build();
        when(storeBalancer.balance()).thenReturn(AwaitBalance.of(Duration.ofSeconds(10)), BalanceNow.of(command));
        when(storeClient.splitRange(eq(LOCAL_STORE_ID), any())).thenReturn(
            CompletableFuture.completedFuture(KVRangeSplitReply.newBuilder().setCode(ReplyCode.Ok).build()));
        storeDescSubject.onNext(storeDescriptors);
        verify(storeBalancer, timeout(1200).times(1)).update(any(Set.class));
        loadRuleSubject.onNext(Map.of(storeBalancer.getClass().getName(), Struct.getDefaultInstance()));
        verify(storeBalancer, timeout(1200).times(1)).update(any(Struct.class));
        verify(storeClient, timeout(1200).times(1)).splitRange(eq(LOCAL_STORE_ID),
            argThat(r -> r.getKvRangeId().equals(id)
                && r.getVer() == command.getExpectedVer()
                && r.getSplitKey().equals(command.getSplitKey())));
    }

    private Set<KVRangeStoreDescriptor> generateDescriptor() {
        KVRangeId id = KVRangeIdUtil.generate();
        List<String> voters = Lists.newArrayList(LOCAL_STORE_ID, "store1");
        List<String> learners = Lists.newArrayList();
        List<KVRangeDescriptor> rangeDescriptors =
            DescriptorUtils.generateRangeDesc(id, Sets.newHashSet(voters), Sets.newHashSet(learners));
        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        for (int i = 0; i < voters.size(); i++) {
            storeDescriptors.add(KVRangeStoreDescriptor.newBuilder()
                .setId(voters.get(i))
                .putStatistics("cpu.usage", 0.1)
                .addRanges(rangeDescriptors.get(i))
                .build());
        }
        return storeDescriptors;
    }
}