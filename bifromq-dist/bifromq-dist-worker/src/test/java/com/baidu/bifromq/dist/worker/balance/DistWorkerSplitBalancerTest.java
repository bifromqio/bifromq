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

package com.baidu.bifromq.dist.worker.balance;

import static com.baidu.bifromq.basekv.store.range.hinter.KVLoadBasedSplitHinter.LOAD_TYPE_AVG_LATENCY_NANOS;
import static com.baidu.bifromq.basekv.store.range.hinter.KVLoadBasedSplitHinter.LOAD_TYPE_IO_DENSITY;
import static com.baidu.bifromq.basekv.store.range.hinter.KVLoadBasedSplitHinter.LOAD_TYPE_IO_LATENCY_NANOS;
import static com.baidu.bifromq.dist.worker.hinter.FanoutSplitHinter.LOAD_TYPE_FANOUT_SCALE;
import static com.baidu.bifromq.dist.worker.hinter.FanoutSplitHinter.LOAD_TYPE_FANOUT_TOPIC_FILTERS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basekv.balance.command.BalanceCommand;
import com.baidu.bifromq.basekv.balance.command.SplitCommand;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.proto.SplitHint;
import com.baidu.bifromq.basekv.proto.State;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.baidu.bifromq.basekv.store.range.hinter.MutationKVLoadBasedSplitHinter;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.dist.worker.hinter.FanoutSplitHinter;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.Test;

public class DistWorkerSplitBalancerTest {
    @Test
    public void noLocalDesc() {
        DistWorkerSplitBalancer balancer = new DistWorkerSplitBalancer("local");
        assertFalse(balancer.balance().isPresent());
    }

    @Test
    public void cpuUsageExceedLimit() {
        DistWorkerSplitBalancer balancer = new DistWorkerSplitBalancer("local");
        balancer.update(Collections.singleton(KVRangeStoreDescriptor
            .newBuilder()
            .setId("local")
            .putStatistics("cpu.usage", 0.75)
            .build()
        ));
        assertFalse(balancer.balance().isPresent());
    }

    @Test
    public void splitHintPreference() {
        KVRangeId rangeId = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> descriptors = Collections.singleton(KVRangeStoreDescriptor
            .newBuilder()
            .setId("local")
            .putStatistics("cpu.usage", 0.65)
            .addRanges(KVRangeDescriptor.newBuilder()
                .setId(rangeId)
                .setRole(RaftNodeStatus.Leader)
                .setState(State.StateType.Normal)
                .addHints(SplitHint.newBuilder()
                    .setType(FanoutSplitHinter.TYPE)
                    .putLoad(LOAD_TYPE_FANOUT_TOPIC_FILTERS, 1)
                    .putLoad(LOAD_TYPE_FANOUT_SCALE, 100000)
                    .setSplitKey(ByteString.copyFromUtf8("fanoutSplitKey")))
                .addHints(SplitHint.newBuilder()
                    .setType(MutationKVLoadBasedSplitHinter.TYPE)
                    .putLoad(LOAD_TYPE_IO_DENSITY, 10)
                    .putLoad(LOAD_TYPE_IO_LATENCY_NANOS, 15)
                    .putLoad(LOAD_TYPE_AVG_LATENCY_NANOS, 100)
                    .setSplitKey(ByteString.copyFromUtf8("splitMutationLoadKey"))
                    .build())
                .build())
            .build()
        );
        DistWorkerSplitBalancer balancer = new DistWorkerSplitBalancer("local", 0.8, 5, 20);
        balancer.update(descriptors);
        Optional<BalanceCommand> command = balancer.balance();
        assertTrue(command.isPresent());
        assertEquals(command.get().getKvRangeId(), rangeId);
        assertEquals(command.get().getToStore(), "local");
        assertEquals(command.get().getExpectedVer(), 0);
        assertEquals(((SplitCommand) command.get()).getSplitKey(), ByteString.copyFromUtf8("fanoutSplitKey"));

        descriptors = Collections.singleton(KVRangeStoreDescriptor
            .newBuilder()
            .setId("local")
            .putStatistics("cpu.usage", 0.65)
            .addRanges(KVRangeDescriptor.newBuilder()
                .setId(rangeId)
                .setRole(RaftNodeStatus.Leader)
                .setState(State.StateType.Normal)
                .addHints(SplitHint.newBuilder()
                    .setType(FanoutSplitHinter.TYPE)
                    .putLoad(LOAD_TYPE_FANOUT_TOPIC_FILTERS, 0)
                    .putLoad(LOAD_TYPE_FANOUT_SCALE, 0))
                .addHints(SplitHint.newBuilder()
                    .setType(MutationKVLoadBasedSplitHinter.TYPE)
                    .putLoad(LOAD_TYPE_IO_DENSITY, 10)
                    .putLoad(LOAD_TYPE_IO_LATENCY_NANOS, 15)
                    .putLoad(LOAD_TYPE_AVG_LATENCY_NANOS, 100)
                    .setSplitKey(ByteString.copyFromUtf8("splitMutationLoadKey"))
                    .build())
                .build())
            .build()
        );
        balancer.update(descriptors);
        command = balancer.balance();
        assertTrue(command.isPresent());
        assertEquals(command.get().getKvRangeId(), rangeId);
        assertEquals(command.get().getToStore(), "local");
        assertEquals(command.get().getExpectedVer(), 0);
        assertEquals(((SplitCommand) command.get()).getSplitKey(), ByteString.copyFromUtf8("splitMutationLoadKey"));
    }

    @Test
    public void hintNoSplitKey() {
        KVRangeId rangeId = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> descriptors = Collections.singleton(KVRangeStoreDescriptor
            .newBuilder()
            .setId("local")
            .putStatistics("cpu.usage", 0.65)
            .addRanges(KVRangeDescriptor.newBuilder()
                .setId(rangeId)
                .setRole(RaftNodeStatus.Leader)
                .setState(State.StateType.Normal)
                .addHints(SplitHint.newBuilder()
                    .setType(MutationKVLoadBasedSplitHinter.TYPE)
                    .putLoad(LOAD_TYPE_IO_DENSITY, 1)
                    .putLoad(LOAD_TYPE_IO_LATENCY_NANOS, 1)
                    .putLoad(LOAD_TYPE_AVG_LATENCY_NANOS, 1)
                    .build())
                .build())
            .build()
        );
        DistWorkerSplitBalancer balancer = new DistWorkerSplitBalancer("local");
        balancer.update(descriptors);
        Optional<BalanceCommand> command = balancer.balance();
        assertFalse(command.isPresent());
    }
}
