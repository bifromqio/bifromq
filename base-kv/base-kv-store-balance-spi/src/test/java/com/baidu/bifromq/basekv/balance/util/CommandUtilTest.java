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

package com.baidu.bifromq.basekv.balance.util;

import com.baidu.bifromq.basekv.balance.BalanceNow;
import com.baidu.bifromq.basekv.balance.BalanceResult;
import com.baidu.bifromq.basekv.balance.BalanceResultType;
import com.baidu.bifromq.basekv.balance.command.ChangeConfigCommand;
import com.baidu.bifromq.basekv.balance.command.MergeCommand;
import com.baidu.bifromq.basekv.balance.command.SplitCommand;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.utils.BoundaryUtil;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.basekv.utils.KeySpaceDAG;
import com.google.protobuf.ByteString;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import static com.baidu.bifromq.basekv.balance.util.CommandUtil.diffBy;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.testng.Assert.*;

public class CommandUtilTest {
    @Test
    public void quitWithMultipleVoters() {
        String localStoreId = "localStore";
        KVRangeId kvRangeId = KVRangeId.newBuilder().setId(1).setEpoch(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setVer(1)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .addVoters("otherStore")
                .build())
            .build();

        BalanceResult result = CommandUtil.quit(localStoreId, kvRangeDescriptor);

        assertEquals(result.type(), BalanceResultType.BalanceNow);
        ChangeConfigCommand command = (ChangeConfigCommand) ((BalanceNow<?>) result).command;

        assertEquals(command.getToStore(), localStoreId);
        assertEquals(command.getKvRangeId(), kvRangeId);
        assertEquals(command.getVoters(), Set.of(localStoreId));
        assertEquals(command.getLearners(), Collections.emptySet());
    }

    @Test
    public void quitWithLearners() {
        String localStoreId = "localStore";
        KVRangeId kvRangeId = KVRangeId.newBuilder().setId(1).setEpoch(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setVer(1)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .addLearners("learnerStore")
                .build())
            .build();

        BalanceResult result = CommandUtil.quit(localStoreId, kvRangeDescriptor);

        assertEquals(result.type(), BalanceResultType.BalanceNow);
        ChangeConfigCommand command = (ChangeConfigCommand) ((BalanceNow<?>) result).command;

        assertEquals(command.getToStore(), localStoreId);
        assertEquals(command.getKvRangeId(), kvRangeId);
        assertEquals(command.getVoters(), Set.of(localStoreId));
        assertEquals(command.getLearners(), Collections.emptySet());
    }

    @Test
    public void quitWithSingleVoterNoLearners() {
        String localStoreId = "localStore";
        KVRangeId kvRangeId = KVRangeId.newBuilder().setId(1).setEpoch(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setVer(1)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .build())
            .build();

        BalanceResult result = CommandUtil.quit(localStoreId, kvRangeDescriptor);

        assertEquals(result.type(), BalanceResultType.BalanceNow);
        ChangeConfigCommand command = (ChangeConfigCommand) ((BalanceNow<?>) result).command;

        assertEquals(command.getToStore(), localStoreId);
        assertEquals(command.getKvRangeId(), kvRangeId);
        assertEquals(command.getVoters(), Collections.emptySet());
        assertEquals(command.getLearners(), Collections.emptySet());
    }


    @Test
    public void noDiff() {
        NavigableMap<Boundary, ClusterConfig> expected = new TreeMap<>(BoundaryUtil::compare);
        expected.put(boundary(null, "a"), ClusterConfig.newBuilder().addVoters("voter1").build());
        expected.put(boundary("a", null), ClusterConfig.newBuilder().addVoters("voter1").build());

        NavigableMap<Boundary, KeySpaceDAG.LeaderRange> current = new TreeMap<>(BoundaryUtil::compare);
        KVRangeId id = KVRangeIdUtil.generate();
        KVRangeId id1 = KVRangeIdUtil.generate();
        current.put(boundary(null, "a"), new KeySpaceDAG.LeaderRange("voter1", KVRangeDescriptor
            .newBuilder()
            .setId(id)
            .setVer(1L)
            .setConfig(ClusterConfig.newBuilder().setCorrelateId("c1").addVoters("voter1").build())
            .build()));
        current.put(boundary("a", null), new KeySpaceDAG.LeaderRange("voter1", KVRangeDescriptor
            .newBuilder()
            .setId(id1)
            .setConfig(ClusterConfig.newBuilder().setCorrelateId("c1").addVoters("voter1").build())
            .build()));
        assertNull(diffBy(expected, current));
    }


    @Test
    public void diffByNull() {
        NavigableMap<Boundary, ClusterConfig> expected = new TreeMap<>(BoundaryUtil::compare);
        expected.put(boundary(null, "a"),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());
        expected.put(boundary("a", null), ClusterConfig.newBuilder().addVoters("voter1").build());

        NavigableMap<Boundary, KeySpaceDAG.LeaderRange> current = new TreeMap<>(BoundaryUtil::compare);
        KVRangeId id = KVRangeIdUtil.generate();
        KVRangeId id1 = KVRangeIdUtil.generate();
        current.put(boundary(null, "a"), new KeySpaceDAG.LeaderRange("voter1", KVRangeDescriptor
            .newBuilder()
            .setId(id)
            .setVer(1L)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter2").addNextVoters("voter3").build())
            .build()));
        current.put(boundary("a", null), new KeySpaceDAG.LeaderRange("voter1", KVRangeDescriptor
            .newBuilder()
            .setId(id1)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter2").build())
            .build()));
        assertNull(diffBy(expected, current));
    }

    @Test
    public void diffByConfigChange() {
        NavigableMap<Boundary, ClusterConfig> expected = new TreeMap<>(BoundaryUtil::compare);
        expected.put(boundary(null, "a"),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());
        expected.put(boundary("a", null), ClusterConfig.newBuilder().addVoters("voter1").build());

        NavigableMap<Boundary, KeySpaceDAG.LeaderRange> current = new TreeMap<>(BoundaryUtil::compare);
        KVRangeId id = KVRangeIdUtil.generate();
        KVRangeId id1 = KVRangeIdUtil.generate();
        current.put(boundary(null, "a"), new KeySpaceDAG.LeaderRange("voter1", KVRangeDescriptor
            .newBuilder()
            .setId(id)
            .setVer(1L)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter2").build())
            .build()));
        current.put(boundary("a", null), new KeySpaceDAG.LeaderRange("voter1", KVRangeDescriptor
            .newBuilder()
            .setId(id1)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter2").build())
            .build()));
        ChangeConfigCommand command = (ChangeConfigCommand) diffBy(expected, current);
        assertNotNull(command);
        assertEquals(command.getToStore(), "voter1");
        assertEquals(command.getKvRangeId(), id);
        assertEquals(command.getExpectedVer(), 1L);
        assertEquals(command.getVoters(), Set.of("voter1"));
        assertEquals(command.getLearners(), Set.of("learner1"));
    }

    @Test
    public void diffBySplit() {
        NavigableMap<Boundary, ClusterConfig> expected = new TreeMap<>(BoundaryUtil::compare);
        expected.put(boundary(null, "a"),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());
        expected.put(boundary("a", "b"),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());
        expected.put(boundary("b", null),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());

        NavigableMap<Boundary, KeySpaceDAG.LeaderRange> current = new TreeMap<>(BoundaryUtil::compare);
        KVRangeId id = KVRangeIdUtil.generate();
        KVRangeId id1 = KVRangeIdUtil.generate();
        current.put(boundary(null, "b"), new KeySpaceDAG.LeaderRange("voter1", KVRangeDescriptor
            .newBuilder()
            .setId(id)
            .setVer(1L)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build())
            .build()));
        current.put(boundary("b", null), new KeySpaceDAG.LeaderRange("voter1", KVRangeDescriptor
            .newBuilder()
            .setId(id1)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build())
            .build()));
        SplitCommand command = (SplitCommand) diffBy(expected, current);
        assertNotNull(command);
        assertEquals(command.getToStore(), "voter1");
        assertEquals(command.getKvRangeId(), id);
        assertEquals(command.getExpectedVer(), 1L);
        assertEquals(command.getSplitKey(), ByteString.copyFromUtf8("a"));
    }

    @Test
    public void diffByConfigChangeDuringMerging() {
        NavigableMap<Boundary, ClusterConfig> expected = new TreeMap<>(BoundaryUtil::compare);
        expected.put(boundary(null, "b"),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());
        expected.put(boundary("b", null),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());

        NavigableMap<Boundary, KeySpaceDAG.LeaderRange> current = new TreeMap<>(BoundaryUtil::compare);
        KVRangeId id = KVRangeIdUtil.generate();
        KVRangeId id1 = KVRangeIdUtil.generate();
        KVRangeId id2 = KVRangeIdUtil.generate();
        current.put(boundary(null, "a"), new KeySpaceDAG.LeaderRange("voter1", KVRangeDescriptor
            .newBuilder()
            .setId(id)
            .setVer(1L)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build())
            .build()));
        current.put(boundary("a", "b"), new KeySpaceDAG.LeaderRange("voter1", KVRangeDescriptor
            .newBuilder()
            .setId(id1)
            .setVer(2L)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").build())
            .build()));
        current.put(boundary("b", null), new KeySpaceDAG.LeaderRange("voter1", KVRangeDescriptor
            .newBuilder()
            .setId(id2)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build())
            .build()));
        ChangeConfigCommand command = (ChangeConfigCommand) diffBy(expected, current);
        assertNotNull(command);
        assertEquals(command.getToStore(), "voter1");
        assertEquals(command.getKvRangeId(), id1);
        assertEquals(command.getExpectedVer(), 2L);
        assertEquals(command.getVoters(), Set.of("voter1"));
        assertEquals(command.getLearners(), Set.of("learner1"));
    }

    @Test
    public void diffByMergeDuringMerging() {
        NavigableMap<Boundary, ClusterConfig> expected = new TreeMap<>(BoundaryUtil::compare);
        expected.put(boundary(null, "b"),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());
        expected.put(boundary("b", null),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());

        NavigableMap<Boundary, KeySpaceDAG.LeaderRange> current = new TreeMap<>(BoundaryUtil::compare);
        KVRangeId id = KVRangeIdUtil.generate();
        KVRangeId id1 = KVRangeIdUtil.generate();
        KVRangeId id2 = KVRangeIdUtil.generate();
        current.put(boundary(null, "a"), new KeySpaceDAG.LeaderRange("voter1", KVRangeDescriptor
            .newBuilder()
            .setId(id)
            .setVer(1L)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build())
            .build()));
        current.put(boundary("a", "b"), new KeySpaceDAG.LeaderRange("voter1", KVRangeDescriptor
            .newBuilder()
            .setId(id1)
            .setVer(2L)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build())
            .build()));
        current.put(boundary("b", null), new KeySpaceDAG.LeaderRange("voter1", KVRangeDescriptor
            .newBuilder()
            .setId(id2)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build())
            .build()));
        MergeCommand command = (MergeCommand) diffBy(expected, current);
        assertNotNull(command);
        assertEquals(command.getToStore(), "voter1");
        assertEquals(command.getKvRangeId(), id);
        assertEquals(command.getExpectedVer(), 1L);
        assertEquals(command.getMergeeId(), id1);
    }

    @Test
    public void diffByNullDuringMerging() {
        NavigableMap<Boundary, ClusterConfig> expected = new TreeMap<>(BoundaryUtil::compare);
        expected.put(boundary(null, "b"),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());
        expected.put(boundary("b", null),
            ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build());

        NavigableMap<Boundary, KeySpaceDAG.LeaderRange> current = new TreeMap<>(BoundaryUtil::compare);
        KVRangeId id = KVRangeIdUtil.generate();
        KVRangeId id1 = KVRangeIdUtil.generate();
        KVRangeId id2 = KVRangeIdUtil.generate();
        current.put(boundary(null, "a"), new KeySpaceDAG.LeaderRange("voter1", KVRangeDescriptor
            .newBuilder()
            .setId(id)
            .setVer(1L)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build())
            .build()));
        current.put(boundary("a", "b"), new KeySpaceDAG.LeaderRange("voter1", KVRangeDescriptor
            .newBuilder()
            .setId(id1)
            .setVer(2L)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").addNextVoters("voter2").build())
            .build()));
        current.put(boundary("b", null), new KeySpaceDAG.LeaderRange("voter1", KVRangeDescriptor
            .newBuilder()
            .setId(id2)
            .setConfig(ClusterConfig.newBuilder().addVoters("voter1").addLearners("learner1").build())
            .build()));
        assertNull(diffBy(expected, current));
    }

    private Boundary boundary(String startKey, String endKey) {
        Boundary.Builder builder = Boundary.newBuilder();
        if (startKey != null) {
            builder.setStartKey(copyFromUtf8(startKey));
        }
        if (endKey != null) {
            builder.setEndKey(copyFromUtf8(endKey));
        }
        return builder.build();
    }
}
