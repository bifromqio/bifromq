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

package com.baidu.bifromq.basekv.raft.functest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.baidu.bifromq.basekv.raft.event.SnapshotRestoredEvent;
import com.baidu.bifromq.basekv.raft.functest.annotation.Cluster;
import com.baidu.bifromq.basekv.raft.functest.template.SharedRaftConfigTestTemplate;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;

public class SnapshotRestoreTest extends SharedRaftConfigTestTemplate {
    @Cluster(v = "V1")
    @Test
    public void snapshotRestoredEventAfterSnapshotInstalled() {
        String leader = group.currentLeader().get();
        group.compact("V1", ByteString.EMPTY, 1).join();
        group.propose("V1", ByteString.copyFromUtf8("Value1")).join();
        group.awaitIndexCommitted("V1", 2);

        group.addRaftNode("V2", 0, 0, ClusterConfig.newBuilder().addVoters("V4").build(), raftConfig());
        group.connect("V2");
        Set<String> newVoters = new HashSet<String>() {{
            add("V1");
            add("V2");
        }};
        group.changeClusterConfig(leader, "cId", newVoters, Collections.emptySet()).join();
        for (String peerId : newVoters) {
            assertTrue(group.awaitIndexCommitted(peerId, 3));
        }
        List<SnapshotRestoredEvent> events = group.snapshotRestoredLogs("V2");
        assertEquals(1, events.size());
        assertEquals("V2", events.get(0).nodeId);
        assertTrue(events.get(0).snapshot.getClusterConfig().getVotersList().contains("V1"));
    }
}
