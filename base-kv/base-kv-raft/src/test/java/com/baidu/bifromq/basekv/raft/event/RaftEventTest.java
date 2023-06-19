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

package com.baidu.bifromq.basekv.raft.event;

import static org.testng.AssertJUnit.assertEquals;

import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.baidu.bifromq.basekv.raft.proto.Snapshot;
import java.util.Collections;
import org.testng.annotations.Test;

public class RaftEventTest {
    @Test
    public void typeMatch() {
        CommitEvent commitEvent = new CommitEvent("S1", 1);
        assertEquals(RaftEventType.COMMIT, commitEvent.type);

        ElectionEvent electionEvent = new ElectionEvent("S1", "S2", 0);
        assertEquals(RaftEventType.ELECTION, electionEvent.type);

        SnapshotRestoredEvent snapshotRestoredEvent = new SnapshotRestoredEvent("S1", Snapshot.getDefaultInstance());
        assertEquals(RaftEventType.SNAPSHOT_RESTORED, snapshotRestoredEvent.type);

        StatusChangedEvent statusChangedEvent = new StatusChangedEvent("S1", RaftNodeStatus.Leader);
        assertEquals(RaftEventType.STATUS_CHANGED, statusChangedEvent.type);

        SyncStateChangedEvent syncStateChangedEvent = new SyncStateChangedEvent("S1", Collections.emptyMap());
        assertEquals(RaftEventType.SYNC_STATE_CHANGED, syncStateChangedEvent.type);
    }

    @Test
    public void equals() {
        assertEquals(new CommitEvent("S1", 1), new CommitEvent("S1", 1));
        assertEquals(new ElectionEvent("S1", "S2", 0), new ElectionEvent("S1", "S2", 0));
        assertEquals(new SnapshotRestoredEvent("S1", Snapshot.getDefaultInstance()),
            new SnapshotRestoredEvent("S1", Snapshot.getDefaultInstance()));
        assertEquals(new StatusChangedEvent("S1", RaftNodeStatus.Leader),
            new StatusChangedEvent("S1", RaftNodeStatus.Leader));
        assertEquals(new SyncStateChangedEvent("S1", Collections.emptyMap()),
            new SyncStateChangedEvent("S1", Collections.emptyMap()));
    }
}
