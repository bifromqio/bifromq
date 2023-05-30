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

package com.baidu.bifromq.basekv.raft;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basekv.raft.proto.LogEntry;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeSyncState;
import com.baidu.bifromq.basekv.raft.proto.Snapshot;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class PeerLogReplicatorStateProbingTest {
    private PeerLogReplicatorStateProbing stateProbing;
    private String peerId = "V1";
    private RaftConfig config = new RaftConfig().setHeartbeatTimeoutTick(5);
    @Mock
    private IRaftStateStore stateStorage;
    @Mock
    private IRaftNodeLogger logger;

    @Test
    public void testInitialize() {
        when(stateStorage.lastIndex()).thenReturn(15L);
        when(stateStorage.latestSnapshot()).thenReturn(Snapshot.newBuilder().setIndex(10L).build());
        stateProbing = new PeerLogReplicatorStateProbing(peerId, config, stateStorage, logger);

        assertEquals(10, stateProbing.matchIndex);
        assertEquals(16, stateProbing.nextIndex);
        assertEquals(stateProbing.state(), RaftNodeSyncState.Probing);
    }

    @Test
    public void testHeartbeat() {
        when(stateStorage.lastIndex()).thenReturn(15L);
        when(stateStorage.latestSnapshot()).thenReturn(Snapshot.newBuilder().setIndex(10L).build());
        stateProbing = new PeerLogReplicatorStateProbing(peerId, config, stateStorage, logger);

        int i = 6;
        while (i-- > 0) {
            assertEquals(stateProbing.tick(), stateProbing);
        }
    }

    @Test
    public void testPauseReplicatingUntilHeartbeatTimeout() {
        when(stateStorage.lastIndex()).thenReturn(0L);
        when(stateStorage.latestSnapshot()).thenReturn(Snapshot.newBuilder().setIndex(0L).build());
        stateProbing = new PeerLogReplicatorStateProbing(peerId, config, stateStorage, logger);

        assertFalse(stateProbing.pauseReplicating());
        stateProbing.replicateTo(1);
        assertEquals(0, stateProbing.catchupRate());
        assertEquals(1, stateProbing.nextIndex);
        assertTrue(stateProbing.pauseReplicating());
        int i = 5;
        while (i-- > 0) {
            stateProbing.tick();
            assertEquals(stateProbing.tick(), stateProbing);
        }
        stateProbing.tick();
        assertFalse(stateProbing.pauseReplicating());
    }

    @Test
    public void testConfirmMatchIgnoreOutdatedIndex() {
        stateProbing = new PeerLogReplicatorStateProbing(peerId, config, stateStorage, 10, 15, logger);

        assertEquals(10, stateProbing.matchIndex());
        assertEquals(15, stateProbing.nextIndex());
        assertEquals(stateProbing, stateProbing.confirmMatch(9));
    }

    @Test
    public void testConfirmMatch() {
        stateProbing = new PeerLogReplicatorStateProbing(peerId, config, stateStorage, 10, 15, logger);
        assertEquals(10, stateProbing.matchIndex);
        assertEquals(15, stateProbing.nextIndex);
        PeerLogReplicatorState nextState = stateProbing.confirmMatch(16);
        assertEquals(0, stateProbing.catchupRate());
        assertEquals(RaftNodeSyncState.Replicating, nextState.state());
        assertEquals(16, nextState.matchIndex());
        assertEquals(17, nextState.nextIndex());
    }

    @Test
    public void testBackoffWithObsoleteIndexWillNotResumeReplicating() {
        when(stateStorage.latestSnapshot()).thenReturn(Snapshot.newBuilder().setIndex(0L).build());
        stateProbing = new PeerLogReplicatorStateProbing(peerId, config, stateStorage, 10, 15, logger);
        stateProbing.replicateTo(20);
        assertTrue(stateProbing.pauseReplicating());
        stateProbing.backoff(30, 10); // not matched
        assertTrue(stateProbing.pauseReplicating());
    }

    @Test
    public void testBackoffWithExpectedIndexWillResumeReplicating() {

        when(stateStorage.entryAt(10)).thenReturn(Optional.of(LogEntry.newBuilder().build()));
        stateProbing = new PeerLogReplicatorStateProbing(peerId, config, stateStorage, 10, 15, logger);

        stateProbing.replicateTo(20);
        assertTrue(stateProbing.pauseReplicating());
        assertEquals(15, stateProbing.nextIndex); //next index won't be update in probing
        PeerLogReplicatorState nextState = stateProbing.backoff(14, 9); // not matched
        assertEquals(stateProbing, nextState);
        assertEquals(9, stateProbing.matchIndex());
        assertEquals(10, stateProbing.nextIndex());
        assertFalse(stateProbing.pauseReplicating());
    }

    @Test
    public void testBackoffWillTransitToSnapshotSyncing() {
        when(stateStorage.entryAt(10)).thenReturn(Optional.ofNullable(null));
        when(stateStorage.latestSnapshot()).thenReturn(Snapshot.newBuilder().setIndex(5L).build());
        stateProbing = new PeerLogReplicatorStateProbing(peerId, config, stateStorage, 10, 15, logger);

        stateProbing.replicateTo(20);
        assertTrue(stateProbing.pauseReplicating());
        PeerLogReplicatorState nextState = stateProbing.backoff(14, 9); // not matched
        assertEquals(RaftNodeSyncState.SnapshotSyncing, nextState.state());
        assertEquals(5, nextState.matchIndex());
        assertEquals(6, nextState.nextIndex());
    }

    @Test
    public void testBackoffNoLess1() {
        when(stateStorage.entryAt(1)).thenReturn(Optional.of(LogEntry.newBuilder().build()));
        stateProbing = new PeerLogReplicatorStateProbing(peerId, config, stateStorage, 1, 2, logger);

        stateProbing.replicateTo(2); // send log entry at index 2
        PeerLogReplicatorState nextState = stateProbing.backoff(1, 1);
        assertEquals(stateProbing, nextState);
        assertEquals(0, nextState.matchIndex());
        assertEquals(1, nextState.nextIndex());
        assertFalse(stateProbing.pauseReplicating());
    }
}
