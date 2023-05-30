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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PeerLogReplicatorStateReplicatingTest {
    PeerLogReplicatorStateReplicating stateReplicating;
    RaftConfig config = new RaftConfig().setElectionTimeoutTick(6).setHeartbeatTimeoutTick(5).setMaxInflightAppends(3);

    @Mock
    IRaftStateStore stateStorage;
    @Mock
    IRaftNodeLogger logger;

    @Before
    public void setup() {
        stateReplicating = new PeerLogReplicatorStateReplicating("V1", config, stateStorage, 1, 2, logger);
    }

    @Test
    public void testInitialize() {
        assertFalse(stateReplicating.pauseReplicating());
        assertEquals(1, stateReplicating.matchIndex());
        assertEquals(2, stateReplicating.nextIndex());
        assertEquals(0, stateReplicating.catchupRate());
    }

    @Test
    public void testCurrentState() {
        assertEquals(RaftNodeSyncState.Replicating, stateReplicating.state());
    }

    @Test
    public void testPauseByMaxInflight() {
        stateReplicating.replicateTo(2);
        assertEquals(1, stateReplicating.matchIndex());
        assertEquals(3, stateReplicating.nextIndex());
        assertFalse(stateReplicating.pauseReplicating());
        stateReplicating.replicateTo(3);
        assertEquals(1, stateReplicating.matchIndex());
        assertEquals(4, stateReplicating.nextIndex());
        assertFalse(stateReplicating.pauseReplicating());
        stateReplicating.replicateTo(4);
        assertEquals(1, stateReplicating.matchIndex());
        assertEquals(5, stateReplicating.nextIndex());
        assertTrue(stateReplicating.pauseReplicating()); // now we have 3 inflight replicating messages

        stateReplicating.confirmMatch(2);
        assertFalse(stateReplicating.pauseReplicating()); // now we have 2 inflight replicating messages
    }

    @Test
    public void testPauseResumedByHeartbeatTimeout() {
        stateReplicating.replicateTo(2);
        stateReplicating.tick();
        assertFalse(stateReplicating.pauseReplicating());

        stateReplicating.replicateTo(3);
        stateReplicating.tick();
        assertFalse(stateReplicating.pauseReplicating());

        stateReplicating.replicateTo(4);
        assertTrue(stateReplicating.pauseReplicating());
        stateReplicating.tick();
        assertTrue(stateReplicating.pauseReplicating());
        // four more ticks to trigger heartbeat timeout
        stateReplicating.tick();
        assertTrue(stateReplicating.pauseReplicating());
        stateReplicating.tick();
        assertTrue(stateReplicating.pauseReplicating());
        stateReplicating.tick();
        assertTrue(stateReplicating.pauseReplicating());
        stateReplicating.tick();
        assertTrue(stateReplicating.pauseReplicating());
        assertFalse(stateReplicating.needHeartbeat());
        stateReplicating.tick();
        assertTrue(stateReplicating.needHeartbeat());
        assertFalse(stateReplicating.pauseReplicating());
    }

    @Test
    public void testConfirm() {
        stateReplicating.replicateTo(2);
        stateReplicating.replicateTo(3);
        stateReplicating.replicateTo(4);

        stateReplicating.confirmMatch(2);
        assertFalse(stateReplicating.pauseReplicating()); // now we have 2 inflight replicating messages
        assertEquals(2, stateReplicating.matchIndex());
        assertEquals(5, stateReplicating.nextIndex());

        stateReplicating.confirmMatch(3);
        assertFalse(stateReplicating.pauseReplicating()); // now we have 1 inflight replicating messages
        assertEquals(3, stateReplicating.matchIndex());
        assertEquals(5, stateReplicating.nextIndex());

        stateReplicating.confirmMatch(4);
        assertFalse(stateReplicating.pauseReplicating()); // now we have 0 inflight replicating messages
        assertEquals(4, stateReplicating.matchIndex());
        assertEquals(5, stateReplicating.nextIndex());
    }

    @Test
    public void testOutOfOrderConfirm() {
        stateReplicating.replicateTo(2);
        stateReplicating.replicateTo(3);
        stateReplicating.replicateTo(4);

        stateReplicating.confirmMatch(4);
        assertFalse(stateReplicating.pauseReplicating()); // now we have 0 inflight replicating messages
        assertEquals(4, stateReplicating.matchIndex());
        assertEquals(5, stateReplicating.nextIndex());

        stateReplicating.confirmMatch(2);
        assertFalse(stateReplicating.pauseReplicating()); // now we have 0 inflight replicating messages
        assertEquals(4, stateReplicating.matchIndex());
        assertEquals(5, stateReplicating.nextIndex());

        stateReplicating.confirmMatch(3);
        assertFalse(stateReplicating.pauseReplicating()); // now we have 0 inflight replicating messages
        assertEquals(4, stateReplicating.matchIndex());
        assertEquals(5, stateReplicating.nextIndex());
    }

    @Test
    public void testCatchupRate() {
        assertEquals(0, stateReplicating.catchupRate());
        stateReplicating.replicateTo(2);
        stateReplicating.tick();
        assertEquals(0, stateReplicating.catchupRate());
        stateReplicating.replicateTo(3);
        stateReplicating.tick();
        assertEquals(0, stateReplicating.catchupRate());
        stateReplicating.replicateTo(4);
        stateReplicating.tick();
        assertEquals(0, stateReplicating.catchupRate());

        stateReplicating.confirmMatch(4);
        assertEquals(0, stateReplicating.catchupRate());
        stateReplicating.tick();
        // catchup rate is calculated in tick
        assertEquals(3, stateReplicating.catchupRate());
        stateReplicating.tick();
        assertEquals(0, stateReplicating.catchupRate());
    }

    @Test
    public void testBackoffIgnoreObsoleteReject() {
        stateReplicating.replicateTo(4);
        stateReplicating.confirmMatch(4);
        PeerLogReplicatorState nextState = stateReplicating.backoff(3, 3);
        assertEquals(stateReplicating, nextState);
    }

    @Test
    public void testBackoffTransitToProbing1() {
        when(stateStorage.entryAt(2)).thenReturn(Optional.ofNullable(LogEntry.newBuilder().build()));

        stateReplicating.replicateTo(4);
        // peerLastIndex is greater than matchIndex
        PeerLogReplicatorState nextState = stateReplicating.backoff(1, 2);
        assertEquals(RaftNodeSyncState.Probing, nextState.state());
        assertEquals(1, nextState.matchIndex());
        assertEquals(2, nextState.nextIndex());
    }

    @Test
    public void testBackoffTransitToProbing2() {
        PeerLogReplicatorState stateReplicating =
            new PeerLogReplicatorStateReplicating("V1", config, stateStorage, 10, 15, logger);

        when(stateStorage.entryAt(8)).thenReturn(Optional.ofNullable(LogEntry.newBuilder().build()));

        stateReplicating.replicateTo(20);
        // peerLastIndex is less than matchIndex
        PeerLogReplicatorState nextState = stateReplicating.backoff(14, 8);
        assertEquals(RaftNodeSyncState.Probing, nextState.state());
        assertEquals(8, nextState.matchIndex());
        assertEquals(9, nextState.nextIndex());
    }

    @Test
    public void testBackoffTransitToSnapshotSyncing() {
        when(stateStorage.entryAt(2)).thenReturn(Optional.ofNullable(null));
        when(stateStorage.latestSnapshot()).thenReturn(Snapshot.newBuilder().setIndex(1L).build());

        stateReplicating.replicateTo(4);
        // peerLastIndex is greater than matchIndex
        PeerLogReplicatorState nextState = stateReplicating.backoff(1, 2);
        assertEquals(RaftNodeSyncState.SnapshotSyncing, nextState.state());
        assertEquals(1, nextState.matchIndex());
        assertEquals(2, nextState.nextIndex());
    }

    @Test
    public void testNoProgressTransitToProbing() {
        // prepare max in-flight appends
        stateReplicating.replicateTo(2);
        stateReplicating.replicateTo(3);
        stateReplicating.replicateTo(4);
        // 5 ticks to trigger heartbeat timeout
        assertEquals(stateReplicating, stateReplicating.tick());
        stateReplicating.replicateTo(5);
        assertEquals(stateReplicating, stateReplicating.tick());
        stateReplicating.replicateTo(6);
        assertEquals(stateReplicating, stateReplicating.tick());
        stateReplicating.replicateTo(7);
        assertEquals(stateReplicating, stateReplicating.tick());
        stateReplicating.replicateTo(7);
        assertEquals(stateReplicating, stateReplicating.tick());
        stateReplicating.replicateTo(7);

        // election timeout
        assertEquals(RaftNodeSyncState.Probing, stateReplicating.tick().state());
    }
}
