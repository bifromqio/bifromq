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

import com.baidu.bifromq.basekv.raft.proto.RaftNodeSyncState;
import com.baidu.bifromq.basekv.raft.proto.Snapshot;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PeerLogReplicatorStateSnapshotSyncingTest {
    PeerLogReplicatorStateSnapshotSyncing stateSnapshotSyncing;
    RaftConfig config = new RaftConfig().setInstallSnapshotTimeoutTick(3);
    Snapshot snapshot = Snapshot.newBuilder()
        .setIndex(10L)
        .build();
    @Mock
    IRaftStateStore stateStorage;
    @Mock
    IRaftNodeLogger logger;

    @Before
    public void setup() {
        when(stateStorage.latestSnapshot()).thenReturn(snapshot);
        stateSnapshotSyncing = new PeerLogReplicatorStateSnapshotSyncing("V1", config, stateStorage, logger);
    }

    @Test
    public void testInitialize() {
        assertFalse(stateSnapshotSyncing.pauseReplicating());
        assertEquals(10, stateSnapshotSyncing.matchIndex());
        assertEquals(11, stateSnapshotSyncing.nextIndex());
        assertEquals(0, stateSnapshotSyncing.catchupRate());
    }

    @Test
    public void testCurrentState() {
        assertEquals(RaftNodeSyncState.SnapshotSyncing, stateSnapshotSyncing.state());
    }

    @Test
    public void testPauseBehavior() {
        stateSnapshotSyncing.replicateTo(10);
        assertTrue(stateSnapshotSyncing.pauseReplicating());
    }

    @Test
    public void testPauseResumedByInstallTimeout() {
        when(stateStorage.lastIndex()).thenReturn(15L);

        stateSnapshotSyncing.replicateTo(10);
        assertTrue(stateSnapshotSyncing.pauseReplicating());
        assertEquals(stateSnapshotSyncing, stateSnapshotSyncing.tick());
        assertFalse(stateSnapshotSyncing.needHeartbeat());
        assertEquals(stateSnapshotSyncing, stateSnapshotSyncing.tick());
        assertTrue(stateSnapshotSyncing.needHeartbeat());
        stateSnapshotSyncing.replicateTo(10);
        PeerLogReplicatorState nextState = stateSnapshotSyncing.tick();
        assertFalse(stateSnapshotSyncing.needHeartbeat());
        assertEquals(RaftNodeSyncState.Probing, nextState.state());
        assertEquals(10, nextState.matchIndex());
        assertEquals(16, nextState.nextIndex());
    }

    @Test
    public void testConfirmTransitToReplicating() {
        stateSnapshotSyncing.replicateTo(10);
        assertTrue(stateSnapshotSyncing.pauseReplicating());
        PeerLogReplicatorState nextState = stateSnapshotSyncing.confirmMatch(10);
        assertEquals(RaftNodeSyncState.Replicating, nextState.state());
        assertEquals(10, nextState.matchIndex());
        assertEquals(11, nextState.nextIndex());
    }

    @Test
    public void testConfirmTransitToSnapshotSyncing() {
        when(stateStorage.latestSnapshot()).thenReturn(Snapshot.newBuilder().setIndex(11L).build());

        stateSnapshotSyncing.replicateTo(10);
        assertTrue(stateSnapshotSyncing.pauseReplicating());
        PeerLogReplicatorState nextState = stateSnapshotSyncing.confirmMatch(10);
        assertEquals(RaftNodeSyncState.SnapshotSyncing, nextState.state());
        assertEquals(11, nextState.matchIndex());
    }

    @Test
    public void testBackoffTransitToSnapshotSyncing() {
        stateSnapshotSyncing.replicateTo(10);
        assertTrue(stateSnapshotSyncing.pauseReplicating());
        PeerLogReplicatorState nextState = stateSnapshotSyncing.backoff(10, 10);
        assertEquals(RaftNodeSyncState.SnapshotSyncing, nextState.state());
        assertEquals(10, nextState.matchIndex());
        assertEquals(11, nextState.nextIndex());
    }

    @Test
    public void testReplicatedToTransitToSnapshotSyncingAgain() {
        Snapshot snapshot = Snapshot.newBuilder()
            .setIndex(20L)
            .build();
        when(stateStorage.latestSnapshot()).thenReturn(snapshot);
        PeerLogReplicatorState nextState = stateSnapshotSyncing.replicateTo(20);
        assertEquals(20, nextState.matchIndex());
        assertEquals(21, nextState.nextIndex());
    }
}
