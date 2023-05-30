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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.baidu.bifromq.basekv.raft.event.SyncStateChangedEvent;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeSyncState;
import com.baidu.bifromq.basekv.raft.proto.Snapshot;
import java.util.HashMap;
import java.util.HashSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PeerLogTrackerTest {
    PeerLogTracker logTracker;
    RaftConfig config;
    IRaftStateStore stateStorage;
    @Mock
    PeerLogReplicatorStateProbing stateProbing;
    @Mock
    IRaftNode.IRaftEventListener statusListener;
    @Mock
    IRaftNodeLogger logger;

    @Before
    public void setup() {
        config = new RaftConfig();
        stateStorage = new InMemoryStateStore("V1", Snapshot.newBuilder()
            .setIndex(0)
            .setTerm(0)
            .setClusterConfig(ClusterConfig.newBuilder().addVoters("V1").addVoters("V2").addVoters("V3").build())
            .build());
        logTracker = new PeerLogTracker(stateStorage.local(), config, stateStorage, statusListener, logger);
    }

    @Test
    public void testCurrentStates() {
        logTracker.startTracking(new HashSet<>() {{
            add("V1");
        }}, true);

        ArgumentCaptor<SyncStateChangedEvent> captured = ArgumentCaptor.forClass(SyncStateChangedEvent.class);
        verify(statusListener).onEvent(captured.capture());
        assertEquals(new HashMap<String, RaftNodeSyncState>() {{
            put("V1", RaftNodeSyncState.Probing);
        }}, captured.getValue().states);

        logTracker.startTracking(new HashSet<>() {{
            add("V2");
        }}, true);

        captured = ArgumentCaptor.forClass(SyncStateChangedEvent.class);
        verify(statusListener, times(2)).onEvent(captured.capture());

        assertEquals(new HashMap<String, RaftNodeSyncState>() {{
            put("V1", RaftNodeSyncState.Probing);
            put("V2", RaftNodeSyncState.Probing);
        }}, captured.getValue().states);
    }

    @Test
    public void testStartTrackAndStopTrack() {
        assertFalse(logTracker.isTracking("Abc"));
        logTracker.startTracking(new HashSet<String>() {{
            add("Abc");
        }}, true);
        assertTrue(logTracker.isTracking("Abc"));
        assertEquals(RaftNodeSyncState.Probing, logTracker.status("Abc"));
    }

    @Test
    public void testStartTrackWithoutNotify() {
        assertFalse(logTracker.isTracking("Abc"));
        logTracker.startTracking(new HashSet<>() {{
            add("Abc");
        }}, false);

        verify(statusListener, times(0)).onEvent(any());
    }

    @Test
    public void testStopTrackPredicateAndNotify() {
        logTracker.startTracking(new HashSet<>() {{
            add("A1");
            add("A2");
            add("B1");
        }}, true);

        ArgumentCaptor<SyncStateChangedEvent> captured = ArgumentCaptor.forClass(SyncStateChangedEvent.class);
        verify(statusListener).onEvent(captured.capture());
        assertEquals(new HashMap<String, RaftNodeSyncState>() {{
            put("A1", RaftNodeSyncState.Probing);
            put("A2", RaftNodeSyncState.Probing);
            put("B1", RaftNodeSyncState.Probing);
        }}, captured.getValue().states);

        logTracker.stopTracking(key -> key.startsWith("A"), true);

        captured = ArgumentCaptor.forClass(SyncStateChangedEvent.class);
        verify(statusListener, times(2)).onEvent(captured.capture());
        assertEquals(new HashMap<String, RaftNodeSyncState>() {{
            put("B1", RaftNodeSyncState.Probing);
        }}, captured.getValue().states);

        assertFalse(logTracker.isTracking("A1"));
        assertFalse(logTracker.isTracking("A2"));
        assertTrue(logTracker.isTracking("B1"));
    }

    @Test
    public void testStopTrackPredicateWithoutNotify() {
        logTracker.startTracking(new HashSet<>() {{
            add("A1");
            add("A2");
            add("B1");
        }}, true);

        ArgumentCaptor<SyncStateChangedEvent> captured = ArgumentCaptor.forClass(SyncStateChangedEvent.class);
        verify(statusListener).onEvent(captured.capture());
        assertEquals(new HashMap<String, RaftNodeSyncState>() {{
            put("A1", RaftNodeSyncState.Probing);
            put("A2", RaftNodeSyncState.Probing);
            put("B1", RaftNodeSyncState.Probing);
        }}, captured.getValue().states);
        logTracker.stopTracking(key -> key.startsWith("A"), false);
        verify(statusListener, times(1)).onEvent(any());

        assertFalse(logTracker.isTracking("A1"));
        assertFalse(logTracker.isTracking("A2"));
        assertTrue(logTracker.isTracking("B1"));
    }

    // TODO: cover replication status notification situations
}
