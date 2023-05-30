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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.raft.proto.LogEntry;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeSyncState;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RaftConfigChangerTest {
    RaftConfigChanger configChanger;
    RaftConfig config = new RaftConfig().setInstallSnapshotTimeoutTick(3).setElectionTimeoutTick(1);
    @Mock
    IRaftStateStore stateStorage;
    @Mock
    PeerLogTracker peerLogTracker;
    @Mock
    IRaftNodeLogger logger;

    @Mock
    IPeerLogReplicator logReplicator;


    @Before
    public void setup() {
        configChanger = new RaftConfigChanger(config, stateStorage, peerLogTracker, logger);
    }

    @Test
    public void testStateAfterInit() {
        Assert.assertEquals(RaftConfigChanger.State.Waiting, configChanger.state());
    }

    @Test
    public void testSubmit() {
        when(stateStorage.latestClusterConfig()).thenReturn(ClusterConfig.newBuilder()
            .addVoters("V1")
            .addVoters("V2")
            .addVoters("V3")
            .build());
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        assertTrue(configChanger.remotePeers().containsAll(Arrays.asList("V1", "V2", "V3")));
        configChanger.submit("cId", new HashSet<>() {{
            add("N1");
            add("N2");
            add("N3");
        }}, new HashSet<>() {{
            add("L1");
        }}, onDone);

        verify(peerLogTracker).startTracking(new HashSet<>() {{
            add("N1");
            add("N2");
            add("N3");
            add("L1");
        }}, true);
        Assert.assertEquals(RaftConfigChanger.State.CatchingUp, configChanger.state());
        assertTrue(configChanger.remotePeers().containsAll(Arrays.asList("V1", "V2", "V3", "N1", "N2", "N3", "L1")));
        assertTrue(!onDone.isDone());
    }

    @Test
    public void testSubmitExceptionally() {
        when(stateStorage.latestClusterConfig()).thenReturn(ClusterConfig.newBuilder()
            .addVoters("V1")
            .addVoters("V2")
            .addVoters("V3")
            .build());

        CompletableFuture<Void> onDone = new CompletableFuture<>();
        configChanger.submit("cId", new HashSet<>() {{
            add("N1");
            add("N2");
            add("N3");
        }}, new HashSet<>() {{
            add("L1");
        }}, onDone);
        verify(peerLogTracker).startTracking(new HashSet<>() {{
            add("N1");
            add("N2");
            add("N3");
            add("L1");
        }}, true);

        CompletableFuture<Void> nextDone = new CompletableFuture<>();
        configChanger.submit("cId", new HashSet<String>() {{
            add("N1");
            add("N2");
            add("N3");
        }}, new HashSet<>() {{
            add("L1");
        }}, nextDone);
        assertTrue(nextDone.isDone() && nextDone.isCompletedExceptionally());

        nextDone = new CompletableFuture<>();
        configChanger.submit("cId", new HashSet<>(), new HashSet<>(), nextDone);
        assertTrue(nextDone.isDone() && nextDone.isCompletedExceptionally());

        nextDone = new CompletableFuture<>();
        configChanger.submit("cId", new HashSet<String>() {{
            add("N1");
        }}, new HashSet<String>() {{
            add("N1");
        }}, nextDone);
        assertTrue(nextDone.isDone() && nextDone.isCompletedExceptionally());
    }

    @Test
    public void testCatchupTimeout() {
        when(stateStorage.latestClusterConfig())
            .thenReturn(ClusterConfig.newBuilder()
                .addVoters("V1")
                .addVoters("V2")
                .addVoters("V3")
                .addLearners("L1")
                .build());
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        configChanger.submit("cId", new HashSet<>() {{
            add("V1");
            add("N2");
            add("N3");
        }}, new HashSet<>() {{
            add("L1");
            add("L2");
        }}, onDone);

        verify(peerLogTracker).startTracking(new HashSet<>() {{
            add("V1");
            add("N2");
            add("N3");
            add("L1");
            add("L2");
        }}, true);

        Assert.assertEquals(RaftConfigChanger.State.CatchingUp, configChanger.state());

        assertTrue(configChanger.remotePeers().containsAll(Arrays.asList("V1", "V2", "V3", "N2", "N3", "L1", "L2")));

        for (int i = 0; i < 12; i++) {
            assertFalse(configChanger.tick(1));
        }
        assertTrue(configChanger.tick(1));

        verify(peerLogTracker).stopTracking(new HashSet<>() {{
            add("N2");
            add("N3");
            add("L2");
        }});

        Assert.assertEquals(RaftConfigChanger.State.Waiting, configChanger.state());

        assertTrue(configChanger.remotePeers().containsAll(Arrays.asList("V1", "V2", "V3", "L1")));
        assertTrue(onDone.isDone() && onDone.isCompletedExceptionally());
    }

    @Test
    public void testDirectlyTransitToTargetConfigCommitting() {
        when(stateStorage.latestClusterConfig()).thenReturn(ClusterConfig.newBuilder()
            .addVoters("V1")
            .addVoters("V2")
            .addVoters("V3")
            .addLearners("L1")
            .build());

        CompletableFuture<Void> onDone = new CompletableFuture<>();
        configChanger.submit("cId", new HashSet<>() {{
            add("V1");
            add("V2");
            add("V3");
        }}, new HashSet<>() {{
            add("L1");
        }}, onDone);
        verify(peerLogTracker).startTracking(new HashSet<>() {{
            add("V1");
            add("V2");
            add("V3");
            add("L1");
        }}, true);

        Assert.assertEquals(RaftConfigChanger.State.CatchingUp, configChanger.state());

        when(peerLogTracker.status(anyString())).thenReturn(RaftNodeSyncState.Replicating);
        when(peerLogTracker.matchIndex(anyString())).thenReturn(10L);

        when(stateStorage.lastIndex()).thenReturn(10L, 11L);
        when(stateStorage.local()).thenReturn("localId");
        assertTrue(configChanger.tick(1));
        Assert.assertEquals(RaftConfigChanger.State.TargetConfigCommitting, configChanger.state());
        assertTrue(configChanger.remotePeers().containsAll(Arrays.asList("V1", "V2", "V3", "L1")));
        assertTrue(!onDone.isDone());

        verify(peerLogTracker).replicateBy("localId", 11);

        ArgumentCaptor<List<LogEntry>> entries = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<Boolean> flush = ArgumentCaptor.forClass(Boolean.class);

        verify(stateStorage).append(entries.capture(), flush.capture());
        assertTrue(flush.getValue());
        assertEquals(1, entries.getValue().size());
        LogEntry logEntry = entries.getValue().get(0);
        assertEquals(1, logEntry.getTerm());
        assertEquals(12, logEntry.getIndex());
        assertTrue(logEntry.hasConfig());
        ClusterConfig targetConfig = logEntry.getConfig();
        assertEquals(new HashSet<String>() {{
            add("V1");
            add("V2");
            add("V3");
        }}, new HashSet(targetConfig.getVotersList()));
        assertEquals(new HashSet<String>() {{
            add("L1");
        }}, new HashSet(targetConfig.getLearnersList()));
        assertTrue(targetConfig.getNextVotersList().isEmpty());
        assertTrue(targetConfig.getNextLearnersList().isEmpty());

        // later ticks won't change state
        assertFalse(configChanger.tick(1));
    }

    @Test
    public void testTransitToJointConfigCommitting() {
        when(stateStorage.latestClusterConfig()).thenReturn(ClusterConfig.newBuilder()
            .addVoters("V1")
            .addVoters("V2")
            .addVoters("V3")
            .addLearners("L1")
            .build());
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        configChanger.submit("cId", new HashSet<>() {{
            add("V1");
            add("N2");
            add("N3");
        }}, new HashSet<>() {{
            add("L1");
            add("L2");
        }}, onDone);
        Assert.assertEquals(RaftConfigChanger.State.CatchingUp, configChanger.state());
        verify(peerLogTracker).startTracking(new HashSet<>() {{
            add("V1");
            add("N2");
            add("N3");
            add("L1");
            add("L2");
        }}, true);
        when(peerLogTracker.status(anyString())).thenReturn(RaftNodeSyncState.Replicating);
        when(peerLogTracker.matchIndex(anyString())).thenReturn(10L);

        when(stateStorage.lastIndex()).thenReturn(10L, 11L);
        when(stateStorage.local()).thenReturn("localId");

        assertTrue(configChanger.tick(1));
        Assert.assertEquals(RaftConfigChanger.State.JointConfigCommitting, configChanger.state());
        assertTrue(configChanger.remotePeers().containsAll(Arrays.asList("V1", "V2", "V3", "N2", "N3", "L1", "L2")));
        assertTrue(!onDone.isDone());

        verify(peerLogTracker).replicateBy("localId", 11);

        ArgumentCaptor<List<LogEntry>> entries = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<Boolean> flush = ArgumentCaptor.forClass(Boolean.class);
        verify(stateStorage).append(entries.capture(), flush.capture());
        assertTrue(flush.getValue());
        assertEquals(1, entries.getValue().size());
        LogEntry logEntry = entries.getValue().get(0);
        assertEquals(1, logEntry.getTerm());
        assertEquals(12, logEntry.getIndex());
        assertTrue(logEntry.hasConfig());
        ClusterConfig jointConfig = logEntry.getConfig();
        assertEquals(new HashSet<String>() {{
            add("V1");
            add("N2");
            add("N3");
        }}, new HashSet(jointConfig.getNextVotersList()));
        assertEquals(new HashSet<String>() {{
            add("L1");
            add("L2");
        }}, new HashSet(jointConfig.getNextLearnersList()));

        // later ticks won't change state
        assertFalse(configChanger.tick(1));
    }

    @Test
    public void testCommitToInWaitingState() {
        // never change state if it's waiting
        Assert.assertEquals(RaftConfigChanger.State.Waiting, configChanger.state());
        assertFalse(configChanger.commitTo(100, 100));
    }

    @Test
    public void testCommitToInCatchingUpState() {
        when(stateStorage.latestClusterConfig()).thenReturn(ClusterConfig.newBuilder()
            .addVoters("V1")
            .addVoters("V2")
            .addVoters("V3")
            .build());
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        configChanger.submit("cId", new HashSet<>() {{
            add("N1");
            add("N2");
            add("N3");
        }}, new HashSet<>() {{
            add("L1");
        }}, onDone);

        verify(peerLogTracker).startTracking(new HashSet<>() {{
            add("N1");
            add("N2");
            add("N3");
            add("L1");
        }}, true);
        Assert.assertEquals(RaftConfigChanger.State.CatchingUp, configChanger.state());
        assertFalse(configChanger.commitTo(100, 100));
    }

    @Test
    public void testAbort() {
        when(stateStorage.latestClusterConfig()).thenReturn(ClusterConfig.newBuilder()
            .addVoters("V1")
            .addVoters("V2")
            .addVoters("V3")
            .build());
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        configChanger.submit("cId", new HashSet<>() {{
            add("N1");
            add("N2");
            add("N3");
        }}, new HashSet<>() {{
            add("L1");
        }}, onDone);
        assertFalse(onDone.isDone());
        configChanger.abort();

        verify(peerLogTracker).startTracking(new HashSet<>() {{
            add("N1");
            add("N2");
            add("N3");
            add("L1");
        }}, true);
        assertTrue(onDone.isCompletedExceptionally());
        assertTrue(configChanger.remotePeers().isEmpty());
    }

    @Test
    public void testAllCatchUpWhenProbingAnyPeer() {
        when(peerLogTracker.status(anyString())).thenReturn(RaftNodeSyncState.Probing);

        assertFalse(configChanger.quorumCatchUp(new HashSet<>() {{
            add("V1");
            add("V2");
            add("V3");
        }}));
    }

    @Test
    public void testQuorumCatchUp() {
        when(stateStorage.lastIndex()).thenReturn(10L);

        when(peerLogTracker.matchIndex(anyString())).thenReturn(10L);
        when(peerLogTracker.status(anyString())).thenReturn(RaftNodeSyncState.Replicating);
        assertTrue(configChanger.quorumCatchUp(new HashSet<String>() {{
            add("V1");
        }}));

        when(stateStorage.lastIndex()).thenReturn(10L);
        when(peerLogTracker.matchIndex(anyString())).thenReturn(10L, 10L);
        when(peerLogTracker.status(anyString())).thenReturn(RaftNodeSyncState.Replicating,
            RaftNodeSyncState.Probing);

        assertFalse(configChanger.quorumCatchUp(new HashSet<String>() {{
            add("V1");
            add("V2");
        }}));


        when(stateStorage.lastIndex()).thenReturn(10L);
        when(peerLogTracker.matchIndex(anyString())).thenReturn(10L, 10L, 10L);
        when(peerLogTracker.status(anyString())).thenReturn(RaftNodeSyncState.Replicating);

        assertTrue(configChanger.quorumCatchUp(new HashSet<String>() {{
            add("V1");
            add("V2");
            add("V3");
        }}));

        when(stateStorage.lastIndex()).thenReturn(10L);
        when(peerLogTracker.matchIndex("V1")).thenReturn(15L);
        when(peerLogTracker.matchIndex("V2")).thenReturn(5L);
        when(peerLogTracker.matchIndex("V3")).thenReturn(10L);
        when(peerLogTracker.catchupRate("V2")).thenReturn(0L);
        when(peerLogTracker.status(anyString())).thenReturn(RaftNodeSyncState.Replicating);
        assertFalse(configChanger.quorumCatchUp(new HashSet<String>() {{
            add("V1");
            add("V2");
            add("V3");
        }}));

        when(stateStorage.lastIndex()).thenReturn(10L);
        when(peerLogTracker.matchIndex("V1")).thenReturn(10L);
        when(peerLogTracker.matchIndex("V2")).thenReturn(5L);
        when(peerLogTracker.matchIndex("V3")).thenReturn(10L);
        when(peerLogTracker.catchupRate("V2")).thenReturn(2L);
        when(peerLogTracker.status(anyString())).thenReturn(RaftNodeSyncState.Replicating);

        assertTrue(configChanger.quorumCatchUp(new HashSet<>() {{
            add("V1");
            add("V2");
            add("V3");
        }}));

        when(stateStorage.lastIndex()).thenReturn(10L);
        when(peerLogTracker.matchIndex("V1")).thenReturn(10L);
        when(peerLogTracker.matchIndex("V2")).thenReturn(5L);
        when(peerLogTracker.matchIndex("V3")).thenReturn(10L);
        when(peerLogTracker.catchupRate("V2")).thenReturn(5L);
        when(peerLogTracker.status("V1")).thenReturn(RaftNodeSyncState.Replicating);
        when(peerLogTracker.status("V2")).thenReturn(RaftNodeSyncState.Replicating);
        when(peerLogTracker.status("V3")).thenReturn(RaftNodeSyncState.Replicating);
        assertTrue(configChanger.quorumCatchUp(new HashSet<>() {{
            add("V1");
            add("V2");
            add("V3");
        }}));
    }
}
