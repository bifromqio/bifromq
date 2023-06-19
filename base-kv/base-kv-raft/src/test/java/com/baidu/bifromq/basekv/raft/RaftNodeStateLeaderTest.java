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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertSame;

import com.baidu.bifromq.basekv.raft.event.CommitEvent;
import com.baidu.bifromq.basekv.raft.event.ElectionEvent;
import com.baidu.bifromq.basekv.raft.event.RaftEventType;
import com.baidu.bifromq.basekv.raft.proto.AppendEntries;
import com.baidu.bifromq.basekv.raft.proto.AppendEntriesReply;
import com.baidu.bifromq.basekv.raft.proto.InstallSnapshot;
import com.baidu.bifromq.basekv.raft.proto.InstallSnapshotReply;
import com.baidu.bifromq.basekv.raft.proto.LogEntry;
import com.baidu.bifromq.basekv.raft.proto.RaftMessage;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeSyncState;
import com.baidu.bifromq.basekv.raft.proto.RequestVote;
import com.baidu.bifromq.basekv.raft.proto.Snapshot;
import com.baidu.bifromq.basekv.raft.proto.TimeoutNow;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftNodeStateLeaderTest extends RaftNodeStateTest {
    private final Logger log = LoggerFactory.getLogger("RaftNodeStateLeaderTest");
    private AutoCloseable closeable;
    @BeforeMethod
    public void openMocks() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterMethod
    public void releaseMocks() throws Exception {
        closeable.close();
    }
    @Test
    public void testProposeWithSlowFollower() {
        RaftNodeStateLeader leader = startUpLeader();
        leader.propose(ByteString.copyFromUtf8("command"), new CompletableFuture<>());
        // mock that only v1 and l1 received the command entry, however, the majority has been achieved
        RaftMessage appendEntriesReply = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                .setAccept(AppendEntriesReply.Accept.newBuilder()
                    .setLastIndex(2)
                    .build())
                .build())
            .build();
        leader.receive("l1", appendEntriesReply);

        // LogEntries now:
        //   index  entry
        //   1      targetConfigEntry
        //   2      commandEntry
        // commitIndex = 2, compact all entries
        leader.compact(ByteString.copyFromUtf8("appSMSnapshot"), 2, new CompletableFuture<>());

        // tick to heartbeat, send empty appendEntry to v1 and v2
        leader.tick();
        leader.tick();
        leader.tick();

        // v1 respond to heartbeat normally
        appendEntriesReply = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                .setAccept(AppendEntriesReply.Accept.newBuilder()
                    .setLastIndex(2)
                    .build())
                .build())
            .build();
        leader.receive("v1", appendEntriesReply);
        PeerLogTracker peerLogTracker = ReflectionUtils.getField(leader, "peerLogTracker");
        assert peerLogTracker != null;
        assertEquals(2, peerLogTracker.matchIndex("v1"));
        // v2 will reject last empty appendEntry since preLogIndex is mismatched, which causes the state of the log
        // tracker changed to SnapshotSyncing
        RaftMessage appendEntriesReplyRejected = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                .setReject(AppendEntriesReply.Reject.newBuilder()
                    .setTerm(1)
                    .setRejectedIndex(2)
                    .setLastIndex(1)
                    .build())
                .setReadIndex(0)
                .build())
            .build();
        leader.receive("v2", appendEntriesReplyRejected);
        assertSame(RaftNodeSyncState.SnapshotSyncing, peerLogTracker.status("v2"));
        // tick to heartbeat, send empty appendEntry to v1 and send snapshot to v2
        leader.tick();
        leader.tick();
        leader.tick();


        // v2 responds to installSnapshot normally, and state of the log tracker change to Probing
        RaftMessage installSnapshotReply = RaftMessage.newBuilder()
            .setTerm(1)
            .setInstallSnapshotReply(InstallSnapshotReply.newBuilder()
                .setRejected(false)
                .setLastIndex(2)
                .build())
            .build();
        leader.receive("v2", installSnapshotReply);
        assertSame(RaftNodeSyncState.Replicating, peerLogTracker.status("v2"));
    }

    @Test
    public void testProposeExceptionally() {
        RaftNodeStateLeader proposeThrottledLeader = startUpLeader();
        ByteString cmd = ByteString.copyFromUtf8("command");
        proposeThrottledLeader.propose(cmd, new CompletableFuture<>());
        proposeThrottledLeader.propose(cmd, new CompletableFuture<>());
        proposeThrottledLeader.propose(cmd, new CompletableFuture<>());
        proposeThrottledLeader.propose(cmd, new CompletableFuture<>());
        proposeThrottledLeader.propose(cmd, new CompletableFuture<>());

        // commit all proposed entry
//        RaftMessage appendEntriesReply = RaftMessage.newBuilder()
//                .setTerm(1)
//                .setAppendEntriesReply(AppendEntriesReply.newBuilder()
//                        .setReject(false)
//                        .setLastIndex(6)
//                        .build())
//                .build();
//        proposeThrottledLeader.receive("v1", appendEntriesReply);
//        proposeThrottledLeader.receive("v2", appendEntriesReply);

        // proposeThrottled
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        proposeThrottledLeader.propose(cmd, onDone);
        assertTrue(onDone.isCompletedExceptionally());


        RaftNodeStateLeader transferringLeader = startUpLeader();
        transferringLeader.transferLeadership("v1", onDone);
        onDone = new CompletableFuture<>();
        transferringLeader.propose(cmd, onDone);
        assertTrue(onDone.isCompletedExceptionally());
    }

    @Test
    public void testReceiveAppendEntriesReplyRejected() {
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateLeader leader = startUpLeader(stateStorage, messages ->
            assertEquals(new HashMap<String, List<RaftMessage>>() {{
                put("v1", Collections.singletonList(RaftMessage.newBuilder()
                    .setTerm(1)
                    .setInstallSnapshot(InstallSnapshot.newBuilder()
                        .setLeaderId(local)
                        .setSnapshot(stateStorage.latestSnapshot())
                        .build())
                    .build()
                ));
            }}, messages));

        RaftMessage appendEntriesReplyRejectedMessage = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                .setReject(AppendEntriesReply.Reject.newBuilder()
                    .setTerm(1)
                    .setRejectedIndex(2)
                    .setLastIndex(1) // <-- switch to snapshot syncing
                    .build())
                .build())
            .build();
        leader.receive("v1", appendEntriesReplyRejectedMessage);
    }

    @Test
    public void testElectionElapsed() {
        // use defaultRaftConfig in RaftNodeStateTest, heartbeatTimeoutTick is 5
        RaftNodeStateLeader leader = startUpLeader();

        PeerLogTracker peerLogTracker = ReflectionUtils.getField(leader, "peerLogTracker");
        assert peerLogTracker != null;

        leader.transferLeadership("v1", new CompletableFuture<>());

        leader.tick();
        leader.tick();
        leader.tick();
        leader.tick();

        // trigger activityTracker to poll
        leader.receive("v2", RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                .setAccept(AppendEntriesReply.Accept.newBuilder()
                    .setLastIndex(2)
                    .build())
                .build())
            .build());

        // electionElapsedTick timeout
        RaftNodeState raftNodeState = leader.tick();

        assertSame(RaftNodeStatus.Leader, raftNodeState.getState());
        // transfer leadership timeout

        leader.tick();
        leader.tick();
        leader.tick();
        leader.tick();
        raftNodeState = leader.tick();

        // activityTracker will tally failed
        assertSame(RaftNodeStatus.Follower, raftNodeState.getState());
    }

    @Test
    public void testTransferLeadershipToIndexMatchedFollower() {
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateLeader leader = startUpLeader(stateStorage, messages ->
            assertEquals(new HashMap<String, List<RaftMessage>>() {{
                put("v1", Collections.singletonList(RaftMessage.newBuilder()
                    .setTerm(1)
                    .setTimeoutNow(TimeoutNow.newBuilder().build())
                    .build()));
            }}, messages));

        leader.transferLeadership("v1", new CompletableFuture<>());
    }

    @Test
    public void testTransferLeadershipToIndexBehindFollower() {
        AtomicInteger onMessageReadyIndex = new AtomicInteger();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateLeader leader = startUpLeader(stateStorage, messages -> {
            if (onMessageReadyIndex.get() == 0) {
                onMessageReadyIndex.incrementAndGet();
                // entry for proposed command
            } else if (onMessageReadyIndex.get() == 1) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(new HashMap<String, List<RaftMessage>>() {{
                    put("v1", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(1)
                        .setAppendEntries(AppendEntries.newBuilder()
                            .setLeaderId(local)
                            .setPrevLogTerm(1)
                            .setPrevLogIndex(2)
                            .setCommitIndex(1)
                            .setReadIndex(0)
                            .build())
                        .build()));
                }}, messages);
            } else if (onMessageReadyIndex.get() == 2) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(new HashMap<String, List<RaftMessage>>() {{
                    put("v1", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(1)
                        .setTimeoutNow(TimeoutNow.newBuilder().build())
                        .build()));
                }}, messages);
            }
        });

        leader.propose(ByteString.copyFromUtf8("command"), new CompletableFuture<>());

        leader.transferLeadership("v1", new CompletableFuture<>());

        RaftMessage appendEntriesReply = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                .setAccept(AppendEntriesReply.Accept.newBuilder()
                    .setLastIndex(3)
                    .build())
                .build())
            .build();
        leader.receive("v1", appendEntriesReply);
    }

    @Test
    public void testTransferLeadershipExceptionally() {
        RaftNodeStateLeader leader = startUpLeader();
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        leader.transferLeadership("v1", onDone);
        assertFalse(onDone.isDone());

        onDone = new CompletableFuture<>();
        leader.transferLeadership("v1", onDone);
        assertTrue(onDone.isCompletedExceptionally());

        onDone = new CompletableFuture<>();
        leader.transferLeadership("v2", onDone);
        assertTrue(onDone.isCompletedExceptionally());

        onDone = new CompletableFuture<>();
        leader.transferLeadership(local, onDone);
        assertTrue(onDone.isCompletedExceptionally());

        onDone = new CompletableFuture<>();
        leader.transferLeadership("notInClusterConfig", onDone);
        assertTrue(onDone.isCompletedExceptionally());
    }

    @Test
    public void testChangeClusterConfig() {
        RaftNodeStateLeader leader = startUpLeader();
        // nextVoters doesn't include local and v1
        Set<String> nextVoters = new HashSet<>() {{
            add("v2");
            add("v3");
        }};
        Set<String> nextLearners = new HashSet<>() {{
            add("l2");
        }};
        CompletableFuture<Void> onDone = new CompletableFuture<>();

        leader.changeClusterConfig("cId", nextVoters, nextLearners, onDone);

        RaftConfigChanger raftConfigChanger = ReflectionUtils.getField(leader, "configChanger");
        assert raftConfigChanger != null;
        assertEquals(RaftConfigChanger.State.CatchingUp, raftConfigChanger.state());

        // broadcast appendEntries and wait all nextVoters catchup
        leader.tick();
        leader.tick();
        leader.receive("v3", RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                .setAccept(AppendEntriesReply.Accept.newBuilder()
                    .setLastIndex(1)
                    .build())
                .build())
            .build());
        leader.tick();
        assertEquals(RaftConfigChanger.State.JointConfigCommitting, raftConfigChanger.state());

        leader.transferLeadership("v1", new CompletableFuture<>());

        leader.stableTo(2);
        RaftMessage appendEntriesReply = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                .setAccept(AppendEntriesReply.Accept.newBuilder()
                    .setLastIndex(2)
                    .build())
                .build())
            .build();
        leader.receive("v2", appendEntriesReply);
        leader.receive("v3", appendEntriesReply);
        // broadcast appendEntries(including targetConfigEntry)
        leader.tick();
        leader.tick();
        leader.tick();
        assertEquals(RaftConfigChanger.State.TargetConfigCommitting, raftConfigChanger.state());

        leader.stableTo(3);

        // broadcast appendEntries(including targetConfigEntry)
        leader.tick();

        appendEntriesReply = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                .setAccept(AppendEntriesReply.Accept.newBuilder()
                    .setLastIndex(3)
                    .build())
                .build())
            .build();
        leader.receive("v2", appendEntriesReply);
        RaftNodeState raftNodeState = leader.receive("v3", appendEntriesReply);
        assertSame(RaftConfigChanger.State.Waiting, raftConfigChanger.state());
        assertSame(RaftNodeStatus.Follower, raftNodeState.getState());
    }

    @Test
    public void testReceiveHigherTermMessage() {
        RaftNodeStateLeader leader = startUpLeader();
        RaftMessage vote = RaftMessage.newBuilder()
            .setTerm(2)
            .setRequestVote(RequestVote.newBuilder()
                .setLastLogTerm(1)
                .setLastLogIndex(2)
                .build())
            .build();
        RaftNodeState raftNodeState = leader.receive("v3", vote); // v3 is not a member
        assertSame(RaftNodeStatus.Leader, raftNodeState.getState());

        leader = startUpLeader();
        vote = RaftMessage.newBuilder()
            .setTerm(2)
            .setRequestVote(RequestVote.newBuilder()
                .setLastLogTerm(1)
                .setLastLogIndex(2)
                .build())
            .build();
        raftNodeState = leader.receive("v2", vote); // v2 is a member
        assertSame(RaftNodeStatus.Follower, raftNodeState.getState());

        leader = startUpLeader(event -> {
            if (event.type == RaftEventType.ELECTION) {
                ElectionEvent electionEvent = ((ElectionEvent) event);
                assertEquals("v1", electionEvent.leaderId);
                assertEquals(2, electionEvent.term);
            }
        });
        RaftMessage appendEntries = RaftMessage.newBuilder()
            .setTerm(2)
            .setAppendEntries(AppendEntries.newBuilder()
                .setLeaderId("v1")
                .build())
            .build();
        raftNodeState = leader.receive("v1", appendEntries);
        assertSame(RaftNodeStatus.Follower, raftNodeState.getState());
    }

    private RaftNodeStateLeader startUpLeader() {
        return startUpLeader(null, null, null);
    }

    private RaftNodeStateLeader startUpLeader(IRaftNode.IRaftEventListener listener) {
        return startUpLeader(null, null, listener);
    }

    private RaftNodeStateLeader startUpLeader(IRaftStateStore stateStorage,
                                              IRaftNode.IRaftMessageSender raftMessageListener) {
        return startUpLeader(stateStorage, raftMessageListener, null);
    }

    // after startUp with this method, commitIndex = 2, layout of LogEntries:
    //   index  entry
    //   1      jointConfigEntry
    //   2      targetConfigEntry
    private RaftNodeStateLeader startUpLeader(IRaftStateStore stateStorage,
                                              IRaftNode.IRaftMessageSender msgSender,
                                              IRaftNode.IRaftEventListener evtListener) {
        if (stateStorage == null) {
            stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
                .setClusterConfig(clusterConfig).build());

        }
        AtomicInteger onMessageReadyIndex = new AtomicInteger();
        AtomicInteger onCommitIndex = new AtomicInteger();
        IRaftStateStore finalStateStorage = stateStorage;
        RaftNodeStateLeader leader = new RaftNodeStateLeader(1, 0, defaultRaftConfig, stateStorage,
            log, new LinkedHashMap<>(), messages -> {
            if (onMessageReadyIndex.get() == 0) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(new HashMap<String, List<RaftMessage>>() {{
                    put("l1", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(1)
                        .setAppendEntries(AppendEntries.newBuilder()
                            .setLeaderId(local)
                            .setPrevLogIndex(0)
                            .setPrevLogTerm(0)
                            .setCommitIndex(0)
                            .setReadIndex(0)
                            .build())
                        .build()));
                    put("v1", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(1)
                        .setAppendEntries(AppendEntries.newBuilder()
                            .setLeaderId(local)
                            .setPrevLogIndex(0)
                            .setPrevLogTerm(0)
                            .setCommitIndex(0)
                            .setReadIndex(0)
                            .addAllEntries(toList(finalStateStorage.entries(1, 2,
                                defaultRaftConfig.getMaxSizePerAppend())))
                            .build())
                        .build()));
                    put("v2", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(1)
                        .setAppendEntries(AppendEntries.newBuilder()
                            .setLeaderId(local)
                            .setPrevLogIndex(0)
                            .setPrevLogTerm(0)
                            .setCommitIndex(0)
                            .setReadIndex(0)
                            .addAllEntries(toList(finalStateStorage.entries(1, 2,
                                defaultRaftConfig.getMaxSizePerAppend())))
                            .build())
                        .build()));
                }}, messages);
            } else if (onMessageReadyIndex.get() > 1) {
                if (msgSender != null) {
                    msgSender.send(messages);
                }
            }
        }, event -> {
            switch (event.type) {
                case COMMIT:
                    if (onCommitIndex.get() == 0) {
                        onCommitIndex.incrementAndGet();
                        assertEquals(1, ((CommitEvent) event).index);
                    }
                    break;
                case ELECTION:
                    if (evtListener != null) {
                        evtListener.onEvent(event);
                    }
                    break;
            }
        }, snapshotInstaller, onSnapshotInstalled);
        RaftConfigChanger raftConfigChanger = ReflectionUtils.getField(leader, "configChanger");
        assert raftConfigChanger != null;
        assertEquals(RaftConfigChanger.State.CatchingUp, raftConfigChanger.state());

        for (String peer : clusterConfig.getVotersList()) {
            if (!peer.equals(local)) {
                leader.receive(peer, RaftMessage.newBuilder()
                    .setTerm(1)
                    .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                        .setAccept(AppendEntriesReply.Accept.newBuilder()
                            .setLastIndex(0)
                            .build())
                        .build())
                    .build());
            }
        }

        // 1. all peers caught up
        // 2. configChanger state change to TargetConfigCommitting,
        // 3. tick to broadcast appendEntries containing clusterconfig
        leader.tick();
        assertEquals(RaftConfigChanger.State.TargetConfigCommitting, raftConfigChanger.state());

        // 1. receive jointConfig appendEntriesReply
        // 2. commitTo configChanger and its state change to TargetConfigCommitting
        // 3. tick to trigger heartbeat timeout in which branch appendEntries of targetConfig will be broadcast
        RaftMessage appendEntriesReply = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                .setAccept(AppendEntriesReply.Accept.newBuilder()
                    .setLastIndex(1)
                    .build())
                .build())
            .build();
        leader.receive("v1", appendEntriesReply);
        leader.receive("v2", appendEntriesReply);
        assertEquals(RaftConfigChanger.State.Waiting, raftConfigChanger.state());
        return leader;
    }

    private List<LogEntry> toList(Iterator<LogEntry> itr) {
        List<LogEntry> entries = new ArrayList<>();
        itr.forEachRemaining(entries::add);
        return entries;
    }
}