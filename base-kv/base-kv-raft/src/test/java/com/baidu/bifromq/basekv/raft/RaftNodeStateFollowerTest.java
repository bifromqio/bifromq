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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basekv.raft.event.CommitEvent;
import com.baidu.bifromq.basekv.raft.event.RaftEvent;
import com.baidu.bifromq.basekv.raft.event.RaftEventType;
import com.baidu.bifromq.basekv.raft.event.SnapshotRestoredEvent;
import com.baidu.bifromq.basekv.raft.proto.AppendEntries;
import com.baidu.bifromq.basekv.raft.proto.AppendEntriesReply;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.raft.proto.InstallSnapshot;
import com.baidu.bifromq.basekv.raft.proto.InstallSnapshotReply;
import com.baidu.bifromq.basekv.raft.proto.LogEntry;
import com.baidu.bifromq.basekv.raft.proto.Propose;
import com.baidu.bifromq.basekv.raft.proto.ProposeReply;
import com.baidu.bifromq.basekv.raft.proto.RaftMessage;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.baidu.bifromq.basekv.raft.proto.RequestPreVote;
import com.baidu.bifromq.basekv.raft.proto.RequestPreVoteReply;
import com.baidu.bifromq.basekv.raft.proto.RequestReadIndex;
import com.baidu.bifromq.basekv.raft.proto.RequestReadIndexReply;
import com.baidu.bifromq.basekv.raft.proto.RequestVote;
import com.baidu.bifromq.basekv.raft.proto.RequestVoteReply;
import com.baidu.bifromq.basekv.raft.proto.Snapshot;
import com.baidu.bifromq.basekv.raft.proto.TimeoutNow;
import com.baidu.bifromq.basekv.raft.proto.Voting;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(MockitoJUnitRunner.class)
public class RaftNodeStateFollowerTest extends RaftNodeStateTest {
    private final Logger log = LoggerFactory.getLogger("RaftNodeStateFollowerTest");
    private static final String leader = "v1";

    @Test
    public void testStartUp() {
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower follower = new RaftNodeStateFollower(1, 0, leader, defaultRaftConfig, stateStorage, log,
            msgSender,
            eventListener,
            snapshotInstaller,
            onSnapshotInstalled);
        assertEquals(stateStorage.local(), follower.id);
        assertEquals(RaftNodeStatus.Follower, follower.getState());
        assertEquals(clusterConfig, follower.latestClusterConfig());
        assertEquals(stateStorage.latestSnapshot().getData(), follower.latestSnapshot());
    }

    @Test
    public void testElectionElapsed() {
        RaftConfig raftConfig = new RaftConfig()
            .setPreVote(true)
            .setDisableForwardProposal(false)
            .setElectionTimeoutTick(6)
            .setHeartbeatTimeoutTick(3)
            .setInstallSnapshotTimeoutTick(5)
            .setMaxInflightAppends(3);
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower raftNodeStateFollower = new RaftNodeStateFollower(1, 0, leader, raftConfig,
            stateStorage, log, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);

        Integer randomElectionTimeoutTick =
            ReflectionUtils.getField(raftNodeStateFollower, "randomElectionTimeoutTick");
        for (int i = 0; i < randomElectionTimeoutTick - 1; ++i) {
            raftNodeStateFollower.tick();
        }
        RaftNodeState raftNodeState = raftNodeStateFollower.tick();
        assertSame(RaftNodeStatus.Candidate, raftNodeState.getState());


        // change cluster config to be not promotable
        raftNodeStateFollower = new RaftNodeStateFollower(1, 0, leader, raftConfig,
            stateStorage, log, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);
        stateStorage.append(Collections.singletonList(LogEntry.newBuilder()
            .setConfig(ClusterConfig.newBuilder().addAllVoters(Arrays.asList("v2", "v3")).build())
            .setTerm(2)
            .setIndex(stateStorage.lastIndex() + 1)
            .build()), false);
        randomElectionTimeoutTick = ReflectionUtils.getField(raftNodeStateFollower, "randomElectionTimeoutTick");
        for (int i = 0; i < randomElectionTimeoutTick - 1; ++i) {
            raftNodeStateFollower.tick();
        }
        raftNodeState = raftNodeStateFollower.tick();
        assertSame(RaftNodeStatus.Candidate, raftNodeState.getState());
    }

    @Test
    public void testPropose() {
        AtomicInteger onMessageReadyIndex = new AtomicInteger();
        CompletableFuture<Void> onDoneOk = new CompletableFuture<>();
        CompletableFuture<Void> onDoneExceptionally = new CompletableFuture<>();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower follower = new RaftNodeStateFollower(1, 0, leader, defaultRaftConfig,
            stateStorage, log, messages -> {
            if (onMessageReadyIndex.get() == 0) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(new HashMap<String, List<RaftMessage>>() {{
                    put(leader, Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(1)
                        .setPropose(Propose.newBuilder()
                            .setId(1)
                            .setCommand(command)
                            .build())
                        .build()));
                }}, messages);
            } else if (onMessageReadyIndex.get() == 1) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(new HashMap<String, List<RaftMessage>>() {{
                    put(leader, Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(1)
                        .setPropose(Propose.newBuilder()
                            .setId(2)
                            .setCommand(command)
                            .build())
                        .build()));
                }}, messages);
            }
        }, eventListener, snapshotInstaller, onSnapshotInstalled);

        follower.propose(command, onDoneOk);
        assertFalse(onDoneOk.isDone());

        Map<Integer, CompletableFuture<Void>> idToForwardedProposeMap =
            ReflectionUtils.getField(follower, "idToForwardedProposeMap");
        assertTrue(Objects.requireNonNull(idToForwardedProposeMap).containsKey(1));
        LinkedHashMap<Long, Set<CompletableFuture<Void>>> tickToForwardedProposesMap =
            ReflectionUtils.getField(follower, "tickToForwardedProposesMap");
        assertTrue(Objects.requireNonNull(tickToForwardedProposesMap).get(0L).contains(1));

        RaftMessage proposeReply = RaftMessage.newBuilder()
            .setTerm(1)
            .setProposeReply(ProposeReply.newBuilder()
                .setId(1)
                .setCode(ProposeReply.Code.Success)
                .build())
            .build();
        follower.receive(leader, proposeReply);
        assertTrue(idToForwardedProposeMap.get(1).isDone());

        follower.propose(command, onDoneExceptionally);
        assertFalse(onDoneExceptionally.isDone());

        RaftMessage proposeReplyError = RaftMessage.newBuilder()
            .setTerm(1)
            .setProposeReply(ProposeReply.newBuilder()
                .setId(2)
                .setCode(ProposeReply.Code.DropByLeaderTransferring)
                .build())
            .build();
        follower.receive(leader, proposeReplyError);
        assertTrue(idToForwardedProposeMap.get(2).isCompletedExceptionally());

        follower.propose(command, new CompletableFuture<>());
        assertFalse(idToForwardedProposeMap.isEmpty());
        follower.tick();
        follower.tick();
        follower.tick();
        follower.tick();
        follower.tick();
        follower.tick();
        follower.tick();
        assertTrue(idToForwardedProposeMap.isEmpty());
    }

    @Test
    public void testProposeExceptionally() {
        when(raftStateStorage.latestClusterConfig()).thenReturn(clusterConfig);

        RaftNodeStateFollower disableForwardProposalFollower = new RaftNodeStateFollower(1, 0, null,
            new RaftConfig().setDisableForwardProposal(true).setElectionTimeoutTick(3), raftStateStorage, log,
            msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);
        CompletableFuture<Void> disableForwardProposalOnDone = new CompletableFuture<>();
        disableForwardProposalFollower.propose(command, disableForwardProposalOnDone);
        assertTrue(disableForwardProposalOnDone.isCompletedExceptionally());

        RaftNodeStateFollower noLeaderFollower = new RaftNodeStateFollower(1, 0, null, defaultRaftConfig,
            raftStateStorage, log, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);
        CompletableFuture<Void> nonLeaderOnDone = new CompletableFuture<>();
        noLeaderFollower.propose(command, nonLeaderOnDone);
        assertTrue(nonLeaderOnDone.isCompletedExceptionally());
    }

    @Test
    public void testTransferLeadership() {
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower follower = new RaftNodeStateFollower(1, 0, leader, defaultRaftConfig,
            raftStateStorage, log, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        follower.transferLeadership("v1", onDone);
        assertTrue(onDone.isCompletedExceptionally());
    }

    @Test
    public void testChangeClusterConfig() {
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower follower = new RaftNodeStateFollower(1, 0, leader, defaultRaftConfig,
            raftStateStorage, log, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        follower.changeClusterConfig("cId", Collections.singleton("v3"), Collections.singleton("l4"), onDone);
        assertTrue(onDone.isCompletedExceptionally());
    }

    @Test
    public void testReadIndex() {
        AtomicInteger onMessageReadyIndex = new AtomicInteger();
        CompletableFuture<Long> onDone = new CompletableFuture<>();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower follower = new RaftNodeStateFollower(1, 0, leader, defaultRaftConfig,
            stateStorage, log, messages -> {
            if (onMessageReadyIndex.get() == 0) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(new HashMap<String, List<RaftMessage>>() {{
                    put(leader, Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(1)
                        .setRequestReadIndex(RequestReadIndex.newBuilder()
                            .setId(1)
                            .build())
                        .build()));
                }}, messages);
            }
        }, eventListener, snapshotInstaller, onSnapshotInstalled);

        follower.readIndex(onDone);
        assertFalse(onDone.isDone());

        Map<Integer, CompletableFuture<Long>> idToReadRequestMap =
            ReflectionUtils.getField(follower, "idToReadRequestMap");
        assertTrue(Objects.requireNonNull(idToReadRequestMap).containsKey(1));
        LinkedHashMap<Long, Set<CompletableFuture<Long>>> tickToReadRequestsMap =
            ReflectionUtils.getField(follower, "tickToReadRequestsMap");
        assertTrue(Objects.requireNonNull(tickToReadRequestsMap).get(0L).contains(1));

        RaftMessage readIndexReply = RaftMessage.newBuilder()
            .setTerm(1)
            .setRequestReadIndexReply(RequestReadIndexReply.newBuilder()
                .setId(1)
                .build())
            .build();
        follower.receive("v1", readIndexReply);

        follower.readIndex(new CompletableFuture<>());
        assertFalse(tickToReadRequestsMap.isEmpty());
        follower.tick();
        follower.tick();
        follower.tick();
        follower.tick();
        follower.tick();
        follower.tick();
        follower.tick();
        assertTrue(tickToReadRequestsMap.isEmpty());
    }

    @Test
    public void testReadIndexExceptionally() {
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower follower = new RaftNodeStateFollower(1, 0, null, defaultRaftConfig,
            stateStorage, log, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);

        CompletableFuture<Long> onDone = new CompletableFuture<>();
        follower.readIndex(onDone);
        assertTrue(onDone.isCompletedExceptionally());
    }

    @Test
    public void testReceiveTimeoutNow() {
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower follower = new RaftNodeStateFollower(1, 0, leader, defaultRaftConfig,
            stateStorage, log, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);

        // change cluster config to be not promotable
        stateStorage.append(Collections.singletonList(LogEntry.newBuilder()
            .setConfig(ClusterConfig.newBuilder().addAllVoters(Arrays.asList("v2", "v3")).build())
            .setTerm(1)
            .setIndex(stateStorage.lastIndex() + 1)
            .build()), false);

        RaftMessage timeoutNow = RaftMessage.newBuilder()
            .setTerm(1)
            .setTimeoutNow(TimeoutNow.newBuilder().build())
            .build();
        RaftNodeState raftNodeState = follower.receive(leader, timeoutNow);
        assertSame(RaftNodeStatus.Follower, raftNodeState.getState());

        // change cluster config to be promotable
        stateStorage.append(Collections.singletonList(LogEntry.newBuilder()
            .setConfig(ClusterConfig.newBuilder().addAllVoters(Arrays.asList(local, "v2")).build())
            .setTerm(1)
            .setIndex(stateStorage.lastIndex() + 1)
            .build()), false);

        raftNodeState = follower.receive(leader, timeoutNow);
        assertSame(RaftNodeStatus.Candidate, raftNodeState.getState());
    }

    @Test
    public void testReceivePreVote() {
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        // not in lease will handle preVote
        RaftNodeStateFollower nonInLeaseFollower = new RaftNodeStateFollower(1, 0, null, defaultRaftConfig,
            stateStorage, log, messages -> {
            assertEquals(new HashMap<String, List<RaftMessage>>() {{
                put("v2", Collections.singletonList(RaftMessage.newBuilder()
                    .setTerm(2)
                    .setRequestPreVoteReply(RequestPreVoteReply.newBuilder()
                        .setVoteCouldGranted(true)
                        .build())
                    .build()));
            }}, messages);
        }, eventListener, snapshotInstaller, onSnapshotInstalled);
        RaftMessage higherTermPreVote = RaftMessage.newBuilder()
            .setTerm(2)
            .setRequestPreVote(RequestPreVote.newBuilder()
                .setCandidateId(testCandidateId)
                .setLastLogTerm(0)
                .setLastLogIndex(0)
                .build())
            .build();
        RaftNodeState raftNodeState = nonInLeaseFollower.receive("v2", higherTermPreVote);
        assertSame(RaftNodeStatus.Follower, raftNodeState.getState());

        stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        // inLease && higher term will reject pre-vote
        RaftNodeStateFollower inLeaseFollower = new RaftNodeStateFollower(1, 0, leader, defaultRaftConfig,
            stateStorage, log,
            messages -> {
                assertEquals(new HashMap<String, List<RaftMessage>>() {{
                    put("v2", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(2)
                        .setRequestPreVoteReply(RequestPreVoteReply.newBuilder()
                            .setVoteCouldGranted(false)
                            .build())
                        .build()));
                }}, messages);
            }, eventListener, snapshotInstaller, onSnapshotInstalled);
        raftNodeState = inLeaseFollower.receive("v2", higherTermPreVote);
        assertSame(RaftNodeStatus.Follower, raftNodeState.getState());

        RaftMessage matchedTermPreVote = RaftMessage.newBuilder()
            .setTerm(2)
            .setRequestPreVote(RequestPreVote.newBuilder()
                .setCandidateId(testCandidateId)
                .setLastLogTerm(0)
                .setLastLogIndex(0)
                .build())
            .build();
        // request will be ignored
        raftNodeState = inLeaseFollower.receive("v2", matchedTermPreVote);
        assertSame(RaftNodeStatus.Follower, raftNodeState.getState());

    }

    @Test
    public void testReceiveRequestVoteFacingDisruption() {
        // higherTerm && !leaderTransfer && inLease() && not a member, prevent from being disrupted
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower inLeaseFollower = new RaftNodeStateFollower(1, 0, leader, defaultRaftConfig,
            stateStorage, log, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);
        RaftMessage nonLeaderTransferRequest = RaftMessage.newBuilder()
            .setTerm(2)
            .setRequestVote(RequestVote.newBuilder()
                .setCandidateId(testCandidateId)
                .setLastLogTerm(0)
                .setLastLogIndex(0)
                .build())
            .build();
        inLeaseFollower.receive(testCandidateId, nonLeaderTransferRequest);
        assertEquals(1, inLeaseFollower.currentTerm());

        stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        // higherTerm && !leaderTransfer && !inLease()
        RaftNodeStateFollower nonInLeaseFollower = new RaftNodeStateFollower(1, 0, null, defaultRaftConfig,
            stateStorage, log, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);
        nonInLeaseFollower.receive("v2", nonLeaderTransferRequest);
        assertEquals(2, nonInLeaseFollower.currentTerm());

        // higherTerm && leaderTransfer && inLease()
        RaftMessage leaderTransferRequest = RaftMessage.newBuilder()
            .setTerm(3)
            .setRequestVote(RequestVote.newBuilder()
                .setCandidateId(testCandidateId)
                .setLastLogTerm(0)
                .setLastLogIndex(0)
                .setLeaderTransfer(true)
                .build())
            .build();
        inLeaseFollower.receive("v2", leaderTransferRequest);
        assertEquals(3, inLeaseFollower.currentTerm());
    }

    @Test
    public void testReceiveRequestVoteWeatherGranted() {
        AtomicInteger onMessageReadyIndex = new AtomicInteger();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower inLeaseFollower = new RaftNodeStateFollower(1, 0, leader, defaultRaftConfig,
            stateStorage, log, messages -> {
            if (onMessageReadyIndex.get() == 0 || onMessageReadyIndex.get() == 1 || onMessageReadyIndex.get() == 3) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(new HashMap<String, List<RaftMessage>>() {{
                    put("v2", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(2)
                        .setRequestVoteReply(RequestVoteReply.newBuilder()
                            .setVoteGranted(true)
                            .build())
                        .build()));
                }}, messages);
            } else if (onMessageReadyIndex.get() == 2) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(new HashMap<String, List<RaftMessage>>() {{
                    put("v2", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(2)
                        .setRequestVoteReply(RequestVoteReply.newBuilder()
                            .setVoteGranted(false)
                            .build())
                        .build()));
                }}, messages);
            }
        }, eventListener, snapshotInstaller, onSnapshotInstalled);

        RaftMessage transferVote = RaftMessage.newBuilder()
            .setTerm(2)
            .setRequestVote(RequestVote.newBuilder()
                .setCandidateId("v2")
                .setLastLogTerm(2)
                .setLastLogIndex(0)
                .setLeaderTransfer(true)
                .build())
            .build();

        // inLease && !votingPresent
        inLeaseFollower.receive("v2", transferVote);
        assertTrue(stateStorage.currentVoting().isPresent());

        // inLease && votingPresent && votingTerm != askedTerm
        stateStorage.saveVoting(Voting.newBuilder().setTerm(1).setFor("v1").build());
        inLeaseFollower.receive("v2", transferVote);

        // inLease && votingPresent && votingTerm == askedTerm && votingFor != candidateId
        stateStorage.saveVoting(Voting.newBuilder().setTerm(2).setFor("v1").build());
        inLeaseFollower.receive("v2", transferVote);

        // inLease && votingPresent && votingTerm == askedTerm && votingFor == testCandidateId, repeated vote
        stateStorage.saveVoting(Voting.newBuilder().setTerm(2).setFor("v2").build());
        inLeaseFollower.receive("v2", transferVote);

        onMessageReadyIndex.set(0);
        stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower noInLeaseFollower = new RaftNodeStateFollower(1, 0, null, defaultRaftConfig,
            stateStorage, log, messages -> {
            if (onMessageReadyIndex.get() == 0 || onMessageReadyIndex.get() == 1 || onMessageReadyIndex.get() == 2) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(new HashMap<String, List<RaftMessage>>() {{
                    put("v2", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(2)
                        .setRequestVoteReply(RequestVoteReply.newBuilder()
                            .setVoteGranted(true)
                            .build())
                        .build()));
                }}, messages);
            }
        }, eventListener, snapshotInstaller, onSnapshotInstalled);

        // !inLease && !votingPresent
        noInLeaseFollower.receive("v2", transferVote);
        assertTrue(stateStorage.currentVoting().isPresent());

        // !inLease && votingPresent && votingTerm != currentTerm
        stateStorage.saveVoting(Voting.newBuilder().setTerm(1).setFor("v2").build());
        noInLeaseFollower.receive("v2", transferVote);

        // !inLease && votingPresent && votingTerm == currentTerm
        stateStorage.saveVoting(Voting.newBuilder().setTerm(2).setFor("v2").build());
        noInLeaseFollower.receive("v2", transferVote);
    }

    @Test
    public void testReceiveRequestVoteUptoDate() {
        AtomicInteger onMessageReadyIndex = new AtomicInteger();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower noInLeaseFollower = new RaftNodeStateFollower(1, 0, null, defaultRaftConfig,
            stateStorage, log, messages -> {
            if (onMessageReadyIndex.get() == 0) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(new HashMap<String, List<RaftMessage>>() {{
                    put("v2", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(1)
                        .setRequestVoteReply(RequestVoteReply.newBuilder()
                            .setVoteGranted(false)
                            .build())
                        .build()));
                }}, messages);
            } else if (onMessageReadyIndex.get() == 1) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(new HashMap<String, List<RaftMessage>>() {{
                    put("v2", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(1)
                        .setRequestVoteReply(RequestVoteReply.newBuilder()
                            .setVoteGranted(true)
                            .build())
                        .build()));
                }}, messages);

            } else if (onMessageReadyIndex.get() == 2) {
                onMessageReadyIndex.incrementAndGet();
                assertEquals(new HashMap<String, List<RaftMessage>>() {{
                    put("v2", Collections.singletonList(RaftMessage.newBuilder()
                        .setTerm(2)
                        .setRequestVoteReply(RequestVoteReply.newBuilder()
                            .setVoteGranted(true)
                            .build())
                        .build()));
                }}, messages);
            }
        }, eventListener, snapshotInstaller, onSnapshotInstalled);

        // requestLastLogTerm == localLastLogTerm && requestLastLogIndex < localLastLogIndex
        stateStorage.append(Collections.singletonList(LogEntry.newBuilder()
            .setTerm(1)
            .setIndex(stateStorage.lastIndex() + 1)
            .setData(ByteString.EMPTY)
            .build()), false);
        RaftMessage vote = RaftMessage.newBuilder()
            .setTerm(1)
            .setRequestVote(RequestVote.newBuilder()
                .setCandidateId("v2")
                .setLastLogTerm(1)
                .setLastLogIndex(0)
                .build())
            .build();
        noInLeaseFollower.receive("v2", vote);

        // requestLastLogTerm == localLastLogTerm && requestLastLogIndex > localLastLogIndex
        vote = RaftMessage.newBuilder()
            .setTerm(1)
            .setRequestVote(RequestVote.newBuilder()
                .setCandidateId("v2")
                .setLastLogTerm(1)
                .setLastLogIndex(3)
                .build())
            .build();
        noInLeaseFollower.receive("v2", vote);

        // requestLastLogTerm > localLastLogTerm
        vote = RaftMessage.newBuilder()
            .setTerm(2)
            .setRequestVote(RequestVote.newBuilder()
                .setCandidateId("v2")
                .setLastLogTerm(2)
                .setLastLogIndex(0)
                .build())
            .build();
        noInLeaseFollower.receive("v2", vote);
    }

    @Test
    public void testReceiveObsoleteSnapshot() {
        Snapshot snapshot = Snapshot.newBuilder()
            .setClusterConfig(clusterConfig)
            .setIndex(5)
            .setTerm(1)
            .build();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", snapshot);

        RaftNodeStateFollower noInLeaseFollower = new RaftNodeStateFollower(1, 0, null,
            defaultRaftConfig, stateStorage, log, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);

        // obsolete or duplicated snapshot
        RaftMessage installSnapshot = RaftMessage.newBuilder()
            .setTerm(1)
            .setInstallSnapshot(InstallSnapshot.newBuilder()
                .setLeaderId("newLeader")
                .setSnapshot(Snapshot.newBuilder()
                    .setTerm(0)
                    .setIndex(0)
                    .setData(ByteString.copyFrom("snapshot".getBytes()))
                    .build())
                .build())
            .build();

        noInLeaseFollower.receive("newLeader", installSnapshot);
        verify(msgSender, times(0)).send(anyMap());
        verify(eventListener, times(0)).onEvent(any());
    }

    @Test
    public void testSnapshotInstallFailed() {
        Snapshot snapshot = Snapshot.newBuilder()
            .setClusterConfig(clusterConfig)
            .setIndex(0)
            .setTerm(1)
            .build();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", snapshot);

        RaftNodeStateFollower noInLeaseFollower = new RaftNodeStateFollower(1, 0, null,
            defaultRaftConfig, stateStorage, log, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);

        RaftMessage installSnapshot = RaftMessage.newBuilder()
            .setTerm(1)
            .setInstallSnapshot(InstallSnapshot.newBuilder()
                .setLeaderId("newLeader")
                .setSnapshot(Snapshot.newBuilder()
                    .setTerm(1)
                    .setIndex(0)
                    .setData(ByteString.copyFrom("snapshot".getBytes()))
                    .build())
                .build())
            .build();

        when(snapshotInstaller.install(any(ByteString.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Test Exception")));

        noInLeaseFollower.receive("newLeader", installSnapshot);

        ArgumentCaptor<Map<String, List<RaftMessage>>> msgCaptor = ArgumentCaptor.forClass(Map.class);
        verify(msgSender, times(1)).send(msgCaptor.capture());
        assertEquals(new HashMap<String, List<RaftMessage>>() {{
            put("newLeader", Collections.singletonList(RaftMessage.newBuilder()
                .setTerm(1)
                .setInstallSnapshotReply(InstallSnapshotReply.newBuilder()
                    .setRejected(true)
                    .setLastIndex(0)
                    .build())
                .build()));
        }}, msgCaptor.getValue());
        verify(eventListener, times(0)).onEvent(any());
    }

    @Test
    public void testSnapshotRestore() {
        Snapshot snapshot = Snapshot.newBuilder()
            .setClusterConfig(clusterConfig)
            .setIndex(0)
            .setTerm(1)
            .build();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", snapshot);

        RaftNodeStateFollower noInLeaseFollower = new RaftNodeStateFollower(1, 0, null,
            defaultRaftConfig, stateStorage, log, msgSender, eventListener, snapshotInstaller, onSnapshotInstalled);

        RaftMessage installSnapshot = RaftMessage.newBuilder()
            .setTerm(1)
            .setInstallSnapshot(InstallSnapshot.newBuilder()
                .setLeaderId("newLeader")
                .setSnapshot(Snapshot.newBuilder()
                    .setTerm(0)
                    .setIndex(0)
                    .setData(ByteString.copyFrom("snapshot".getBytes()))
                    .build())
                .build())
            .build();

        when(snapshotInstaller.install(any(ByteString.class))).thenReturn(CompletableFuture.completedFuture(null));
        noInLeaseFollower.receive("newLeader", installSnapshot);

        ArgumentCaptor<Map<String, List<RaftMessage>>> msgCaptor = ArgumentCaptor.forClass(Map.class);
        verify(msgSender, times(1)).send(msgCaptor.capture());
        assertEquals(new HashMap<String, List<RaftMessage>>() {{
            put("newLeader", Collections.singletonList(RaftMessage.newBuilder()
                .setTerm(1)
                .setInstallSnapshotReply(InstallSnapshotReply.newBuilder()
                    .setRejected(false)
                    .setLastIndex(0)
                    .build())
                .build()));
        }}, msgCaptor.getValue());

        ArgumentCaptor<RaftEvent> eventCaptor = ArgumentCaptor.forClass(RaftEvent.class);
        verify(eventListener, times(2)).onEvent(eventCaptor.capture());

        List<RaftEvent> events = eventCaptor.getAllValues();

        assertEquals(RaftEventType.SNAPSHOT_RESTORED, events.get(0).type);
        assertEquals(installSnapshot.getInstallSnapshot().getSnapshot(),
            ((SnapshotRestoredEvent) events.get(0)).snapshot);

        assertEquals(RaftEventType.COMMIT, events.get(1).type);
        assertEquals(0L, ((CommitEvent) events.get(1)).index);
    }

    @Test
    public void testReceiveAppendEntries() {
        AtomicInteger onMessageReadyIndex = new AtomicInteger();
        IRaftStateStore stateStorage = new InMemoryStateStore("testLocal", Snapshot.newBuilder()
            .setClusterConfig(clusterConfig).build());

        RaftNodeStateFollower noInLeaseFollower = new RaftNodeStateFollower(1, 0, null, defaultRaftConfig,
            stateStorage, log, messages -> {
            switch (onMessageReadyIndex.get()) {
                case 0:
                    onMessageReadyIndex.incrementAndGet();
                    assertEquals(new HashMap<String, List<RaftMessage>>() {{
                        put("newLeader", Collections.singletonList(RaftMessage.newBuilder()
                            .setTerm(1)
                            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                                .setAccept(AppendEntriesReply.Accept.newBuilder()
                                    .setLastIndex(1)
                                    .build())
                                .build())
                            .build()));
                    }}, messages);
                    break;
                case 1:
                    onMessageReadyIndex.incrementAndGet();
                    assertEquals(new HashMap<String, List<RaftMessage>>() {{
                        put("newLeader", Collections.singletonList(RaftMessage.newBuilder()
                            .setTerm(1)
                            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                                .setReject(AppendEntriesReply.Reject.newBuilder()
                                    .setTerm(1)
                                    .setRejectedIndex(2)
                                    .setLastIndex(1)
                                    .build())
                                .build())
                            .build()));
                    }}, messages);
                    break;
                case 2:
                    assertEquals(new HashMap<String, List<RaftMessage>>() {{
                        put("newLeader", Collections.singletonList(RaftMessage.newBuilder()
                            .setTerm(2)
                            .setAppendEntriesReply(AppendEntriesReply.newBuilder()
                                .setAccept(AppendEntriesReply.Accept.newBuilder()
                                    .setLastIndex(2)
                                    .build())
                                .build())
                            .build()));
                    }}, messages);
                    break;
            }
        }, eventListener,
            appSMSnapshot -> {
                assertEquals(ByteString.copyFromUtf8("snapshot"), appSMSnapshot);
                return CompletableFuture.completedFuture(null);
            }, onSnapshotInstalled);
        stateStorage.addStableListener(noInLeaseFollower::stableTo);
        // appended entries
        RaftMessage appendEntries = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntries(AppendEntries.newBuilder()
                .setLeaderId("newLeader")
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .setCommitIndex(1)
                .addAllEntries(Collections.singleton(LogEntry.newBuilder()
                    .setTerm(1)
                    .setIndex(1)
                    .setData(ByteString.EMPTY)
                    .build()))
                .build())
            .build();
        noInLeaseFollower.receive("newLeader", appendEntries);

        // obsolete entries
        appendEntries = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntries(AppendEntries.newBuilder()
                .setLeaderId("newLeader")
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .setCommitIndex(1)
                .addAllEntries(Collections.singleton(LogEntry.newBuilder()
                    .setTerm(1)
                    .setIndex(1)
                    .setData(ByteString.EMPTY)
                    .build()))
                .build())
            .build();
        noInLeaseFollower.receive("newLeader", appendEntries);

        // rejected entries
        appendEntries = RaftMessage.newBuilder()
            .setTerm(1)
            .setAppendEntries(AppendEntries.newBuilder()
                .setLeaderId("newLeader")
                .setPrevLogIndex(2)
                .setPrevLogTerm(1)
                .setCommitIndex(1)
                .addAllEntries(Collections.singleton(LogEntry.newBuilder()
                    .setTerm(1)
                    .setIndex(2)
                    .setData(ByteString.EMPTY)
                    .build()))
                .build())
            .build();
        noInLeaseFollower.receive("newLeader", appendEntries);

        // higher term
        appendEntries = RaftMessage.newBuilder()
            .setTerm(2)
            .setAppendEntries(AppendEntries.newBuilder()
                .setLeaderId("newLeader")
                .setPrevLogIndex(1)
                .setPrevLogTerm(1)
                .setCommitIndex(1)
                .addAllEntries(Collections.singleton(LogEntry.newBuilder()
                    .setTerm(2)
                    .setIndex(2)
                    .setData(ByteString.EMPTY)
                    .build()))
                .build())
            .build();
        noInLeaseFollower.receive("newLeader", appendEntries);
    }
}