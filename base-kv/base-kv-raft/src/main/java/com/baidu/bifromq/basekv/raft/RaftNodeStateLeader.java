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

import static com.baidu.bifromq.basekv.raft.RaftConfigChanger.State.JointConfigCommitting;
import static com.baidu.bifromq.basekv.raft.RaftConfigChanger.State.TargetConfigCommitting;

import com.baidu.bifromq.basekv.raft.exception.DropProposalException;
import com.baidu.bifromq.basekv.raft.exception.LeaderTransferException;
import com.baidu.bifromq.basekv.raft.exception.ReadIndexException;
import com.baidu.bifromq.basekv.raft.exception.RecoveryException;
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
import com.baidu.bifromq.basekv.raft.proto.RaftNodeSyncState;
import com.baidu.bifromq.basekv.raft.proto.RequestReadIndex;
import com.baidu.bifromq.basekv.raft.proto.RequestReadIndexReply;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;

class RaftNodeStateLeader extends RaftNodeState {
    private static class LeaderTransferTask {
        final String nextLeader;
        private final CompletableFuture<Void> onDone;

        LeaderTransferTask(String nextLeader, CompletableFuture<Void> onDone) {
            this.nextLeader = nextLeader;
            this.onDone = onDone;
        }

        void abort(Throwable t) {
            onDone.completeExceptionally(t);
        }

        void done() {
            onDone.complete(null);
        }
    }

    private final QuorumTracker activityTracker;
    private final PeerLogTracker peerLogTracker;
    private final RaftConfigChanger configChanger;
    private final ReadProgressTracker readProgressTracker;
    private LeaderTransferTask leaderTransferTask;
    private int electionElapsedTick;

    RaftNodeStateLeader(long term,
                        long commitIndex,
                        RaftConfig config,
                        IRaftStateStore stateStorage,
                        Logger log,
                        LinkedHashMap<Long, ProposeTask> uncommittedProposals,
                        IRaftNode.IRaftMessageSender sender,
                        IRaftNode.IRaftEventListener listener,
                        IRaftNode.ISnapshotInstaller installer,
                        OnSnapshotInstalled onSnapshotInstalled,
                        String... tags) {
        super(term,
            commitIndex,
            config,
            stateStorage,
            log,
            uncommittedProposals,
            sender,
            listener,
            installer,
            onSnapshotInstalled,
            tags);
        peerLogTracker = new PeerLogTracker(id(), config, stateStorage, listener, this);
        configChanger = new RaftConfigChanger(config, stateStorage, peerLogTracker, this);
        ClusterConfig clusterConfig = stateStorage.latestClusterConfig();
        activityTracker = new QuorumTracker(clusterConfig, this);
        readProgressTracker = new ReadProgressTracker(stateStorage, this);
        // track peers in current config
        Set<String> peersToStartTracking = new HashSet<>(clusterConfig.getVotersList());
        peersToStartTracking.addAll(clusterConfig.getLearnersList());
        peerLogTracker.startTracking(peersToStartTracking, false);
        // confirm progress of the local tracker immediately
        peerLogTracker.confirmMatch(stateStorage.local(), stateStorage.lastIndex());
        // trigger a dummy cluster config change which won't actually change cluster's current config, except append
        // the latest cluster config as the first entry in current term. This helps:
        // 1) concluding commitIndex of current term quickly
        // 2) resuming potential cluster config change process which has started but not finished in previous term
        // of course, this also introduces a small gap during which config change call will be rejected.
        if (isJoint(clusterConfig)) {
            logDebug("Resume cluster config change process in current term");
            changeClusterConfig(clusterConfig.getCorrelateId(),
                new HashSet<>(clusterConfig.getNextVotersList()),
                new HashSet<>(clusterConfig.getNextLearnersList()), new CompletableFuture<>());

        } else {
            logDebug("Propose cluster config as first log entry in current term to conclude commit index");
            changeClusterConfig(clusterConfig.getCorrelateId(),
                new HashSet<>(clusterConfig.getVotersList()),
                new HashSet<>(clusterConfig.getLearnersList()), new CompletableFuture<>());
        }
    }

    @Override
    RaftNodeStatus getState() {
        return RaftNodeStatus.Leader;
    }

    @Override
    String currentLeader() {
        // leader is me
        return stateStorage.local();
    }

    @Override
    RaftNodeState stepDown(CompletableFuture<Boolean> onDone) {
        logDebug("leader is asked to step down to follower");
        RaftNodeState nextState = new RaftNodeStateFollower(
            currentTerm(),
            commitIndex,
            null,
            config,
            stateStorage,
            log,
            uncommittedProposals,
            sender,
            listener,
            snapshotInstaller,
            onSnapshotInstalled,
            tags);
        configChanger.abort();
        readProgressTracker.abort();
        onDone.complete(true);
        return nextState;
    }

    @Override
    RaftNodeState recover(CompletableFuture<Void> onDone) {
        onDone.completeExceptionally(RecoveryException.notLostQuorum());
        return this;
    }

    @Override
    RaftNodeState tick() {
        electionElapsedTick++;
        peerLogTracker.tick();
        if (configChanger.tick(currentTerm())) {
            // there is a state change after tick
            if (configChanger.state() == JointConfigCommitting || configChanger.state() == TargetConfigCommitting) {
                logDebug("{} cluster config is activated in current term",
                    configChanger.state() == JointConfigCommitting ? "Joint" : "Target");
                ClusterConfig clusterConfig = stateStorage.latestClusterConfig();
                activityTracker.refresh(clusterConfig);
                electionElapsedTick = 0; // to prevent leader from quorum check failed prematurely
                if (leaderTransferTask != null) {
                    if (!clusterConfig.getVotersList().contains(leaderTransferTask.nextLeader)
                        && !clusterConfig.getNextVotersList().contains(leaderTransferTask.nextLeader)) {
                        logDebug("Abort leadership transfer, new leader[{}] has been removed from new cluster config",
                            leaderTransferTask.nextLeader);
                        leaderTransferTask.abort(LeaderTransferException.notFoundOrQualified());
                        leaderTransferTask = null;
                    }
                }
            }
        }
        if (electionElapsedTick >= config.getElectionTimeoutTick()) {
            electionElapsedTick = 0;
            if (leaderTransferTask != null) {
                logDebug("Leadership cannot be transferred to {} in electionTimeoutTicks[{}]",
                    leaderTransferTask.nextLeader, config.getElectionTimeoutTick());
                leaderTransferTask.abort(LeaderTransferException.transferTimeout());
                leaderTransferTask = null;
            }
            // local always active
            activityTracker.poll(stateStorage.local(), true);
            QuorumTracker.JointVoteResult quorumCheckResult = activityTracker.tally();
            if (quorumCheckResult.result == QuorumTracker.VoteResult.Won) {
                // prepare for next round check
                logTrace("Quorum check succeed[{}], leadership remain", quorumCheckResult);
                activityTracker.reset();
            } else {
                // at the end of an election timeout, leader is out of touch with the majority,
                // step down as a follower and abort any pending futures
                logWarn("Quorum check failed[{}], leader stepped down to follower", quorumCheckResult);
                configChanger.abort();
                readProgressTracker.abort();
                return new RaftNodeStateFollower(
                    currentTerm(), // update term
                    commitIndex,
                    null,
                    config,
                    stateStorage,
                    log,
                    uncommittedProposals,
                    sender,
                    listener,
                    snapshotInstaller,
                    onSnapshotInstalled,
                    tags
                );
            }
        }
        Map<String, List<RaftMessage>> appendEntriesToSend = prepareAppendEntriesIfAbsent(false);
        if (!appendEntriesToSend.isEmpty()) {
            submitRaftMessages(appendEntriesToSend);
        }
        return this;
    }

    @Override
    void propose(ByteString fsmCmd, CompletableFuture<Long> onDone) {
        if (leaderTransferTask != null) {
            logDebug("Dropped proposal due to transferring leadership");
            onDone.completeExceptionally(DropProposalException.transferringLeader());
            return;
        }
        if (isProposeThrottled()) {
            logDebug("Dropped proposal due to log growing[uncommittedProposals:{}] "
                    + "exceeds threshold[maxUncommittedProposals:{}]",
                uncommittedProposals.size(), maxUncommittedProposals);
            onDone.completeExceptionally(DropProposalException.throttledByThreshold());
            return;
        }
        LogEntry entry = LogEntry.newBuilder()
            .setTerm(currentTerm())
            .setIndex(stateStorage.lastIndex() + 1)
            .setData(fsmCmd)
            .build();
        stateStorage.append(Collections.singletonList(entry), !config.isAsyncAppend());
        // update self progress
        peerLogTracker.replicateBy(stateStorage.local(), stateStorage.lastIndex());

        ProposeTask prev =
            uncommittedProposals.put(entry.getIndex(), new ProposeTask(entry.getTerm(), onDone));
        assert prev == null;

        Map<String, List<RaftMessage>> appendEntriesToSend = prepareAppendEntriesIfAbsent(false);
        if (!appendEntriesToSend.isEmpty()) {
            submitRaftMessages(appendEntriesToSend);
        }
    }

    @Override
    RaftNodeState stableTo(long stabledIndex) {
        // update self progress
        logTrace("Log entries before index[{}] stabilized", stabledIndex);
        peerLogTracker.confirmMatch(stateStorage.local(), stabledIndex);
        return commit();
    }

    @Override
    void readIndex(CompletableFuture<Long> onDone) {
        // if no commit in current term
        if (commitIndexNotConfirmed()) {
            logDebug("No log entry of current term committed");
            onDone.completeExceptionally(ReadIndexException.commitIndexNotConfirmed());
            return;
        }

        if (config.isReadOnlyLeaderLeaseMode() && leaderTransferTask == null) {
            // if there is a running leader transfer task we need to fall back to msg-based approach
            onDone.complete(commitIndex);
        } else {
            readProgressTracker.add(commitIndex, onDone);
            if (readProgressTracker.underConfirming() > config.getReadOnlyBatch()) {
                // don't wait for next tick
                // readIndex broadcast may be throttled by maxInflightAppends
                Map<String, List<RaftMessage>> appendEntriesToSend = prepareAppendEntriesIfAbsent(true);
                submitRaftMessages(appendEntriesToSend);
            }
        }
    }

    @Override
    RaftNodeState receive(String fromPeer, RaftMessage message) {
        logTrace("Receive[{}] from {}", message, fromPeer);
        RaftNodeState nextState = this;
        if (message.getTerm() > currentTerm()) {
            switch (message.getMessageTypeCase()) {
                case REQUESTPREVOTE -> {
                    sendRequestPreVoteReply(fromPeer, message.getTerm(), false);
                    return nextState;
                }
                case REQUESTPREVOTEREPLY -> {
                    // the out-dated pre-vote reply must be ignored
                    return nextState;
                }
                case REQUESTVOTE -> {
                    // prevent from being disrupted
                    boolean leaderTransfer = message.getRequestVote().getLeaderTransfer();
                    if (!leaderTransfer && !voters().contains(fromPeer)) {
                        // request vote is not for transferring leadership
                        // if the vote coming a member
                        logDebug("Vote[{}] from candidate[{}] not granted, lease is not expired",
                            message.getTerm(), fromPeer);

                        sendRequestVoteReply(fromPeer, message.getTerm(), false);
                        return nextState;
                    }
                    if (leaderTransferTask != null) {
                        if (leaderTransfer) {
                            // notify caller transfer succeed only when step down because of leaderTransfer explicitly
                            leaderTransferTask.done();
                        } else {
                            // leader step down by another candidate
                            leaderTransferTask.abort(LeaderTransferException.stepDownByOther());
                        }
                        leaderTransferTask = null;
                    }
                }
            }
            // transition to follower according to $3.3 in raft paper
            // abort on-going config change and readIndex request if any
            logDebug("Got higher term[{}] message[{}] from peer[{}], start to step down",
                message.getTerm(), message.getMessageTypeCase(), fromPeer);
            configChanger.abort();
            readProgressTracker.abort();
            nextState = new RaftNodeStateFollower(
                message.getTerm(), // update term
                commitIndex,
                null,
                config,
                stateStorage,
                log,
                uncommittedProposals,
                sender,
                listener,
                snapshotInstaller,
                onSnapshotInstalled,
                tags
            );
            nextState.receive(fromPeer, message);
        } else if (message.getTerm() < currentTerm()) {
            handleLowTermMessage(fromPeer, message);
            return nextState;
        } else {
            switch (message.getMessageTypeCase()) {
                case APPENDENTRIESREPLY -> {
                    return handleAppendEntriesReply(fromPeer, message.getAppendEntriesReply());
                }
                case INSTALLSNAPSHOTREPLY -> {
                    return handleInstallSnapshotReply(fromPeer, message.getInstallSnapshotReply());
                }
                case REQUESTREADINDEX -> handleRequestReadIndex(fromPeer, message.getRequestReadIndex());
                case PROPOSE -> handlePropose(fromPeer, message.getPropose());
                case REQUESTPREVOTE -> sendRequestPreVoteReply(fromPeer, currentTerm(), false);

                // ignore other messages
            }
        }
        return nextState;
    }

    @Override
    void transferLeadership(String newLeader, CompletableFuture<Void> onDone) {
        if (commitIndexNotConfirmed()) {
            onDone.completeExceptionally(LeaderTransferException.leaderNotReady());
            return;
        }
        if (leaderTransferTask != null) {
            onDone.completeExceptionally(LeaderTransferException.transferringInProgress());
            return;
        }
        if (newLeader.equals(stateStorage.local())) {
            onDone.completeExceptionally(LeaderTransferException.selfTransfer());
            return;
        }
        ClusterConfig clusterConfig = stateStorage.latestClusterConfig();
        if (clusterConfig.getLearnersList().contains(newLeader)
            || (!clusterConfig.getVotersList().contains(newLeader)
            && !clusterConfig.getNextVotersList().contains(newLeader))) {
            onDone.completeExceptionally(LeaderTransferException.notFoundOrQualified());
            return;
        }
        // reset tick since leader transfer is expected to finish in one election timeout
        electionElapsedTick = 0;
        leaderTransferTask = new LeaderTransferTask(newLeader, onDone);
        if (peerLogTracker.matchIndex(newLeader) == stateStorage.lastIndex()) {
            sendTimeoutNow(newLeader);
        } else {
            submitRaftMessages(newLeader, prepareAppendEntriesForPeer(newLeader, true));
        }
    }

    @Override
    void changeClusterConfig(String correlateId,
                             Set<String> nextVoters,
                             Set<String> nextLearners,
                             CompletableFuture<Void> onDone) {
        configChanger.submit(correlateId, nextVoters, nextLearners, onDone);
    }

    @Override
    void onSnapshotRestored(ByteString fsmSnapshot, Throwable ex) {
        // ignore in leader state
    }

    private RaftNodeState handleAppendEntriesReply(String fromPeer, AppendEntriesReply reply) {
        if (!peerLogTracker.isTracking(fromPeer)) {
            logDebug("No tracker available for peer[{}]", fromPeer);
            return this;
        }
        activityTracker.poll(fromPeer, true);
        if (!config.isReadOnlyLeaderLeaseMode()) {
            // check if there is any pending read index could be confirmed by quorum
            readProgressTracker.confirm(reply.getReadIndex(), fromPeer);
        }
        if (peerLogTracker.status(fromPeer) != RaftNodeSyncState.SnapshotSyncing) {
            // ignore heartbeat reply during snapshot syncing
            if (reply.getResultCase() == AppendEntriesReply.ResultCase.REJECT) {
                AppendEntriesReply.Reject reject = reply.getReject();
                logDebug("Follower[{}] with last entry[index:{},term:{}] rejected entries appending from index[{}]",
                    fromPeer, reject.getLastIndex(), reject.getTerm(), reject.getRejectedIndex());
                peerLogTracker.backoff(fromPeer, reject.getRejectedIndex(), reject.getLastIndex());
                List<RaftMessage> messages = prepareAppendEntriesForPeer(fromPeer, true);
                submitRaftMessages(fromPeer, messages);
                return this;
            }
            AppendEntriesReply.Accept accept = reply.getAccept();
            logTrace("Follower[{}] accepted entries, and advance match index[{}]",
                fromPeer, accept.getLastIndex());
            peerLogTracker.confirmMatch(fromPeer, accept.getLastIndex());
            if (leaderTransferTask != null && fromPeer.equals(leaderTransferTask.nextLeader)
                && peerLogTracker.matchIndex(fromPeer) == stateStorage.lastIndex()) {
                logInfo("Started leadership transfer by sending TimeoutNow to follower[{}]", fromPeer);
                sendTimeoutNow(leaderTransferTask.nextLeader);
            }
            return commit();
        }
        return this;
    }

    private RaftNodeState handleInstallSnapshotReply(String fromPeer, InstallSnapshotReply reply) {
        if (!peerLogTracker.isTracking(fromPeer)) {
            logDebug("No tracker available for peer[{}] when handleInstallSnapshotReply", fromPeer);
            return this;
        }
        activityTracker.poll(fromPeer, true);
        if (!config.isReadOnlyLeaderLeaseMode()) {
            // check if there is any pending read index could be confirmed by quorum
            readProgressTracker.confirm(reply.getReadIndex(), fromPeer);
        }
        if (peerLogTracker.status(fromPeer) != RaftNodeSyncState.SnapshotSyncing) {
            return this;
        }
        if (reply.getRejected()) {
            logDebug("Follower[{}] rejected snapshot with last entry of index[{}]",
                fromPeer, reply.getLastIndex());
            peerLogTracker.backoff(fromPeer, reply.getLastIndex(), reply.getLastIndex());
            return this;
        } else {
            logDebug("Follower[{}] installed snapshot, advance last index[{}]", fromPeer, reply.getLastIndex());
            peerLogTracker.confirmMatch(fromPeer, reply.getLastIndex());
            return commit();
        }
    }

    private Map<String, List<RaftMessage>> prepareAppendEntriesIfAbsent(boolean forceHeartbeat) {
        Map<String, List<RaftMessage>> appendEntriesToSend = new HashMap<>();
        for (String peer : configChanger.remotePeers()) {
            appendEntriesToSend.computeIfAbsent(peer, p -> prepareAppendEntriesForPeer(p, forceHeartbeat));
        }
        return appendEntriesToSend;
    }

    private List<RaftMessage> prepareAppendEntriesForPeer(String peer, boolean forceHeartbeat) {
        List<RaftMessage> messages = new ArrayList<>();
        long readIndex = readProgressTracker.highestReadIndex();
        switch (peerLogTracker.status(peer)) {
            case SnapshotSyncing -> {
                Snapshot snapshot = stateStorage.latestSnapshot();
                if (!peerLogTracker.pauseReplicating(peer)) {
                    if (snapshot.getIndex() == peerLogTracker.matchIndex(peer)) {
                        // the tracker still tracking latest snapshot
                        logDebug("Prepared snapshot[index:{},term:{}] for peer[{}] when {}",
                            snapshot.getIndex(), snapshot.getTerm(), peer, peerLogTracker.status(peer));
                        messages.add(RaftMessage.newBuilder()
                            .setTerm(currentTerm())
                            .setInstallSnapshot(InstallSnapshot.newBuilder()
                                .setLeaderId(stateStorage.local())
                                .setSnapshot(snapshot)
                                .setReadIndex(readIndex)
                                .build())
                            .build());
                        peerLogTracker.replicateBy(peer, snapshot.getIndex());
                    } else {
                        logDebug("New snapshot[index:{},term:{}] generated, reset the tracker for peer[{}]",
                            snapshot.getIndex(), snapshot.getTerm(), peer);
                        // there is a new snapshot generated, reset the tracker explicitly using previous snapshot
                        peerLogTracker.backoff(peer, peerLogTracker.matchIndex(peer), peerLogTracker.matchIndex(peer));
                    }
                    break;
                }
                if (forceHeartbeat || peerLogTracker.needHeartbeat(peer)) {
                    // send heartbeats during installing snapshot
                    logTrace("Prepare heartbeat after entry[index:{},term:{}] for peer[{}] with readIndex[{}] when {}",
                        snapshot.getIndex(), snapshot.getTerm(), peer, readIndex, peerLogTracker.status(peer));
                    messages.add(RaftMessage.newBuilder()
                        .setTerm(currentTerm())
                        .setAppendEntries(AppendEntries
                            .newBuilder()
                            .setLeaderId(stateStorage.local())
                            .setPrevLogIndex(snapshot.getIndex())
                            .setPrevLogTerm(snapshot.getTerm())
                            // prevent follower from advancing commit index too earlier
                            .setCommitIndex(snapshot.getIndex())
                            .setReadIndex(readIndex)
                            .build())
                        .build());
                    peerLogTracker.replicateBy(peer, snapshot.getIndex());
                }
            }
            case Probing -> {
                if (!peerLogTracker.pauseReplicating(peer) || forceHeartbeat || peerLogTracker.needHeartbeat(peer)) {
                    long nextIndex = Math.max(peerLogTracker.nextIndex(peer), stateStorage.firstIndex());
                    long preLogIndex = nextIndex - 1;
                    long preLogTerm = stateStorage.entryAt(preLogIndex)
                        .map(LogEntry::getTerm).orElseGet(() -> stateStorage.latestSnapshot().getTerm());
                    // no entries to append
                    logDebug("Prepare probing after entry[index:{},term:{}] for peer[{}] with readIndex[{}] when {}",
                        preLogIndex, preLogTerm, peer, readIndex, peerLogTracker.status(peer));
                    messages.add(RaftMessage.newBuilder()
                        .setTerm(currentTerm())
                        .setAppendEntries(AppendEntries
                            .newBuilder()
                            .setLeaderId(stateStorage.local())
                            .setPrevLogIndex(preLogIndex)
                            .setPrevLogTerm(preLogTerm)
                            // tell follower the minimum commit index to prevent it from committing mismatched entries
                            .setCommitIndex(Math.min(preLogIndex, commitIndex))
                            .setReadIndex(readIndex)
                            .build())
                        .build());
                    peerLogTracker.replicateBy(peer, preLogIndex);
                }
            }
            case Replicating -> {
                // maybe there is a compaction happened before, so logEntry pointed by nextIndex may not be available
                long nextIndex = Math.max(peerLogTracker.nextIndex(peer), stateStorage.firstIndex());
                long preLogIndex = nextIndex - 1;
                long preLogTerm = stateStorage.entryAt(preLogIndex)
                    .map(LogEntry::getTerm).orElseGet(() -> stateStorage.latestSnapshot().getTerm());
                if (!peerLogTracker.pauseReplicating(peer) && nextIndex <= stateStorage.lastIndex()) {
                    Iterator<LogEntry> entries = stateStorage.entries(nextIndex,
                        stateStorage.lastIndex() + 1, config.getMaxSizePerAppend());
                    AppendEntries.Builder builder = AppendEntries
                        .newBuilder()
                        .setLeaderId(stateStorage.local())
                        .setPrevLogIndex(preLogIndex)
                        .setPrevLogTerm(preLogTerm)
                        .setCommitIndex(commitIndex) // tell follower the latest commit index
                        .setReadIndex(readIndex);
                    entries.forEachRemaining(builder::addEntries);
                    AppendEntries appendEntries = builder.build();
                    messages.add(RaftMessage.newBuilder()
                        .setTerm(currentTerm())
                        .setAppendEntries(appendEntries)
                        .build());

                    assert appendEntries.getEntriesCount() != 0;
                    logTrace("Prepare {} entries after "
                            + "entry[index:{},term:{}] for peer[{}] with readIndex[{}] when {}",
                        appendEntries.getEntriesCount(),
                        preLogIndex,
                        preLogTerm,
                        peer,
                        readIndex,
                        peerLogTracker.status(peer));
                    peerLogTracker.replicateBy(peer,
                        appendEntries.getEntries(appendEntries.getEntriesCount() - 1).getIndex());
                    break;
                }
                if (forceHeartbeat || peerLogTracker.needHeartbeat(peer)) {
                    // no entries to append
                    logTrace("Prepare heartbeat after "
                            + "entry[index:{},term:{}] for peer[{}] with readIndex[{}] when {}",
                        preLogIndex, preLogTerm, peer, readIndex, peerLogTracker.status(peer));
                    messages.add(RaftMessage.newBuilder()
                        .setTerm(currentTerm())
                        .setAppendEntries(AppendEntries
                            .newBuilder()
                            .setLeaderId(stateStorage.local())
                            .setPrevLogIndex(preLogIndex)
                            .setPrevLogTerm(preLogTerm)
                            .setCommitIndex(commitIndex) // tell follower the latest commit index
                            .setReadIndex(readIndex)
                            .build())
                        .build());
                    peerLogTracker.replicateBy(peer, preLogIndex);
                }
            }
        }
        return messages;
    }

    private RaftNodeState commit() {
        List<String> voters = stateStorage.latestClusterConfig().getVotersList();
        List<String> nextVoters = stateStorage.latestClusterConfig().getNextVotersList();

        List<Long> mIdx = voters.stream()
            .map(peerLogTracker::matchIndex)
            .sorted(Long::compareTo)
            .collect(Collectors.toList());
        long newCommitIndex = mIdx.get(mIdx.size() - ((mIdx.size() >> 1) + 1));
        if (!nextVoters.isEmpty()) {
            // in joint-consensus, take the lease commitIndex of two voter groups
            mIdx = nextVoters.stream().map(peerLogTracker::matchIndex)
                .collect(Collectors.toList());
            mIdx.sort(Long::compareTo);
            newCommitIndex = Math.min(newCommitIndex, mIdx.get(mIdx.size() - ((mIdx.size() >> 1) + 1)));
        }

        // only commit in leader's term according to $3.6
        Optional<LogEntry> committed = stateStorage.entryAt(newCommitIndex);
        if (committed.isPresent() && committed.get().getTerm() != currentTerm()) {
            return this;
        }
        // we may draw conclusion about same commitIndex multiple times
        boolean needNotify = commitIndex != newCommitIndex;
        // log entry of current term committed, advance the commitIndex;
        if (needNotify) {
            commitIndex = newCommitIndex;
        }
        RaftNodeState nextState = this;
        if (configChanger.commitTo(commitIndex, currentTerm())) {
            // config changer state changed after committing some log entries
            switch (configChanger.state()) {
                case Waiting -> {
                    String localId = stateStorage.local();
                    if (!stateStorage.latestClusterConfig().getVotersList().contains(localId)) {
                        // leader has been removed from the latest config, say goodbye to
                        // all remote peers both in prev and current config
                        Set<String> allRemotePeers = new HashSet<>(configChanger.prevConfig().getVotersList());
                        allRemotePeers.addAll(configChanger.prevConfig().getLearnersList());
                        allRemotePeers.addAll(configChanger.remotePeers());
                        allRemotePeers.remove(stateStorage.local());
                        if (!allRemotePeers.isEmpty()) {
                            Map<String, List<RaftMessage>> appendEntriesToSend = allRemotePeers.stream()
                                .collect(Collectors.toMap(
                                    peerId -> peerId,
                                    peerId -> prepareAppendEntriesForPeer(peerId, true)
                                ));
                            logDebug("Leader is about to step down, send final heartbeats to all remote peers[{}]",
                                allRemotePeers);
                            submitRaftMessages(appendEntriesToSend);
                        }
                        // target config has been committed, step down if local server has been removed from voters
                        // abort pending read index requests if any
                        logDebug("Leader stepped down due to being removed from cluster config");
                        readProgressTracker.abort();
                        nextState = new RaftNodeStateFollower(
                            currentTerm(),
                            commitIndex,
                            null,
                            config,
                            stateStorage,
                            log,
                            uncommittedProposals,
                            sender,
                            listener,
                            snapshotInstaller,
                            onSnapshotInstalled,
                            tags
                        );
                        // don't notify rep status change since leader has stepped down now
                        configChanger.confirmCommit(false);
                    } else {
                        // say goodbye to all removed peers
                        Set<String> removedPeers = new HashSet<>(configChanger.prevConfig().getVotersList());
                        removedPeers.addAll(configChanger.prevConfig().getLearnersList());
                        removedPeers.removeAll(configChanger.remotePeers());
                        removedPeers.remove(stateStorage.local());
                        if (!removedPeers.isEmpty()) {
                            Map<String, List<RaftMessage>> appendEntriesToSend = removedPeers.stream()
                                .collect(Collectors.toMap(
                                    peerId -> peerId,
                                    peerId -> prepareAppendEntriesForPeer(peerId, true)
                                ));
                            logDebug("Send final heartbeats to removed peers[{}]", removedPeers);
                            submitRaftMessages(appendEntriesToSend);
                        }
                        // confirm the commit so that peers tracker of removed peers could be cleaned
                        configChanger.confirmCommit(true);
                    }
                }
                case TargetConfigCommitting -> {
                    // joint config has committed and now target config is appended to log
                    ClusterConfig clusterConfig = stateStorage.latestClusterConfig();
                    activityTracker.refresh(clusterConfig);
                    if (leaderTransferTask != null
                        && !clusterConfig.getVotersList().contains(leaderTransferTask.nextLeader)) {
                        // abort the transfer
                        logInfo("Aborted transfer leadership to follower[{}], it's removed from target cluster config",
                            leaderTransferTask.nextLeader);
                        leaderTransferTask.abort(LeaderTransferException.notFoundOrQualified());
                        leaderTransferTask = null;
                    }
                }
            }
        }
        if (needNotify) {
            notifyCommit();
        }
        return nextState;
    }

    private void handleRequestReadIndex(String fromPeer, RequestReadIndex request) {
        logTrace("Received forwarded ReadIndex request from peer[{}]", fromPeer);
        CompletableFuture<Long> onDone = new CompletableFuture<>();
        onDone.whenComplete((readIndex, e) -> {
            // must be executed in raft thread
            if (e != null) {
                logDebug("Failed to finish forwarded ReadIndex request from peer[{}]", fromPeer, e);
            } else {
                // don't pass exception, let follower abort itself
                submitRaftMessages(fromPeer, RaftMessage.newBuilder()
                    .setTerm(currentTerm())
                    .setRequestReadIndexReply(RequestReadIndexReply.newBuilder()
                        .setId(request.getId())
                        .setReadIndex(readIndex)
                        .build())
                    .build());
            }
        });
        readIndex(onDone);
    }

    private void handlePropose(String fromPeer, Propose propose) {
        logTrace("Received forwarded Propose request from peer[{}]", fromPeer);
        CompletableFuture<Long> onDone = new CompletableFuture<>();
        onDone.whenComplete((v, e) -> {
            // must be executed in raft thread
            if (e != null) {
                logDebug("Failed to finish forwarded Propose request from peer[{}]", fromPeer, e);
                submitRaftMessages(fromPeer, RaftMessage.newBuilder()
                    .setTerm(currentTerm())
                    .setProposeReply(ProposeReply.newBuilder()
                        .setId(propose.getId())
                        // only two exceptions available now
                        .setCode(e.getClass() == DropProposalException.TransferringLeaderException.class ?
                            ProposeReply.Code.DropByLeaderTransferring :
                            ProposeReply.Code.DropByMaxUnappliedEntries
                        )
                        .build())
                    .build());
            } else {
                submitRaftMessages(fromPeer, RaftMessage.newBuilder()
                    .setTerm(currentTerm())
                    .setProposeReply(ProposeReply.newBuilder()
                        .setId(propose.getId())
                        .setCode(ProposeReply.Code.Success)
                        .setLogIndex(v)
                        .build())
                    .build());
            }
        });
        propose(propose.getCommand(), onDone);
    }

    private void sendTimeoutNow(String toPeer) {
        submitRaftMessages(toPeer,
            RaftMessage.newBuilder().setTerm(currentTerm()).setTimeoutNow(TimeoutNow.getDefaultInstance()).build());
    }

    private boolean isJoint(ClusterConfig clusterConfig) {
        return !clusterConfig.getNextVotersList().isEmpty();
    }

    private boolean commitIndexNotConfirmed() {
        Optional<LogEntry> committed = stateStorage.entryAt(commitIndex);
        return !committed.map(logEntry -> logEntry.getTerm() == currentTerm())
            .orElseGet(() -> stateStorage.latestSnapshot().getTerm() == currentTerm());

    }
}
