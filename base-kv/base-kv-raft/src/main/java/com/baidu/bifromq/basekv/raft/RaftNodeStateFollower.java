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

import static com.baidu.bifromq.basekv.raft.exception.DropProposalException.superseded;

import com.baidu.bifromq.basekv.raft.exception.ClusterConfigChangeException;
import com.baidu.bifromq.basekv.raft.exception.DropProposalException;
import com.baidu.bifromq.basekv.raft.exception.LeaderTransferException;
import com.baidu.bifromq.basekv.raft.exception.ReadIndexException;
import com.baidu.bifromq.basekv.raft.exception.RecoveryException;
import com.baidu.bifromq.basekv.raft.proto.AppendEntries;
import com.baidu.bifromq.basekv.raft.proto.AppendEntriesReply;
import com.baidu.bifromq.basekv.raft.proto.InstallSnapshot;
import com.baidu.bifromq.basekv.raft.proto.InstallSnapshotReply;
import com.baidu.bifromq.basekv.raft.proto.LogEntry;
import com.baidu.bifromq.basekv.raft.proto.Propose;
import com.baidu.bifromq.basekv.raft.proto.ProposeReply;
import com.baidu.bifromq.basekv.raft.proto.RaftMessage;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.baidu.bifromq.basekv.raft.proto.RequestReadIndex;
import com.baidu.bifromq.basekv.raft.proto.RequestReadIndexReply;
import com.baidu.bifromq.basekv.raft.proto.RequestVote;
import com.baidu.bifromq.basekv.raft.proto.Snapshot;
import com.baidu.bifromq.basekv.raft.proto.Voting;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;

class RaftNodeStateFollower extends RaftNodeState {
    private static class StabilizingTask {
        int pendingReplyCount = 0;
        long readIndex = -1;
        boolean committed = false;
    }

    private final TreeMap<Long, StabilizingTask> stabilizingIndexes = new TreeMap<>(Long::compareTo);
    private final LinkedHashMap<Long, Set<Integer>> tickToReadRequestsMap;
    private final Map<Integer, CompletableFuture<Long>> idToReadRequestMap;
    private final LinkedHashMap<Long, Set<Integer>> tickToForwardedProposesMap;
    private final Map<Integer, CompletableFuture<Long>> idToForwardedProposeMap;
    private int randomElectionTimeoutTick;
    private long currentTick;
    private int electionElapsedTick;
    private String currentLeader; // leader in current term
    private InstallSnapshot currentISSRequest;
    private int forwardReqId = 0;

    RaftNodeStateFollower(long term,
                          long commitIndex,
                          String leader,
                          RaftConfig config,
                          IRaftStateStore stateStorage,
                          Logger log,
                          IRaftNode.IRaftMessageSender sender,
                          IRaftNode.IRaftEventListener listener,
                          IRaftNode.ISnapshotInstaller installer,
                          OnSnapshotInstalled onSnapshotInstalled,
                          String... tags) {
        this(term,
            commitIndex,
            leader,
            config,
            stateStorage,
            log,
            new LinkedHashMap<>(),
            sender,
            listener,
            installer,
            onSnapshotInstalled,
            tags);
    }

    RaftNodeStateFollower(long term,
                          long commitIndex,
                          String leader,
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
        currentLeader = leader;
        randomElectionTimeoutTick = randomizeElectionTimeoutTick();
        tickToReadRequestsMap = new LinkedHashMap<>();
        idToReadRequestMap = new HashMap<>();
        tickToForwardedProposesMap = new LinkedHashMap<>();
        idToForwardedProposeMap = new HashMap<>();
    }

    @Override
    RaftNodeStatus getState() {
        return RaftNodeStatus.Follower;
    }

    @Override
    String currentLeader() {
        return currentLeader;
    }

    @Override
    RaftNodeState stepDown() {
        return this;
    }

    @Override
    RaftNodeState recover(CompletableFuture<Void> onDone) {
        onDone.completeExceptionally(RecoveryException.notLostQuorum());
        return this;
    }

    @Override
    RaftNodeState tick() {
        currentTick++;
        electionElapsedTick++;
        for (Iterator<Map.Entry<Long, Set<Integer>>> it = tickToReadRequestsMap.entrySet().iterator();
             it.hasNext(); ) {
            Map.Entry<Long, Set<Integer>> entry = it.next();
            if (entry.getKey() + 2L * config.getHeartbeatTimeoutTick() < currentTick) {
                // pending elapsed ticks exceed two times heartbeatTimeout, doing cleanup
                it.remove();
                entry.getValue().forEach(pendingReadId -> {
                    CompletableFuture<Long> pendingOnDone = idToReadRequestMap.remove(pendingReadId);
                    if (pendingOnDone != null && !pendingOnDone.isDone()) {
                        // if not finished by requestReadIndexReply then abort it
                        logDebug("Aborted forwarded timed-out ReadIndex request[{}]", pendingReadId);
                        pendingOnDone.completeExceptionally(ReadIndexException.forwardTimeout());
                    }
                });
            } else {
                break;
            }
        }
        for (Iterator<Map.Entry<Long, Set<Integer>>> it = tickToForwardedProposesMap.entrySet().iterator();
             it.hasNext(); ) {
            Map.Entry<Long, Set<Integer>> entry = it.next();
            if (entry.getKey() + 2L * config.getHeartbeatTimeoutTick() < currentTick) {
                // pending elapsed ticks exceed two times heartbeatTimeout, doing cleanup
                it.remove();
                entry.getValue().forEach(pendingProposalId -> {
                    CompletableFuture<Long> pendingOnDone = idToForwardedProposeMap.remove(pendingProposalId);
                    if (pendingOnDone != null && !pendingOnDone.isDone()) {
                        // if not finished by proposeReply then abort it
                        logDebug("Aborted forwarded timed-out Propose request[{}]", pendingProposalId);
                        pendingOnDone.completeExceptionally(DropProposalException.forwardTimeout());
                    }
                });
            } else {
                break;
            }
        }
        if (electionElapsedTick >= randomElectionTimeoutTick) {
            electionElapsedTick = 0;
            abortPendingRequests();
            logDebug("Transit to candidate due to election timeout[{}]", randomElectionTimeoutTick);
            return new RaftNodeStateCandidate(
                currentTerm(),
                commitIndex,
                config,
                stateStorage,
                log,
                uncommittedProposals,
                sender,
                listener,
                snapshotInstaller,
                onSnapshotInstalled,
                tags)
                .campaign(config.isPreVote(), false);
        }
        return this;
    }

    @Override
    void propose(ByteString fsmCmd, CompletableFuture<Long> onDone) {
        if (config.isDisableForwardProposal()) {
            logDebug("Forward proposal to leader is disabled");
            onDone.completeExceptionally(DropProposalException.leaderForwardDisabled());
            return;
        }
        if (currentLeader == null) {
            logDebug("Dropped proposal due to no leader elected in current term");
            onDone.completeExceptionally(DropProposalException.NoLeader());
            return;
        }
        if (isProposeThrottled()) {
            logDebug("Dropped proposal due to log growing[uncommittedProposals:{}] "
                    + "exceeds threshold[maxUncommittedProposals:{}]",
                uncommittedProposals.size(), maxUncommittedProposals);
            onDone.completeExceptionally(DropProposalException.throttledByThreshold());
            return;
        }
        int forwardProposalId = nextForwardReqId();
        tickToForwardedProposesMap.compute(currentTick, (k, v) -> {
            if (v == null) {
                v = new HashSet<>();
            }
            v.add(forwardProposalId);
            return v;
        });
        idToForwardedProposeMap.put(forwardProposalId, onDone);
        submitRaftMessages(currentLeader, RaftMessage.newBuilder()
            .setTerm(currentTerm())
            .setPropose(Propose.newBuilder()
                .setId(forwardProposalId)
                .setCommand(fsmCmd)
                .build())
            .build());
    }

    @Override
    RaftNodeState stableTo(long stabledIndex) {
        // send append entries reply for stabilized requests
        Set<Long> toRemove = new HashSet<>();
        for (Long index : stabilizingIndexes.keySet()) {
            if (index <= stabledIndex) {
                StabilizingTask task = stabilizingIndexes.get(index);
                while (task.pendingReplyCount-- > 0) {
                    if (currentLeader != null) {
                        logTrace("Entries below index[{}] stabilized, reply to leader[{}]",
                            index, currentLeader);
                        submitRaftMessages(currentLeader, RaftMessage.newBuilder()
                            .setTerm(currentTerm())
                            .setAppendEntriesReply(
                                AppendEntriesReply.newBuilder()
                                    .setAccept(AppendEntriesReply.Accept.newBuilder()
                                        .setLastIndex(index)
                                        .build())
                                    .setReadIndex(task.readIndex)
                                    .build())
                            .build());
                    }
                }
                if (task.committed) {
                    commitIndex = index;
                    logTrace("Advanced commitIndex[{}]", commitIndex);
                    notifyCommit();
                }
                toRemove.add(index);
            } else {
                break;
            }
        }
        stabilizingIndexes.keySet().removeAll(toRemove);
        return this;
    }

    @Override
    void readIndex(CompletableFuture<Long> onDone) {
        if (currentLeader == null) {
            logDebug("Dropped ReadIndex forwarding due to no leader elected in current term");
            onDone.completeExceptionally(ReadIndexException.noLeader());
        } else {
            int forwardReadId = nextForwardReqId();
            tickToReadRequestsMap.compute(currentTick, (k, v) -> {
                if (v == null) {
                    v = new HashSet<>();
                }
                v.add(forwardReadId);
                return v;
            });
            idToReadRequestMap.put(forwardReadId, onDone);
            submitRaftMessages(currentLeader, RaftMessage.newBuilder()
                .setTerm(currentTerm())
                .setRequestReadIndex(RequestReadIndex.newBuilder()
                    .setId(forwardReadId)
                    .build())
                .build());
        }
    }

    @Override
    RaftNodeState receive(String fromPeer, RaftMessage message) {
        logTrace("Receive[{}] from {}", message, fromPeer);
        RaftNodeState nextState = this;
        if (message.getTerm() > currentTerm()) {
            switch (message.getMessageTypeCase()) {
                case REQUESTPREVOTE:
                    if (!inLease()) {
                        handlePreVote(fromPeer, message.getTerm(), message.getRequestPreVote());
                    } else {
                        sendRequestPreVoteReply(fromPeer, message.getTerm(), false);
                    }
                    return nextState;
                case REQUESTPREVOTEREPLY:
                    // ignore the higher term pre-vote reply which may be a delayed reply of previous pre-vote
                    return nextState;
                case REQUESTVOTE:
                    // prevent from being disrupted
                    boolean leaderTransfer = message.getRequestVote().getLeaderTransfer();
                    if (!leaderTransfer && inLease() && !voters().contains(fromPeer)) {
                        logDebug("Vote[{}] from candidate[{}] not granted, lease is not expired",
                            message.getTerm(), fromPeer);
                        sendRequestVoteReply(fromPeer, message.getTerm(), false);
                        return nextState;
                    }
                    // fallthrough
                default:
                    logDebug("Higher term[{}] message[{}] received from peer[{}]",
                        message.getTerm(), message.getMessageTypeCase(), fromPeer);
                    stateStorage.saveTerm(message.getTerm());
                    // clear the known leader from prev term, otherwise canGrant may report wrong result
                    currentLeader = null;
            }
        } else if (message.getTerm() < currentTerm()) {
            handleLowTermMessage(fromPeer, message);
            return nextState;
        }
        // term match
        switch (message.getMessageTypeCase()) {
            case APPENDENTRIES:
                electionElapsedTick = 0; // reset tick
                randomElectionTimeoutTick = randomizeElectionTimeoutTick();
                handleAppendEntries(fromPeer, message.getAppendEntries());
                break;
            case INSTALLSNAPSHOT:
                electionElapsedTick = 0; // reset tick
                randomElectionTimeoutTick = randomizeElectionTimeoutTick();
                handleSnapshot(fromPeer, message.getInstallSnapshot());
                break;
            case REQUESTPREVOTE:
                // reject the pre-vote
                sendRequestPreVoteReply(fromPeer, currentTerm(), false);
                break;
            case REQUESTVOTE:
                handleVote(fromPeer, message.getRequestVote());
                break;
            case REQUESTREADINDEXREPLY:
                handleRequestReadIndexReply(message.getRequestReadIndexReply());
                break;
            case PROPOSEREPLY:
                handleProposeReply(message.getProposeReply());
                break;
            case TIMEOUTNOW:
                nextState = handleTimeoutNow(fromPeer);
            default:
        }
        return nextState;
    }

    @Override
    void transferLeadership(String newLeader, CompletableFuture<Void> onDone) {
        onDone.completeExceptionally(LeaderTransferException.notLeader());
    }

    @Override
    void changeClusterConfig(String correlateId,
                             Set<String> newVoters,
                             Set<String> newLearners,
                             CompletableFuture<Void> onDone) {
        // TODO: support leader forward
        onDone.completeExceptionally(ClusterConfigChangeException.notLeader());
    }

    @Override
    void onSnapshotRestored(ByteString fsmSnapshot, Throwable ex) {
        if (ex != null) {
            logWarn("Failed to restore snapshot", ex);
        }
        if (currentISSRequest == null) {
            return;
        }
        InstallSnapshot iss = currentISSRequest;
        Snapshot snapshot = iss.getSnapshot();
        if (snapshot.getData() != fsmSnapshot) {
            logDebug("Skip reply for old snapshot installation");
            return;
        }
        currentISSRequest = null;
        RaftMessage reply;
        if (ex != null) {
            logWarn("Snapshot[index:{},term:{}] rejected by FSM", snapshot.getIndex(), snapshot.getTerm(), ex);
            reply = RaftMessage.newBuilder()
                .setTerm(currentTerm())
                .setInstallSnapshotReply(
                    InstallSnapshotReply
                        .newBuilder()
                        .setRejected(true)
                        .setLastIndex(snapshot.getIndex())
                        .setReadIndex(iss.getReadIndex())
                        .build())
                .build();
        } else {
            logDebug("Snapshot[index:{},term:{}] accepted by FSM", snapshot.getIndex(), snapshot.getTerm());
            try {
                stateStorage.applySnapshot(snapshot);
                // reset commitIndex to last index in snapshot
                commitIndex = snapshot.getIndex();
                reply = RaftMessage.newBuilder()
                    .setTerm(currentTerm())
                    .setInstallSnapshotReply(
                        InstallSnapshotReply
                            .newBuilder()
                            .setRejected(false)
                            .setLastIndex(snapshot.getIndex())
                            .setReadIndex(iss.getReadIndex())
                            .build())
                    .build();
                notifySnapshotRestored();
                abortPendingRequests();
                // Abort all uncommitted proposals inherited from previous leader state, since they may be superseded by snapshot
                for (Iterator<Map.Entry<Long, ProposeTask>> it = uncommittedProposals.entrySet().iterator();
                     it.hasNext(); ) {
                    Map.Entry<Long, ProposeTask> entry = it.next();
                    entry.getValue().future.completeExceptionally(superseded());
                    it.remove();
                }
            } catch (Throwable e) {
                logError("Failed to apply snapshot[index:{}, term:{}]", snapshot.getIndex(), snapshot.getTerm(), e);
                reply = RaftMessage.newBuilder()
                    .setTerm(currentTerm())
                    .setInstallSnapshotReply(
                        InstallSnapshotReply
                            .newBuilder()
                            .setRejected(true)
                            .setLastIndex(snapshot.getIndex())
                            .setReadIndex(iss.getReadIndex())
                            .build())
                    .build();
            }
        }
        submitRaftMessages(iss.getLeaderId(), reply);
    }

    private void handleAppendEntries(String fromLeader, AppendEntries appendEntries) {
        if (!appendEntries.getLeaderId().equals(currentLeader)) {
            logDebug("Leader[{}] of current term[{}] elected", appendEntries.getLeaderId(), currentTerm());
            currentLeader = appendEntries.getLeaderId();
        }
        if (!entryMatch(appendEntries.getPrevLogIndex(), appendEntries.getPrevLogTerm())) {
            // prevLogEntry mismatch, reject
            logDebug("Rejected {} entries from leader[{}] due to mismatched last entry[index:{},term:{}]",
                appendEntries.getEntriesCount(),
                fromLeader,
                appendEntries.getPrevLogIndex(),
                appendEntries.getPrevLogTerm());
            Optional<LogEntry> lastEntry = stateStorage.entryAt(stateStorage.lastIndex());
            long lastEntryTerm = lastEntry.map(LogEntry::getTerm)
                .orElseGet(() -> stateStorage.latestSnapshot().getTerm());
            submitRaftMessages(fromLeader, RaftMessage.newBuilder()
                .setTerm(currentTerm())
                .setAppendEntriesReply(
                    AppendEntriesReply.newBuilder()
                        .setReject(AppendEntriesReply.Reject.newBuilder()
                            .setLastIndex(stateStorage.lastIndex())
                            .setTerm(lastEntryTerm)
                            .setRejectedIndex(appendEntries.getPrevLogIndex())
                            .build())
                        .setReadIndex(appendEntries.getReadIndex())
                        .build())
                .build());
        } else {
            if (appendEntries.getEntriesCount() > 0) {
                // prevLogEntry match, filter out duplicated append entries requests
                long newLastIndex = appendEntries.getEntries(appendEntries.getEntriesCount() - 1).getIndex();
                logDebug("Append {} entries after entry[index:{},term:{}]",
                    appendEntries.getEntriesCount(),
                    appendEntries.getPrevLogIndex(),
                    appendEntries.getPrevLogTerm());
                stabilizingIndexes.compute(newLastIndex, (k, v) -> {
                    if (v == null) {
                        v = new StabilizingTask();
                    }
                    v.pendingReplyCount++;
                    v.readIndex = appendEntries.getReadIndex();
                    return v;
                });
                // the higher index tasks are obsolete
                stabilizingIndexes.tailMap(newLastIndex, false).clear();
                stateStorage.append(appendEntries.getEntriesList(), !config.isAsyncAppend());
            } else {
                // heartbeat, reply immediately
                submitRaftMessages(currentLeader, RaftMessage.newBuilder()
                    .setTerm(currentTerm())
                    .setAppendEntriesReply(
                        AppendEntriesReply.newBuilder()
                            .setAccept(AppendEntriesReply.Accept.newBuilder()
                                .setLastIndex(appendEntries.getPrevLogIndex())
                                .build())
                            .setReadIndex(appendEntries.getReadIndex())
                            .build())
                    .build());
            }
            long newCommitIndex = appendEntries.getCommitIndex();
            if (commitIndex < newCommitIndex) {
                if (stabilizingIndexes.isEmpty()) {
                    // all entries have been stabilized
                    if (newCommitIndex <= stateStorage.lastIndex()) {
                        commitIndex = newCommitIndex;
                        logTrace("Advanced commitIndex[{}]", commitIndex);
                        // report to application
                        notifyCommit();
                    } else {
                        // entries between lastIndex and newCommitIndex missing, probably because the channel between
                        // leader and follower is lossy.
                        // In this case add it to stabilizingIndexes
                        log.debug("Entries[from:{},to:{}] missing locally", stateStorage.lastIndex(), newCommitIndex);
                        stabilizingIndexes.compute(stateStorage.lastIndex(), (k, v) -> {
                            if (v == null) {
                                v = new StabilizingTask();
                            }
                            v.committed = true; // commit after stabilized
                            return v;
                        });
                    }
                    return;
                }
                if (newCommitIndex < stabilizingIndexes.firstKey()) {
                    // if the new commitIndex has been stabilized locally, then advance local commitIndex directly
                    commitIndex = newCommitIndex;
                    logTrace("Advanced commitIndex[{}]", commitIndex);
                    // report to application
                    notifyCommit();
                } else {
                    if (newCommitIndex > stabilizingIndexes.lastKey()) {
                        // if the newCommitIndex is greater than the largest local stabilizing index
                        // add it to stabilizingIndexes and mark it has been committed
                        logDebug("Entries[from:{},to:{}] missing locally",
                            stabilizingIndexes.lastKey(), newCommitIndex);
                    }
                    stabilizingIndexes.compute(stateStorage.lastIndex(), (k, v) -> {
                        if (v == null) {
                            v = new StabilizingTask();
                        }
                        v.committed = true; // notify commit after stabilized
                        return v;
                    });
                }
            }
        }
    }

    private void handleSnapshot(String fromLeader, InstallSnapshot installSnapshot) {
        if (!installSnapshot.getLeaderId().equals(currentLeader)) {
            logDebug("Leader[{}] of current term elected", installSnapshot.getLeaderId());
            currentLeader = installSnapshot.getLeaderId();
        }
        Snapshot snapshot = installSnapshot.getSnapshot();

        Snapshot latestSnapshot = stateStorage.latestSnapshot();
        if (latestSnapshot.getIndex() > snapshot.getIndex() && latestSnapshot.getTerm() > snapshot.getTerm()) {
            // ignore obsolete snapshot
            logDebug("Ignore obsolete snapshot[index:{},term:{}] from peer[{}]",
                snapshot.getIndex(), snapshot.getTerm(), fromLeader);
            return;
        }

        currentISSRequest = installSnapshot;
        submitSnapshot(snapshot.getData());
        logDebug("Snapshot[index:{},term:{}] from peer[{}] submitted to FSM",
            snapshot.getIndex(), snapshot.getTerm(), fromLeader);
    }

    private void handleVote(String fromPeer, RequestVote request) {
        boolean canGrantVote = canGrantVote(request.getCandidateId(), request.getLeaderTransfer());
        boolean isLogUpToDate = isUpToDate(request.getLastLogTerm(), request.getLastLogIndex());
        boolean vote = canGrantVote && isLogUpToDate;
        if (vote) {
            // persist the state
            stateStorage.saveVoting(Voting.newBuilder()
                .setTerm(currentTerm()).setFor(request.getCandidateId()).build());
            // reset election tick when grant a vote
            electionElapsedTick = 0;
        }
        logDebug("Vote for peer[{}] with last log[index={}, term={}]? {}, grant? {}, log up-to-date? {}",
            fromPeer, request.getLastLogIndex(), request.getLastLogTerm(), vote, canGrantVote, isLogUpToDate);
        sendRequestVoteReply(fromPeer, currentTerm(), vote);
    }

    private void handleRequestReadIndexReply(RequestReadIndexReply reply) {
        CompletableFuture<Long> pendingOnDone = idToReadRequestMap.get(reply.getId());
        if (pendingOnDone != null) {
            pendingOnDone.complete(reply.getReadIndex());
        }
    }

    private void handleProposeReply(ProposeReply reply) {
        CompletableFuture<Long> pendingOnDone = idToForwardedProposeMap.get(reply.getId());
        if (pendingOnDone != null) {
            switch (reply.getCode()) {
                case Success -> pendingOnDone.complete(reply.getLogIndex());
                case DropByLeaderTransferring -> pendingOnDone.completeExceptionally(
                    DropProposalException.transferringLeader());
                case DropByMaxUnappliedEntries -> pendingOnDone.completeExceptionally(
                    DropProposalException.throttledByThreshold());
            }
        }
    }

    private RaftNodeState handleTimeoutNow(String fromLeader) {
        if (promotable()) {
            logInfo("Transited to candidate now by request from current leader[{}]", fromLeader);
            abortPendingRequests();
            return new RaftNodeStateCandidate(
                currentTerm(),
                commitIndex,
                config,
                stateStorage,
                log,
                uncommittedProposals,
                sender,
                listener,
                snapshotInstaller,
                onSnapshotInstalled,
                tags
            ).campaign(config.isPreVote(), true);
        }
        return this;
    }

    private int nextForwardReqId() {
        return forwardReqId = (forwardReqId + 1) % Integer.MAX_VALUE;
    }

    private boolean inLease() {
        // if quorum check enabled in local, then check if it's keeping touch with a known leader
        // Note: the known 'leader' may be the leader of isolated minority if it doesn't enable checkQuorum
        return currentLeader != null && electionElapsedTick < config.getElectionTimeoutTick();
    }

    private boolean canGrantVote(String candidateId, boolean leaderTransfer) {
        Optional<Voting> vote = stateStorage.currentVoting();
        // repeated vote or no leader elected while have not voted yet
        return (vote.isPresent() && vote.get().getTerm() == currentTerm() && vote.get().getFor().equals(candidateId))
            || ((currentLeader == null || leaderTransfer)
            && (vote.isEmpty() || vote.get().getTerm() < currentTerm()));
    }

    private void abortPendingRequests() {
        for (Iterator<Map.Entry<Long, Set<Integer>>> it = tickToReadRequestsMap.entrySet().iterator();
             it.hasNext(); ) {
            Map.Entry<Long, Set<Integer>> entry = it.next();
            it.remove();
            entry.getValue().forEach(pendingReadId -> {
                CompletableFuture<Long> pendingOnDone = idToReadRequestMap.remove(pendingReadId);
                if (pendingOnDone != null && !pendingOnDone.isDone()) {
                    // if not finished by requestReadIndexReply then abort it
                    pendingOnDone.completeExceptionally(ReadIndexException.forwardTimeout());
                }
            });
        }
        for (Iterator<Map.Entry<Long, Set<Integer>>> it = tickToForwardedProposesMap.entrySet().iterator();
             it.hasNext(); ) {
            Map.Entry<Long, Set<Integer>> entry = it.next();
            it.remove();
            entry.getValue().forEach(pendingProposalId -> {
                CompletableFuture<Long> pendingOnDone = idToForwardedProposeMap.remove(pendingProposalId);
                if (pendingOnDone != null && !pendingOnDone.isDone()) {
                    // if not finished by requestReadIndexReply then abort it
                    pendingOnDone.completeExceptionally(DropProposalException.forwardTimeout());
                }
            });
        }
    }

    private boolean entryMatch(long index, long term) {
        Optional<LogEntry> entry = stateStorage.entryAt(index);
        if (entry.isPresent()) {
            return entry.get().getTerm() == term;
        } else {
            Snapshot snapshot = stateStorage.latestSnapshot();
            return snapshot.getIndex() == index && snapshot.getTerm() == term;
        }
    }
}