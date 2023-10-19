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

package com.baidu.bifromq.basekv.store.wal;

import com.baidu.bifromq.basekv.proto.KVRangeCommand;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeSnapshot;
import com.baidu.bifromq.basekv.raft.event.ElectionEvent;
import com.baidu.bifromq.basekv.raft.event.SnapshotRestoredEvent;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.raft.proto.LogEntry;
import com.baidu.bifromq.basekv.raft.proto.RaftMessage;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeSyncState;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import lombok.AllArgsConstructor;

public interface IKVRangeWAL {
    @AllArgsConstructor
    class SnapshotInstallTask {
        public final ByteString snapshot;
        public final CompletableFuture<Void> onDone = new CompletableFuture<>();
    }

    String storeId();

    KVRangeId rangeId();

    boolean isLeader();

    Optional<String> currentLeader();

    RaftNodeStatus currentState();

    Observable<RaftNodeStatus> state();

    Observable<ElectionEvent> election();

    ClusterConfig clusterConfig();

    KVRangeSnapshot latestSnapshot();

    CompletableFuture<Long> propose(KVRangeCommand command);

    Observable<Map<String, RaftNodeSyncState>> replicationStatus();

    IKVRangeWALSubscription subscribe(long lastFetchedIndex, IKVRangeWALSubscriber subscriber, Executor executor);

    CompletableFuture<LogEntry> once(long lastFetchedIndex, Predicate<LogEntry> condition, Executor executor);

    Observable<Long> commitIndex();

    Observable<SnapshotRestoredEvent> snapshotRestoreEvent();

    CompletableFuture<Iterator<LogEntry>> retrieveCommitted(long fromIndex, long maxSize);

    CompletableFuture<Long> readIndex();

    CompletableFuture<Void> transferLeadership(String peerId);

    boolean stepDown();

    CompletableFuture<Void> changeClusterConfig(String correlateId, Set<String> voters, Set<String> learners);

    CompletableFuture<Void> compact(KVRangeSnapshot snapshot);

    Observable<SnapshotInstallTask> snapshotInstallTask();

    Observable<Map<String, List<RaftMessage>>> peerMessages();

    CompletableFuture<Void> recover();

    void receivePeerMessages(String peerId, List<RaftMessage> messages);

    long logDataSize();

    void tick();

    void start();

    CompletableFuture<Void> close();

    CompletableFuture<Void> destroy();
}
