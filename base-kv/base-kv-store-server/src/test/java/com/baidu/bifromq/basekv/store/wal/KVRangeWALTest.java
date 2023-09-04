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

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.baidu.bifromq.basekv.proto.KVRangeCommand;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.raft.IRaftStateStore;
import com.baidu.bifromq.basekv.raft.InMemoryStateStore;
import com.baidu.bifromq.basekv.raft.RaftConfig;
import com.baidu.bifromq.basekv.raft.event.CommitEvent;
import com.baidu.bifromq.basekv.raft.event.ElectionEvent;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.raft.proto.RaftMessage;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.baidu.bifromq.basekv.raft.proto.Snapshot;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.observers.TestObserver;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class KVRangeWALTest {

    private KVRangeId id = KVRangeIdUtil.generate();

    private String replicaId = "s1";

    private IRaftStateStore raftStateStorage = new InMemoryStateStore(replicaId, Snapshot.newBuilder()
        .setClusterConfig(ClusterConfig.newBuilder()
            .addVoters(replicaId)
            .build())
        .build());

    private RaftConfig config = new RaftConfig();
    private ScheduledExecutorService ticker;
    @Mock
    private IKVRangeWALStoreEngine walStorageEngine;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        raftStateStorage = new InMemoryStateStore(replicaId, Snapshot.newBuilder()
            .setClusterConfig(ClusterConfig.newBuilder()
                .addVoters(replicaId)
                .build())
            .build());
        ticker = new ScheduledThreadPoolExecutor(1);
    }

    @AfterMethod
    public void teardown() throws Exception {
        MoreExecutors.shutdownAndAwaitTermination(ticker, 5, TimeUnit.SECONDS);
        raftStateStorage.stop();
        closeable.close();
    }


    @Test
    public void testId() {
        when(walStorageEngine.id()).thenReturn(replicaId);
        when(walStorageEngine.get(id)).thenReturn(raftStateStorage);

        KVRangeWAL wal = new KVRangeWAL("testcluster", id, walStorageEngine, config, 1024);
        assertEquals(wal.storeId(), replicaId);
    }

    @SneakyThrows
    @Test
    public void testElectionObservable() {
        when(walStorageEngine.id()).thenReturn(replicaId);
        when(walStorageEngine.get(id)).thenReturn(raftStateStorage);

        KVRangeWAL wal = new KVRangeWAL("testcluster", id, walStorageEngine, config, 1024);
        TestObserver<ElectionEvent> testObserver = new TestObserver<>();
        wal.election().subscribe(testObserver);
        wal.start();
        ScheduledFuture tickTask = ticker.scheduleAtFixedRate(wal::tick, 0, 100, TimeUnit.MILLISECONDS);

        testObserver.awaitCount(1);
        assertTrue(wal.isLeader());
        assertTrue(wal.currentLeader().isPresent());
        ElectionEvent election = testObserver.values().get(0);
        assertEquals(election.leaderId, replicaId);
        assertEquals(election.term, 1);

        tickTask.cancel(true);
        wal.close().join();
        testObserver.assertComplete();
    }

    @Test
    public void testCommitIndexObservable() {
        when(walStorageEngine.id()).thenReturn(replicaId);
        when(walStorageEngine.get(id)).thenReturn(raftStateStorage);

        KVRangeWAL wal = new KVRangeWAL("testcluster", id, walStorageEngine, config, 1024);
        wal.start();
        TestObserver<Long> testObserver = new TestObserver<>();
        wal.commitIndex().subscribe(testObserver);
        wal.onRaftEvent(new CommitEvent(replicaId, 10L));
        testObserver.awaitCount(1);
        testObserver.assertValue(10L);
        wal.close().join();
        testObserver.assertComplete();
    }

    @Test
    public void testSnapshotObservable() {
        when(walStorageEngine.id()).thenReturn(replicaId);
        when(walStorageEngine.get(id)).thenReturn(raftStateStorage);

        KVRangeWAL wal = new KVRangeWAL("testcluster", id, walStorageEngine, config, 1024);
        wal.start();

        TestObserver<IKVRangeWAL.SnapshotInstallTask> testObserver = new TestObserver<>();
        wal.snapshotInstallTask().subscribe(testObserver);
        CompletableFuture<Void> onDone = wal.installSnapshot(ByteString.EMPTY);
        testObserver.awaitCount(1);
        IKVRangeWAL.SnapshotInstallTask task = testObserver.values().get(0);
        assertEquals(task.snapshot, ByteString.EMPTY);
        assertEquals(task.onDone, onDone);
        wal.close().join();
        testObserver.assertComplete();
    }

    @Test
    public void testPeerMessagesObservable() {
        when(walStorageEngine.id()).thenReturn(replicaId);
        when(walStorageEngine.get(id)).thenReturn(raftStateStorage);

        KVRangeWAL wal = new KVRangeWAL("testcluster", id, walStorageEngine, config, 1024);
        wal.start();
        TestObserver<Map<String, List<RaftMessage>>> testObserver = new TestObserver<>();
        wal.peerMessages().subscribe(testObserver);
        wal.sendRaftMessages(Collections.emptyMap());
        testObserver.awaitCount(1);
        testObserver.assertValueAt(0, Collections.emptyMap());
        wal.close().join();
        testObserver.assertComplete();
    }

    @Test
    public void testProposeOnce() {
        when(walStorageEngine.id()).thenReturn(replicaId);
        when(walStorageEngine.get(id)).thenReturn(raftStateStorage);

        KVRangeWAL wal = new KVRangeWAL("testcluster", id, walStorageEngine, config, 1024);
        wal.start();
        ScheduledFuture<?> tickTask = ticker.scheduleAtFixedRate(wal::tick, 0, 100, TimeUnit.MILLISECONDS);
        await().until(() -> wal.currentLeader().isPresent());
        wal.propose(KVRangeCommand.getDefaultInstance())
            .handle((v, e) -> {
                if (e != null) {
                    fail();
                }
                return null;
            })
            .join();
        tickTask.cancel(true);
        wal.close().join();
    }

    @Test
    public void testProposeOnceButFailed() {
        when(walStorageEngine.id()).thenReturn(replicaId);
        when(walStorageEngine.get(id)).thenReturn(new InMemoryStateStore(replicaId, Snapshot.newBuilder()
            .setClusterConfig(ClusterConfig.newBuilder()
                .addVoters(replicaId)
                .addVoters("FakeReplica")
                .build())
            .build()));

        KVRangeWAL wal = new KVRangeWAL("testcluster", id, walStorageEngine, config, 1024);
        wal.start();
        ScheduledFuture tickTask = ticker.scheduleAtFixedRate(wal::tick, 0, 100, TimeUnit.MILLISECONDS);
        await().until(() -> wal.currentState() == RaftNodeStatus.Candidate);
        wal.propose(KVRangeCommand.getDefaultInstance())
            .handle((v, e) -> {
                if (e == null) {
                    fail();
                }
                return null;
            }).join();
        tickTask.cancel(true);
        wal.close().join();
    }
}
