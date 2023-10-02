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

package com.baidu.bifromq.basekv.store;

import static com.baidu.bifromq.basekv.Constants.EMPTY_RANGE;
import static com.baidu.bifromq.basekv.Constants.FULL_RANGE;
import static com.baidu.bifromq.basekv.TestUtil.isDevEnv;
import static com.baidu.bifromq.basekv.proto.State.StateType.Merged;
import static com.baidu.bifromq.basekv.proto.State.StateType.Normal;
import static com.baidu.bifromq.basekv.utils.KeyRangeUtil.combine;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static java.util.Collections.emptySet;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.TestCoProcFactory;
import com.baidu.bifromq.basekv.localengine.memory.InMemKVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.rocksdb.RocksDBKVEngineConfigurator;
import com.baidu.bifromq.basekv.proto.EnsureRange;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeMessage;
import com.baidu.bifromq.basekv.proto.KVRangeSnapshot;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.proto.LoadHint;
import com.baidu.bifromq.basekv.proto.State;
import com.baidu.bifromq.basekv.proto.StoreMessage;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeSyncState;
import com.baidu.bifromq.basekv.raft.proto.Snapshot;
import com.baidu.bifromq.basekv.store.exception.KVRangeException;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class KVRangeStoreTest {
    private String DB_NAME = "testDB";
    private String DB_CHECKPOINT_DIR_NAME = "testDB_cp";
    private String DB_WAL_NAME = "testWAL";
    private String DB_WAL_CHECKPOINT_DIR = "testWAL_cp";
    private KVRangeStoreOptions options = new KVRangeStoreOptions();
    private IKVRangeStore rangeStore;
    private IStoreMessenger messenger;
    private PublishSubject<StoreMessage> incomingStoreMessage = PublishSubject.create();
    private ExecutorService queryExecutor;
    private ScheduledExecutorService tickTaskExecutor;
    private ScheduledExecutorService bgTaskExecutor;

    public Path dbRootDir;
    private AutoCloseable closeable;

    @BeforeMethod(alwaysRun = true)
    public void setup() throws IOException {
        closeable = MockitoAnnotations.openMocks(this);
        options.getKvRangeOptions().getWalRaftConfig().setAsyncAppend(false);
        options.getKvRangeOptions().getWalRaftConfig().setInstallSnapshotTimeoutTick(10);

        queryExecutor = new ThreadPoolExecutor(2, 2, 0L,
            TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
            EnvProvider.INSTANCE.newThreadFactory("query-executor"));
        tickTaskExecutor = new ScheduledThreadPoolExecutor(2,
            EnvProvider.INSTANCE.newThreadFactory("tick-task-executor"));
        bgTaskExecutor = new ScheduledThreadPoolExecutor(1,
            EnvProvider.INSTANCE.newThreadFactory("bg-task-executor"));

        if (!isDevEnv()) {
            options.setWalEngineConfigurator(new InMemKVEngineConfigurator());
            options.setDataEngineConfigurator(new InMemKVEngineConfigurator());
        } else {
            dbRootDir = Files.createTempDirectory("");
            (((RocksDBKVEngineConfigurator) options.getDataEngineConfigurator()))
                .setDbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR_NAME)
                    .toString())
                .setDbRootDir(Paths.get(dbRootDir.toString(), DB_NAME).toString());
            ((RocksDBKVEngineConfigurator) options.getWalEngineConfigurator())
                .setDbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_WAL_CHECKPOINT_DIR).toString())
                .setDbRootDir(Paths.get(dbRootDir.toString(), DB_WAL_NAME).toString());
        }

        rangeStore =
            new KVRangeStore("testCluster",
                options,
                new TestCoProcFactory(),
                queryExecutor,
                tickTaskExecutor,
                bgTaskExecutor);
        messenger = new IStoreMessenger() {
            @Override
            public void send(StoreMessage message) {
                KVRangeMessage payload = message.getPayload();
                if (!payload.hasHostStoreId()) {
                    incomingStoreMessage.onNext(message.toBuilder()
                        .setPayload(payload.toBuilder()
                            .setHostStoreId(rangeStore.id())
                            .build())
                        .build());
                    return;
                }
                if (payload.getHostStoreId().equals(rangeStore.id())) {
                    incomingStoreMessage.onNext(message);
                }
            }

            @Override
            public Observable<StoreMessage> receive() {
                return incomingStoreMessage;
            }

            @Override
            public void close() {

            }
        };

        rangeStore.start(messenger);
        assertTrue(rangeStore.bootstrap());
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        rangeStore.stop();
        queryExecutor.shutdownNow();
        tickTaskExecutor.shutdownNow();
        bgTaskExecutor.shutdownNow();
        if (dbRootDir != null) {
            try {
                Files.walk(dbRootDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            } catch (IOException e) {
                log.error("Failed to delete db root dir", e);
            }
            dbRootDir = null;
        }
        closeable.close();
        log.info("Shutdown read task executor");
    }

    @Test(groups = "integration")
    public void testBootStrap() {
        KVRangeDescriptor descriptor = firstRangeDescriptor();
        KVRangeId id = descriptor.getId();
        assertEquals(descriptor.toBuilder().clearStatistics().setHlc(0).build(), KVRangeDescriptor.newBuilder()
            .setId(id)
            .setVer(0)
            .setRole(RaftNodeStatus.Leader)
            .setState(Normal)
            .setRange(FULL_RANGE)
            .setConfig(ClusterConfig.newBuilder().addVoters(rangeStore.id()).build())
            .putSyncState(rangeStore.id(), RaftNodeSyncState.Replicating)
            .setLoadHint(LoadHint.newBuilder().setLoad(0.0).build())
            .build());
    }

    @Test(groups = "integration")
    public void testKeyExist() {
        KVRangeDescriptor rangeDescriptor = firstRangeDescriptor();
        KVRangeId id = rangeDescriptor.getId();
        waitNormalState(rangeDescriptor, 5000);
        // split into [, key4) and [key4, )
        rangeStore.split(0, id, copyFromUtf8("key4")).toCompletableFuture().join();

        rangeStore.put(1, id, copyFromUtf8("key1"), copyFromUtf8("hello")).toCompletableFuture().join();
        {
            log.info("Test KeyExist");
            assertTrue(rangeStore.exist(1, id, copyFromUtf8("key1"), true).toCompletableFuture().join());

            assertFalse(rangeStore.exist(1, id, copyFromUtf8("key2"), true).toCompletableFuture().join());
        }
        {
            log.info("Test KeyExist with version mismatch");
            try {
                rangeStore.exist(0, id, copyFromUtf8("key2"), true).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.BadVersion);
            }
        }
        {
            log.info("Test KeyExist with out-of-range key");
            try {
                rangeStore.exist(1, id, copyFromUtf8("key4"), true).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.InternalException);
            }
        }
    }

    @Test(groups = "integration")
    public void testPutKey() {
        KVRangeDescriptor rangeDescriptor = firstRangeDescriptor();
        KVRangeId id = rangeDescriptor.getId();
        waitNormalState(rangeDescriptor, 5000);
        // split into [, key4) and [key4, )
        rangeStore.split(0, id, copyFromUtf8("key4")).toCompletableFuture().join();
        {
            log.info("Test PutKV");
            assertEquals(
                rangeStore.put(1, id, copyFromUtf8("key1"), copyFromUtf8("hello")).toCompletableFuture().join(),
                ByteString.empty());

            assertEquals(
                rangeStore.put(1, id, copyFromUtf8("key2"), copyFromUtf8("hello")).toCompletableFuture().join(),
                ByteString.empty());

            assertEquals(
                rangeStore.put(1, id, copyFromUtf8("key3"), copyFromUtf8("hello")).toCompletableFuture().join(),
                ByteString.empty());
        }
        {
            log.info("Test PutKV with version mismatch");
            try {
                rangeStore.put(0, id, copyFromUtf8("key1"), copyFromUtf8("hello")).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.BadVersion);
            }
        }
        {
            log.info("Test PutKV with out-of-range key");
            try {
                rangeStore.put(1, id, copyFromUtf8("key4"), copyFromUtf8("hello")).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.InternalException);
            }
        }
    }

    @Test(groups = "integration")
    public void testGetKey() {
        KVRangeDescriptor rangeDescriptor = firstRangeDescriptor();
        KVRangeId id = rangeDescriptor.getId();
        waitNormalState(rangeDescriptor, 5000);
        // split into [, key4) and [key4, )
        rangeStore.split(0, id, copyFromUtf8("key4")).toCompletableFuture().join();

        rangeStore.put(1, id, copyFromUtf8("key1"), copyFromUtf8("hello")).toCompletableFuture().join();
        {
            log.info("Test Get");
            assertEquals(rangeStore.get(1, id, copyFromUtf8("key1"), true).toCompletableFuture().join().get(),
                copyFromUtf8("hello"));
            assertEquals(rangeStore.get(1, id, copyFromUtf8("key1"), false).toCompletableFuture().join().get(),
                copyFromUtf8("hello"));
            assertFalse(rangeStore.get(1, id, copyFromUtf8("key2"), true).toCompletableFuture().join().isPresent());
        }
        {
            log.info("Test Get with version mismatch");
            try {
                rangeStore.get(0, id, copyFromUtf8("key1"), true).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.BadVersion);
            }
        }
        {
            log.info("Test Get with out-of-range key");
            try {
                rangeStore.get(1, id, copyFromUtf8("key4"), true).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.InternalException);
            }
        }
    }

    @Test(groups = "integration")
    public void testDeleteKey() {
        KVRangeDescriptor rangeDescriptor = firstRangeDescriptor();
        KVRangeId id = rangeDescriptor.getId();
        waitNormalState(rangeDescriptor, 5000);
        // split into [, key4) and [key4, )
        rangeStore.split(0, id, copyFromUtf8("key4")).toCompletableFuture().join();
        rangeStore.put(1, id, copyFromUtf8("key1"), copyFromUtf8("hello")).toCompletableFuture().join();

        {
            log.info("Test Delete");
            ByteString delVal = rangeStore.delete(1, id, copyFromUtf8("key1")).toCompletableFuture().join();
            assertEquals(delVal, copyFromUtf8("hello"));

            assertFalse(
                rangeStore.get(1, id, copyFromUtf8("key1"), true).toCompletableFuture().join().isPresent());

            assertEquals(rangeStore.delete(1, id, copyFromUtf8("key2")).toCompletableFuture().join(),
                ByteString.empty());
        }
        {
            log.info("Test Delete with version mismatch");
            try {
                rangeStore.delete(0, id, copyFromUtf8("key1")).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.BadVersion);
            }
        }
        {
            log.info("Test Delete with out-of-range key");
            try {
                rangeStore.delete(1, id, copyFromUtf8("key4")).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.InternalException);
            }
        }
    }

    @Test(groups = "integration")
    public void testExecROCoProc() {
        KVRangeDescriptor rangeDescriptor = firstRangeDescriptor();
        KVRangeId id = rangeDescriptor.getId();
        waitNormalState(rangeDescriptor, 5000);
        // split into [, key4) and [key4, )
        rangeStore.split(0, id, copyFromUtf8("key4")).toCompletableFuture().join();

        rangeStore.put(1, id, copyFromUtf8("key1"), copyFromUtf8("hello")).toCompletableFuture().join();

        {
            log.info("Test exec ReadOnly Co-Proc");
            assertEquals(
                rangeStore.queryCoProc(1, id, toInput(copyFromUtf8("key1")), true).toCompletableFuture().join()
                    .getRaw(),
                copyFromUtf8("hello"));
            assertEquals(
                rangeStore.queryCoProc(1, id, toInput(copyFromUtf8("key1")), false).toCompletableFuture().join()
                    .getRaw(),
                copyFromUtf8("hello"));
        }
        {
            log.info("Test exec ReadOnly CoProc with version mismatch");
            try {
                rangeStore.queryCoProc(0, id, toInput(copyFromUtf8("key1")), true).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.BadVersion);
            }
        }
        {
            log.info("Test exec ReadOnly Range with wrong ranges");
            try {
                rangeStore.queryCoProc(1, id, toInput(copyFromUtf8("key4")), true).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.InternalException);
            }
        }
    }

    @Test(groups = "integration")
    public void testExecRWCoProc() {
        KVRangeDescriptor rangeDescriptor = firstRangeDescriptor();
        KVRangeId id = rangeDescriptor.getId();
        waitNormalState(rangeDescriptor, 5000);
        // split into [, key4) and [key4, )
        rangeStore.split(0, id, copyFromUtf8("key4")).toCompletableFuture().join();
        rangeStore.put(1, id, copyFromUtf8("key1"), copyFromUtf8("hello")).toCompletableFuture().join();

        {
            log.info("Test exec ReadWrite Co-Proc");
            assertEquals(
                rangeStore.mutateCoProc(1, id, RWCoProcInput.newBuilder().setRaw(copyFromUtf8("key1_world")).build())
                    .toCompletableFuture().join().getRaw(),
                copyFromUtf8("hello"));

            assertTrue(rangeStore.get(1, id, copyFromUtf8("key1"), true).toCompletableFuture().join().isPresent());
        }
        {
            log.info("Test exec ReadWrite CoProc with version mismatch");
            try {
                rangeStore.mutateCoProc(0, id, RWCoProcInput.newBuilder().setRaw(copyFromUtf8("key1_hello")).build())
                    .toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.BadVersion);
            }
        }
        {
            log.info("Test exec ReadWrite Range with wrong ranges");
            try {
                rangeStore.mutateCoProc(1, id, RWCoProcInput.newBuilder().setRaw(copyFromUtf8("key4")).build())
                    .toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.InternalException);
            }
        }
    }

    @Test(groups = "integration")
    public void testSplit() {
        KVRangeDescriptor rangeDescriptor = firstRangeDescriptor();
        KVRangeId id = rangeDescriptor.getId();
        waitNormalState(rangeDescriptor, 5000);
        {
            log.info("Normal split test");
            // normal split
            rangeStore.split(0, id, copyFromUtf8("a")).toCompletableFuture().join();
            List<KVRangeDescriptor> ls = await().forever().until(() -> rangeStore.describe().blockingFirst(),
                storeDescriptor -> storeDescriptor.getRangesList().size() == 2).getRangesList();
            assertEquals(combine(ls.get(0).getRange(), ls.get(1).getRange()), FULL_RANGE);
        }
        {
            log.info("Split with version mismatch test");
            // version mismatch
            try {
                rangeStore.split(0, id, copyFromUtf8("a")).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.BadVersion);
            }
        }
        {
            log.info("Split with invalid split key test");
            // invalid split key
            try {
                rangeStore.split(1, id, copyFromUtf8("a")).toCompletableFuture().join();
                fail();
            } catch (Throwable e) {
                assertTrue(e.getCause() instanceof KVRangeException.BadRequest);
            }
        }
    }

    @Test(groups = "integration")
    public void testMerge() {
        KVRangeDescriptor rangeDescriptor = firstRangeDescriptor();
        KVRangeId id = rangeDescriptor.getId();
        waitNormalState(rangeDescriptor, 5000);
        log.info("Splitting bucket");
        rangeStore.split(0, id, copyFromUtf8("d")).toCompletableFuture().join();
        // [-,d) [d,-)
        KVRangeStoreDescriptor storeDescriptor = await().until(() ->
                rangeStore.describe().blockingFirst(),
            storeDesc -> {
                if (storeDesc.getRangesList().size() != 2) {
                    return false;
                }
                return storeDesc.getRangesList().stream()
                    .allMatch(d -> d.getState() == Normal && d.getRole() == RaftNodeStatus.Leader);
            }
        );
        log.info("{}", storeDescriptor);
        KVRangeDescriptor merger = storeDescriptor.getRangesList().get(0).getId().equals(id) ?
            storeDescriptor.getRangesList().get(0) : storeDescriptor.getRangesList().get(1);
        KVRangeDescriptor mergee = storeDescriptor.getRangesList().get(1).getId().equals(id) ?
            storeDescriptor.getRangesList().get(0) : storeDescriptor.getRangesList().get(1);
        log.info("Start Merging");
        rangeStore.merge(merger.getVer(), merger.getId(), mergee.getId()).toCompletableFuture().join();
        KVRangeDescriptor mergeeDesc = await().atMost(Duration.ofSeconds(10000)).until(() ->
                rangeStore.describe()
                    .flatMap(sd -> Observable.fromIterable(sd.getRangesList()))
                    .filter(rd -> rd.getId().equals(mergee.getId())).blockingFirst(),
            rangeDesc -> rangeDesc.getState() == Merged && rangeDesc.getRole() == RaftNodeStatus.Leader
        );
        log.info("Mergee Descriptor: {}", mergeeDesc);
        rangeStore.changeReplicaConfig(mergeeDesc.getVer(), mergeeDesc.getId(),
            Sets.newHashSet(mergeeDesc.getConfig().getVotersList()), emptySet()).toCompletableFuture().join();
        storeDescriptor = await().atMost(Duration.ofSeconds(10000)).until(() ->
            rangeStore.describe().blockingFirst(), storeDesc -> storeDesc.getRangesList().size() == 1
        );
        log.info("Store Descriptor: {}", storeDescriptor);
    }

    @Test(groups = "integration")
    public void testRangeEnsure() {
        KVRangeDescriptor rangeDescriptor = firstRangeDescriptor();
        KVRangeId id = rangeDescriptor.getId();
        waitNormalState(rangeDescriptor, 5000);

        KVRangeId newId = KVRangeIdUtil.next(id);
        incomingStoreMessage.onNext(StoreMessage.newBuilder()
            .setFrom("newVoter")
            .setSrcRange(newId)
            .setPayload(KVRangeMessage.newBuilder()
                .setRangeId(newId)
                .setHostStoreId(rangeStore.id())
                .setEnsureRange(EnsureRange.newBuilder()
                    .setInitSnapshot(Snapshot.newBuilder()
                        .setClusterConfig(ClusterConfig.getDefaultInstance())
                        .setTerm(0)
                        .setIndex(0)
                        .setData(KVRangeSnapshot.newBuilder()
                            .setId(newId)
                            .setVer(0)
                            .setLastAppliedIndex(0)
                            .setState(State.newBuilder().setType(Normal).build())
                            .setRange(EMPTY_RANGE)
                            .build().toByteString())
                        .build())
                    .build())
                .build())
            .build());
        await().until(() -> rangeStore.describe().blockingFirst(),
            storeDescriptor -> storeDescriptor.getRangesList().size() == 2).getRangesList();
    }

    private void waitNormalState(KVRangeDescriptor rangeDescriptor, long timeoutInMS) {
        await().atMost(Duration.ofMillis(timeoutInMS))
            .until(() -> rangeDescriptor.getState() == Normal && rangeDescriptor.getRole() == RaftNodeStatus.Leader);
    }

    private long reqId() {
        return System.nanoTime();
    }

    private KVRangeDescriptor firstRangeDescriptor() {
        return rangeStore.describe().mapOptional(this::mapToLeader).blockingFirst();
    }

    private Optional<KVRangeDescriptor> mapToLeader(KVRangeStoreDescriptor storeDescriptor) {
        KVRangeDescriptor descriptor = storeDescriptor.getRangesList().get(0);
        if (descriptor.getRole() == RaftNodeStatus.Leader) {
            return Optional.of(descriptor);
        }
        return Optional.empty();
    }

    private ROCoProcInput toInput(ByteString raw) {
        return ROCoProcInput.newBuilder().setRaw(raw).build();
    }
}
