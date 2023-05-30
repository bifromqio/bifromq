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

import static com.baidu.bifromq.basekv.TestUtil.isDevEnv;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.baidu.bifromq.basekv.TestUtil;
import com.baidu.bifromq.basekv.localengine.InMemoryKVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.KVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.RocksDBKVEngineConfigurator;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.raft.IRaftStateStore;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.raft.proto.Snapshot;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

@Slf4j
public class KVRangeWALStoreEngineTest {
    private static final String DB_NAME = "testDB";
    private static final String DB_CHECKPOINT_DIR = "testDB_cp";
    private String dbPath;


    private KVEngineConfigurator engineConfigurator;

    @Rule
    public TemporaryFolder dbRootDir = new TemporaryFolder();

    private ScheduledExecutorService bgMgmtTaskExecutor;

    @Before
    public void setup() {
        bgMgmtTaskExecutor = Executors.newSingleThreadScheduledExecutor();
        if (isDevEnv()) {
            engineConfigurator = InMemoryKVEngineConfigurator.builder().build();
        } else {
            dbPath = Paths.get(dbRootDir.getRoot().toString(), DB_NAME).toString();
            engineConfigurator = new RocksDBKVEngineConfigurator()
                .setDbCheckpointRootDir(Paths.get(dbRootDir.getRoot().toString(), DB_CHECKPOINT_DIR).toString())
                .setDbRootDir(dbPath);
        }
    }

    @After
    public void teardown() {
        MoreExecutors.shutdownAndAwaitTermination(bgMgmtTaskExecutor, 5, TimeUnit.SECONDS);
        TestUtil.deleteDir(dbRootDir.getRoot().toString());
    }

    @Test
    public void testStartAndStop() {
        try {
            KVRangeWALStorageEngine stateStorageEngine =
                new KVRangeWALStorageEngine(null, engineConfigurator);
            stateStorageEngine.start(bgMgmtTaskExecutor);
            if (engineConfigurator instanceof RocksDBKVEngineConfigurator) {
                assertTrue((new File(dbPath)).isDirectory());
            }
            assertTrue(stateStorageEngine.allKVRangeIds().isEmpty());
            stateStorageEngine.stop();
        } catch (Exception e) {
            log.error("Failed to init", e);
            fail();
        }
    }

    @Test
    public void testNewRaftStateStorage() {
        try {
            KVRangeId testId = KVRangeIdUtil.generate();
            KVRangeWALStorageEngine stateStorageEngine =
                new KVRangeWALStorageEngine(null, engineConfigurator);
            stateStorageEngine.start(bgMgmtTaskExecutor);
            Snapshot snapshot = Snapshot.newBuilder()
                .setIndex(0)
                .setTerm(0)
                .setClusterConfig(ClusterConfig.newBuilder()
                    .addVoters(stateStorageEngine.id())
                    .build())
                .build();
            IRaftStateStore stateStorage = stateStorageEngine.newRaftStateStorage(testId, snapshot);
            assertEquals(stateStorageEngine.id(), stateStorage.local());
            assertEquals(0, stateStorage.lastIndex());
            assertEquals(1, stateStorage.firstIndex());
            assertFalse(stateStorage.currentVoting().isPresent());
            assertEquals(snapshot, stateStorage.latestSnapshot());
            assertEquals(snapshot.getClusterConfig(), stateStorage.latestClusterConfig());

            assertTrue(stateStorageEngine.has(testId));
            assertEquals(stateStorage, stateStorageEngine.get(testId));
            assertEquals(1, stateStorageEngine.allKVRangeIds().size());
            assertTrue(stateStorageEngine.allKVRangeIds().contains(testId));
            stateStorageEngine.stop();
        } catch (Exception e) {
            log.error("Failed to init", e);
            fail();
        }
    }

    @Test
    public void testLoadExistingRaftStateStorage() {
        if (engineConfigurator instanceof InMemoryKVEngineConfigurator) {
            return;
        }

        KVRangeId testId1 = KVRangeIdUtil.generate();
        KVRangeId testId2 = KVRangeIdUtil.next(testId1);
        KVRangeWALStorageEngine stateStorageEngine =
            new KVRangeWALStorageEngine(null, engineConfigurator);
        stateStorageEngine.start(bgMgmtTaskExecutor);
        Snapshot snapshot = Snapshot.newBuilder()
            .setIndex(0)
            .setTerm(0)
            .setClusterConfig(ClusterConfig.newBuilder()
                .addVoters(stateStorageEngine.id())
                .build())
            .build();
        stateStorageEngine.newRaftStateStorage(testId1, snapshot);
        stateStorageEngine.newRaftStateStorage(testId2, snapshot);
        assertEquals(2, stateStorageEngine.allKVRangeIds().size());
        stateStorageEngine.stop();

        stateStorageEngine = new KVRangeWALStorageEngine(null, engineConfigurator);
        stateStorageEngine.start(bgMgmtTaskExecutor);
        assertEquals(2, stateStorageEngine.allKVRangeIds().size());
        IRaftStateStore stateStorage = stateStorageEngine.get(testId1);
        assertEquals(stateStorageEngine.id(), stateStorage.local());
        assertEquals(0, stateStorage.lastIndex());
        assertEquals(1, stateStorage.firstIndex());
        assertFalse(stateStorage.currentVoting().isPresent());
        assertEquals(snapshot, stateStorage.latestSnapshot());
        assertEquals(snapshot.getClusterConfig(), stateStorage.latestClusterConfig());
    }

    @Test
    public void testDestroyRaftStateStorage() {
        if (engineConfigurator instanceof InMemoryKVEngineConfigurator) {
            return;
        }

        KVRangeId testId1 = KVRangeIdUtil.generate();
        KVRangeId testId2 = KVRangeIdUtil.next(testId1);
        KVRangeWALStorageEngine stateStorageEngine =
            new KVRangeWALStorageEngine(null, engineConfigurator);
        stateStorageEngine.start(bgMgmtTaskExecutor);
        Snapshot snapshot = Snapshot.newBuilder()
            .setIndex(0)
            .setTerm(0)
            .setClusterConfig(ClusterConfig.newBuilder()
                .addVoters(stateStorageEngine.id())
                .build())
            .build();
        stateStorageEngine.newRaftStateStorage(testId1, snapshot);
        stateStorageEngine.newRaftStateStorage(testId2, snapshot);
        stateStorageEngine.destroy(testId1);
        assertEquals(1, stateStorageEngine.allKVRangeIds().size());
        assertFalse(stateStorageEngine.has(testId1));
        assertTrue(stateStorageEngine.has(testId2));
        stateStorageEngine.stop();

        stateStorageEngine = new KVRangeWALStorageEngine(null, engineConfigurator);
        stateStorageEngine.start(bgMgmtTaskExecutor);
        assertEquals(1, stateStorageEngine.allKVRangeIds().size());
        assertTrue(stateStorageEngine.has(testId2));
    }
}
