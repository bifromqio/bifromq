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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.baidu.bifromq.basekv.MockableTest;
import com.baidu.bifromq.basekv.TestUtil;
import com.baidu.bifromq.basekv.localengine.KVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.memory.InMemKVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.rocksdb.RocksDBKVEngineConfigurator;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.raft.IRaftStateStore;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.raft.proto.LogEntry;
import com.baidu.bifromq.basekv.raft.proto.Snapshot;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.protobuf.ByteString;
import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class KVRangeWALStoreEngineTest extends MockableTest {
    private static final String DB_NAME = "testDB";
    private static final String DB_CHECKPOINT_DIR = "testDB_cp";
    private String dbPath;
    private KVEngineConfigurator<?> engineConfigurator;
    public Path dbRootDir;

    @SneakyThrows
    @Override
    protected void doSetup(Method method) {
        dbRootDir = Files.createTempDirectory("");
        dbPath = Paths.get(dbRootDir.toString(), DB_NAME).toString();
        engineConfigurator = new RocksDBKVEngineConfigurator()
            .setDbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR).toString())
            .setDbRootDir(dbPath);
    }

    protected void doTeardown(Method method) {
        if (dbRootDir != null) {
            TestUtil.deleteDir(dbRootDir.toString());
            dbRootDir.toFile().delete();
        }
    }

    @Test
    public void testStartAndStop() {
        try {
            KVRangeWALStorageEngine stateStorageEngine =
                new KVRangeWALStorageEngine("testcluster", null, engineConfigurator);
            stateStorageEngine.start();
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
                new KVRangeWALStorageEngine("testcluster", null, engineConfigurator);
            stateStorageEngine.start();
            Snapshot snapshot = Snapshot.newBuilder()
                .setIndex(0)
                .setTerm(0)
                .setClusterConfig(ClusterConfig.newBuilder()
                    .addVoters(stateStorageEngine.id())
                    .build())
                .build();
            IRaftStateStore stateStorage = stateStorageEngine.newRaftStateStorage(testId, snapshot);
            assertEquals(stateStorage.local(), stateStorageEngine.id());
            assertEquals(stateStorage.lastIndex(), 0);
            assertEquals(stateStorage.firstIndex(), 1);
            assertFalse(stateStorage.currentVoting().isPresent());
            assertEquals(stateStorage.latestSnapshot(), snapshot);
            assertEquals(stateStorage.latestClusterConfig(), snapshot.getClusterConfig());

            assertTrue(stateStorageEngine.has(testId));
            assertEquals(stateStorageEngine.get(testId), stateStorage);
            assertEquals(stateStorageEngine.allKVRangeIds().size(), 1);
            assertTrue(stateStorageEngine.allKVRangeIds().contains(testId));
            stateStorageEngine.stop();
        } catch (Exception e) {
            log.error("Failed to init", e);
            fail();
        }
    }

    @Test
    public void testLoadExistingRaftStateStorage() {
        if (engineConfigurator instanceof InMemKVEngineConfigurator) {
            return;
        }

        KVRangeId testId1 = KVRangeIdUtil.generate();
        KVRangeId testId2 = KVRangeIdUtil.next(testId1);
        KVRangeWALStorageEngine stateStorageEngine =
            new KVRangeWALStorageEngine("testcluster", null, engineConfigurator);
        stateStorageEngine.start();
        Snapshot snapshot = Snapshot.newBuilder()
            .setIndex(0)
            .setTerm(0)
            .setClusterConfig(ClusterConfig.newBuilder()
                .addVoters(stateStorageEngine.id())
                .build())
            .build();
        IRaftStateStore walStore1 = stateStorageEngine.newRaftStateStorage(testId1, snapshot);
        IRaftStateStore walStore2 = stateStorageEngine.newRaftStateStorage(testId2, snapshot);
        assertEquals(stateStorageEngine.allKVRangeIds().size(), 2);
        walStore1.append(Collections.singletonList(LogEntry.newBuilder()
            .setData(ByteString.copyFromUtf8("Hello"))
            .setIndex(1)
            .build()), true);
        assertEquals(walStore1.lastIndex(), 1);
        stateStorageEngine.stop();

        stateStorageEngine = new KVRangeWALStorageEngine("testcluster", null, engineConfigurator);
        stateStorageEngine.start();
        assertEquals(stateStorageEngine.allKVRangeIds().size(), 2);
        IRaftStateStore stateStorage = stateStorageEngine.get(testId1);
        assertEquals(stateStorage.local(), stateStorageEngine.id());
        assertEquals(stateStorage.lastIndex(), 1);
        assertEquals(stateStorage.firstIndex(), 1);
        assertFalse(stateStorage.currentVoting().isPresent());
        assertEquals(stateStorage.latestSnapshot(), snapshot);
        assertEquals(stateStorage.latestClusterConfig(), snapshot.getClusterConfig());
    }

    @Test
    public void testDestroyRaftStateStorage() {
        if (engineConfigurator instanceof InMemKVEngineConfigurator) {
            return;
        }

        KVRangeId testId1 = KVRangeIdUtil.generate();
        KVRangeId testId2 = KVRangeIdUtil.next(testId1);
        KVRangeWALStorageEngine stateStorageEngine =
            new KVRangeWALStorageEngine("testcluster", null, engineConfigurator);
        stateStorageEngine.start();
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
        assertEquals(stateStorageEngine.allKVRangeIds().size(), 1);
        assertFalse(stateStorageEngine.has(testId1));
        assertTrue(stateStorageEngine.has(testId2));
        stateStorageEngine.stop();

        stateStorageEngine = new KVRangeWALStorageEngine("testcluster", null, engineConfigurator);
        stateStorageEngine.start();
        assertEquals(stateStorageEngine.allKVRangeIds().size(), 1);
        assertTrue(stateStorageEngine.has(testId2));
    }
}
