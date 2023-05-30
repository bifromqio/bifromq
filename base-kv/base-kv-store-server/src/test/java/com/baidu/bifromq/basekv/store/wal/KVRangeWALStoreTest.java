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
import static com.baidu.bifromq.baseutils.ThreadUtil.threadFactory;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

import com.baidu.bifromq.basekv.TestUtil;
import com.baidu.bifromq.basekv.localengine.InMemoryKVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.KVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.RocksDBKVEngineConfigurator;
import com.baidu.bifromq.basekv.raft.BasicStateStoreTest;
import com.baidu.bifromq.basekv.raft.IRaftStateStore;
import com.baidu.bifromq.basekv.raft.proto.Snapshot;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.common.util.concurrent.MoreExecutors;
import java.nio.file.Paths;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class KVRangeWALStoreTest extends BasicStateStoreTest {
    private static final String DB_NAME = "testDB";
    private static final String DB_CHECKPOINT_DIR = "testDB_cp";
    private KVRangeWALStorageEngine stateStorageEngine;
    private ScheduledExecutorService bgMgmtTaskExecutor;

    @Rule
    public TemporaryFolder dbRootDir = new TemporaryFolder();

    @Before
    public void setup() {
        bgMgmtTaskExecutor = newSingleThreadScheduledExecutor(threadFactory("bg-executor"));
        KVEngineConfigurator walConfigurator;
        if (isDevEnv()) {
            walConfigurator = new InMemoryKVEngineConfigurator();
        } else {
            walConfigurator = new RocksDBKVEngineConfigurator()
                .setDbCheckpointRootDir(Paths.get(dbRootDir.getRoot().toString(), DB_CHECKPOINT_DIR).toString())
                .setDbRootDir(Paths.get(dbRootDir.getRoot().toString(), DB_NAME).toString());
        }
        stateStorageEngine = new KVRangeWALStorageEngine(null, walConfigurator);
        stateStorageEngine.start(bgMgmtTaskExecutor);
    }

    @After
    public void teardown() {
        MoreExecutors.shutdownAndAwaitTermination(bgMgmtTaskExecutor, 5, TimeUnit.SECONDS);
        stateStorageEngine.stop();
        TestUtil.deleteDir(dbRootDir.getRoot().toString());
    }

    @Override
    protected String localId() {
        return stateStorageEngine.id();
    }

    @Override
    protected IRaftStateStore createStorage(String id, Snapshot snapshot) {
        return stateStorageEngine.newRaftStateStorage(KVRangeIdUtil.generate(), snapshot);
    }
}
