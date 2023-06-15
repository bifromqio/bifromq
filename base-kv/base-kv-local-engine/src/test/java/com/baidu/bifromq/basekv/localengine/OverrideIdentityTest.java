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

package com.baidu.bifromq.basekv.localengine;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.junit.Assert.assertEquals;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class OverrideIdentityTest {
    private String DB_NAME = "testDB";
    private String DB_CHECKPOINT_DIR = "testDB_cp";
    private String cf = "TestCF";
    private IKVEngine engine;
    private AtomicReference<String> cp = new AtomicReference<>();
    private ScheduledExecutorService bgTaskExecutor;

    @Rule
    public TemporaryFolder dbRootDir = new TemporaryFolder();

    @Before
    public void setup() {
        bgTaskExecutor =
            newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Checkpoint GC").build());
    }

    @After
    public void teardown() {
        MoreExecutors.shutdownAndAwaitTermination(bgTaskExecutor, 5, TimeUnit.SECONDS);
    }

    @Test
    public void testOverrideIdentity() {
        String overrideIdentity = UUID.randomUUID().toString();
        KVEngineConfigurator configurator = new RocksDBKVEngineConfigurator()
            .setDbRootDir(Paths.get(dbRootDir.getRoot().toString(), DB_NAME).toString())
            .setDbCheckpointRootDir(Paths.get(dbRootDir.getRoot().toString(), DB_CHECKPOINT_DIR).toString());
        engine = KVEngineFactory.create(overrideIdentity, List.of(IKVEngine.DEFAULT_NS, cf),
            this::isUsed, configurator);
        engine.start(bgTaskExecutor);
        assertEquals(overrideIdentity, engine.id());
        engine.stop();
        // restart without overrideIdentity specified
        configurator = new RocksDBKVEngineConfigurator()
            .setDbRootDir(Paths.get(dbRootDir.getRoot().toString(), DB_NAME).toString())
            .setDbCheckpointRootDir(Paths.get(dbRootDir.getRoot().toString(), DB_CHECKPOINT_DIR).toString());

        engine = KVEngineFactory.create(null, List.of(IKVEngine.DEFAULT_NS, cf),
            this::isUsed, configurator);
        engine.start(bgTaskExecutor);

        assertEquals(overrideIdentity, engine.id());
        engine.stop();
        // restart with different overrideIdentity specified
        String another = UUID.randomUUID().toString();
        configurator = new RocksDBKVEngineConfigurator()
            .setDbRootDir(Paths.get(dbRootDir.getRoot().toString(), DB_NAME).toString())
            .setDbCheckpointRootDir(Paths.get(dbRootDir.getRoot().toString(), DB_CHECKPOINT_DIR).toString());

        engine = KVEngineFactory.create(another, List.of(IKVEngine.DEFAULT_NS, cf),
            this::isUsed, configurator);
        engine.start(bgTaskExecutor);

        assertEquals(overrideIdentity, engine.id());
        engine.stop();
    }

    @Test
    public void testCanOnlyOverrideWhenInit() {
        KVEngineConfigurator configurator = new RocksDBKVEngineConfigurator()
            .setDbRootDir(Paths.get(dbRootDir.getRoot().toString(), DB_NAME).toString())
            .setDbCheckpointRootDir(Paths.get(dbRootDir.getRoot().toString(), DB_CHECKPOINT_DIR).toString());

        engine = KVEngineFactory.create(null, List.of(IKVEngine.DEFAULT_NS, cf),
            this::isUsed, configurator);
        engine.start(bgTaskExecutor);
        String identity = engine.id();
        engine.stop();
        // restart with overrideIdentity specified
        String overrideIdentity = UUID.randomUUID().toString();
        configurator = new RocksDBKVEngineConfigurator()
            .setDbRootDir(Paths.get(dbRootDir.getRoot().toString(), DB_NAME).toString())
            .setDbCheckpointRootDir(Paths.get(dbRootDir.getRoot().toString(), DB_CHECKPOINT_DIR).toString());

        engine = KVEngineFactory.create(overrideIdentity, List.of(IKVEngine.DEFAULT_NS, cf),
            this::isUsed, configurator);
        engine.start(bgTaskExecutor);

        assertEquals(identity, engine.id());
        engine.stop();
    }

    private boolean isUsed(String checkpointId) {
        return checkpointId.equals(cp.get());
    }
}
