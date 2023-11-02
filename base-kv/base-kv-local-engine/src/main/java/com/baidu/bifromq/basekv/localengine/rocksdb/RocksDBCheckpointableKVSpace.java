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

package com.baidu.bifromq.basekv.localengine.rocksdb;

import static com.baidu.bifromq.basekv.localengine.rocksdb.Keys.LATEST_CP_KEY;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.baidu.bifromq.basekv.localengine.KVEngineException;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

@Slf4j
public class RocksDBCheckpointableKVSpace extends RocksDBKVSpace {
    private static final String CP_SUFFIX = ".cp";
    private RocksDBKVEngine engine;
    private final File cpRootDir;
    private final Checkpoint checkpoint;
    private final AtomicReference<String> latestCheckpointId = new AtomicReference<>();

    @SneakyThrows
    public RocksDBCheckpointableKVSpace(String id,
                                        ColumnFamilyDescriptor cfDesc,
                                        ColumnFamilyHandle cfHandle,
                                        RocksDB db,
                                        RocksDBKVEngineConfigurator configurator,
                                        RocksDBKVEngine engine,
                                        Runnable onDestroy,
                                        String... tags) {
        super(id, cfDesc, cfHandle, db, configurator, engine, onDestroy, tags);
        this.engine = engine;
        cpRootDir = new File(configurator.getDbCheckpointRootDir(), id);
        this.checkpoint = Checkpoint.create(db);
        Files.createDirectories(cpRootDir.getAbsoluteFile().toPath());
    }

    @Override
    protected IRocksDBKVSpaceCheckpoint doCheckpoint() {
        String cpId = genCheckpointId();
        File cpDir = Paths.get(cpRootDir.getAbsolutePath(), cpId).toFile();
        try {
            // flush before checkpointing
            db.put(cfHandle, LATEST_CP_KEY, cpId.getBytes());
            doFlush();
            checkpoint.createCheckpoint(cpDir.toString());
            latestCheckpointId.set(cpId);
            return new RocksDBKVSpaceCheckpoint(id, cpId, cpDir, this::isLatest);
        } catch (Throwable e) {
            throw new KVEngineException("Checkpoint key range error", e);
        }
    }

    @SneakyThrows
    @Override
    protected IRocksDBKVSpaceCheckpoint doLoadLatestCheckpoint() {
        byte[] cpIdBytes = db.get(cfHandle, LATEST_CP_KEY);
        if (cpIdBytes != null) {
            String cpId = new String(cpIdBytes, UTF_8);
            File cpDir = Paths.get(cpRootDir.getAbsolutePath(), cpId).toFile();
            // cleanup obsolete checkpoints
            for (String obsoleteId : obsoleteCheckpoints(cpId)) {
                try {
                    cleanCheckpoint(obsoleteId);
                } catch (Throwable e) {
                    log.error("Clean checkpoint[{}] for kvspace[{}] error", obsoleteId, id, e);
                }
            }
            log.debug("Load latest checkpoint[{}] of kvspace[{}] in engine[{}] at path[{}]",
                cpId, id, engine.id(), cpDir);
            return new RocksDBKVSpaceCheckpoint(id, cpId, cpDir, this::isLatest);
        }
        return doCheckpoint();
    }

    @Override
    protected void doClose() {
        super.doClose();
        checkpoint.close();
    }

    private String genCheckpointId() {
        // we need generate global unique checkpoint id, since it will be used in raft snapshot
        return UUID.randomUUID() + CP_SUFFIX;
    }

    private boolean isLatest(String cpId) {
        return cpId.equals(latestCheckpointId.get());
    }

    private File checkpointDir(String cpId) {
        return Paths.get(cpRootDir.getAbsolutePath(), cpId).toFile();
    }

    private Iterable<String> obsoleteCheckpoints(String skipId) {
        File[] cpDirList = cpRootDir.listFiles();
        if (cpDirList == null) {
            return Collections.emptyList();
        }
        return Arrays.stream(cpDirList)
            .filter(File::isDirectory)
            .map(File::getName)
            .filter(cpId -> !skipId.equals(cpId))
            .collect(Collectors.toList());
    }

    private void cleanCheckpoint(String cpId) {
        log.debug("Delete checkpoint[{}] of kvspace[{}]", cpId, id);
        try {
            Files.walkFileTree(checkpointDir(cpId).toPath(), EnumSet.noneOf(FileVisitOption.class), Integer.MAX_VALUE,
                new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                        throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                        throws IOException {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
        } catch (IOException e) {
            log.error("Failed to clean checkpoint[{}] for kvspace[{}] at path:{}", cpId, id, checkpointDir(cpId));
        }
    }
}
