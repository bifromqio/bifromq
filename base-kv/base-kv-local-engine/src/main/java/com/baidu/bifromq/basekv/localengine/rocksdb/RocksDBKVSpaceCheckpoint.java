/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

import static com.baidu.bifromq.basekv.localengine.IKVEngine.DEFAULT_NS;
import static com.baidu.bifromq.basekv.localengine.rocksdb.Keys.toMetaKey;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.baidu.bifromq.basekv.localengine.ISyncContext;
import com.baidu.bifromq.basekv.localengine.KVEngineException;
import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
import java.lang.ref.Cleaner;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;

class RocksDBKVSpaceCheckpoint extends RocksDBKVSpaceReader implements IRocksDBKVSpaceCheckpoint {
    private static final Cleaner CLEANER = Cleaner.create();

    private record ClosableResources(
        String id,
        String cpId,
        File cpDir,
        ColumnFamilyDescriptor defaultCFDesc,
        ColumnFamilyHandle defaultCFHandle,
        ColumnFamilyDescriptor cfDesc,
        ColumnFamilyHandle cfHandle,
        RocksDB roDB,
        DBOptions dbOptions,
        Predicate<String> isLatest,
        Logger log
    ) implements Runnable {
        @Override
        public void run() {
            log.debug("Clean up checkpoint[{}] of kvspace[{}]", cpId, id);
            roDB.destroyColumnFamilyHandle(defaultCFHandle);
            defaultCFDesc.getOptions().close();

            roDB.destroyColumnFamilyHandle(cfHandle);
            cfDesc.getOptions().close();

            roDB.close();
            dbOptions.close();

            if (!isLatest.test(cpId)) {
                log.debug("delete checkpoint[{}] of kvspace[{}] in path: {}", cpId, id, cpDir.getAbsolutePath());
                try {
                    Files.walkFileTree(cpDir.toPath(), EnumSet.noneOf(FileVisitOption.class),
                        Integer.MAX_VALUE,
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
                    log.error("Failed to clean checkpoint at path:{}", cpDir);
                }
            }
        }
    }

    private final String cpId;
    private final DBOptions dbOptions;
    private final RocksDB roDB;
    private final ColumnFamilyDescriptor cfDesc;
    private final ColumnFamilyHandle cfHandle;
    private final ColumnFamilyDescriptor defaultCFDesc;
    private final ColumnFamilyHandle defaultCFHandle;
    private final Cleaner.Cleanable cleanable;

    RocksDBKVSpaceCheckpoint(String id, String cpId, File cpDir, Predicate<String> isLatest, String... metricTags) {
        super(id, metricTags);
        this.cpId = cpId;
        try {
            defaultCFDesc = new ColumnFamilyDescriptor(DEFAULT_NS.getBytes());
            defaultCFDesc.getOptions().setTableFormatConfig(new BlockBasedTableConfig()
                .setNoBlockCache(true)
                .setBlockCacheCompressed(null)
                .setBlockCache(null));
            dbOptions = new DBOptions();
            cfDesc = new ColumnFamilyDescriptor(id.getBytes());
            cfDesc.getOptions().setTableFormatConfig(new BlockBasedTableConfig()
                .setNoBlockCache(true)
                .setBlockCacheCompressed(null)
                .setBlockCache(null));

            List<ColumnFamilyDescriptor> cfDescs = new ArrayList<>();
            cfDescs.add(defaultCFDesc);
            cfDescs.add(cfDesc);

            List<ColumnFamilyHandle> handles = new ArrayList<>();
            roDB = RocksDB.openReadOnly(dbOptions, cpDir.getAbsolutePath(), cfDescs, handles);

            defaultCFHandle = handles.get(0);
            cfHandle = handles.get(1);
            cleanable = CLEANER.register(this, new ClosableResources(id,
                cpId,
                cpDir,
                defaultCFDesc,
                defaultCFHandle,
                cfDesc,
                cfHandle,
                roDB,
                dbOptions,
                isLatest,
                log));
        } catch (RocksDBException e) {
            throw new KVEngineException("Failed to open checkpoint", e);
        }
        log.debug("Checkpoint[{}] of kvspace[{}] created", cpId, id);
    }

    @Override
    public String cpId() {
        return cpId;
    }

    @Override
    protected Optional<ByteString> doMetadata(ByteString metaKey) {
        try {
            byte[] valBytes = roDB.get(cfHandle(), toMetaKey(metaKey));
            if (valBytes == null) {
                return Optional.empty();
            }
            return Optional.of(unsafeWrap(valBytes));
        } catch (RocksDBException e) {
            throw new KVEngineException("Failed to read metadata", e);
        }
    }

    @Override
    public void close() {
        cleanable.clean();
    }

    @Override
    protected RocksDB db() {
        return roDB;
    }

    @Override
    protected ColumnFamilyHandle cfHandle() {
        return cfHandle;
    }

    @Override
    protected ISyncContext.IRefresher newRefresher() {
        return new ISyncContext.IRefresher() {

            @Override
            public void runIfNeeded(Runnable runnable) {
                // no need to do any refresh, since it's readonly
            }

            @Override
            public <T> T call(Supplier<T> supplier) {
                return supplier.get();
            }
        };
    }
}
