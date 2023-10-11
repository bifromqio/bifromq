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
import static com.baidu.bifromq.basekv.localengine.rocksdb.Keys.META_SECTION_END;
import static com.baidu.bifromq.basekv.localengine.rocksdb.Keys.META_SECTION_START;
import static com.baidu.bifromq.basekv.localengine.rocksdb.Keys.fromMetaKey;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.localengine.IKVSpace;
import com.baidu.bifromq.basekv.localengine.IKVSpaceReader;
import com.baidu.bifromq.basekv.localengine.IKVSpaceWriter;
import com.baidu.bifromq.basekv.localengine.ISyncContext;
import com.baidu.bifromq.basekv.localengine.KVEngineException;
import com.baidu.bifromq.basekv.localengine.KVSpaceDescriptor;
import com.baidu.bifromq.basekv.localengine.SyncContext;
import com.baidu.bifromq.basekv.localengine.metrics.KVSpaceMeters;
import com.baidu.bifromq.basekv.localengine.metrics.KVSpaceMetric;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

@Slf4j
public class RocksDBKVSpace extends RocksDBKVSpaceReader implements IKVSpace {
    private enum State {
        Opening, Closing, Closed
    }

    private static final String CP_SUFFIX = ".cp";
    private final AtomicReference<State> state = new AtomicReference<>(State.Opening);
    private final RocksDB db;
    private final RocksDBKVEngineConfigurator configurator;
    private final File cpRootDir;
    private final ColumnFamilyDescriptor cfDesc;
    private final ColumnFamilyHandle cfHandle;
    private final WriteOptions writeOptions;
    private final AtomicReference<CompletableFuture<Long>> flushFutureRef = new AtomicReference<>();
    private final IWriteStatsRecorder writeStats;
    private final ExecutorService compactionExecutor;
    private final ExecutorService flushExecutor;
    private final Checkpoint checkpoint;
    private final LoadingCache<String, RocksDBKVSpaceCheckpoint> checkpoints;
    private final RocksDBKVEngine engine;
    private final Runnable onDestroy;
    private final AtomicBoolean compacting = new AtomicBoolean(false);
    private final BehaviorSubject<Map<ByteString, ByteString>> metadataSubject = BehaviorSubject.create();
    private final ISyncContext syncContext = new SyncContext();
    private final ISyncContext.IRefresher metadataRefresher = syncContext.refresher();
    private final MetricManager metricMgr;
    private final String[] tags;
    private RocksDBKVSpaceCheckpoint latestCheckpoint;
    private volatile long lastCompactAt;
    private volatile long nextCompactAt;

    public RocksDBKVSpace(String id,
                          ColumnFamilyDescriptor cfDesc,
                          ColumnFamilyHandle cfHandle,
                          RocksDB db,
                          RocksDBKVEngineConfigurator configurator,
                          RocksDBKVEngine engine,
                          Runnable onDestroy,
                          String... tags) {
        this(id, cfDesc, cfHandle, db, configurator, engine, onDestroy, false, tags);
    }

    @SneakyThrows
    private RocksDBKVSpace(String id,
                           ColumnFamilyDescriptor cfDesc,
                           ColumnFamilyHandle cfHandle,
                           RocksDB db,
                           RocksDBKVEngineConfigurator configurator,
                           RocksDBKVEngine engine,
                           Runnable onDestroy,
                           boolean isCreate,
                           String... tags) {
        super(id, Tags.of(tags));
        this.tags = tags;
        this.db = db;
        this.onDestroy = onDestroy;
        this.cfDesc = cfDesc;
        this.cfHandle = cfHandle;
        this.configurator = configurator;
        this.writeStats = configurator.isManualCompaction() ? new RocksDBKVSpaceCompactionTrigger(
            configurator.getCompactMinTombstoneKeys(),
            configurator.getCompactMinTombstoneRanges(),
            configurator.getCompactTombstoneKeysRatio(),
            this::scheduleCompact) : NoopWriteStatsRecorder.INSTANCE;
        this.engine = engine;
        this.checkpoint = Checkpoint.create(db);
        compactionExecutor = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
            EnvProvider.INSTANCE.newThreadFactory("keyrange-compactor"));
        flushExecutor = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            EnvProvider.INSTANCE.newThreadFactory("keyrange-flusher"));
        writeOptions = new WriteOptions().setDisableWAL(configurator.isDisableWAL());
        if (!configurator.isDisableWAL() && !configurator.isAsyncWALFlush()) {
            writeOptions.setSync(configurator.isFsyncWAL());
        }
        cpRootDir = new File(configurator.getDbCheckpointRootDir(), id);
        Files.createDirectories(cpRootDir.getAbsoluteFile().toPath());

        checkpoints = Caffeine.newBuilder()
            .weakValues()
            .evictionListener((RemovalListener<String, RocksDBKVSpaceCheckpoint>) (key, value, cause) -> {
                if (value != null) {
                    value.close();
                }
            })
            .build(cpId -> {
                synchronized (this) {
                    File cpDir = checkpointDir(cpId);
                    if (cpDir.exists()) {
                        try {
                            return new RocksDBKVSpaceCheckpoint(id, cpId, cpDir, configurator, tags);
                        } catch (Throwable e) {
                            log.error("Failed to init checkpoint[{}] for key range[{}]", cpId, id, e);
                            return null;
                        }
                    }
                    return null;
                }
            });
        metricMgr = new MetricManager(tags);
        if (!isCreate) {
            load();
        }
    }

    public Observable<Map<ByteString, ByteString>> metadata() {
        return metadataSubject;
    }

    protected Optional<ByteString> doMetadata(ByteString metaKey) {
        return metadataRefresher.call(() -> {
            Map<ByteString, ByteString> metaMap = metadataSubject.getValue();
            if (metaMap != null) {
                return Optional.ofNullable(metaMap.get(metaKey));
            }
            return Optional.empty();
        });
    }

    @Override
    public KVSpaceDescriptor describe() {
        return new KVSpaceDescriptor(id, collectStats());
    }

    private Map<String, Double> collectStats() {
        Map<String, Double> stats = new HashMap<>();
        stats.put("size", (double) size());
        // TODO: more stats
        return stats;
    }

    private void load() {
        loadLatestCheckpoint();
        loadMetadata();
    }

    private void loadMetadata() {
        metadataRefresher.runIfNeeded(() -> {
            Map<ByteString, ByteString> metaMap = new HashMap<>();
            try (RocksDBKVEngineIterator itr =
                     new RocksDBKVEngineIterator(db(), cfHandle(), META_SECTION_START, META_SECTION_END)) {
                for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                    metaMap.put(fromMetaKey(itr.key()), unsafeWrap(itr.value()));
                }
            }
            if (!metaMap.isEmpty()) {
                metadataSubject.onNext(Collections.unmodifiableMap(metaMap));
            }
        });
    }

    @Override
    public CompletableFuture<Long> flush() {
        CompletableFuture<Long> flushFuture;
        if (flushFutureRef.compareAndSet(null, flushFuture = new CompletableFuture<>())) {
            doFlush(flushFuture);
        } else {
            flushFuture = flushFutureRef.get();
            if (flushFuture == null) {
                // try again
                return flush();
            }
        }
        return flushFuture;
    }

    private void doFlush(CompletableFuture<Long> onDone) {
        flushExecutor.submit(() -> {
            long flashStartAt = System.nanoTime();
            try {
                doFlush();
                flushFutureRef.compareAndSet(onDone, null);
                onDone.complete(flashStartAt);
            } catch (Throwable e) {
                flushFutureRef.compareAndSet(onDone, null);
                onDone.completeExceptionally(new KVEngineException("KeyRange flush error", e));
            }
        });
    }

    private void doFlush() {
        metricMgr.flushTimer.record(() -> {
            if (configurator.isDisableWAL()) {
                log.debug("KeyRange[{}] flush start", id);
                try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
                    db.flush(flushOptions, cfHandle);
                    log.debug("KeyRange[{}] flush complete", id);
                } catch (Throwable e) {
                    log.error("KeyRange[{}] flush error", id, e);
                    throw new KVEngineException("KeyRange flush error", e);
                }
            } else if (configurator.isAtomicFlush()) {
                log.debug("KeyRange[{}] flush wal start", id);
                try {
                    db.flushWal(configurator.isFsyncWAL());
                    log.debug("KeyRange[{}] flush complete", id);
                } catch (Throwable e) {
                    log.error("KeyRange[{}] flush error", id, e);
                    throw new KVEngineException("KeyRange flush error", e);
                }
            }
        });
    }

    @Override
    public void destroy() {
        try {
            db.dropColumnFamily(cfHandle);
            close();
        } catch (RocksDBException e) {
            throw new KVEngineException("Destroy KVRange error", e);
        } finally {
            onDestroy.run();
        }
    }

    @Override
    public String checkpoint() {
        return metricMgr.checkpointTimer.record(() -> {
            synchronized (this) {
                String cpId = genCheckpointId();
                File cpDir = Paths.get(cpRootDir.getAbsolutePath(), cpId).toFile();
                log.debug("Checkpointing key range[{}] in path[{}]", id, cpDir);
                try {
                    // flush before checkpointing
                    db.put(cfHandle, LATEST_CP_KEY, cpId.getBytes());
                    doFlush();
                    checkpoint.createCheckpoint(cpDir.toString());
                    loadLatestCheckpoint();
                } catch (Throwable e) {
                    log.error("Failed to checkpoint key range[{}] in path[{}]", id, cpDir, e);
                    throw new KVEngineException("Checkpoint key range error", e);
                }
                return cpId;
            }
        });
    }

    @Override
    public Optional<String> latestCheckpoint() {
        synchronized (this) {
            return Optional.ofNullable(latestCheckpoint == null ? null : latestCheckpoint.cpId());
        }
    }

    @Override
    public Optional<IKVSpaceReader> open(String checkpointId) {
        return Optional.ofNullable(checkpoints.get(checkpointId));
    }

    @Override
    public IKVSpaceWriter toWriter() {
        return new RocksDBKVSpaceWriter(id, db, cfHandle, engine, writeOptions, syncContext, writeStats.newRecorder(),
            metadataUpdated -> {
                if (metadataUpdated) {
                    this.loadMetadata();
                }
            }, tags);
    }

    //For internal use only
    IKVSpaceWriter toWriter(RocksDBKVSpaceWriterHelper helper) {
        return new RocksDBKVSpaceWriter(id, db, cfHandle, engine, syncContext, helper, writeStats.newRecorder(),
            metadataUpdated -> {
                if (metadataUpdated) {
                    this.loadMetadata();
                }
            }, tags
        );
    }

    void gc() {
        metricMgr.gcTimer.record(() -> {
            synchronized (this) {
                for (String cpId : checkpoints()) {
                    try {
                        cleanCheckpoint(cpId);
                    } catch (Throwable e) {
                        log.error("Clean checkpoint[{}] for key range[{}] error", cpId, id, e);
                    }
                }
            }
        });
    }

    void close() {
        if (state.compareAndSet(State.Opening, State.Closing)) {
            super.close();
            log.debug("Close key range[{}]", id);
            cfDesc.getOptions().close();
            synchronized (compacting) {
                db.destroyColumnFamilyHandle(cfHandle);
            }
            checkpoint.close();
            writeOptions.close();
            metadataSubject.onComplete();
            metricMgr.close();
            state.set(State.Closed);
        }
    }

    private String genCheckpointId() {
        // we need generate global unique checkpoint id, since it will be used in raft snapshot
        return UUID.randomUUID() + CP_SUFFIX;
    }

    @Override
    protected RocksDB db() {
        return db;
    }

    @Override
    protected ColumnFamilyHandle cfHandle() {
        return cfHandle;
    }

    @Override
    protected ISyncContext.IRefresher newRefresher() {
        return syncContext.refresher();
    }

    private File checkpointDir(String cpId) {
        return Paths.get(cpRootDir.getAbsolutePath(), cpId).toFile();
    }

    @SneakyThrows
    private void loadLatestCheckpoint() {
        byte[] cpIdBytes = db.get(cfHandle, LATEST_CP_KEY);
        if (cpIdBytes != null) {
            String cpId = new String(cpIdBytes, UTF_8);
            latestCheckpoint = checkpoints.get(cpId);
        } else {
            latestCheckpoint = null;
        }
    }

    protected Iterable<String> checkpoints() {
        File[] cpDirList = cpRootDir.listFiles();
        if (cpDirList == null) {
            return Collections.emptyList();
        }
        return Arrays.stream(cpDirList)
            .filter(File::isDirectory)
            .map(File::getName)
            .filter(cpId -> !cpId.equals(latestCheckpoint.cpId()) && checkpoints.getIfPresent(cpId) == null)
            .collect(Collectors.toList());
    }

    protected void cleanCheckpoint(String cpId) {
        log.debug("Deleting checkpoint[{}] for key range[{}]", cpId, id);
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
            log.error("Failed to clean checkpoint[{}] for key range[{}] at path:{}", cpId, id, checkpointDir(cpId));
        }
    }

    private void scheduleCompact() {
        if (state.get() != State.Opening) {
            return;
        }
        metricMgr.compactionSchedCounter.increment();
        if (compacting.compareAndSet(false, true)) {
            compact();
        } else {
            nextCompactAt = System.nanoTime();
        }
    }

    private void compact() {
        lastCompactAt = System.nanoTime();
        writeStats.reset();
        log.debug("KeyRange[{}] compaction start", id);
        compactionExecutor.execute(metricMgr.compactionTimer.wrap(() -> {
            try (CompactRangeOptions options = new CompactRangeOptions()
                .setBottommostLevelCompaction(CompactRangeOptions.BottommostLevelCompaction.kSkip)
                .setExclusiveManualCompaction(false)) {
                synchronized (compacting) {
                    if (cfHandle.isOwningHandle()) {
                        db.compactRange(cfHandle, null, null, options);
                    }
                }
                log.debug("KeyRange[{}] compacted", id);
            } catch (Throwable e) {
                log.error("KeyRange[{}] compaction error", id, e);
            } finally {
                compacting.set(false);
                if (nextCompactAt > lastCompactAt) {
                    scheduleCompact();
                }
            }
        }));
    }

    private class MetricManager {
        private Gauge checkpointGauge; // hold a strong reference
        private Timer checkpointTimer;
        private Counter compactionSchedCounter;
        private Timer compactionTimer;
        private Timer gcTimer;
        private Timer flushTimer;

        MetricManager(String... tags) {
            Tags metricTags = Tags.of(tags);
            checkpointGauge = KVSpaceMeters.getGauge(id, KVSpaceMetric.CheckpointNumGauge, () -> {
                File[] cpDirList = cpRootDir.listFiles();
                return cpDirList != null ? cpDirList.length : 0;
            }, metricTags);
            checkpointTimer = KVSpaceMeters.getTimer(id, KVSpaceMetric.CheckpointTimer, metricTags);
            compactionSchedCounter = KVSpaceMeters.getCounter(id, KVSpaceMetric.CompactionCounter, metricTags);
            compactionTimer = KVSpaceMeters.getTimer(id, KVSpaceMetric.CompactionTimer, metricTags);
            gcTimer = KVSpaceMeters.getTimer(id, KVSpaceMetric.GCTimer, metricTags);
            flushTimer = KVSpaceMeters.getTimer(id, KVSpaceMetric.FlushTimer, metricTags);
        }

        void close() {
            checkpointGauge.close();
            checkpointGauge = null;
            checkpointTimer = null;
            compactionSchedCounter = null;
            compactionTimer = null;
            gcTimer = null;
            flushTimer = null;
        }
    }
}
