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

import static com.baidu.bifromq.basekv.localengine.rocksdb.Keys.META_SECTION_END;
import static com.baidu.bifromq.basekv.localengine.rocksdb.Keys.META_SECTION_START;
import static com.baidu.bifromq.basekv.localengine.rocksdb.Keys.fromMetaKey;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.localengine.IKVSpace;
import com.baidu.bifromq.basekv.localengine.IKVSpaceCheckpoint;
import com.baidu.bifromq.basekv.localengine.IKVSpaceWriter;
import com.baidu.bifromq.basekv.localengine.ISyncContext;
import com.baidu.bifromq.basekv.localengine.KVEngineException;
import com.baidu.bifromq.basekv.localengine.KVSpaceDescriptor;
import com.baidu.bifromq.basekv.localengine.SyncContext;
import com.baidu.bifromq.basekv.localengine.metrics.KVSpaceMeters;
import com.baidu.bifromq.basekv.localengine.metrics.KVSpaceMetric;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

@Slf4j
abstract class RocksDBKVSpace extends RocksDBKVSpaceReader implements IKVSpace {
    private enum State {
        Init, Opening, Destroying, Closing, Terminated
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.Init);
    protected final RocksDB db;
    private final RocksDBKVEngineConfigurator configurator;
    private final ColumnFamilyDescriptor cfDesc;
    protected final ColumnFamilyHandle cfHandle;
    private final WriteOptions writeOptions;
    private final AtomicReference<CompletableFuture<Long>> flushFutureRef = new AtomicReference<>();
    private final IWriteStatsRecorder writeStats;
    private final ExecutorService compactionExecutor;
    private final ExecutorService flushExecutor;
    private final Cache<String, IRocksDBKVSpaceCheckpoint> checkpoints;
    private final RocksDBKVEngine engine;
    private final Runnable onDestroy;
    private final AtomicBoolean compacting = new AtomicBoolean(false);
    private final BehaviorSubject<Map<ByteString, ByteString>> metadataSubject = BehaviorSubject.create();
    private final RocksDBKVEngineIterator metaItr;
    private final ISyncContext syncContext = new SyncContext();
    private final ISyncContext.IRefresher metadataRefresher = syncContext.refresher();
    private final MetricManager metricMgr;
    private final String[] tags;
    // keep a strong ref to latest checkpoint
    private IKVSpaceCheckpoint latestCheckpoint;
    private volatile long lastCompactAt;
    private volatile long nextCompactAt;

    @SneakyThrows
    public RocksDBKVSpace(String id,
                          ColumnFamilyDescriptor cfDesc,
                          ColumnFamilyHandle cfHandle,
                          RocksDB db,
                          RocksDBKVEngineConfigurator configurator,
                          RocksDBKVEngine engine,
                          Runnable onDestroy,
                          String... tags) {
        super(id, Tags.of(tags));
        this.tags = tags;
        this.db = db;
        this.onDestroy = onDestroy;
        this.cfDesc = cfDesc;
        this.cfHandle = cfHandle;
        this.configurator = configurator;
        this.writeStats = configurator.isManualCompaction() ? new RocksDBKVSpaceCompactionTrigger(id,
            configurator.getCompactMinTombstoneKeys(),
            configurator.getCompactMinTombstoneRanges(),
            configurator.getCompactTombstoneKeysRatio(),
            this::scheduleCompact) : NoopWriteStatsRecorder.INSTANCE;
        this.engine = engine;
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

        metaItr = new RocksDBKVEngineIterator(db, cfHandle, null, META_SECTION_START, META_SECTION_END);

        checkpoints = Caffeine.newBuilder().weakValues().build();
        metricMgr = new MetricManager(tags);
    }

    public RocksDBKVSpace open() {
        if (state.compareAndSet(State.Init, State.Opening)) {
            load();
        }
        return this;
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
            metaItr.refresh();
            for (metaItr.seekToFirst(); metaItr.isValid(); metaItr.next()) {
                metaMap.put(fromMetaKey(metaItr.key()), unsafeWrap(metaItr.value()));
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

    protected void doFlush() {
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
        if (state.compareAndSet(State.Opening, State.Destroying)) {
            try {
                synchronized (compacting) {
                    db.dropColumnFamily(cfHandle);
                }
                doClose();
            } catch (RocksDBException e) {
                throw new KVEngineException("Destroy KVRange error", e);
            } finally {
                onDestroy.run();
                state.set(State.Terminated);
            }
        }
    }

    @Override
    public String checkpoint() {
        return metricMgr.checkpointTimer.record(() -> {
            synchronized (this) {
                IRocksDBKVSpaceCheckpoint cp = doCheckpoint();
                checkpoints.put(cp.cpId(), cp);
                latestCheckpoint = cp;
                return cp.cpId();
            }
        });
    }

    protected abstract IRocksDBKVSpaceCheckpoint doCheckpoint();

    @Override
    public Optional<IKVSpaceCheckpoint> open(String checkpointId) {
        return Optional.ofNullable(checkpoints.getIfPresent(checkpointId));
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

    void close() {
        if (state.compareAndSet(State.Opening, State.Closing)) {
            try {
                doClose();
            } finally {
                state.set(State.Terminated);
            }
        }
    }

    protected void doClose() {
        log.debug("Close key range[{}]", id);
        metricMgr.close();
        checkpoints.asMap().forEach((cpId, cp) -> cp.close());
        metaItr.close();
        cfDesc.getOptions().close();
        synchronized (compacting) {
            db.destroyColumnFamilyHandle(cfHandle);
        }
        writeOptions.close();
        metadataSubject.onComplete();
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

    @SneakyThrows
    private void loadLatestCheckpoint() {
        IRocksDBKVSpaceCheckpoint checkpoint = doLoadLatestCheckpoint();
        assert !checkpoints.asMap().containsKey(checkpoint.cpId());
        checkpoints.put(checkpoint.cpId(), checkpoint);
        latestCheckpoint = checkpoint;
    }

    protected abstract IRocksDBKVSpaceCheckpoint doLoadLatestCheckpoint();

    private void scheduleCompact() {
        if (state.get() != State.Opening) {
            return;
        }
        metricMgr.compactionSchedCounter.increment();
        if (compacting.compareAndSet(false, true)) {
            compactionExecutor.execute(metricMgr.compactionTimer.wrap(() -> {
                log.debug("KeyRange[{}] compaction start", id);
                lastCompactAt = System.nanoTime();
                writeStats.reset();
                try (CompactRangeOptions options = new CompactRangeOptions()
                    .setBottommostLevelCompaction(CompactRangeOptions.BottommostLevelCompaction.kSkip)
                    .setExclusiveManualCompaction(false)) {
                    synchronized (compacting) {
                        if (state.get() == State.Opening) {
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
        } else {
            nextCompactAt = System.nanoTime();
        }
    }

    private class MetricManager {
        private final Gauge blockCacheSizeGauge;
        private final Gauge tableReaderSizeGauge;
        private final Gauge memtableSizeGauges;
        private final Gauge pinedMemorySizeGauges;
        private final Gauge checkpointGauge; // hold a strong reference
        private final Timer checkpointTimer;
        private final Counter compactionSchedCounter;
        private final Timer compactionTimer;
        private final Timer flushTimer;

        MetricManager(String... tags) {
            Tags metricTags = Tags.of(tags);
            checkpointGauge =
                KVSpaceMeters.getGauge(id, KVSpaceMetric.CheckpointNumGauge, checkpoints::estimatedSize, metricTags);
            checkpointTimer = KVSpaceMeters.getTimer(id, KVSpaceMetric.CheckpointTimer, metricTags);
            compactionSchedCounter = KVSpaceMeters.getCounter(id, KVSpaceMetric.CompactionCounter, metricTags);
            compactionTimer = KVSpaceMeters.getTimer(id, KVSpaceMetric.CompactionTimer, metricTags);
            flushTimer = KVSpaceMeters.getTimer(id, KVSpaceMetric.FlushTimer, metricTags);

            blockCacheSizeGauge = KVSpaceMeters.getGauge(id, KVSpaceMetric.BlockCache, () -> {
                try {
                    if (!((BlockBasedTableConfig) cfDesc.getOptions().tableFormatConfig()).noBlockCache()) {
                        return db.getLongProperty(cfHandle, "rocksdb.block-cache-usage");
                    }
                    return 0;
                } catch (RocksDBException e) {
                    log.warn("Unable to get long property {}", "rocksdb.block-cache-usage", e);
                    return 0;
                }
            }, metricTags);

            tableReaderSizeGauge = KVSpaceMeters.getGauge(id, KVSpaceMetric.TableReader, () -> {
                try {
                    return db.getLongProperty(cfHandle, "rocksdb.estimate-table-readers-mem");
                } catch (RocksDBException e) {
                    log.warn("Unable to get long property {}", "rocksdb.estimate-table-readers-mem", e);
                    return 0;
                }
            }, metricTags);

            memtableSizeGauges = KVSpaceMeters.getGauge(id, KVSpaceMetric.MemTable, () -> {
                try {
                    return db.getLongProperty(cfHandle, "rocksdb.cur-size-all-mem-tables");
                } catch (RocksDBException e) {
                    log.warn("Unable to get long property {}", "rocksdb.cur-size-all-mem-tables", e);
                    return 0;
                }
            }, metricTags);

            pinedMemorySizeGauges = KVSpaceMeters.getGauge(id, KVSpaceMetric.PinnedMem, () -> {
                try {
                    if (!((BlockBasedTableConfig) cfDesc.getOptions().tableFormatConfig()).noBlockCache()) {
                        return db.getLongProperty(cfHandle, "rocksdb.block-cache-pinned-usage");
                    }
                    return 0;
                } catch (RocksDBException e) {
                    log.warn("Unable to get long property {}", "rocksdb.block-cache-pinned-usage", e);
                    return 0;
                }
            }, metricTags);
        }

        void close() {
            blockCacheSizeGauge.close();
            memtableSizeGauges.close();
            tableReaderSizeGauge.close();
            pinedMemorySizeGauges.close();
            checkpointGauge.close();
            checkpointTimer.close();
            compactionSchedCounter.close();
            compactionTimer.close();
            flushTimer.close();
        }
    }
}
