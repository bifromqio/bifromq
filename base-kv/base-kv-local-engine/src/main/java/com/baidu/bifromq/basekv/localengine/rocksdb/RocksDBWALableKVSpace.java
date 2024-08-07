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

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.localengine.IWALableKVSpace;
import com.baidu.bifromq.basekv.localengine.KVEngineException;
import com.baidu.bifromq.basekv.localengine.metrics.KVSpaceMeters;
import com.baidu.bifromq.basekv.localengine.metrics.KVSpaceMetric;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

public class RocksDBWALableKVSpace
    extends RocksDBKVSpace<RocksDBWALableKVEngine, RocksDBWALableKVSpace, RocksDBWALableKVEngineConfigurator>
    implements IWALableKVSpace {
    private final RocksDBWALableKVEngineConfigurator configurator;
    private final WriteOptions writeOptions;
    private final AtomicReference<CompletableFuture<Long>> flushFutureRef = new AtomicReference<>();
    private final ExecutorService flushExecutor;
    private final MetricManager metricMgr;

    public RocksDBWALableKVSpace(String id, ColumnFamilyDescriptor cfDesc,
                                 ColumnFamilyHandle cfHandle, RocksDB db,
                                 RocksDBWALableKVEngineConfigurator configurator,
                                 RocksDBWALableKVEngine engine,
                                 Runnable onDestroy,
                                 String... tags) {
        super(id, cfDesc, cfHandle, db, configurator, engine, onDestroy, tags);
        this.configurator = configurator;
        writeOptions = new WriteOptions().setDisableWAL(false);
        if (!configurator.asyncWALFlush()) {
            writeOptions.setSync(configurator.fsyncWAL());
        }
        flushExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry, new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                EnvProvider.INSTANCE.newThreadFactory("kvspace-flusher-" + id)), "flusher", "kvspace",
            Tags.of(metricTags));
        metricMgr = new MetricManager();
    }

    @Override
    protected void doClose() {
        final CompletableFuture<Long> flushTaskFuture = Optional.ofNullable(flushFutureRef.get()).orElseGet(() -> {
            CompletableFuture<Long> lastOne = new CompletableFuture<>();
            flushExecutor.submit(() -> lastOne.complete(System.nanoTime()));
            return lastOne;
        });
        flushExecutor.shutdown();
        try {
            flushTaskFuture.join();
        } catch (Throwable e) {
            log.debug("Flush error during closing", e);
        }
        writeOptions.close();
        metricMgr.close();
        super.doClose();
    }

    @Override
    protected WriteOptions writeOptions() {
        return writeOptions;
    }

    @Override
    public CompletableFuture<Long> flush() {
        if (state() != State.Opening) {
            return CompletableFuture.failedFuture(new KVEngineException("KVSpace not open"));
        }
        if (!configurator.asyncWALFlush()) {
            return CompletableFuture.completedFuture(System.nanoTime());
        }
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
                log.trace("KVSpace[{}] flush wal start", id);
                try {
                    Timer.Sample start = Timer.start();
                    db.flushWal(configurator.fsyncWAL());
                    start.stop(metricMgr.flushTimer);
                    log.trace("KVSpace[{}] flush complete", id);
                } catch (Throwable e) {
                    log.error("KVSpace[{}] flush error", id, e);
                    throw new KVEngineException("KVSpace flush error", e);
                }
                flushFutureRef.compareAndSet(onDone, null);
                onDone.complete(flashStartAt);
            } catch (Throwable e) {
                flushFutureRef.compareAndSet(onDone, null);
                onDone.completeExceptionally(new KVEngineException("KVSpace flush error", e));
            }
        });
    }

    private class MetricManager {
        private final Timer flushTimer;

        MetricManager() {
            flushTimer = KVSpaceMeters.getTimer(id, KVSpaceMetric.FlushTimer, Tags.of(metricTags));
        }

        void close() {
            flushTimer.close();
        }
    }
}
