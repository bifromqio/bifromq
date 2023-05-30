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

import static com.baidu.bifromq.baseutils.ThreadUtil.threadFactory;
import static java.lang.Math.max;
import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.junit.Assert.fail;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ColumnFamilyOptionsInterface;
import org.rocksdb.CompactionPriority;
import org.rocksdb.DBOptions;
import org.rocksdb.DBOptionsInterface;
import org.rocksdb.Env;
import org.rocksdb.MutableColumnFamilyOptionsInterface;
import org.rocksdb.MutableDBOptionsInterface;
import org.rocksdb.RateLimiter;
import org.rocksdb.RocksIterator;
import org.rocksdb.Statistics;
import org.rocksdb.util.SizeUnit;

@Slf4j
public class RocksDBCompactionTest {
    protected static final String NS = "test-namespace";
    protected final AtomicReference<String> cp = new AtomicReference<>();
    protected RocksDBKVEngine kvEngine;
    private ScheduledExecutorService bgTaskExecutor;
    private Path dataDir;

    @Before
    public void setup() {
        dataDir = Paths.get(System.getProperty("user.dir"), "data");
        dataDir.toFile().mkdirs();
        bgTaskExecutor = newScheduledThreadPool(4, threadFactory("bg-executor-%d"));
        start();
    }

    @After
    public void teardown() {
        stop();
        MoreExecutors.shutdownAndAwaitTermination(bgTaskExecutor, 5, TimeUnit.SECONDS);
    }

    private void start() {
        String DB_NAME = "testDB";
        String DB_CHECKPOINT_DIR = "testDB_cp";
        String uid = ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE) + "";
        RocksDBKVEngineConfigurator configurator = new RocksDBKVEngineConfigurator(
            new RocksDBKVEngineConfigurator.DBOptionsConfigurator() {
                @Override
                public void config(DBOptionsInterface<DBOptions> targetOption) {
                    targetOption.setEnv(Env.getDefault())
                        .setAtomicFlush(true)
                        .setCreateIfMissing(true)
                        .setCreateMissingColumnFamilies(true)
                        .setRecycleLogFileNum(10)
                        .setAvoidUnnecessaryBlockingIO(true)
                        .setStatistics(gcable(new Statistics()))
                        .setRateLimiter(gcable(new RateLimiter(512 * SizeUnit.MB,
                            RateLimiter.DEFAULT_REFILL_PERIOD_MICROS,
                            RateLimiter.DEFAULT_FAIRNESS,
                            RateLimiter.DEFAULT_MODE, true)));
                }

                @Override
                public void config(MutableDBOptionsInterface<DBOptions> targetOption) {
                    targetOption
                        .setMaxOpenFiles(20)
                        .setStatsDumpPeriodSec(10)
                        .setMaxBackgroundJobs(max(getRuntime().availableProcessors(), 2));
                }
            },
            new RocksDBKVEngineConfigurator.CFOptionsConfigurator() {
                @Override
                public void config(String name,
                                   ColumnFamilyOptionsInterface<ColumnFamilyOptions> targetOption) {
                    targetOption
                        .setMinWriteBufferNumberToMerge(2)
                        .setCompactionPriority(CompactionPriority.ByCompensatedSize);
                }

                @Override
                public void config(String name,
                                   MutableColumnFamilyOptionsInterface<ColumnFamilyOptions> targetOption) {
                    targetOption.setDisableAutoCompactions(true)
                        .setWriteBufferSize(64 * 1024 * 1024)
                        .setMaxWriteBufferNumber(3);
                }
            })
            .setDbCheckpointRootDir(Paths.get(dataDir.toString(), uid, DB_CHECKPOINT_DIR).toString())
            .setDbRootDir(Paths.get(dataDir.toString(), uid, DB_NAME).toString());

        kvEngine = new RocksDBKVEngine(null, List.of(IKVEngine.DEFAULT_NS, NS),
            this::isUsed, configurator, Duration.ofSeconds(-1));
        kvEngine.start(bgTaskExecutor);
    }

    private void stop() {
        kvEngine.stop();
    }

    @Test
    @Ignore
    public void testSeekPerf() throws InterruptedException {
        int keyCount = 10000000;
        CountDownLatch countDownLatch = new CountDownLatch(keyCount);
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        ByteString existKey = key.concat(TestUtil.toByteString(keyCount));
        ScheduledExecutorService writeExecutor = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService readExecutor = Executors.newSingleThreadScheduledExecutor();
        kvEngine.put(IKVEngine.DEFAULT_NS, existKey, value.concat(TestUtil.toByteString(keyCount)));
        Runnable write = () -> {
            int batchId = kvEngine.startBatch();
            for (int i = 1; i <= keyCount; i++) {
                kvEngine.put(batchId, IKVEngine.DEFAULT_NS, key.concat(TestUtil.toByteString(i)), value.concat(
                    TestUtil.toByteString(i)));
                kvEngine.delete(batchId, IKVEngine.DEFAULT_NS, key.concat(TestUtil.toByteString(i)));
                if (i % (keyCount / 100) == 0) {
                    log.info("Write {} %", i / (keyCount / 100));
                    kvEngine.endBatch(batchId);
                    batchId = kvEngine.startBatch();
                }
                countDownLatch.countDown();
            }
        };
        writeExecutor.execute(write);
//        write.run();
        log.info("Start seek");
        IKVEngineIterator itr = kvEngine.newIterator(IKVEngine.DEFAULT_NS);
        Runnable seek = () -> {
            itr.refresh();
            int i = 0;
            while (i++ < 10) {
                long start = System.nanoTime();
                itr.seek(key.concat(TestUtil.toByteString(ThreadLocalRandom.current().nextInt(0, keyCount))));
                log.info("Seek {} cost {} ns", i, System.nanoTime() - start);
                if (itr.isValid()) {
                    start = System.nanoTime();
                    itr.next();
                    log.info("Next {} cost {} ns", i, System.nanoTime() - start);
                }
            }
        };
        Runnable get = () -> {
            int i = 0;
            while (i++ < 10) {
                long start = System.nanoTime();
                Optional<ByteString> valOpt = kvEngine.get(IKVEngine.DEFAULT_NS, existKey);
                if (valOpt.isPresent()) {
                    log.info("Get hit {} cost {} ns", i, System.nanoTime() - start);
                } else {
                    log.info("Get miss {} cost {} ns", i, System.nanoTime() - start);
                }
            }
        };
//        readExecutor.scheduleAtFixedRate(seek, 0, 2, TimeUnit.SECONDS);
        readExecutor.scheduleAtFixedRate(get, 0, 2, TimeUnit.SECONDS);
        countDownLatch.await();
        Runnable compact = () -> {
            log.info("Manual Compaction");
            long start = System.nanoTime();
            kvEngine.compactRange(IKVEngine.DEFAULT_NS, null, null);
            log.info("Compaction cost {} ns", Duration.ofNanos(System.nanoTime() - start).toMillis());
        };
        writeExecutor.scheduleAtFixedRate(compact, 0, 5, TimeUnit.SECONDS);
        Thread.sleep(600000);
    }

    @SneakyThrows
    @Test
    @Ignore
    public void testCompaction() {
        int keyCount = 10000000;
        CountDownLatch countDownLatch = new CountDownLatch(keyCount);
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        ByteString existKey = key.concat(TestUtil.toByteString(keyCount));
        ScheduledExecutorService writeExecutor = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService readExecutor = Executors.newSingleThreadScheduledExecutor();
        kvEngine.put(IKVEngine.DEFAULT_NS, existKey, value.concat(TestUtil.toByteString(keyCount)));
        Runnable write = () -> {
            int batchId = kvEngine.startBatch();
            for (int i = 1; i <= keyCount; i++) {
                kvEngine.put(batchId, IKVEngine.DEFAULT_NS, key.concat(TestUtil.toByteString(i)), value.concat(
                    TestUtil.toByteString(i)));
                kvEngine.delete(batchId, IKVEngine.DEFAULT_NS, key.concat(TestUtil.toByteString(i)));
                if (i % (keyCount / 100) == 0) {
                    log.info("Write {} %", i / (keyCount / 100));
                    kvEngine.endBatch(batchId);
                    batchId = kvEngine.startBatch();
                }
                countDownLatch.countDown();
            }
        };
        writeExecutor.execute(write);

//        Runnable compact = () -> {
//            log.info("Compacting");
//            List<CompletableFuture<Void>> futures = new LinkedList<>();
//            for (int i = 0; i < 100; i++) {
//                futures.add(kvEngine.compactRange(DEFAULT_NS, ByteString.copyFromUtf8(i + ""),
//                        ByteString.copyFromUtf8(i + 1 + "")));
//            }
//            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
//            log.info("Compacted");
//        };
//        readExecutor.scheduleAtFixedRate(compact, 1, 1, TimeUnit.SECONDS);
        Thread.sleep(6000000);
    }

    private byte[] toBytesNativeOrder(long l) {
        return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.nativeOrder()).putLong(l).array();
    }

    private ByteString toByteStringNativeOrder(long l) {
        return UnsafeByteOperations.unsafeWrap(toBytesNativeOrder(l));
    }


    private long toLongNativeOrder(ByteString b) {
        assert b.size() == Long.BYTES;
        ByteBuffer buffer = b.asReadOnlyByteBuffer().order(ByteOrder.nativeOrder());
        return buffer.getLong();
    }


    private void assertGood(RocksIterator it, boolean good) {
        try {
            it.status();
            if (!good) {
                fail();
            }
        } catch (Throwable e) {
            if (good) {
                fail();
            }
        }
    }

    protected boolean isUsed(String checkpointId) {
        return checkpointId.equals(cp.get());
    }
}
