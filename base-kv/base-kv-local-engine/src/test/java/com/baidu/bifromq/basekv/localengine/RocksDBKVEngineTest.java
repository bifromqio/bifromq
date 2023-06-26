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

import static com.google.protobuf.ByteString.EMPTY;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

@Slf4j
public class RocksDBKVEngineTest extends AbstractKVEngineTest {
    public Path dbRootDir;
    private ScheduledExecutorService bgTaskExecutor;

    @BeforeMethod
    public void setup() throws IOException {
        dbRootDir = Files.createTempDirectory("");
        bgTaskExecutor =
                newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Checkpoint GC").build());
        start();
    }

    @AfterMethod
    public void teardown() {
        stop();
        MoreExecutors.shutdownAndAwaitTermination(bgTaskExecutor, 5, TimeUnit.SECONDS);
        TestUtil.deleteDir(dbRootDir.toString());
    }

    private void start() {
        String DB_NAME = "testDB";
        String DB_CHECKPOINT_DIR = "testDB_cp";
        RocksDBKVEngineConfigurator configurator = new RocksDBKVEngineConfigurator()
            .setDbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR).toString())
            .setDbRootDir(Paths.get(dbRootDir.toString(), DB_NAME).toString());
        kvEngine = new RocksDBKVEngine(null, List.of(IKVEngine.DEFAULT_NS, NS),
            this::isUsed, configurator, Duration.ofSeconds(-1));
        kvEngine.start(bgTaskExecutor);
    }

    private void stop() {
        kvEngine.stop();
    }

    @Test
    public void testReclaimNativeResourcesOnGC() {
        IKVEngineIterator it1 = kvEngine.newIterator(IKVEngine.DEFAULT_NS);
        IKVEngineIterator delegate = TestUtil.getField(it1, "delegate");
        IKVEngineIterator delegate1 = TestUtil.getField(delegate, "delegate");
        RocksIterator internalIt1 = TestUtil.getField(delegate1, "rocksIterator");
        assertGood(internalIt1, true);
        it1 = null;
        System.gc();
        assertGood(internalIt1, false);

        // read from checkpoint
        String cpId = kvEngine.checkpoint();
        IKVEngineIterator it3 = kvEngine.newIterator(cpId, IKVEngine.DEFAULT_NS);
        RocksIterator internalIt3 = TestUtil.getField(it3, "rocksIterator");
        assertGood(internalIt3, true);
        it3 = null;
        System.gc();
        assertGood(internalIt3, false);
    }

    @Test
    public void testIteratorClosedByFinalizer() {
        IKVEngineIterator it1 = kvEngine.newIterator(IKVEngine.DEFAULT_NS);
    }

    @Test
    public void testReusedIteratorWithDeleteRange2() throws RocksDBException {
        String dbPath = dbRootDir.toString();
        try (Options options = new Options()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true);
             RocksDB db = RocksDB.open(options, dbPath)) {
            // Init some data
            db.put("key1".getBytes(), "value1".getBytes());

            // Test: test reused iterator with DeleteRange
            try (RocksIterator iterator = db.newIterator()) {
                // In fact, calling close() on default CF has no effect
                db.getDefaultColumnFamily().close();

                iterator.seekToFirst();
                assertTrue(iterator.isValid());
                assertArrayEquals("key1".getBytes(), iterator.key());
                assertArrayEquals("value1".getBytes(), iterator.value());
                iterator.next();
                assertFalse(iterator.isValid());

                // Insert 'key2' then iterate again
                db.put("key2".getBytes(), "value2".getBytes());

                iterator.refresh();
                iterator.seekToFirst();
                assertTrue(iterator.isValid());
                assertArrayEquals("key1".getBytes(), iterator.key());
                assertArrayEquals("value1".getBytes(), iterator.value());
                iterator.next();
                assertTrue(iterator.isValid());
                assertArrayEquals("key2".getBytes(), iterator.key());
                assertArrayEquals("value2".getBytes(), iterator.value());
                iterator.next();
                assertFalse(iterator.isValid());

                // Delete 'key2' then iterate again
//                db.deleteRange("key2".getBytes(), "key3".getBytes());
//                db.flush(new FlushOptions());
                db.singleDelete("key2".getBytes());

                /*
                 * Here the test case is fine when updating to as follows:
                 *  db.delete("key2".getBytes());
                 *   or
                 *  iterator = db.newIterator();
                 */
                iterator.refresh();
                iterator.seekToFirst();
                assertTrue(iterator.isValid());
                assertArrayEquals("key1".getBytes(), iterator.key());
                assertArrayEquals("value1".getBytes(), iterator.value());
                iterator.next();
                // Error: expect not exist key2 anymore, but got it
                assertFalse(iterator.isValid());
            }
        }
    }

    @Test
    public void testDeleteRangeAndRefreshIterator() {
        int rangeId = kvEngine.registerKeyRange(IKVEngine.DEFAULT_NS, null, RangeUtil.upperBound(copyFromUtf8("Key")));

        ByteString key1 = copyFromUtf8("Key1");
        ByteString val1 = copyFromUtf8("Val1");
        ByteString key2 = copyFromUtf8("Key2");
        ByteString val2 = copyFromUtf8("Val2");
        // Init some data
        kvEngine.put(rangeId, key1, val1);

        // Test: test reused iterator with DeleteRange
        try (IKVEngineIterator iterator = kvEngine.newIterator(rangeId)) {

            iterator.seekToFirst();
            assertTrue(iterator.isValid());
            assertEquals(iterator.key(), key1);
            assertEquals(iterator.value(), val1);
            iterator.next();
            assertFalse(iterator.isValid());

            // Insert 'key2' then iterate again
            kvEngine.put(rangeId, key2, val2);

            iterator.refresh();
            iterator.seekToFirst();
            assertTrue(iterator.isValid());
            assertEquals(iterator.key(), key1);
            assertEquals(iterator.value(), val1);
            iterator.next();
            assertTrue(iterator.isValid());
            assertEquals(iterator.key(), key2);
            assertEquals(iterator.value(), val2);
            iterator.next();
            assertFalse(iterator.isValid());

            // Delete 'key2' then iterate again
            kvEngine.delete(rangeId, key2);

            /*
             * Here the test case is fine when updating to as follows:
             *  db.delete("key2".getBytes());
             *   or
             *  iterator = db.newIterator();
             */
//            kvStore.delete(NS, key2);

            iterator.refresh();
            iterator.seekToFirst();
            assertTrue(iterator.isValid());
            assertEquals(iterator.key(), key1);
            assertEquals(iterator.value(), val1);
            iterator.next();
            // Error: expect not exist key2 anymore, but got it
            assertFalse(iterator.isValid());
        }
    }

    @Test
    public void testSizeAfterRestart() {
        int rangeId = kvEngine.registerKeyRange(IKVEngine.DEFAULT_NS, null, RangeUtil.upperBound(copyFromUtf8("key")));

        for (int i = 0; i < 1000; i++) {
            kvEngine.put(rangeId, copyFromUtf8("key" + i), copyFromUtf8("value" + i));
        }

        stop();
        start();
        rangeId = kvEngine.registerKeyRange(IKVEngine.DEFAULT_NS, null, RangeUtil.upperBound(copyFromUtf8("key")));
        for (int i = 0; i < 1000; i++) {
            assertTrue(kvEngine.get(rangeId, copyFromUtf8("key" + i)).isPresent());
        }

        rangeId = kvEngine.registerKeyRange(IKVEngine.DEFAULT_NS, copyFromUtf8("key0"), copyFromUtf8("key999"));
        long size = kvEngine.size(rangeId);
        assertTrue(size > 0);

        rangeId = kvEngine.registerKeyRange(IKVEngine.DEFAULT_NS, copyFromUtf8("key0"), null);
        size = kvEngine.size(rangeId);
        assertTrue(size > 0);

        rangeId = kvEngine.registerKeyRange(IKVEngine.DEFAULT_NS, null, RangeUtil.upperBound(copyFromUtf8("key")));
        size = kvEngine.size(rangeId);
        assertTrue(size > 0);

        rangeId = kvEngine.registerKeyRange(IKVEngine.DEFAULT_NS, EMPTY, copyFromUtf8("key0"));
        size = kvEngine.size(rangeId);
        assertEquals(size, 0);

        rangeId = kvEngine.registerKeyRange(IKVEngine.DEFAULT_NS, EMPTY, EMPTY);
        size = kvEngine.size(rangeId);
        assertEquals(size, 0);
    }

    @Test
    public void testSizeAfterCompactRange() {
        for (int i = 0; i < 1000; i++) {
            kvEngine.put(IKVEngine.DEFAULT_NS, copyFromUtf8("key" + i), copyFromUtf8("value" + i));
        }

        long size = kvEngine.size(IKVEngine.DEFAULT_NS);
        log.info("Size = {} bytes", size);
        assertNotEquals(size, 0);

        kvEngine.clearRange(IKVEngine.DEFAULT_NS);
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
}
