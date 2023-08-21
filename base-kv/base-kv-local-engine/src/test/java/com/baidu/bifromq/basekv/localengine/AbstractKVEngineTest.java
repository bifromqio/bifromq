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

import static com.baidu.bifromq.basekv.localengine.RangeUtil.upperBound;
import static com.google.protobuf.ByteString.EMPTY;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public abstract class AbstractKVEngineTest {
    protected static final String NS = "test-namespace";
    protected final AtomicReference<String> cp = new AtomicReference<>();
    protected IKVEngine kvEngine;

    public static long toLong(ByteString b) {
        assert b.size() == Long.BYTES;
        ByteBuffer buffer = b.asReadOnlyByteBuffer();
        return buffer.getLong();
    }

    @BeforeMethod
    public abstract void setup() throws IOException;

    @AfterMethod
    public abstract void teardown();

    @Test
    public void testIdentity() {
        assertFalse(kvEngine.id().isEmpty());
    }

    @Test
    public void testRange() {
        int rangeId = kvEngine.registerKeyRange(NS, null, null);
        kvEngine.unregisterKeyRange(rangeId);
    }

    @Test
    public void testBatch() {
        int rangeId = kvEngine.registerKeyRange(NS, null, null);
        try {
            int batchId = kvEngine.startBatch();
            assertEquals(kvEngine.batchSize(batchId), 0);
            kvEngine.insert(batchId, rangeId, copyFromUtf8("key"), copyFromUtf8("value"));
            assertFalse(kvEngine.exist(rangeId, copyFromUtf8("key")));
            assertEquals(kvEngine.batchSize(batchId), 1);
            kvEngine.endBatch(batchId);
        } catch (Exception e) {
            fail();
        }
        assertTrue(kvEngine.exist(rangeId, copyFromUtf8("key")));

        try {
            int batchId = kvEngine.startBatch();
            kvEngine.put(batchId, rangeId, copyFromUtf8("key1"), copyFromUtf8("value"));
            assertFalse(kvEngine.exist(rangeId, copyFromUtf8("key1")));
            kvEngine.abortBatch(batchId);
        } catch (Exception e) {
            fail();
        }
        assertFalse(kvEngine.exist(rangeId, copyFromUtf8("key1")));
    }

    @Test
    public void testEmptyKey() {
        int rangeId = kvEngine.registerKeyRange(NS, null, null);
        ByteString value = copyFromUtf8("value");
        kvEngine.put(rangeId, EMPTY, value);
        assertTrue(kvEngine.exist(rangeId, EMPTY));
        assertEquals(kvEngine.get(rangeId, EMPTY).get(), value);
        try (IKVEngineIterator itr = kvEngine.newIterator(rangeId)) {
            itr.seekToFirst();
            assertEquals(itr.key(), EMPTY);
            assertEquals(itr.value(), value);
        }
        try (IKVEngineIterator itr = kvEngine.newIterator(rangeId, null, EMPTY)) {
            itr.seekToFirst();
            assertFalse(itr.isValid());
        }
        try (IKVEngineIterator itr = kvEngine.newIterator(rangeId, EMPTY, null)) {
            itr.seekToFirst();
            assertEquals(itr.key(), EMPTY);
            assertEquals(itr.value(), value);
        }
        kvEngine.delete(rangeId, EMPTY);
        assertFalse(kvEngine.exist(rangeId, EMPTY));
    }

    @Test
    public void testSkip() {
        int rangeId = kvEngine.registerKeyRange(NS, null, null);
        int batchId = kvEngine.startBatch();
        for (long i = 0; i < 1000; i++) {
            kvEngine.put(batchId, rangeId, toByteString(i), copyFromUtf8("value" + i));
        }
        kvEngine.endBatch(batchId);
        assertTrue(toLong(kvEngine.skip(rangeId, 500)) > 490);

        rangeId = kvEngine.registerKeyRange(NS, null, toByteString(500));
        assertTrue(toLong(kvEngine.skip(rangeId, 100)) > 90);

        rangeId = kvEngine.registerKeyRange(NS, toByteString(0), null);
        assertTrue(toLong(kvEngine.skip(rangeId, 100)) > 90);

        rangeId = kvEngine.registerKeyRange(NS, toByteString(100), toByteString(500));
        assertTrue(toLong(kvEngine.skip(rangeId, 100)) > 190);
    }

    @Test
    public void testStats() {
        int rangeId = kvEngine.registerKeyRange(NS, null, null);
        long size = kvEngine.size(rangeId);
        assertEquals(size, 0);

        int batchId = kvEngine.startBatch();
        kvEngine.put(batchId, rangeId, copyFromUtf8("key1"), copyFromUtf8("value"));
        kvEngine.put(batchId, rangeId, copyFromUtf8("key2"), copyFromUtf8("value"));
        kvEngine.put(batchId, rangeId, copyFromUtf8("key3"), copyFromUtf8("value"));
        kvEngine.endBatch(batchId);

        rangeId = kvEngine.registerKeyRange(NS, copyFromUtf8("key1"), copyFromUtf8("key100"));
        size = kvEngine.size(rangeId);
        assertTrue(size > 0);

        rangeId = kvEngine.registerKeyRange(NS, copyFromUtf8("key1"), null);
        size = kvEngine.size(rangeId);
        assertTrue(size > 0);

        rangeId = kvEngine.registerKeyRange(NS, null, upperBound(copyFromUtf8("key")));
        size = kvEngine.size(rangeId);
        assertTrue(size > 0);

        rangeId = kvEngine.registerKeyRange(NS, EMPTY, copyFromUtf8("key1"));
        size = kvEngine.size(rangeId);
        assertEquals(size, 0);

        rangeId = kvEngine.registerKeyRange(NS, EMPTY, EMPTY);
        size = kvEngine.size(rangeId);
        assertEquals(size, 0);
    }

    @Test
    public void testCheckpoint() {
        int rangeId = kvEngine.registerKeyRange(NS, null, null);

        ByteString key = copyFromUtf8("key1");
        ByteString value1 = copyFromUtf8("value1");
        ByteString value2 = copyFromUtf8("value2");
        kvEngine.put(rangeId, key, value1);
        String cpId = kvEngine.checkpoint();
        assertTrue(kvEngine.hasCheckpoint(cpId));

        kvEngine.put(rangeId, key, value2);

        try (IKVEngineIterator kvItr = kvEngine.newIterator(cpId, rangeId)) {
            kvItr.seekToFirst();
            assertTrue(kvItr.isValid());
            assertEquals(kvItr.key(), key);
            assertEquals(kvItr.value(), value1);
        }
        assertTrue(kvEngine.exist(cpId, rangeId, key));
        assertEquals(kvEngine.get(cpId, rangeId, key).get(), value1);
    }

    @Test
    public void testInsert() {
        int rangeId = kvEngine.registerKeyRange(NS, null, null);

        assertFalse(kvEngine.exist(rangeId, copyFromUtf8("key1")));
        kvEngine.insert(rangeId, copyFromUtf8("key1"), copyFromUtf8("value1"));
        assertTrue(kvEngine.exist(rangeId, copyFromUtf8("key1")));
        assertEquals(kvEngine.get(rangeId, copyFromUtf8("key1")).get(), copyFromUtf8("value1"));
        try {
            kvEngine.insert(rangeId, copyFromUtf8("key1"), copyFromUtf8("value2"));
            fail();
        } catch (Throwable e) {
            assertTrue(e instanceof AssertionError);
        }
    }

    @Test
    public void testPut() {
        int rangeId = kvEngine.registerKeyRange(NS, null, null);
        kvEngine.put(rangeId, copyFromUtf8("key1"), copyFromUtf8("value1"));
        assertEquals(kvEngine.get(rangeId, copyFromUtf8("key1")).get(), copyFromUtf8("value1"));
        assertTrue(kvEngine.exist(rangeId, copyFromUtf8("key1")));

        kvEngine.put(rangeId, copyFromUtf8("key1"), copyFromUtf8("value2"));
        assertEquals(kvEngine.get(rangeId, copyFromUtf8("key1")).get(), copyFromUtf8("value2"));
    }

    @Test
    public void testNull() {
        int rangeId = kvEngine.registerKeyRange(NS, null, null);

        assertFalse(kvEngine.get(rangeId, EMPTY).isPresent());
        kvEngine.put(rangeId, EMPTY, EMPTY);
        assertTrue(kvEngine.get(rangeId, EMPTY).isPresent());
    }

    @Test
    public void testIteratorWithOpenRange() {
        int rangeId = kvEngine.registerKeyRange(NS, null, null);

        for (long i = 1; i <= 100; i++) {
            kvEngine.put(rangeId, toByteString(i), EMPTY);
        }
        try (IKVEngineIterator itr = kvEngine.newIterator(rangeId, EMPTY, EMPTY)) {
            itr.seekToFirst();
            assertFalse(itr.isValid());
        }
        try (IKVEngineIterator itr = kvEngine.newIterator(rangeId, null, toByteString(1))) {
            itr.seekToFirst();
            assertFalse(itr.isValid());
        }
        try (IKVEngineIterator itr = kvEngine.newIterator(rangeId, toByteString(1), upperBound(copyFromUtf8("Key")))) {
            itr.seekToFirst();
            assertTrue(itr.isValid());
            assertEquals(toLong(itr.key()), 1);
        }
        try (IKVEngineIterator itr = kvEngine.newIterator(rangeId, null, upperBound(copyFromUtf8("Key")))) {
            itr.seekToFirst();
            assertTrue(itr.isValid());
            assertEquals(toLong(itr.key()), 1);
        }
    }

    @Test
    public void testIteratorRefresh() {
        ByteString key = copyFromUtf8("key1");
        ByteString val = copyFromUtf8("value1");
        int rangeId = kvEngine.registerKeyRange(NS, null, null);
        IKVEngineIterator itr = kvEngine.newIterator(rangeId);
        itr.seek(key);
        assertFalse(itr.isValid());

        kvEngine.put(rangeId, key, val);

        itr.seek(key);
        assertFalse(itr.isValid());

        itr.refresh();
        itr.seek(key);
        assertTrue(itr.isValid());
    }

    @Test
    public void testOpenMultipleIteratorsOfSameCheckpoint() throws InterruptedException {
        int rangeId = kvEngine.registerKeyRange(NS, null, null);

        ByteString key = copyFromUtf8("key1");
        ByteString value1 = copyFromUtf8("value1");
        ByteString value2 = copyFromUtf8("value2");
        kvEngine.put(rangeId, key, value1);
        String cpId = kvEngine.checkpoint();
        assertTrue(kvEngine.hasCheckpoint(cpId));

        kvEngine.put(rangeId, key, value2);
        CountDownLatch countDownLatch = new CountDownLatch(3);
        ForkJoinPool.commonPool().submit(() -> {
            try {
                try (IKVEngineIterator kvItr = kvEngine.newIterator(cpId, rangeId)) {
                    kvItr.seekToFirst();
                    assertTrue(kvItr.isValid());
                    assertEquals(kvItr.key(), key);
                    assertEquals(kvItr.value(), value1);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            countDownLatch.countDown();
        });
        ForkJoinPool.commonPool().submit(() -> {
            try {
                try (IKVEngineIterator kvItr = kvEngine.newIterator(cpId, rangeId)) {
                    kvItr.seekToFirst();
                    assertTrue(kvItr.isValid());
                    assertEquals(kvItr.key(), key);
                    assertEquals(kvItr.value(), value1);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            countDownLatch.countDown();
        });
        ForkJoinPool.commonPool().submit(() -> {
            try {
                try (IKVEngineIterator kvItr = kvEngine.newIterator(cpId, rangeId)) {
                    kvItr.seekToFirst();
                    assertTrue(kvItr.isValid());
                    assertEquals(kvItr.key(), key);
                    assertEquals(kvItr.value(), value1);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            countDownLatch.countDown();
        });
        countDownLatch.await();
    }

    @Test
    public void testCheckpointGC() {
        String cpId1 = kvEngine.checkpoint();
        String cpId2 = kvEngine.checkpoint();
        String cpId3 = kvEngine.checkpoint();
        cp.set(cpId1);
        // latest cp always treated as in-use
        await().until(
            () -> kvEngine.hasCheckpoint(cpId1) && !kvEngine.hasCheckpoint(cpId2) && kvEngine.hasCheckpoint(cpId3));
    }

    @Test
    public void testClearRange() {
        int rangeId = kvEngine.registerKeyRange(NS, null, null);

        ByteString skey = copyFromUtf8("Key1");
        ByteString ekey = copyFromUtf8("Key10");
        kvEngine.put(rangeId, skey, ByteString.EMPTY);
        kvEngine.clearSubRange(rangeId, skey, ekey);
        assertFalse(kvEngine.exist(rangeId, skey));

        kvEngine.put(rangeId, skey, ByteString.EMPTY);
        assertTrue(kvEngine.exist(rangeId, skey));
        kvEngine.clearRange(rangeId);
        assertFalse(kvEngine.exist(rangeId, skey));

        kvEngine.put(rangeId, skey, ByteString.EMPTY);
        assertTrue(kvEngine.exist(rangeId, skey));
        kvEngine.clearSubRange(rangeId, skey, upperBound(copyFromUtf8("Key")));
        assertFalse(kvEngine.exist(rangeId, skey));

        kvEngine.put(rangeId, skey, ByteString.EMPTY);
        assertTrue(kvEngine.exist(rangeId, skey));
        kvEngine.clearSubRange(rangeId, copyFromUtf8("Key2"), upperBound(copyFromUtf8("Key")));
        assertTrue(kvEngine.exist(rangeId, skey));

        kvEngine.put(rangeId, skey, ByteString.EMPTY);
        assertTrue(kvEngine.exist(rangeId, skey));
        kvEngine.clearSubRange(rangeId, copyFromUtf8("Key3"), upperBound(copyFromUtf8("Key")));
        assertTrue(kvEngine.exist(rangeId, skey));
    }

    @Test
    public void testClearRangeThenInsert() {
        int rangeId = kvEngine.registerKeyRange(NS, null, null);

        ByteString key1 = copyFromUtf8("key1");
        ByteString key2 = copyFromUtf8("key2");
        ByteString value1 = copyFromUtf8("def");
        kvEngine.put(rangeId, key1, value1);
        assertTrue(kvEngine.exist(rangeId, key1));
        int batchId = kvEngine.startBatch();
        kvEngine.clearSubRange(batchId, rangeId, key1, key2);
        kvEngine.put(batchId, rangeId, key1, value1);
        kvEngine.endBatch(batchId);
        assertTrue(kvEngine.exist(rangeId, key1));
    }

    protected boolean isUsed(String checkpointId) {
        return checkpointId.equals(cp.get());
    }

    private ByteString toByteString(long l) {
        return unsafeWrap(ByteBuffer.allocate(Long.BYTES).putLong(l).array());
    }
}
