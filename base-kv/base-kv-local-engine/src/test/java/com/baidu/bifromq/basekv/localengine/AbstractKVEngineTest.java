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

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basekv.localengine.proto.KeyBoundary;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Optional;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public abstract class AbstractKVEngineTest {
    protected IKVEngine engine;

    @BeforeMethod
    public void setup() throws IOException {
        engine = newEngine();
        engine.start();
    }

    @AfterMethod
    public void teardown() {
        engine.stop();
    }

    protected abstract IKVEngine newEngine();

    @Test
    public void createIfMissing() {
        String rangeId = "test_range1";
        IKVSpace keyRange = engine.createIfMissing(rangeId);
        IKVSpace keyRange1 = engine.createIfMissing(rangeId);
        assertEquals(keyRange1, keyRange);
    }

    @Test
    public void size() {
        String rangeId = "test_range1";
        String rangeId1 = "test_range2";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace keyRange = engine.createIfMissing(rangeId);
        assertEquals(keyRange.size(), 0);
        keyRange.toWriter().put(key, value).done();
        assertTrue(keyRange.size() > 0);

        IKVSpace keyRange1 = engine.createIfMissing(rangeId1);
        assertEquals(keyRange1.size(), 0);
    }

    @Test
    public void metadata() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace keyRange = engine.createIfMissing(rangeId);
        keyRange.toWriter().metadata(key, value).done();
        assertTrue(keyRange.metadata(key).isPresent());
        assertEquals(keyRange.metadata(key).get(), value);
    }

    @Test
    public void describe() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace keyRange = engine.createIfMissing(rangeId);
        KVSpaceDescriptor descriptor = keyRange.describe();
        assertEquals(descriptor.id(), rangeId);
        assertEquals(descriptor.metrics().get("size"), 0);

        keyRange.toWriter().put(key, value).metadata(key, value).done();
        descriptor = keyRange.describe();
        assertTrue(descriptor.metrics().get("size") > 0);
    }

    @Test
    public void keyRangeDestroy() {
        String rangeId = "test_range1";
        IKVSpace range = engine.createIfMissing(rangeId);
        assertTrue(engine.ranges().containsKey(rangeId));
        range.destroy();
        assertTrue(engine.ranges().isEmpty());
        assertFalse(engine.ranges().containsKey(rangeId));
    }

    @Test
    public void keyRangeDestroyAndCreate() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace range = engine.createIfMissing(rangeId);
        range.toWriter().put(key, value).done();
        range.destroy();

        range = engine.createIfMissing(rangeId);
        assertFalse(range.exist(key));
    }

    @Test
    public void checkpoint() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace keyRange = engine.createIfMissing(rangeId);
        assertFalse(keyRange.latestCheckpoint().isPresent());

        String checkpointId = keyRange.checkpoint();
        assertEquals(checkpointId, keyRange.latestCheckpoint().get());
        Optional<IKVSpaceReader> checkpoint = keyRange.open(checkpointId);
        assertTrue(checkpoint.isPresent());
        assertEquals(checkpoint.get(), keyRange.open(checkpointId).get());

        keyRange.toWriter().put(key, value).done();
        assertFalse(checkpoint.get().exist(key));

        checkpoint = keyRange.open(keyRange.checkpoint());
        assertTrue(checkpoint.isPresent());
        assertTrue(checkpoint.get().exist(key));
    }

    @Test
    public void gc() {
        String rangeId = "test_range1";
        IKVSpace keyRange = engine.createIfMissing(rangeId);
        String checkpointId = keyRange.checkpoint();
        Optional<IKVSpaceReader> checkpoint = keyRange.open(checkpointId);
        String checkpointId1 = keyRange.checkpoint();
        assertNotEquals(checkpointId1, checkpointId);

        checkpoint = null;
        await().forever().until(() -> {
            System.gc();
            return keyRange.open(checkpointId).isEmpty();
        });
    }

    @Test
    public void exist() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace keyRange = engine.createIfMissing(rangeId);
        assertFalse(keyRange.exist(key));

        IKVSpaceWriter rangeWriter = keyRange.toWriter().put(key, value);
        assertFalse(keyRange.exist(key));

        rangeWriter.done();
        assertTrue(keyRange.exist(key));
    }

    @Test
    public void get() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace keyRange = engine.createIfMissing(rangeId);
        assertFalse(keyRange.get(key).isPresent());

        IKVSpaceWriter rangeWriter = keyRange.toWriter().put(key, value);
        assertFalse(keyRange.get(key).isPresent());

        rangeWriter.done();
        assertTrue(keyRange.get(key).isPresent());
        assertEquals(keyRange.get(key).get(), value);
    }

    @Test
    public void iterator() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace keyRange = engine.createIfMissing(rangeId);

        try (IKVSpaceIterator keyRangeIterator = keyRange.newIterator()) {
            keyRangeIterator.seekToFirst();
            assertFalse(keyRangeIterator.isValid());
            keyRange.toWriter().put(key, value).done();

            keyRangeIterator.seekToFirst();
            assertFalse(keyRangeIterator.isValid());
            keyRangeIterator.refresh();

            keyRangeIterator.seekToFirst();
            assertTrue(keyRangeIterator.isValid());
            assertEquals(keyRangeIterator.key(), key);
            assertEquals(keyRangeIterator.value(), value);
            keyRangeIterator.next();
            assertFalse(keyRangeIterator.isValid());

            keyRangeIterator.seekToLast();
            assertTrue(keyRangeIterator.isValid());
            assertEquals(keyRangeIterator.key(), key);
            assertEquals(keyRangeIterator.value(), value);
            keyRangeIterator.next();
            assertFalse(keyRangeIterator.isValid());

            keyRangeIterator.seekForPrev(key);
            assertTrue(keyRangeIterator.isValid());
            assertEquals(keyRangeIterator.key(), key);
            assertEquals(keyRangeIterator.value(), value);
            keyRangeIterator.next();
            assertFalse(keyRangeIterator.isValid());
        }
    }

    @Test
    public void iterateSubBoundary() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace keyRange = engine.createIfMissing(rangeId);

        try (IKVSpaceIterator keyRangeIterator = keyRange.newIterator(KeyBoundary.newBuilder()
            .setStartKey(key)
            .build())) {
            keyRangeIterator.seekToFirst();
            assertFalse(keyRangeIterator.isValid());
            keyRange.toWriter().put(key, value).done();

            keyRangeIterator.seekToFirst();
            assertFalse(keyRangeIterator.isValid());
            keyRangeIterator.refresh();

            keyRangeIterator.seekToFirst();
            assertTrue(keyRangeIterator.isValid());
            assertEquals(keyRangeIterator.key(), key);
            assertEquals(keyRangeIterator.value(), value);
            keyRangeIterator.next();
            assertFalse(keyRangeIterator.isValid());

            keyRangeIterator.seekToLast();
            assertTrue(keyRangeIterator.isValid());
            assertEquals(keyRangeIterator.key(), key);
            assertEquals(keyRangeIterator.value(), value);
            keyRangeIterator.next();
            assertFalse(keyRangeIterator.isValid());

            keyRangeIterator.seekForPrev(key);
            assertTrue(keyRangeIterator.isValid());
            assertEquals(keyRangeIterator.key(), key);
            assertEquals(keyRangeIterator.value(), value);
            keyRangeIterator.next();
            assertFalse(keyRangeIterator.isValid());
        }
        try (IKVSpaceIterator keyRangeIterator = keyRange.newIterator(KeyBoundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("0"))
            .setEndKey(ByteString.copyFromUtf8("9"))
            .build())) {
            keyRangeIterator.seekToFirst();
            assertFalse(keyRangeIterator.isValid());

            keyRange.toWriter().put(key, value).done();

            keyRangeIterator.refresh();

            keyRangeIterator.seekToFirst();
            assertFalse(keyRangeIterator.isValid());
        }
    }

    @Test
    public void writer() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace keyRange = engine.createIfMissing(rangeId);
        keyRange.toWriter()
            .metadata(key, value)
            .put(key, value)
            .delete(key).done();
        assertFalse(keyRange.exist(key));

        IKVSpaceWriter rangeWriter = keyRange.toWriter();
        assertEquals(rangeWriter.metadata(key).get(), value);
        rangeWriter.insert(key, value).done();
        assertTrue(keyRange.exist(key));

        keyRange.toWriter().clear().done();
        assertFalse(keyRange.exist(key));
    }

    @Test
    public void clearSubBoundary() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace keyRange = engine.createIfMissing(rangeId);
        keyRange.toWriter().put(key, value).done();

        keyRange.toWriter()
            .clear(KeyBoundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("0"))
                .setEndKey(ByteString.copyFromUtf8("9"))
                .build())
            .done();
        assertTrue(keyRange.exist(key));
    }

    @Test
    public void migrateTo() {
        String leftRangeId = "test_range1";
        String rightRangeId = "test_range2";
        ByteString key1 = ByteString.copyFromUtf8("1");
        ByteString value1 = ByteString.copyFromUtf8("1");
        ByteString key2 = ByteString.copyFromUtf8("6");
        ByteString value2 = ByteString.copyFromUtf8("6");
        ByteString splitKey = ByteString.copyFromUtf8("5");

        ByteString metaKey = ByteString.copyFromUtf8("metaKey");
        ByteString metaVal = ByteString.copyFromUtf8("metaVal");


        IKVSpace leftRange = engine.createIfMissing(leftRangeId);
        leftRange.toWriter()
            .put(key1, value1)
            .put(key2, value2)
            .done();
        IKVSpaceWriter leftRangeWriter = leftRange.toWriter();
        leftRangeWriter
            .migrateTo(rightRangeId, KeyBoundary.newBuilder().setStartKey(splitKey).build())
            .metadata(metaKey, metaVal);
        leftRangeWriter.done();

        IKVSpace rightRange = engine.createIfMissing(rightRangeId);

        assertFalse(leftRange.metadata(metaKey).isPresent());
        assertTrue(rightRange.metadata(metaKey).isPresent());
        assertEquals(rightRange.metadata(metaKey).get(), metaVal);
        assertFalse(leftRange.exist(key2));
        assertTrue(rightRange.exist(key2));
    }

    @Test
    public void migrateFrom() {
        String leftRangeId = "test_range1";
        String rightRangeId = "test_range2";
        ByteString key1 = ByteString.copyFromUtf8("1");
        ByteString value1 = ByteString.copyFromUtf8("1");
        ByteString key2 = ByteString.copyFromUtf8("6");
        ByteString value2 = ByteString.copyFromUtf8("6");
        ByteString splitKey = ByteString.copyFromUtf8("5");

        ByteString metaKey = ByteString.copyFromUtf8("metaKey");
        ByteString metaVal = ByteString.copyFromUtf8("metaVal");


        IKVSpace leftRange = engine.createIfMissing(leftRangeId);
        leftRange.toWriter()
            .put(key1, value1)
            .done();
        assertFalse(leftRange.exist(key2));
        IKVSpace rightRange = engine.createIfMissing(rightRangeId);
        rightRange.toWriter()
            .put(key2, value2)
            .done();
        assertTrue(rightRange.exist(key2));

        IKVSpaceWriter leftRangeWriter = leftRange.toWriter();
        leftRangeWriter
            .migrateFrom(rightRangeId, KeyBoundary.newBuilder().setStartKey(splitKey).build())
            .metadata(metaKey, metaVal);
        leftRangeWriter
            .metadata(metaKey, metaVal)
            .done();

        assertTrue(leftRange.metadata(metaKey).isPresent());
        assertTrue(rightRange.metadata(metaKey).isPresent());

        assertEquals(rightRange.metadata(metaKey).get(), metaVal);
        assertEquals(rightRange.metadata(metaKey).get(), metaVal);

        assertTrue(leftRange.exist(key2));
        assertFalse(rightRange.exist(key2));
    }
}
