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

package com.baidu.bifromq.basekv.store.range;

import static com.baidu.bifromq.basekv.Constants.FULL_RANGE;
import static com.baidu.bifromq.basekv.TestUtil.isDevEnv;
import static com.baidu.bifromq.basekv.localengine.IKVEngine.DEFAULT_NS;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.baidu.bifromq.basekv.TestUtil;
import com.baidu.bifromq.basekv.localengine.IKVEngine;
import com.baidu.bifromq.basekv.localengine.InMemoryKVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.KVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.KVEngineFactory;
import com.baidu.bifromq.basekv.localengine.RocksDBKVEngineConfigurator;
import com.baidu.bifromq.basekv.proto.KVRangeSnapshot;
import com.baidu.bifromq.basekv.proto.Range;
import com.baidu.bifromq.basekv.proto.State;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class KVRangeStateTest {
    @Rule
    public TemporaryFolder dbRootDir = new TemporaryFolder();
    private String DB_NAME = "testDB";
    private String DB_CHECKPOINT_DIR_NAME = "testDB_cp";
    private ScheduledExecutorService bgMgmtTaskExecutor;
    private KVEngineConfigurator configurator = null;
    private IKVEngine kvEngine;

    @Before
    public void setup() {
        bgMgmtTaskExecutor =
            newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Checkpoint GC").build());
        if (!isDevEnv()) {
            configurator = new InMemoryKVEngineConfigurator();
        } else {
            configurator = new RocksDBKVEngineConfigurator()
                .setDbCheckpointRootDir(Paths.get(dbRootDir.getRoot().toString(), DB_CHECKPOINT_DIR_NAME)
                    .toString())
                .setDbRootDir(Paths.get(dbRootDir.getRoot().toString(), DB_NAME).toString());
        }

        kvEngine = KVEngineFactory.create(null, List.of(DEFAULT_NS), cpId -> false, configurator);
        kvEngine.start(bgMgmtTaskExecutor);
    }

    @After
    public void teardown() {
        kvEngine.stop();
        MoreExecutors.shutdownAndAwaitTermination(bgMgmtTaskExecutor, 5, TimeUnit.SECONDS);
        if (configurator instanceof RocksDBKVEngineConfigurator) {
            TestUtil.deleteDir(dbRootDir.getRoot().toString());
        }
    }

    @Test
    public void initWithSnapshot() {
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(0)
            .setLastAppliedIndex(0)
            .setState(State.newBuilder().setType(State.StateType.Normal).build())
            .setRange(FULL_RANGE)
            .build();
        KVRangeState accessor = new KVRangeState(snapshot, kvEngine);
        IKVRangeState.KVRangeMeta meta = accessor.metadata().blockingFirst();
        assertEquals(snapshot.getVer(), meta.ver);
        assertEquals(snapshot.getRange(), meta.range);
        assertEquals(snapshot.getState(), meta.state);
        assertEquals(snapshot.getLastAppliedIndex(), accessor.getReader().lastAppliedIndex());
    }

    @Test
    public void initExistingRange() {
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(0)
            .setLastAppliedIndex(0)
            .setState(State.newBuilder().setType(State.StateType.Normal).build())
            .setRange(FULL_RANGE)
            .build();
        new KVRangeState(snapshot, kvEngine);

        KVRangeState accessor = new KVRangeState(snapshot.getId(), kvEngine);
        IKVRangeState.KVRangeMeta meta = accessor.metadata().blockingFirst();
        assertEquals(snapshot.getVer(), meta.ver);
        assertEquals(snapshot.getRange(), meta.range);
        assertEquals(snapshot.getState(), meta.state);
        assertEquals(snapshot.getLastAppliedIndex(), accessor.getReader().lastAppliedIndex());
    }

    @Test
    public void metadata() {
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(0)
            .setLastAppliedIndex(0)
            .setState(State.newBuilder().setType(State.StateType.Normal).build())
            .setRange(FULL_RANGE)
            .build();
        KVRangeState accessor = new KVRangeState(snapshot, kvEngine);
        accessor.getWriter().resetVer(2).close();
        IKVRangeState.KVRangeMeta meta = accessor.metadata().blockingFirst();
        assertEquals(2, meta.ver);
    }

    @Test
    public void checkpoint() {
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(0)
            .setLastAppliedIndex(0)
            .setState(State.newBuilder().setType(State.StateType.Normal).build())
            .setRange(FULL_RANGE)
            .build();
        KVRangeState accessor = new KVRangeState(snapshot, kvEngine);
        assertFalse(accessor.hasCheckpoint(snapshot));

        KVRangeSnapshot snap = accessor.checkpoint();

        assertTrue(accessor.hasCheckpoint(snap));
        assertTrue(snap.hasCheckpointId());
        assertEquals(snapshot.getId(), snap.getId());
        assertEquals(snapshot.getVer(), snap.getVer());
        assertEquals(snapshot.getLastAppliedIndex(), snap.getLastAppliedIndex());
        assertEquals(snapshot.getState(), snap.getState());
        assertEquals(snapshot.getRange(), snap.getRange());
    }

    @Test
    public void openCheckpoint() {
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(0)
            .setLastAppliedIndex(0)
            .setState(State.newBuilder().setType(State.StateType.Normal).build())
            .setRange(FULL_RANGE)
            .build();
        KVRangeState accessor = new KVRangeState(snapshot, kvEngine);
        snapshot = accessor.checkpoint();

        ByteString key = ByteString.copyFromUtf8("Key");
        ByteString val = ByteString.copyFromUtf8("Value");
        IKVRangeWriter rangeWriter = accessor.getWriter();
        rangeWriter.kvWriter().put(key, val);
        rangeWriter.close();

        IKVIterator itr = accessor.open(snapshot);
        itr.seekToFirst();
        assertFalse(itr.isValid());

        snapshot = accessor.checkpoint();
        itr = accessor.open(snapshot);
        itr.seekToFirst();
        assertTrue(itr.isValid());
        assertEquals(key, itr.key());
        assertEquals(val, itr.value());
        itr.next();
        assertFalse(itr.isValid());
    }

    @Test
    public void readWrite() {
        Range range = Range.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("c"))
            .build();
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(0)
            .setLastAppliedIndex(0)
            .setState(State.newBuilder().setType(State.StateType.Normal).build())
            .setRange(range)
            .build();
        KVRangeState accessor = new KVRangeState(snapshot, kvEngine);
        IKVRangeWriter rangeWriter = accessor.getWriter();
        IKVRangeReader rangeReader = accessor.getReader();
        ByteString key = ByteString.copyFromUtf8("aKey");
        ByteString val = ByteString.copyFromUtf8("Value");
        rangeWriter.kvWriter().put(key, val);
        rangeWriter.close();
        assertFalse(rangeReader.kvReader().exist(key));
        rangeReader.refresh();
        assertEquals(val, rangeReader.kvReader().get(key).get());

        // make a range change
        Range newRange = range.toBuilder().setStartKey(ByteString.copyFromUtf8("b")).build();
        accessor.getWriter().resetVer(1).setRange(newRange).close();
        rangeReader.refresh();
        assertEquals(1, rangeReader.ver());
        assertEquals(newRange, rangeReader.kvReader().range());
        IKVIterator itr = rangeReader.kvReader().iterator();
        itr.seekToFirst();
        assertFalse(itr.isValid());
    }

    @Test
    public void borrowReader() {
        Range range = Range.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("c"))
            .build();
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(0)
            .setLastAppliedIndex(0)
            .setState(State.newBuilder().setType(State.StateType.Normal).build())
            .setRange(range)
            .build();
        KVRangeState accessor = new KVRangeState(snapshot, kvEngine);
        IKVRangeWriter rangeWriter = accessor.getWriter();
        IKVRangeReader rangeReader1 = accessor.borrow();
        IKVRangeReader rangeReader2 = accessor.borrow();
        assertTrue(rangeReader1 != rangeReader2);
        ByteString key = ByteString.copyFromUtf8("aKey");
        ByteString val = ByteString.copyFromUtf8("Value");

        assertFalse(rangeReader1.kvReader().exist(key));
        assertFalse(rangeReader2.kvReader().exist(key));

        rangeWriter.kvWriter().put(key, val);
        rangeWriter.close();

        rangeReader1.refresh();
        assertTrue(rangeReader1.kvReader().exist(key));
        assertFalse(rangeReader2.kvReader().exist(key));

        accessor.returnBorrowed(rangeReader2);
        assertTrue(rangeReader2 == accessor.borrow());
        assertTrue(rangeReader2.kvReader().exist(key));
    }

    @Test
    public void borrowReaderConcurrently() {
        Range range = Range.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("c"))
            .build();
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(0)
            .setLastAppliedIndex(0)
            .setState(State.newBuilder().setType(State.StateType.Normal).build())
            .setRange(range)
            .build();
        KVRangeState accessor = new KVRangeState(snapshot, kvEngine);
        IKVRangeWriter rangeWriter = accessor.getWriter();
        IKVRangeReader rangeReader1 = accessor.borrow();
        IKVRangeReader rangeReader2 = accessor.borrow();
        accessor.returnBorrowed(rangeReader1);
        accessor.returnBorrowed(rangeReader2);
        AtomicReference<IKVRangeReader> t1Borrowed = new AtomicReference<>();
        AtomicReference<IKVRangeReader> t2Borrowed = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean(false);
        Thread t1 = new Thread(() -> {
            if (stop.get()) {
                return;
            }
            t1Borrowed.set(accessor.borrow());
            IKVRangeReader t1Reader = t1Borrowed.getAndSet(null);
            accessor.returnBorrowed(t1Reader);
        });
        Thread t2 = new Thread(() -> {
            if (stop.get()) {
                return;
            }
            t2Borrowed.set(accessor.borrow());
            IKVRangeReader t2Reader = t2Borrowed.getAndSet(null);
            accessor.returnBorrowed(t2Reader);
        });
        t1.start();
        t2.start();
        AtomicBoolean success = new AtomicBoolean(true);
        try {
            await().atMost(Duration.ofSeconds(5)).until(() -> {
                IKVRangeReader t1Reader = t1Borrowed.get();
                IKVRangeReader t2Reader = t2Borrowed.get();
                return t1Reader != null && t2Reader != null && t1Reader == t2Reader; // this should not be true
            });
            success.set(false);
        } catch (Throwable e) {

        } finally {
            stop.set(true);
        }
        assertTrue(success.get());
    }

    @Test
    public void resetFromCheckpoint() {
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(0)
            .setLastAppliedIndex(0)
            .setState(State.newBuilder().setType(State.StateType.Normal).build())
            .setRange(FULL_RANGE)
            .build();
        KVRangeState accessor = new KVRangeState(snapshot, kvEngine);
        snapshot = accessor.checkpoint();
        IKVRangeWriter rangeWriter = accessor.getWriter();
        ByteString key = ByteString.copyFromUtf8("aKey");
        ByteString val = ByteString.copyFromUtf8("Value");
        rangeWriter.kvWriter().put(key, val);
        rangeWriter.close();

        accessor.reset(snapshot).close();
        IKVRangeReader rangeReader = accessor.getReader();
        assertFalse(rangeReader.kvReader().exist(key));
    }

    @Test
    public void destroy() {
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(0)
            .setLastAppliedIndex(0)
            .setState(State.newBuilder().setType(State.StateType.Normal).build())
            .setRange(FULL_RANGE)
            .build();
        KVRangeState accessor = new KVRangeState(snapshot, kvEngine);
        IKVRangeWriter rangeWriter = accessor.getWriter();
        ByteString key = ByteString.copyFromUtf8("aKey");
        ByteString val = ByteString.copyFromUtf8("Value");
        rangeWriter.kvWriter().put(key, val);
        rangeWriter.close();

        accessor.destroy(true);

        accessor = new KVRangeState(snapshot.getId(), kvEngine);
        IKVRangeReader rangeReader = accessor.getReader();
        assertEquals(-1, rangeReader.ver());

        accessor = new KVRangeState(snapshot, kvEngine);
        rangeReader = accessor.getReader();
        assertEquals(0, rangeReader.ver());
        assertFalse(rangeReader.kvReader().exist(key));
    }

    @Test
    public void destroyButKeepData() {
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(0)
            .setLastAppliedIndex(0)
            .setState(State.newBuilder().setType(State.StateType.Normal).build())
            .setRange(FULL_RANGE)
            .build();
        KVRangeState accessor = new KVRangeState(snapshot, kvEngine);
        IKVRangeWriter rangeWriter = accessor.getWriter();
        ByteString key = ByteString.copyFromUtf8("aKey");
        ByteString val = ByteString.copyFromUtf8("Value");
        rangeWriter.kvWriter().put(key, val);
        rangeWriter.close();

        accessor.destroy(false);

        accessor = new KVRangeState(snapshot.getId(), kvEngine);
        IKVRangeReader rangeReader = accessor.getReader();

        accessor = new KVRangeState(snapshot, kvEngine);
        rangeReader = accessor.getReader();
        assertTrue(rangeReader.kvReader().exist(key));
    }
}
