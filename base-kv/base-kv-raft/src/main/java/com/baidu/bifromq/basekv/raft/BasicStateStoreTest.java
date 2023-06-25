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

package com.baidu.bifromq.basekv.raft;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.raft.proto.LogEntry;
import com.baidu.bifromq.basekv.raft.proto.Snapshot;
import com.baidu.bifromq.basekv.raft.proto.Voting;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;

public abstract class BasicStateStoreTest {

    protected abstract IRaftStateStore createStorage(String id, Snapshot initSnapshot);

    protected abstract String localId();

    @Test
    public void testInit() {
        Snapshot initSnapshot = Snapshot.newBuilder()
            .setClusterConfig(ClusterConfig.newBuilder()
                .addVoters(localId())
                .addVoters(localId() + "_2")
                .addVoters(localId() + "_3")
                .build())
            .setIndex(0)
            .setTerm(0)
            .build();
        IRaftStateStore stateStorage = createStorage(localId(), initSnapshot);
        assertEquals(localId(), stateStorage.local());
        assertEquals(1, stateStorage.firstIndex());
        assertEquals(0, stateStorage.lastIndex());
        assertEquals(initSnapshot, stateStorage.latestSnapshot());
        assertEquals(initSnapshot.getClusterConfig(), stateStorage.latestClusterConfig());
        Assert.assertFalse(stateStorage.entryAt(1).isPresent());
        Assert.assertFalse(stateStorage.entryAt(0).isPresent());
        try {
            stateStorage.entries(0, 1, -1);
            Assert.fail();
        } catch (Exception e) {
            assertTrue(e instanceof IndexOutOfBoundsException);
        }
    }

    @Test
    public void testSaveAndLoadCurrentTerm() {
        IRaftStateStore stateStorage = setupStateStorage();
        assertEquals(0, stateStorage.currentTerm());
        stateStorage.saveTerm(0);
        assertEquals(0, stateStorage.currentTerm());
        stateStorage.saveTerm(1);
        assertEquals(1, stateStorage.currentTerm());
        try {
            stateStorage.saveTerm(0);
            fail();
        } catch (Throwable t) {

        }
    }

    @Test
    public void testSaveAndLoadVoting() {
        IRaftStateStore stateStorage = setupStateStorage();
        Assert.assertFalse(stateStorage.currentVoting().isPresent());
        Voting voting = Voting.newBuilder().setFor(localId()).setTerm(1L).build();
        stateStorage.saveVoting(voting);
        assertEquals(voting, stateStorage.currentVoting().get());
    }

    @Test
    public void testSyncAppend() {
        IRaftStateStore stateStorage = setupStateStorage();
        List<Long> stabledIndexes = new ArrayList<>();
        stateStorage.addStableListener(stabledIndexes::add);
        int count = 10;
        while (count-- > 0) {
            LogEntry entry = LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(stateStorage.lastIndex() + 1)
                .setData(ByteString.EMPTY)
                .build();
            stateStorage.append(singletonList(entry), true);
            assertEquals(stateStorage.lastIndex(), stateStorage.entryAt(stateStorage.lastIndex()).get().getIndex());
        }
        for (int i = 0; i < stabledIndexes.size(); i++) {
            assertEquals(Long.valueOf(i + 1), stabledIndexes.get(i));
        }
    }

    @Test
    public void testAppendFirstEntryFailedWithLowerBoundCheck() {
        ClusterConfig initialClusterConfig = ClusterConfig.newBuilder()
            .addVoters(localId())
            .addVoters(localId() + "_2")
            .addVoters(localId() + "_3")
            .build();

        Snapshot initSnapshot = Snapshot.newBuilder()
            .setClusterConfig(initialClusterConfig)
            .setIndex(5)
            .setTerm(1)
            .build();
        IRaftStateStore stateStorage = createStorage(localId(), initSnapshot);
        try {
            stateStorage.append(asList(LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(5)
                .setData(ByteString.EMPTY)
                .build()), true);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof IndexOutOfBoundsException);
        }
        try {
            stateStorage.append(asList(LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(7)
                .setData(ByteString.EMPTY)
                .build()), true);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof IndexOutOfBoundsException);
        }
        stateStorage.append(asList(
            LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(6)
                .setData(ByteString.EMPTY)
                .build(),
            LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(7)
                .setData(ByteString.EMPTY)
                .build()), true);

        try {
            stateStorage.append(asList(LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(5)
                .setData(ByteString.EMPTY)
                .build()), true);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof IndexOutOfBoundsException);
        }
        try {
            stateStorage.append(asList(LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(9)
                .setData(ByteString.EMPTY)
                .build()), true);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof IndexOutOfBoundsException);
        }
    }

    @Test
    public void testAsyncAppend() throws InterruptedException {
        IRaftStateStore stateStorage = setupStateStorage();
        List<Long> stabledIndexes = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        stateStorage.addStableListener((stableIndex) -> {
            stabledIndexes.add(stableIndex);
            countDownLatch.countDown();
        });
        int count = 10;
        while (count-- > 0) {
            LogEntry entry = LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(stateStorage.lastIndex() + 1)
                .setData(ByteString.EMPTY)
                .build();
            stateStorage.append(singletonList(entry), false);
            assertEquals(stateStorage.lastIndex(), stateStorage.entryAt(stateStorage.lastIndex()).get().getIndex());
        }
        countDownLatch.await();
        assertTrue(1 <= stabledIndexes.size());
        assertTrue(stabledIndexes.get(0) >= 1);
    }

    @Test
    public void testTruncateAndAppend() {
        IRaftStateStore stateStorage = setupStateStorage();
        ClusterConfig updatedClusterConfig = ClusterConfig.newBuilder()
            .addVoters(localId())
            .build();
        int count = 10;
        while (count-- > 0) {
            LogEntry.Builder entryBuilder = LogEntry.newBuilder();
            if (count == 5) {
                entryBuilder
                    .setTerm(1)
                    .setIndex(stateStorage.lastIndex() + 1)
                    .setConfig(updatedClusterConfig);
            } else {
                entryBuilder
                    .setTerm(1)
                    .setIndex(stateStorage.lastIndex() + 1)
                    .setData(ByteString.copyFromUtf8("Data:" + (stateStorage.lastIndex() + 1)));
            }
            LogEntry entry = entryBuilder.build();
            stateStorage.append(singletonList(entry), true);
            assertEquals(entry.getIndex(), stateStorage.lastIndex());
        }
        count = 5;
        List<LogEntry> newEntries = new ArrayList<>();
        while (count <= 8) {
            // prepare entries of [5,8]
            newEntries.add(LogEntry.newBuilder()
                .setTerm(2)
                .setIndex(count)
                .setData(ByteString.copyFromUtf8("Data:" + count))
                .build());
            count++;
        }
        assertEquals(updatedClusterConfig, stateStorage.latestClusterConfig());
        stateStorage.append(newEntries, true);
        assertNotEquals(stateStorage.latestClusterConfig(), updatedClusterConfig);
        assertEquals(1, stateStorage.firstIndex());
        assertEquals(8, stateStorage.lastIndex());
        Iterator<LogEntry> entries5To8 = stateStorage.entries(5, 9, -1);
        for (int i = 0; entries5To8.hasNext(); i++) {
            LogEntry entry = entries5To8.next();
            assertEquals(2, entry.getTerm());
            assertEquals(i + 5, entry.getIndex());
        }
    }

    @Test
    public void testTruncateWholeLog() {
        ClusterConfig initialClusterConfig = ClusterConfig.newBuilder()
            .addVoters(localId())
            .addVoters(localId() + "_2")
            .addVoters(localId() + "_3")
            .build();

        Snapshot initSnapshot = Snapshot.newBuilder()
            .setClusterConfig(initialClusterConfig)
            .setIndex(5)
            .setTerm(1)
            .build();
        IRaftStateStore stateStorage = createStorage(localId(), initSnapshot);
        // log starts from 6
        assertEquals(6, stateStorage.firstIndex());
        assertEquals(5, stateStorage.lastIndex());

        List<LogEntry> newEntries = asList(LogEntry.newBuilder()
            .setTerm(1)
            .setIndex(6)
            .setData(ByteString.EMPTY)
            .build());
        stateStorage.append(newEntries, true);
        assertEquals(6, stateStorage.firstIndex());
        assertEquals(6, stateStorage.lastIndex());

        newEntries = asList(LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(6)
                .setData(ByteString.EMPTY)
                .build(),
            LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(7)
                .setData(ByteString.EMPTY)
                .build());
        stateStorage.append(newEntries, true);
        assertEquals(6, stateStorage.firstIndex());
        assertEquals(7, stateStorage.lastIndex());

        newEntries = asList(LogEntry.newBuilder()
            .setTerm(1)
            .setIndex(6)
            .setData(ByteString.EMPTY)
            .build());
        stateStorage.append(newEntries, true);
        assertEquals(6, stateStorage.firstIndex());
        assertEquals(6, stateStorage.lastIndex());
    }

    @Test
    public void testTruncateAndAppendFromZeroIndex() {
        IRaftStateStore stateStorage = setupStateStorage();
        List<Long> stabledIndexes = new ArrayList<>();
        stateStorage.addStableListener(stabledIndexes::add);
        int count = 5;
        while (count-- > 0) {
            LogEntry entry = LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(stateStorage.lastIndex() + 1)
                .setData(ByteString.EMPTY)
                .build();
            stateStorage.append(singletonList(entry), true);
        }

        int i = 1;
        List<LogEntry> newEntries = new ArrayList<>();
        while (i <= 5) {
            // prepare entries of [5,8]
            newEntries.add(LogEntry.newBuilder()
                .setTerm(2)
                .setIndex(i)
                .setData(ByteString.copyFromUtf8("Data:" + count))
                .build());
            i++;
        }
        stateStorage.append(newEntries, true);
        assertEquals(1, stateStorage.firstIndex());
        assertEquals(5, stateStorage.lastIndex());
        assertEquals(2, stateStorage.entryAt(stateStorage.firstIndex()).get().getTerm());
        assertEquals(2, stateStorage.entryAt(stateStorage.lastIndex()).get().getTerm());
    }

    @Test
    public void testApplyPartialSnapshot() {
        IRaftStateStore stateStorage = setupStateStorage();
        ClusterConfig updatedClusterConfig = ClusterConfig.newBuilder()
            .addVoters(localId())
            .build();
        int count = 10;
        while (count-- > 0) {
            LogEntry.Builder entryBuilder = LogEntry.newBuilder();
            if (count == 5) {
                entryBuilder
                    .setTerm(1)
                    .setIndex(stateStorage.lastIndex() + 1)
                    .setConfig(updatedClusterConfig);
            } else {
                entryBuilder
                    .setTerm(1)
                    .setIndex(stateStorage.lastIndex() + 1)
                    .setData(ByteString.copyFromUtf8("Data:" + (stateStorage.lastIndex() + 1)));
            }
            stateStorage.append(singletonList(entryBuilder.build()), true);
        }
        assertEquals(updatedClusterConfig, stateStorage.latestClusterConfig());
        Snapshot snapshot = Snapshot.newBuilder()
            .setClusterConfig(stateStorage.latestClusterConfig())
            .setData(ByteString.EMPTY)
            .setTerm(1)
            .setIndex(3)
            .build();
        stateStorage.applySnapshot(snapshot);
        assertEquals(updatedClusterConfig, stateStorage.latestClusterConfig());
        assertEquals(4, stateStorage.firstIndex());
        assertEquals(10, stateStorage.lastIndex());
    }

    @Test
    public void testApplyFullSnapshot() {
        IRaftStateStore stateStorage = setupStateStorage();
        ClusterConfig updatedClusterConfig = ClusterConfig.newBuilder()
            .addVoters(localId())
            .build();
        int count = 10;
        while (count-- > 0) {
            LogEntry.Builder entryBuilder = LogEntry.newBuilder();
            if (count == 5) {
                entryBuilder
                    .setTerm(1)
                    .setIndex(stateStorage.lastIndex() + 1)
                    .setConfig(updatedClusterConfig);
            } else {
                entryBuilder
                    .setTerm(1)
                    .setIndex(stateStorage.lastIndex() + 1)
                    .setData(ByteString.copyFromUtf8("Data:" + (stateStorage.lastIndex() + 1)));
            }
            stateStorage.append(singletonList(entryBuilder.build()), true);
        }
        Snapshot snapshot = Snapshot.newBuilder()
            .setClusterConfig(stateStorage.latestClusterConfig())
            .setData(ByteString.EMPTY)
            .setTerm(1)
            .setIndex(10)
            .build();
        stateStorage.applySnapshot(snapshot);
        assertEquals(updatedClusterConfig, stateStorage.latestClusterConfig());
        assertEquals(11, stateStorage.firstIndex());
        assertEquals(10, stateStorage.lastIndex());
    }

    @Test
    public void testApplyLargerSnapshot() {
        IRaftStateStore stateStorage = setupStateStorage();
        int count = 10;
        while (count-- > 0) {
            LogEntry entry = LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(stateStorage.lastIndex() + 1)
                .setData(ByteString.copyFromUtf8("Data:" + (stateStorage.lastIndex() + 1)))
                .build();
            stateStorage.append(singletonList(entry), true);
        }
        ClusterConfig updatedClusterConfig = ClusterConfig.newBuilder()
            .addVoters(localId())
            .build();
        Snapshot snapshot = Snapshot.newBuilder()
            .setClusterConfig(updatedClusterConfig)
            .setData(ByteString.EMPTY)
            .setTerm(1)
            .setIndex(15)
            .build();
        stateStorage.applySnapshot(snapshot);
        assertEquals(updatedClusterConfig, stateStorage.latestClusterConfig());
        assertEquals(16, stateStorage.firstIndex());
        assertEquals(15, stateStorage.lastIndex());
    }

    @Test
    public void testApplyDifferentSnapshot() {
        IRaftStateStore stateStorage = setupStateStorage();
        int count = 10;
        while (count-- > 0) {
            LogEntry entry = LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(stateStorage.lastIndex() + 1)
                .setData(ByteString.copyFromUtf8("Data:" + (stateStorage.lastIndex() + 1)))
                .build();
            stateStorage.append(singletonList(entry), true);
        }
        ClusterConfig cc = ClusterConfig.newBuilder().addVoters("V1").build();
        Snapshot snapshot = Snapshot.newBuilder()
            .setClusterConfig(cc)
            .setData(ByteString.EMPTY)
            .setTerm(2) // wrong term
            .setIndex(3)
            .build();
        stateStorage.applySnapshot(snapshot);
        assertEquals(3, stateStorage.lastIndex());
        assertEquals(4, stateStorage.firstIndex());
        assertEquals(3, stateStorage.latestSnapshot().getIndex());
        assertEquals(2, stateStorage.latestSnapshot().getTerm());
        assertEquals(cc, stateStorage.latestClusterConfig());
    }

    @Test
    public void testFetchEntries() {
        IRaftStateStore stateStorage = setupStateStorage();
        Iterator<LogEntry> entries = stateStorage.entries(stateStorage.firstIndex(), stateStorage.lastIndex(), -1);
        assertFalse(entries.hasNext());
        int count = 10;
        while (count-- > 0) {
            LogEntry entry = LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(stateStorage.lastIndex() + 1)
                .setData(ByteString.copyFromUtf8("Data:" + (stateStorage.lastIndex() + 1)))
                .build();
            stateStorage.append(singletonList(entry), true);
        }
        AtomicInteger counter = new AtomicInteger(0);
        stateStorage.entries(stateStorage.firstIndex(), stateStorage.lastIndex() + 1, -1)
            .forEachRemaining(e -> counter.incrementAndGet());
        assertEquals(10, counter.get());
        counter.set(0);
        stateStorage.entries(stateStorage.firstIndex(), stateStorage.lastIndex() + 1, 2)
            .forEachRemaining(e -> counter.incrementAndGet());
        assertEquals(1, counter.get());

        counter.set(0);
        stateStorage.entries(stateStorage.firstIndex(), stateStorage.lastIndex() + 1, 10)
            .forEachRemaining(e -> counter.incrementAndGet());
        assertEquals(2, counter.get());
    }

    @Test
    public void testAppendClusterConfigEntry() {
        IRaftStateStore stateStorage = setupStateStorage();
        int count = 10;
        while (count-- > 0) {
            LogEntry.Builder entryBuilder = LogEntry.newBuilder();
            if (count == 5) {
                entryBuilder
                    .setTerm(1)
                    .setIndex(stateStorage.lastIndex() + 1)
                    .setConfig(ClusterConfig.newBuilder()
                        .addVoters("V1")
                        .addVoters("V2")
                        .addVoters("V3")
                        .addVoters("V4")
                        .addVoters("V5")
                        .build());
            } else {
                entryBuilder
                    .setTerm(1)
                    .setIndex(stateStorage.lastIndex() + 1)
                    .setData(ByteString.copyFromUtf8("Data:" + (stateStorage.lastIndex() + 1)));
            }
            stateStorage.append(singletonList(entryBuilder.build()), true);
        }
        assertEquals(ClusterConfig.newBuilder()
            .addVoters("V1")
            .addVoters("V2")
            .addVoters("V3")
            .addVoters("V4")
            .addVoters("V5")
            .build(), stateStorage.latestClusterConfig());

        stateStorage.append(singletonList(LogEntry.newBuilder()
            .setTerm(1)
            .setIndex(4)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("S1")
                .addVoters("S2")
                .addVoters("S3")
                .build())
            .build()), true);

        assertEquals(ClusterConfig.newBuilder()
            .addVoters("S1")
            .addVoters("S2")
            .addVoters("S3")
            .build(), stateStorage.latestClusterConfig());
    }

    private IRaftStateStore setupStateStorage() {
        ClusterConfig initialClusterConfig = ClusterConfig.newBuilder()
            .addVoters(localId())
            .addVoters(localId() + "_2")
            .addVoters(localId() + "_3")
            .build();

        Snapshot initSnapshot = Snapshot.newBuilder()
            .setClusterConfig(initialClusterConfig)
            .setIndex(0)
            .setTerm(0)
            .build();
        return createStorage(localId(), initSnapshot);
    }
}
