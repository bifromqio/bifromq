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

package com.baidu.bifromq.basekv.store.wal;

import static com.baidu.bifromq.basekv.utils.KVRangeIdUtil.toShortString;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static java.lang.String.format;

import com.baidu.bifromq.basekv.localengine.IKVEngine;
import com.baidu.bifromq.basekv.localengine.IKVEngineIterator;
import com.baidu.bifromq.basekv.localengine.RangeUtil;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.raft.IRaftStateStore;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.raft.proto.LogEntry;
import com.baidu.bifromq.basekv.raft.proto.Snapshot;
import com.baidu.bifromq.basekv.raft.proto.Voting;
import com.baidu.bifromq.basekv.store.exception.KVRangeStoreException;
import com.baidu.bifromq.basekv.store.util.AsyncRunner;
import com.baidu.bifromq.basekv.store.util.KVUtil;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class KVRangeWALStore implements IRaftStateStore {
    private static final StableListener DEFAULT_STABLE_LISTENER = stabledIndex -> {
    };

    interface AppendCallback {
        void onAppend(KVRangeId id, long lastIndex);
    }

    private final String storeId;
    private final KVRangeId rangeId;
    private final IKVEngine kvEngine;
    private final int walKeyRangeId;
    private final TreeMap<Long, ClusterConfig> configEntryMap = Maps.newTreeMap();
    private final AppendCallback appendCallback;
    private final AsyncRunner taskRunner;
    private long currentTerm = 0;
    private Voting currentVoting;
    private Snapshot latestSnapshot;
    private long lastIndex;
    private long prevStabilizedIndex;
    private int logEntriesKeyInfix;

    private volatile StableListener stableListener = DEFAULT_STABLE_LISTENER;


    KVRangeWALStore(KVRangeId rangeId,
                    IKVEngine kvEngine,
                    int walKeyRangeId,
                    AppendCallback appendCallback,
                    Executor bgMgmtExecutor) {
        this.rangeId = rangeId;
        this.kvEngine = kvEngine;
        this.storeId = kvEngine.id();
        this.walKeyRangeId = walKeyRangeId;
        this.appendCallback = appendCallback;
        this.taskRunner = new AsyncRunner(bgMgmtExecutor);
        load();
    }

    @Override
    public String local() {
        return storeId;
    }

    @Override
    public Optional<Voting> currentVoting() {
        return Optional.ofNullable(currentVoting);
    }

    @Override
    public void saveVoting(Voting voting) {
        trace("Save voting: {}", voting);
        kvEngine.put(walKeyRangeId, KVRangeWALKeys.currentVotingKey(rangeId), voting.toByteString());
        kvEngine.flush();
        currentVoting = voting;
    }

    @Override
    public long currentTerm() {
        return currentTerm;
    }

    @Override
    public void saveTerm(long term) {
        trace("Save term: {}", term);
        kvEngine.put(walKeyRangeId, KVRangeWALKeys.currentTermKey(rangeId),
            unsafeWrap(ByteBuffer.allocate(Long.BYTES).putLong(term).array()));
        kvEngine.flush();
        currentTerm = term;
    }

    @Override
    public ClusterConfig latestClusterConfig() {
        if (configEntryMap.isEmpty()) {
            return latestSnapshot.getClusterConfig();
        } else {
            return configEntryMap.lastEntry().getValue();
        }
    }

    @Override
    public void applySnapshot(Snapshot snapshot) {
        long snapLastIndex = snapshot.getIndex();
        long snapLastTerm = snapshot.getTerm();
        Optional<LogEntry> lastEntryInSS = entryAt(snapLastIndex);
        log.debug("Compact logs using snapshot[term={}, index={}]: rangeId={}, storeId={}",
            snapLastTerm, snapLastIndex, toShortString(rangeId), storeId);
        if (lastEntryInSS.isPresent() && lastEntryInSS.get().getTerm() == snapLastTerm) {
            // the snapshot represents partial history, it happens when compacting
            // save snapshot
            kvEngine.put(walKeyRangeId, KVRangeWALKeys.latestSnapshotKey(rangeId), snapshot.toByteString());
            latestSnapshot = snapshot;
            lastIndex = Math.max(lastIndex, snapLastIndex);

            long truncateBeforeIndex = Math.min(lastIndex(), snapLastIndex) + 1;
            while (!configEntryMap.isEmpty()) {
                if (configEntryMap.firstKey() <= truncateBeforeIndex) {
                    configEntryMap.pollFirstEntry();
                } else {
                    break;
                }
            }
            Runnable truncateTask = () -> {
                log.trace("Truncating logs before index[{}]: rangeId={}, storeId={}",
                    truncateBeforeIndex, toShortString(rangeId), storeId);
                try (IKVEngineIterator it = kvEngine.newIterator(walKeyRangeId)) {
                    int batchId = kvEngine.startBatch();
                    // truncate log entry
                    kvEngine.clearSubRange(batchId, walKeyRangeId,
                        KVRangeWALKeys.logEntriesKeyPrefixInfix(rangeId, 0),
                        KVRangeWALKeys.logEntryKey(rangeId, logEntriesKeyInfix, truncateBeforeIndex));
                    // truncate config entry indexes
                    for (it.seek(KVRangeWALKeys.configEntriesKeyPrefixInfix(rangeId, 0));
                         it.isValid() &&
                             it.key().startsWith(KVRangeWALKeys.configEntriesKeyPrefix(rangeId)) &&
                             it.value().asReadOnlyByteBuffer().getLong() < truncateBeforeIndex;
                         it.next()) {
                        kvEngine.delete(batchId, walKeyRangeId, it.key());
                    }
                    kvEngine.endBatch(batchId);
                    kvEngine.flush();
                } catch (Throwable e) {
                    log.error("Unexpected error during truncating log: rangeId={}, storeId={}",
                        toShortString(rangeId), storeId, e);
                } finally {
                    log.debug("Logs truncated before index[{}]: rangeId={}, storeId={}",
                        truncateBeforeIndex, toShortString(rangeId), storeId);
                }
            };
            taskRunner.add(truncateTask);
        } else {
            // the snapshot represents a different history, it happens when installing snapshot from leader
            // save snapshot
            kvEngine.put(walKeyRangeId, KVRangeWALKeys.latestSnapshotKey(rangeId), snapshot.toByteString());
            latestSnapshot = snapshot;
            lastIndex = latestSnapshot.getIndex();
            prevStabilizedIndex = lastIndex;

            // update and save logEntriesKeyInfix
            int lastLogEntriesKeyInfix = logEntriesKeyInfix;
            kvEngine.put(walKeyRangeId, KVRangeWALKeys.logEntriesKeyInfix(rangeId),
                KVUtil.toByteString(logEntriesKeyInfix + 1));
            logEntriesKeyInfix = logEntriesKeyInfix + 1;

            configEntryMap.clear();

            Runnable truncateTask = () -> {
                try {
                    log.trace("Truncating all logs: rangeId={}, storeId={}", toShortString(rangeId), storeId);
                    int batchId = kvEngine.startBatch();
                    // clear entire logs
                    kvEngine.clearSubRange(batchId, walKeyRangeId,
                        KVRangeWALKeys.logEntriesKeyPrefixInfix(rangeId, 0),
                        RangeUtil.upperBound(KVRangeWALKeys.logEntriesKeyPrefixInfix(rangeId, lastLogEntriesKeyInfix)));
                    kvEngine.clearSubRange(batchId, walKeyRangeId,
                        KVRangeWALKeys.configEntriesKeyPrefixInfix(rangeId, 0),
                        RangeUtil.upperBound(
                            KVRangeWALKeys.configEntriesKeyPrefixInfix(rangeId, lastLogEntriesKeyInfix)));
                    kvEngine.endBatch(batchId);
                    kvEngine.flush();
                    log.debug("All logs of truncated: rangeId={}, storeId={}", toShortString(rangeId), storeId);
                } catch (Throwable e) {
                    log.error("Log truncation failed: rangeId={}, storeId={}", toShortString(rangeId), storeId, e);
                }
            };
            taskRunner.add(truncateTask);
        }
    }

    @Override
    public Snapshot latestSnapshot() {
        return latestSnapshot;
    }

    @Override
    public long firstIndex() {
        return latestSnapshot.getIndex() + 1;
    }

    @Override
    public long lastIndex() {
        return lastIndex;
    }

    @Override
    public Optional<LogEntry> entryAt(long index) {
        if (index < firstIndex() || index > lastIndex()) {
            return Optional.empty();
        }
        try {
            ByteString data =
                kvEngine.get(walKeyRangeId, KVRangeWALKeys.logEntryKey(rangeId, logEntriesKeyInfix, index)).get();
            return Optional.of(LogEntry.parseFrom(data));
        } catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse log entry[index={}, rangeId={}]", index, toShortString(rangeId), e);
            return Optional.empty();
        }
    }

    @Override
    public Iterator<LogEntry> entries(long lo, long hi, long maxSize) {
        if (lo < firstIndex()) {
            throw new IndexOutOfBoundsException("lo[" + lo + "] must not be less than firstIndex["
                + firstIndex() + "]");
        }
        if (hi > lastIndex() + 1) {
            throw new IndexOutOfBoundsException("hi[" + hi + "] must not be greater than lastIndex["
                + lastIndex() + "]");
        }
        if (maxSize < 0) {
            maxSize = Long.MAX_VALUE;
        }
        return new LogEntryIterator(lo, hi, maxSize);
    }

    @Override
    public void append(List<LogEntry> entries, boolean flush) {
        assert !entries.isEmpty();
        LogEntry startEntry = entries.get(0);
        if (lastIndex() >= firstIndex()) {
            if (firstIndex() > startEntry.getIndex() || lastIndex() + 1 < startEntry.getIndex()) {
                throw new IndexOutOfBoundsException(format("first index[%d] must be in [%d,%d]",
                    startEntry.getIndex(), firstIndex(), lastIndex() + 1));
            }
        } else {
            if (startEntry.getIndex() != firstIndex()) {
                throw new IndexOutOfBoundsException(format("log index must start from %d", firstIndex()));
            }
        }

        long afterIndex = entries.get(0).getIndex() - 1;

        int batchId = kvEngine.startBatch();
        while (!configEntryMap.isEmpty() && configEntryMap.lastKey() > afterIndex) {
            long removedIndex = configEntryMap.pollLastEntry().getKey();
            kvEngine.delete(batchId, walKeyRangeId,
                KVRangeWALKeys.configEntriesKey(rangeId, logEntriesKeyInfix, removedIndex));
        }
        for (LogEntry entry : entries) {
            if (entry.hasConfig()) {
                configEntryMap.put(entry.getIndex(), entry.getConfig());
                kvEngine.put(batchId, walKeyRangeId,
                    KVRangeWALKeys.configEntriesKey(rangeId, logEntriesKeyInfix, entry.getIndex()),
                    KVUtil.toByteString(entry.getIndex()));
            }
            trace("Append log entry[index={}, term={}, type={}]",
                entry.getIndex(), entry.getTerm(), entry.getTypeCase().name());
            kvEngine.put(batchId, walKeyRangeId,
                KVRangeWALKeys.logEntryKey(rangeId, logEntriesKeyInfix, entry.getIndex()),
                entry.toByteString());
        }
        kvEngine.endBatch(batchId);
        lastIndex = entries.get(entries.size() - 1).getIndex();
        if (flush) {
            kvEngine.flush();
            onStable(lastIndex);
        } else {
            appendCallback.onAppend(rangeId, lastIndex);
        }
    }

    @Override
    public void addStableListener(StableListener listener) {
        stableListener = listener;
    }

    @Override
    public CompletableFuture<Void> stop() {
        log.debug("Stop range wal storage: rangeId={}, storeId={}", toShortString(rangeId), storeId);
        stableListener = DEFAULT_STABLE_LISTENER;
        return taskRunner.awaitDone().toCompletableFuture();
    }

    public void destroy() {
        int batchId = kvEngine.startBatch();
        kvEngine.clearRange(batchId, walKeyRangeId);
        kvEngine.endBatch(batchId);
        kvEngine.unregisterKeyRange(walKeyRangeId);
    }

    public void onStable(long stabledIndex) {
        if (prevStabilizedIndex < stabledIndex) {
            trace("Log entries before index[{}] stabilized", stabledIndex);
            stableListener.onStabilized(stabledIndex);
            prevStabilizedIndex = stabledIndex;
        }
    }

    private void load() {
        loadLogEntryInfix();
        loadVoting();
        loadCurrentTerm();
        loadLatestSnapshot();
        loadConfigEntryIndexes();
        loadLastIndex();
        trace("New raft state storage loaded");
    }

    private void loadVoting() {
        try {
            Optional<ByteString> votingBytes = kvEngine.get(walKeyRangeId, KVRangeWALKeys.currentVotingKey(rangeId));
            if (votingBytes.isPresent()) {
                currentVoting = Voting.parseFrom(votingBytes.get());
            }
        } catch (InvalidProtocolBufferException e) {
            throw new KVRangeStoreException("Failed to parse currentVoting", e);
        }
    }

    private void loadCurrentTerm() {
        currentTerm = kvEngine.get(walKeyRangeId, KVRangeWALKeys.currentTermKey(rangeId))
            .map(KVUtil::toLong)
            .orElse(0L);
    }

    private void loadLatestSnapshot() {
        try {
            ByteString latestSnapshotBytes =
                kvEngine.get(walKeyRangeId, KVRangeWALKeys.latestSnapshotKey(rangeId)).get();
            latestSnapshot = Snapshot.parseFrom(latestSnapshotBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new KVRangeStoreException("Failed to parse snapshot", e);
        }
    }

    private void loadConfigEntryIndexes() {
        try (IKVEngineIterator it = kvEngine.newIterator(walKeyRangeId)) {
            ByteString prefix = KVRangeWALKeys.configEntriesKeyPrefix(rangeId);
            for (it.seek(prefix); it.isValid() && it.key().startsWith(prefix); it.next()) {
                long configEntryIndex = it.value().asReadOnlyByteBuffer().getLong();
                configEntryMap.put(configEntryIndex, loadConfigEntry(configEntryIndex));
            }
        }
    }

    private ClusterConfig loadConfigEntry(long configEntryIndex) {
        try {
            LogEntry logEntry = LogEntry.parseFrom(
                kvEngine.get(walKeyRangeId, KVRangeWALKeys.logEntryKey(rangeId, logEntriesKeyInfix, configEntryIndex))
                    .get());
            assert logEntry.hasConfig();
            return logEntry.getConfig();
        } catch (InvalidProtocolBufferException e) {
            throw new KVRangeStoreException("Failed to parse", e);
        } catch (NoSuchElementException e) {
            log.error("Cluster config not found at index[{}]: kvRangeId={}, storeId={}",
                configEntryIndex, toShortString(rangeId), storeId);
            throw new KVRangeStoreException("config not found", e);
        }
    }

    private void loadLastIndex() {
        try (IKVEngineIterator it = kvEngine.newIterator(walKeyRangeId)) {
            it.seekToLast();
            if (it.isValid() && it.key().startsWith(KVRangeWALKeys.logEntriesKeyPrefix(rangeId))) {
                lastIndex = KVRangeWALKeys.parseLogIndex(it.key());
            } else {
                lastIndex = latestSnapshot.getIndex();
            }
        }
    }

    private void loadLogEntryInfix() {
        logEntriesKeyInfix = kvEngine.get(walKeyRangeId, KVRangeWALKeys.logEntriesKeyInfix(rangeId))
            .map(KVUtil::toInt)
            .orElse(0);
    }

    private class LogEntryIterator implements Iterator<LogEntry> {
        private final long maxIndex;
        private final long maxSize;
        private long currentIndex;
        private long accumulatedSize;
        private final IKVEngineIterator iterator;

        private LogEntryIterator(long startIndex, long endIndex, long maxSize) {
            this.currentIndex = startIndex;
            this.maxIndex = endIndex;
            this.maxSize = maxSize;
            this.iterator = kvEngine.newIterator(walKeyRangeId,
                KVRangeWALKeys.logEntryKey(rangeId, logEntriesKeyInfix, currentIndex),
                RangeUtil.upperBound(KVRangeWALKeys.logEntriesKeyPrefixInfix(rangeId, logEntriesKeyInfix)));
            this.iterator.seek(KVRangeWALKeys.logEntryKey(rangeId, logEntriesKeyInfix, currentIndex));
        }

        @Override
        public boolean hasNext() {
            boolean has = currentIndex < maxIndex && accumulatedSize <= maxSize;
            if (!has) {
                // the iterator can be closed automically by Cleaner if not scanned to the end
                iterator.close();
            }
            return has;
        }

        @Override
        public LogEntry next() {
            if (!iterator.isValid()) {
                throw new NoSuchElementException();
            } else {
                try {
                    LogEntry entry = LogEntry.parseFrom(iterator.value());
                    accumulatedSize += entry.getData().size();
                    currentIndex++;
                    iterator.next();
                    return entry;
                } catch (InvalidProtocolBufferException e) {
                    throw new KVRangeStoreException("Log data corruption", e);
                }
            }
        }
    }

    private void trace(String msg, Object... args) {
        if (log.isTraceEnabled()) {
            log.trace(msg, args);
        }
    }
}
