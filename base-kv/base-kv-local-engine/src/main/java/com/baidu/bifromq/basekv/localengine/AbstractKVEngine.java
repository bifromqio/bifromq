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

import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractKVEngine<K extends AbstractKeyRange, B extends AbstractWriteBatch> implements IKVEngine {
    protected final String overrideIdentity;
    private final Set<String> namespaces;
    private final Predicate<String> checkpointInUse;
    private final Map<String, K> defaultRanges = new HashMap<>();
    private final ConcurrentHashMap<Integer, K> ranges = new ConcurrentHashMap<>();
    private final Map<Integer, B> writeBatches = new ConcurrentHashMap<>();
    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);

    private MetricManager metricMgr;

    public AbstractKVEngine(String overrideIdentity,
                            List<String> namespaces,
                            Predicate<String> checkpointInUse) {
        this.overrideIdentity = overrideIdentity;
        this.namespaces = new HashSet<>(namespaces);
        this.namespaces.add(DEFAULT_NS);
        this.namespaces.forEach(ns -> defaultRanges.put(ns, newKeyRange(0, ns, null, null)));
        this.checkpointInUse = checkpointInUse == null ? cpId -> true : checkpointInUse;
    }

    @Override
    public final void start(ScheduledExecutorService bgTaskExecutor, String... metricTags) {
        if (state.compareAndSet(State.INIT, State.STARTING)) {
            try {
                doStart(bgTaskExecutor, metricTags);
                metricMgr = new MetricManager(metricTags);
                state.set(State.STARTED);
                afterStart();
            } catch (Throwable e) {
                state.set(State.FATAL_FAILURE);
                throw e;
            }
        }
    }

    protected abstract void doStart(ScheduledExecutorService bgTaskExecutor, String... metricTags);

    protected void afterStart() {

    }

    @Override
    public final void stop() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            try {
                doStop();
                metricMgr.close();
            } finally {
                state.set(State.STOPPED);
            }
        }
    }

    protected abstract void doStop();

    @Override
    public final int registerKeyRange(String namespace, ByteString start, ByteString end) {
        checkState();
        checkNS(namespace);
        checkRange(start, end);
        while (true) {
            int id = ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE);
            K keyRange = newKeyRange(id, namespace, start, end);
            if (ranges.putIfAbsent(id, keyRange) != null) {
                continue;
            }
            return id;
        }
    }

    @Override
    public final void unregisterKeyRange(int rangeId) {
        checkState();
        checkRangeId(rangeId);
        ranges.remove(rangeId);
    }

    protected abstract K newKeyRange(int id, String namespace, ByteString start, ByteString end);

    @Override
    public final ByteString skip(String namespace, long count) {
        checkState();
        checkNS(namespace);
        return skip(defaultRanges.get(namespace), count);
    }

    @Override
    public final ByteString skip(int rangeId, long count) {
        checkState();
        checkRangeId(rangeId);
        return skip(ranges.get(rangeId), count);
    }

    private ByteString skip(K keyRange, long count) {
        return metricMgr.skipCallTimer.record(() -> doSkip(keyRange, count));
    }

    protected abstract ByteString doSkip(K keyRange, long count);

    @Override
    public final long size(int rangeId) {
        checkState();
        checkRangeId(rangeId);
        K range = ranges.get(rangeId);
        return size(range, range.start, range.end);
    }

    @Override
    public final long size(String namespace) {
        checkState();
        checkNS(namespace);
        K range = defaultRanges.get(namespace);
        return size(range, range.start, range.end);
    }

    @Override
    public final long size(String namespace, ByteString start, ByteString end) {
        checkState();
        checkNS(namespace);
        return size(defaultRanges.get(namespace), start, end);
    }

    private long size(K range, ByteString start, ByteString end) {
        return metricMgr.sizeCallTimer.record(() -> size(start, end, range));
    }

    protected abstract long size(ByteString start, ByteString end, K range);

    @Override
    public final long size(String checkpointId, String namespace) {
        checkState();
        checkNS(namespace);
        K range = defaultRanges.get(namespace);
        return size(checkpointId, range, range.start, range.end);
    }

    @Override
    public final long size(String checkpointId, String namespace, ByteString start, ByteString end) {
        checkState();
        checkNS(namespace);
        return size(checkpointId, defaultRanges.get(namespace), start, end);
    }

    @Override
    public final long size(String checkpointId, int rangeId) {
        checkState();
        checkRangeId(rangeId);
        K range = ranges.get(rangeId);
        return size(checkpointId, range, range.start, range.end);
    }

    protected abstract long size(String checkpointId, K range, ByteString start, ByteString end);

    protected final boolean inUse(String checkpointId) {
        return checkpointInUse.test(checkpointId);
    }

    @Override
    public final String checkpoint() {
        checkState();
        String checkpointId = UUID.randomUUID().toString();
        checkpoint(checkpointId);
        return checkpointId;
    }

    @Override
    public final boolean exist(String namespace, ByteString key) {
        checkState();
        checkNS(namespace);
        return exist(defaultRanges.get(namespace), key);
    }

    @Override
    public final boolean exist(int rangeId, ByteString key) {
        checkState();
        checkInRange(key, rangeId);
        return exist(ranges.get(rangeId), key);
    }

    private boolean exist(K range, ByteString key) {
        return metricMgr.existCallTimer.record(() -> exist(key, range));
    }

    protected abstract boolean exist(ByteString key, K range);

    @Override
    public final boolean exist(String checkpointId, int rangeId, ByteString key) {
        checkState();
        checkInRange(key, rangeId);
        return exist(checkpointId, ranges.get(rangeId), key);
    }

    @Override
    public final boolean exist(String checkpointId, String namespace, ByteString key) {
        checkState();
        checkNS(namespace);
        return exist(checkpointId, defaultRanges.get(namespace), key);
    }

    protected abstract boolean exist(String checkpointId, K range, ByteString key);

    @Override
    public final Optional<ByteString> get(String namespace, ByteString key) {
        checkState();
        checkNS(namespace);
        return get(defaultRanges.get(namespace), key);
    }

    @Override
    public final Optional<ByteString> get(int rangeId, ByteString key) {
        checkState();
        checkInRange(key, rangeId);
        return get(ranges.get(rangeId), key);
    }

    private Optional<ByteString> get(K range, ByteString key) {
        return metricMgr.getCallTimer.record(() -> get(key, range));
    }

    protected abstract Optional<ByteString> get(ByteString key, K range);

    @Override
    public final Optional<ByteString> get(String checkpointId, int rangeId, ByteString key) {
        checkState();
        checkInRange(key, rangeId);
        return get(checkpointId, ranges.get(rangeId), key);
    }

    @Override
    public final Optional<ByteString> get(String checkpointId, String namespace, ByteString key) {
        checkState();
        checkNS(namespace);
        return get(checkpointId, defaultRanges.get(namespace), key);
    }

    protected abstract Optional<ByteString> get(String checkpointId, K range, ByteString key);

    @Override
    public final IKVEngineIterator newIterator(String namespace) {
        checkState();
        checkNS(namespace);
        K range = defaultRanges.get(namespace);
        return newIterator(range, range.start, range.end);
    }

    @Override
    public final IKVEngineIterator newIterator(int rangeId) {
        checkState();
        checkRangeId(rangeId);
        K range = ranges.get(rangeId);
        return newIterator(range, range.start, range.end);
    }

    @Override
    public final IKVEngineIterator newIterator(int rangeId, ByteString start, ByteString end) {
        checkState();
        checkInRange(start, end, rangeId);
        K range = ranges.get(rangeId);
        return newIterator(range, start, end);
    }

    @Override
    public final IKVEngineIterator newIterator(String namespace, ByteString start, ByteString end) {
        checkState();
        checkNS(namespace);
        return newIterator(defaultRanges.get(namespace), start, end);
    }

    private IKVEngineIterator newIterator(K range, ByteString start, ByteString end) {
        return metricMgr.iterNewCallTimer.record(() ->
            new MonitoredKVIterator(newIterator(start, end, range)));
    }

    protected abstract IKVEngineIterator newIterator(ByteString start, ByteString end, K range);

    @Override
    public final IKVEngineIterator newIterator(String checkpointId, String namespace) {
        checkState();
        checkNS(namespace);
        K range = defaultRanges.get(namespace);
        return newIterator(checkpointId, range.ns, range.start, range.end);
    }

    @Override
    public final IKVEngineIterator newIterator(String checkpointId, int rangeId) {
        checkState();
        checkRangeId(rangeId);
        K range = ranges.get(rangeId);
        return newIterator(checkpointId, range.ns, range.start, range.end);
    }

    @Override
    public final IKVEngineIterator newIterator(String checkpointId, int rangeId, ByteString start, ByteString end) {
        checkState();
        checkInRange(start, end, rangeId);
        K range = ranges.get(rangeId);
        return newIterator(checkpointId, range.ns, start, end);
    }

    protected abstract IKVEngineIterator newIterator(String checkpointId,
                                                     String namespace, ByteString start, ByteString end);

    protected abstract B newWriteBatch(int batchId);

    @Override
    public final int startBatch() {
        checkState();
        while (true) {
            int batchId = ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE);
            B batch = newWriteBatch(batchId);
            if (writeBatches.putIfAbsent(batchId, batch) != null) {
                batch.abort();
                continue;
            }
            log.trace("Batch[{}] started", batchId);
            return batchId;
        }
    }

    @Override
    public final void endBatch(int batchId) {
        checkState();
        checkBatchId(batchId);
        B writeBatch = writeBatches.remove(batchId);
        metricMgr.batchSizeSummary.record(writeBatch.count());
        metricMgr.batchWriteCallTimer.record(writeBatch::end);
        log.trace("Batch[{}] committed", batchId);
    }

    @Override
    public final void abortBatch(int batchId) {
        checkState();
        checkBatchId(batchId);
        writeBatches.remove(batchId).abort();
        log.trace("Batch[{}] aborted", batchId);
    }

    protected B getBatch(int batchId) {
        return writeBatches.get(batchId);
    }

    @Override
    public final int batchSize(int batchId) {
        checkState();
        checkBatchId(batchId);
        return writeBatches.get(batchId).count();
    }

    @Override
    public final void delete(int batchId, int rangeId, ByteString key) {
        checkState();
        checkBatchId(batchId);
        checkInRange(key, rangeId);
        delete(batchId, ranges.get(rangeId), key);
    }

    @Override
    public final void delete(int batchId, String namespace, ByteString key) {
        checkState();
        checkNS(namespace);
        delete(batchId, defaultRanges.get(namespace), key);
    }

    protected abstract void delete(int batchId, K range, ByteString key);

    @Override
    public final void delete(int rangeId, ByteString key) {
        checkState();
        checkInRange(key, rangeId);
        delete(ranges.get(rangeId), key);
    }

    @Override
    public final void delete(String namespace, ByteString key) {
        checkState();
        checkNS(namespace);
        delete(defaultRanges.get(namespace), key);
    }

    private void delete(K range, ByteString key) {
        metricMgr.delCallTimer.record(() -> doDelete(range, key));
    }

    protected abstract void doDelete(K range, ByteString key);

    @Override
    public final void clearRange(String namespace) {
        checkState();
        checkNS(namespace);
        K range = defaultRanges.get(namespace);
        clearSubRange(range, range.start, range.end);
    }

    @Override
    public final void clearRange(int rangeId) {
        checkState();
        checkRangeId(rangeId);
        K range = ranges.get(rangeId);
        clearSubRange(range, range.start, range.end);
    }

    @Override
    public final void clearRange(int batchId, int rangeId) {
        checkState();
        checkBatchId(batchId);
        checkRangeId(rangeId);
        K range = ranges.get(rangeId);
        clearSubRange(batchId, range, range.start, range.end);

    }

    @Override
    public final void clearRange(int batchId, String namespace) {
        checkState();
        checkNS(namespace);
        K range = defaultRanges.get(namespace);
        clearSubRange(batchId, range, range.start, range.end);
    }

    @Override
    public final void clearSubRange(int batchId, int rangeId, ByteString start, ByteString end) {
        checkState();
        checkBatchId(batchId);
        checkInRange(start, end, rangeId);
        clearSubRange(batchId, ranges.get(rangeId), start, end);
    }

    @Override
    public final void clearSubRange(int batchId, String namespace, ByteString start, ByteString end) {
        checkState();
        checkNS(namespace);
        clearSubRange(batchId, defaultRanges.get(namespace), start, end);
    }

    protected abstract void clearSubRange(int batchId, K range, ByteString start, ByteString end);

    @Override
    public final void clearSubRange(int rangeId, ByteString start, ByteString end) {
        checkState();
        checkInRange(start, end, rangeId);
        clearSubRange(ranges.get(rangeId), start, end);
    }

    @Override
    public final void clearSubRange(String namespace, ByteString start, ByteString end) {
        checkState();
        checkNS(namespace);
        clearSubRange(defaultRanges.get(namespace), start, end);
    }

    private void clearSubRange(K range, ByteString start, ByteString end) {
        metricMgr.clearRangeCallTimer.record(() -> doClearSubRange(range, start, end));
    }

    protected abstract void doClearSubRange(K range, ByteString start, ByteString end);

    @Override
    public final void insert(int batchId, int rangeId, ByteString key, ByteString value) {
        checkState();
        checkBatchId(batchId);
        checkInRange(key, rangeId);
        insert(batchId, ranges.get(rangeId), key, value);
    }

    @Override
    public final void insert(int batchId, String namespace, ByteString key, ByteString value) {
        checkState();
        checkBatchId(batchId);
        checkNS(namespace);
        insert(batchId, defaultRanges.get(namespace), key, value);
    }

    protected abstract void insert(int batchId, K range, ByteString key, ByteString value);

    @Override
    public final void insert(int rangeId, ByteString key, ByteString value) {
        checkState();
        checkInRange(key, rangeId);
        insert(ranges.get(rangeId), key, value);
    }

    @Override
    public final void insert(String namespace, ByteString key, ByteString value) {
        checkState();
        checkNS(namespace);
        insert(defaultRanges.get(namespace), key, value);
    }

    private void insert(K range, ByteString key, ByteString value) {
        metricMgr.insertCallTimer.record(() -> doInsert(range, key, value));
    }

    protected abstract void doInsert(K range, ByteString key, ByteString value);

    @Override
    public void put(int batchId, int rangeId, ByteString key, ByteString value) {
        checkState();
        checkBatchId(batchId);
        checkInRange(key, rangeId);
        put(batchId, ranges.get(rangeId), key, value);
    }

    @Override
    public void put(int batchId, String namespace, ByteString key, ByteString value) {
        checkState();
        checkNS(namespace);
        put(batchId, defaultRanges.get(namespace), key, value);
    }

    protected abstract void put(int batchId, K range, ByteString key, ByteString value);

    @Override
    public final void put(int rangeId, ByteString key, ByteString value) {
        checkState();
        checkInRange(key, rangeId);
        put(ranges.get(rangeId), key, value);
    }

    @Override
    public final void put(String namespace, ByteString key, ByteString value) {
        checkState();
        checkNS(namespace);
        put((defaultRanges.get(namespace)), key, value);
    }

    private void put(K range, ByteString key, ByteString value) {
        metricMgr.putCallTimer.record(() -> doPut(range, key, value));
    }

    protected abstract void doPut(K range, ByteString key, ByteString value);

    @Override
    public final void flush() {
        checkState();
        metricMgr.flushCallTimer.record(this::doFlush);
    }

    protected void doFlush() {

    }

    protected void checkState() {
        assert state.get() == State.STARTED : "Not started";
    }

    protected final State state() {
        return state.get();
    }

    protected enum State {
        INIT, STARTING, STARTED, FATAL_FAILURE, STOPPING, STOPPED
    }

    private void checkBatchId(int batchId) {
        assert writeBatches.containsKey(batchId) : String.format("Batch[%s] not found", batchId);
    }

    private void checkRange(ByteString start, ByteString end) {
        assert RangeUtil.isValid(start, end) : "Invalid range: start must be less than end";
    }

    private void checkRangeId(int rangeId) {
        assert ranges.containsKey(rangeId) : String.format("Range[%s] not found", rangeId);
    }

    private void checkNS(String ns) {
        assert namespaces.contains(ns) : String.format("Namespace[%s] not found", ns);
    }

    private void checkInRange(ByteString key, int rangeId) {
        checkRangeId(rangeId);
        assert inRange(key, ranges.get(rangeId)) : "Key must be in range";
    }

    private void checkInRange(ByteString start, ByteString end, int rangeId) {
        checkRangeId(rangeId);
        assert inRange(start, end, ranges.get(rangeId)) : "Range must be sub-range";
    }

    private boolean inRange(ByteString key, K range) {
        return RangeUtil.inRange(key, range.start, range.end);
    }

    private boolean inRange(ByteString start, ByteString end, K range) {
        return RangeUtil.inRange(start, end, range.start, range.end);
    }

    private class MonitoredKVIterator implements IKVEngineIterator {
        final IKVEngineIterator delegate;

        private MonitoredKVIterator(IKVEngineIterator delegate) {
            this.delegate = delegate;
        }

        @Override
        public ByteString key() {
            return delegate.key();
        }

        @Override
        public ByteString value() {
            return delegate.value();
        }

        @Override
        public boolean isValid() {
            return delegate.isValid();
        }

        @Override
        public void next() {
            metricMgr.iterNextCallTimer.record(delegate::next);
        }

        @Override
        public void prev() {
            metricMgr.iterPrevCallTimer.record(delegate::prev);
        }

        @Override
        public void seekToFirst() {
            metricMgr.iterSeekToFirstCallTimer.record(delegate::seekToFirst);
        }

        @Override
        public void seekToLast() {
            metricMgr.iterSeekToLastCallTimer.record(delegate::seekToLast);
        }

        @Override
        public void seek(ByteString target) {
            metricMgr.iterSeekCallTimer.record(() -> delegate.seek(target));
        }

        @Override
        public void seekForPrev(ByteString target) {
            metricMgr.iterSeekForPrevCallTimer.record(() -> delegate.seekForPrev(target));
        }

        @Override
        public void refresh() {
            metricMgr.iterRefreshTimer.record(delegate::refresh);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private class MetricManager {
        private final Gauge activeKeyRangesGauge;
        private final Gauge activeWriteBatchGauge;
        private final Timer sizeCallTimer;
        private final Timer skipCallTimer;
        private final Timer existCallTimer;
        private final Timer getCallTimer;
        private final Timer iterNewCallTimer;
        private final Timer iterSeekCallTimer;
        private final Timer iterSeekForPrevCallTimer;
        private final Timer iterSeekToFirstCallTimer;
        private final Timer iterSeekToLastCallTimer;
        private final Timer iterNextCallTimer;
        private final Timer iterPrevCallTimer;
        private final Timer iterRefreshTimer;
        private final Timer insertCallTimer;
        private final Timer putCallTimer;
        private final Timer delCallTimer;
        private final Timer clearRangeCallTimer;
        private final Timer batchWriteCallTimer;
        private final Timer flushCallTimer;

        private final DistributionSummary batchSizeSummary;

        MetricManager(String... metricTags) {
            Tags tags = Tags.of(metricTags);
            activeKeyRangesGauge = Gauge.builder("basekv.le.active.ranges", () -> defaultRanges.size() + ranges.size())
                .tags(tags)
                .register(Metrics.globalRegistry);
            activeWriteBatchGauge = Gauge.builder("basekv.le.active.batches", writeBatches::size)
                .tags(tags)
                .register(Metrics.globalRegistry);
            sizeCallTimer = Timer.builder("basekv.le.call.time")
                .tags(tags)
                .tag("op", "size")
                .register(Metrics.globalRegistry);
            skipCallTimer = Timer.builder("basekv.le.call.time")
                .tags(tags)
                .tag("op", "skip")
                .register(Metrics.globalRegistry);
            existCallTimer = Timer.builder("basekv.le.call.time")
                .tags(tags)
                .tag("op", "exist")
                .register(Metrics.globalRegistry);
            getCallTimer = Timer.builder("basekv.le.call.time")
                .tags(tags)
                .tag("op", "get")
                .register(Metrics.globalRegistry);
            iterNewCallTimer = Timer.builder("basekv.le.call.time")
                .tags(tags)
                .tag("op", "newitr")
                .register(Metrics.globalRegistry);
            iterSeekCallTimer = Timer.builder("basekv.le.call.time")
                .tags(tags)
                .tag("op", "seek")
                .register(Metrics.globalRegistry);
            iterSeekForPrevCallTimer = Timer.builder("basekv.le.call.time")
                .tags(tags)
                .tag("op", "pseek")
                .register(Metrics.globalRegistry);
            iterSeekToFirstCallTimer = Timer.builder("basekv.le.call.time")
                .tags(tags)
                .tag("op", "fseek")
                .register(Metrics.globalRegistry);
            iterSeekToLastCallTimer = Timer.builder("basekv.le.call.time")
                .tags(tags)
                .tag("op", "lseek")
                .register(Metrics.globalRegistry);
            iterNextCallTimer = Timer.builder("basekv.le.call.time")
                .tags(tags)
                .tag("op", "next")
                .register(Metrics.globalRegistry);
            iterPrevCallTimer = Timer.builder("basekv.le.call.time")
                .tags(tags)
                .tag("op", "prev")
                .register(Metrics.globalRegistry);
            iterRefreshTimer = Timer.builder("basekv.le.call.time")
                .tags(tags)
                .tag("op", "refresh")
                .register(Metrics.globalRegistry);
            insertCallTimer = Timer.builder("basekv.le.call.time")
                .tags(tags)
                .tag("op", "insert")
                .register(Metrics.globalRegistry);
            putCallTimer = Timer.builder("basekv.le.call.time")
                .tags(tags)
                .tag("op", "put")
                .register(Metrics.globalRegistry);
            delCallTimer = Timer.builder("basekv.le.call.time")
                .tags(tags)
                .tag("op", "del")
                .register(Metrics.globalRegistry);
            clearRangeCallTimer = Timer.builder("basekv.le.call.time")
                .tags(tags)
                .tag("op", "clear")
                .register(Metrics.globalRegistry);
            batchWriteCallTimer = Timer.builder("basekv.le.call.time")
                .tags(tags)
                .tag("op", "bwrite")
                .register(Metrics.globalRegistry);
            flushCallTimer = Timer.builder("basekv.le.call.time")
                .tags(tags)
                .tag("op", "flush")
                .register(Metrics.globalRegistry);
            batchSizeSummary = DistributionSummary.builder("basekv.le.batch.size")
                .tags(tags)
                .register(Metrics.globalRegistry);
        }

        void close() {
            Metrics.globalRegistry.remove(activeKeyRangesGauge);
            Metrics.globalRegistry.remove(activeWriteBatchGauge);
            Metrics.globalRegistry.remove(sizeCallTimer);
            Metrics.globalRegistry.remove(skipCallTimer);

            Metrics.globalRegistry.remove(existCallTimer);
            Metrics.globalRegistry.remove(getCallTimer);
            Metrics.globalRegistry.remove(iterNewCallTimer);
            Metrics.globalRegistry.remove(iterNextCallTimer);
            Metrics.globalRegistry.remove(iterPrevCallTimer);
            Metrics.globalRegistry.remove(iterSeekCallTimer);
            Metrics.globalRegistry.remove(iterSeekForPrevCallTimer);
            Metrics.globalRegistry.remove(iterSeekToFirstCallTimer);
            Metrics.globalRegistry.remove(iterSeekToLastCallTimer);
            Metrics.globalRegistry.remove(iterRefreshTimer);
            Metrics.globalRegistry.remove(insertCallTimer);
            Metrics.globalRegistry.remove(putCallTimer);
            Metrics.globalRegistry.remove(delCallTimer);
            Metrics.globalRegistry.remove(clearRangeCallTimer);
            Metrics.globalRegistry.remove(batchWriteCallTimer);
            Metrics.globalRegistry.remove(flushCallTimer);
            Metrics.globalRegistry.remove(batchSizeSummary);
        }
    }
}
