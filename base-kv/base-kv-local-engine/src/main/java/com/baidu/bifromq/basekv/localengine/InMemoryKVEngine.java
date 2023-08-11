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
import static com.google.protobuf.ByteString.unsignedLexicographicalComparator;

import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InMemoryKVEngine extends AbstractKVEngine<InMemoryKVEngine.KeyRange, InMemoryKVEngine.WriteBatch> {
    private final String identity;

    private final Comparator<ByteString> comparator = unsignedLexicographicalComparator();
    private final Map<String, ConcurrentSkipListMap<ByteString, ByteString>> nsData = new HashMap<>();

    private final ConcurrentHashMap<String, Checkpoint> openedCheckpoints = new ConcurrentHashMap<>();

    private final InMemoryKVEngineConfigurator configurator;

    private final Map<String, Long> averageKVSizeMap = new HashMap<>();
    private final Map<String, Long> approximateKVCountMap = new HashMap<>();
    private final LongAdder readKeys = new LongAdder();
    private final LongAdder readBytes = new LongAdder();
    private final LongAdder writtenKeys = new LongAdder();
    private final LongAdder writtenBytes = new LongAdder();
    private final Duration checkpointAge;
    private ScheduledExecutorService gcExecutor;
    private volatile ScheduledFuture<?> gcFuture;

    public InMemoryKVEngine(String overrideIdentity,
                            List<String> namespaces,
                            Predicate<String> checker,
                            InMemoryKVEngineConfigurator c) {
        this(overrideIdentity, namespaces, checker, c, Duration.ofMillis(5));
    }

    public InMemoryKVEngine(String overrideIdentity,
                            List<String> namespaces,
                            Predicate<String> checker,
                            InMemoryKVEngineConfigurator c,
                            Duration checkpointAge) {
        super(overrideIdentity, namespaces, checker);
        this.configurator = c;
        this.checkpointAge = checkpointAge;
        if (namespaces.isEmpty()) {
            throw new IllegalArgumentException("Namespaces mustn't be empty");
        }
        Set<String> nsList = new HashSet<>(namespaces);
        nsList.add(DEFAULT_NS);
        nsList.forEach(namespace -> {
            nsData.put(namespace, new ConcurrentSkipListMap<>(comparator));
            averageKVSizeMap.put(namespace, 0L);
            approximateKVCountMap.put(namespace, 0L);
        });
        if (overrideIdentity != null && !overrideIdentity.trim().isEmpty()) {
            identity = overrideIdentity;
        } else {
            identity = UUID.randomUUID().toString();
        }
    }

    @Override
    protected void doStart(ScheduledExecutorService bgTaskExecutor, String... metricTags) {
        this.gcExecutor = bgTaskExecutor;
        log.debug("InMemoryKVEngine[{}] initialized", id());
    }

    @Override
    protected void afterStart() {
        scheduleNextGC();
    }

    @SneakyThrows
    @Override
    protected void doStop() {
        log.debug("Stopping InMemoryKVEngine");
        if (gcFuture != null) {
            gcFuture.cancel(true);
            if (!gcFuture.isCancelled()) {
                try {
                    gcFuture.get();
                } catch (Throwable e) {
                    log.error("Error during stop gc task", e);
                }
            }
        }
    }

    @Override
    public String id() {
        return identity;
    }

    @Override
    protected KeyRange newKeyRange(int id, String namespace, ByteString start, ByteString end) {
        return new KeyRange(id, namespace, start, end);
    }

    protected ByteString doSkip(KeyRange keyRange, long count) {
        String namespace = keyRange.ns;
        ByteString lowerBound = keyRange.start;
        ByteString upperBound = keyRange.end;
        if (lowerBound == null && upperBound == null) {
            return nsData.get(namespace).keySet().stream().skip(count).findFirst().orElse(ByteString.EMPTY);
        }
        if (lowerBound == null) {
            return nsData.get(namespace).headMap(upperBound).keySet()
                .stream().skip(count).findFirst().orElse(ByteString.EMPTY);
        }
        if (upperBound == null) {
            return nsData.get(namespace).tailMap(lowerBound).keySet()
                .stream().skip(count).findFirst().orElse(ByteString.EMPTY);
        }
        return nsData.get(namespace).subMap(lowerBound, upperBound).keySet()
            .stream().skip(count).findFirst().orElse(ByteString.EMPTY);
    }

    @Override
    protected long size(ByteString start, ByteString end, KeyRange range) {
        ConcurrentNavigableMap<ByteString, ByteString> data;
        String namespace = range.ns;
        if (start == null && end == null) {
            data = nsData.get(namespace);
        } else if (start == null) {
            data = nsData.get(namespace).headMap(end);
        } else if (end == null) {
            data = nsData.get(namespace).tailMap(start);
        } else {
            data = nsData.get(namespace).subMap(start, end);
        }
        // this may take a long time
        return data.entrySet()
            .stream()
            .map(entry -> entry.getKey().size() + entry.getValue().size())
            .reduce(0, Integer::sum);
    }

    protected long size(KeyRange range) {
        return size(range.ns, range.start, range.end);
    }

    protected long size(String checkpointId, KeyRange range, ByteString start, ByteString end) {
        String namespace = range.ns;
        if (openedCheckpoints.containsKey(checkpointId)) {
            ConcurrentNavigableMap<ByteString, ByteString> data;
            Checkpoint nsData = openedCheckpoints.get(checkpointId);
            if (start == null && end == null) {
                data = nsData.get(namespace);
            } else if (start == null) {
                data = nsData.get(namespace).headMap(end);
            } else if (end == null) {
                data = nsData.get(namespace).tailMap(start);
            } else {
                data = nsData.get(namespace).subMap(start, end);
            }
            // this may take a long time
            return data.entrySet()
                .stream()
                .map(entry -> entry.getKey().size() + entry.getValue().size())
                .reduce(0, Integer::sum);
        } else {
            throw new KVEngineException("Checkpoint[" + checkpointId + "] not found");
        }
    }

    @Override
    public void checkpoint(String checkpointId) {
        checkState();
        Checkpoint nsDataCheckpoint = new Checkpoint();
        nsData.forEach((ns, data) -> nsDataCheckpoint.put(ns, data.clone()));
        openedCheckpoints.put(checkpointId, nsDataCheckpoint);
    }

    @Override
    public boolean hasCheckpoint(String checkpointId) {
        checkState();
        return openedCheckpoints.containsKey(checkpointId);
    }

    protected boolean exist(ByteString key, KeyRange range) {
        return nsData.get(range.ns).containsKey(key);
    }

    protected boolean exist(String checkpointId, KeyRange range, ByteString key) {
        if (openedCheckpoints.containsKey(checkpointId)) {
            return openedCheckpoints.get(checkpointId).get(range.ns).containsKey(key);
        } else {
            throw new KVEngineException("Checkpoint[" + checkpointId + "] not found");
        }
    }

    protected Optional<ByteString> get(ByteString key, KeyRange range) {
        Optional<ByteString> value = Optional.ofNullable(nsData.get(range.ns).get(key));
        readKeys.increment();
        readBytes.add(value.orElse(EMPTY).size());
        return value;
    }

    protected Optional<ByteString> get(String checkpointId, KeyRange range, ByteString key) {
        if (openedCheckpoints.containsKey(checkpointId)) {
            Optional<ByteString> value = Optional.ofNullable(openedCheckpoints
                .get(checkpointId).get(range.ns).get(key));
            readKeys.increment();
            readBytes.add(value.orElse(EMPTY).size());
            return value;
        } else {
            throw new KVEngineException("Checkpoint[" + checkpointId + "] not found");
        }
    }

    protected IKVEngineIterator newIterator(ByteString start, ByteString end, KeyRange range) {
        return new InMemoryKVEngineIterator(nsData.get(range.ns), start, end, readKeys, readKeys);
    }

    @Override
    protected IKVEngineIterator newIterator(String checkpointId, String namespace, ByteString start, ByteString end) {
        if (openedCheckpoints.containsKey(checkpointId)) {
            return new InMemoryKVEngineIterator(openedCheckpoints.get(checkpointId).get(namespace),
                start, end, readKeys, readKeys);
        } else {
            throw new KVEngineException("Checkpoint[" + checkpointId + "] not found");
        }
    }

    @Override
    protected WriteBatch newWriteBatch(int batchId) {
        return new WriteBatch(batchId);
    }

    protected void delete(int batchId, KeyRange range, ByteString key) {
        getBatch(batchId).delete(range, key);
    }

    protected void doDelete(KeyRange range, ByteString key) {
        ByteString oldValue = nsData.get(range.ns).remove(key);
        writtenKeys.increment();
        if (oldValue != null) {
            approximateKVCountMap.put(range.ns, Math.max(approximateKVCountMap.get(range.ns) - 1, 0));
        }
    }

    protected void clearSubRange(int batchId, KeyRange range, ByteString start, ByteString end) {
        getBatch(batchId).deleteRange(range.ns, start, end);
    }

    protected void doClearSubRange(KeyRange range, ByteString start, ByteString end) {
        String namespace = range.ns;
        NavigableSet<ByteString> inRangeKeys;
        if (start == null && end == null) {
            inRangeKeys = nsData.get(namespace).keySet();
        } else if (start == null) {
            inRangeKeys = nsData.get(namespace).headMap(end).keySet();
        } else if (end == null) {
            inRangeKeys = nsData.get(namespace).tailMap(start).keySet();
        } else {
            inRangeKeys = nsData.get(namespace).subMap(start, end).keySet();
        }
        inRangeKeys.forEach(k -> {
            ByteString oldV = nsData.get(namespace).remove(k);
            writtenKeys.increment();
            if (oldV != null) {
                approximateKVCountMap.put(namespace, Math.max(approximateKVCountMap.get(namespace) - 1, 0));
            }
        });
    }

    protected void insert(int batchId, KeyRange range, ByteString key, ByteString value) {
        assert !exist(range.ns, key);
        getBatch(batchId).put(range, key, value);
    }

    protected void doInsert(KeyRange range, ByteString key, ByteString value) {
        String namespace = range.ns;
        assert !exist(namespace, key);
        ByteString oldValue = nsData.get(namespace).put(key, value);
        writtenKeys.increment();
        writtenBytes.add(value.size());
        if (oldValue == null) {
            approximateKVCountMap.put(namespace, approximateKVCountMap.get(namespace) + 1);
        }
        calculateAverageKVSize(namespace, key, value);
    }

    protected void put(int batchId, KeyRange range, ByteString key, ByteString value) {
        getBatch(batchId).put(range, key, value);
    }

    protected void doPut(KeyRange range, ByteString key, ByteString value) {
        String namespace = range.ns;
        ByteString oldValue = nsData.get(namespace).put(key, value);
        writtenKeys.increment();
        writtenBytes.add(value.size());
        if (oldValue == null) {
            approximateKVCountMap.put(namespace, approximateKVCountMap.get(namespace) + 1);
        }
        calculateAverageKVSize(namespace, key, value);
    }

    private void scheduleNextGC() {
        if (state() != State.STARTED) {
            return;
        }
        log.debug("Checkpoint collector scheduled[identity={}]", identity);
        gcFuture = gcExecutor.schedule(this::gc, configurator.getGcIntervalInSec(), TimeUnit.SECONDS);
    }

    private void gc() {
        if (state() != State.STARTED) {
            return;
        }
        openedCheckpoints.keySet()
            .removeIf(checkpointId -> !inUse(checkpointId)
                && openedCheckpoints.get(checkpointId).older(checkpointAge));
        scheduleNextGC();
    }

    private void calculateAverageKVSize(String namespace, ByteString key, ByteString value) {
        if (averageKVSizeMap.get(namespace) == 0) {
            averageKVSizeMap.put(namespace, (long) (key.size() + value.size()));
        } else {
            averageKVSizeMap.put(namespace, (averageKVSizeMap.get(namespace) + key.size() + value.size()) / 2);
        }
    }

    protected static class KeyRange extends AbstractKeyRange {
        public KeyRange(int id, String ns, ByteString start, ByteString end) {
            super(id, ns, start, end);
        }
    }

    protected interface KVAction {
        KVAction.Type type();

        enum Type {Put, Delete, DeleteRange}
    }

    protected class WriteBatch extends AbstractWriteBatch<KeyRange> {
        long writtenKeys = 0;
        long writtenBytes = 0;
        Map<String, List<KVAction>> actions = new HashMap<>();

        protected WriteBatch(int batchId) {
            super(batchId);
        }

        @Override
        public int count() {
            return actions.values().stream().map(List::size).reduce(0, Integer::sum);
        }

        @Override
        public void insert(KeyRange range, ByteString key, ByteString value) {

        }

        public void put(KeyRange range, ByteString key, ByteString val) {
            actions.computeIfAbsent(range.ns, k -> new LinkedList<>()).add(new Put(key, val));
            writtenKeys++;
            writtenBytes += val.size();
        }

        public void delete(KeyRange range, ByteString key) {
            actions.computeIfAbsent(range.ns, k -> new LinkedList<>()).add(new Delete(key));
            writtenBytes++;
        }

        @Override
        public void end() {
            for (String ns : actions.keySet()) {
                for (KVAction action : actions.get(ns)) {
                    switch (action.type()) {
                        case Put: {
                            WriteBatch.Put put = (WriteBatch.Put) action;
                            ByteString oldValue = nsData.get(ns).put(put.key, put.value);
                            if (oldValue == null) {
                                calculateAverageKVSize(ns, put.key, put.value);
                                approximateKVCountMap.put(ns, approximateKVCountMap.get(ns) + 1);
                            }
                            break;
                        }
                        case Delete: {
                            WriteBatch.Delete delete = (WriteBatch.Delete) action;
                            ByteString oldValue = nsData.get(ns).remove(delete.key);
                            if (oldValue != null) {
                                approximateKVCountMap.put(ns, Math.max(approximateKVCountMap.get(ns) - 1, 0));
                            }
                            break;
                        }
                        case DeleteRange:
                            WriteBatch.DeleteRange deleteRange = (WriteBatch.DeleteRange) action;
                            NavigableSet<ByteString> inRangeKeys;
                            if (deleteRange.startKey == null && deleteRange.endKey == null) {
                                inRangeKeys = nsData.get(ns).keySet();
                            } else if (deleteRange.startKey == null) {
                                inRangeKeys = nsData.get(ns).headMap(deleteRange.endKey).keySet();
                            } else if (deleteRange.endKey == null) {
                                inRangeKeys = nsData.get(ns).tailMap(deleteRange.startKey).keySet();
                            } else {
                                inRangeKeys = nsData.get(ns).subMap(deleteRange.startKey, deleteRange.endKey).keySet();
                            }
                            inRangeKeys.forEach(k -> {
                                ByteString oldValue = nsData.get(ns).remove(k);
                                if (oldValue != null) {
                                    approximateKVCountMap.put(ns, Math.max(approximateKVCountMap.get(ns) - 1, 0));
                                }
                            });
                            break;
                    }
                }
            }
            InMemoryKVEngine.this.writtenKeys.add(writtenKeys);
            InMemoryKVEngine.this.writtenBytes.add(writtenBytes);
            log.trace("Batch[{}] committed", batchId);
        }

        @Override
        public void abort() {

        }

        public void deleteRange(String ns, ByteString startKey, ByteString endKey) {
            actions.computeIfAbsent(ns, k -> new LinkedList<>()).add(new DeleteRange(startKey, endKey));
            writtenBytes++;
        }


        @AllArgsConstructor
        class Put implements KVAction {
            final ByteString key;
            final ByteString value;

            @Override
            public Type type() {
                return Type.Put;
            }
        }

        @AllArgsConstructor
        class Delete implements KVAction {
            final ByteString key;

            @Override
            public Type type() {
                return Type.Delete;
            }

        }

        @AllArgsConstructor
        class DeleteRange implements KVAction {
            final ByteString startKey;
            final ByteString endKey;

            @Override
            public Type type() {
                return Type.DeleteRange;
            }
        }
    }

    private static class Checkpoint extends HashMap<String, ConcurrentSkipListMap<ByteString, ByteString>> {
        private final long createTs = System.nanoTime();

        public Checkpoint() {
        }

        public boolean older(Duration age) {
            return age.compareTo(Duration.ofNanos(System.nanoTime() - createTs)) < 0;
        }
    }
}
