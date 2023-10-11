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

import static com.google.protobuf.ByteString.unsignedLexicographicalComparator;

import com.baidu.bifromq.basekv.proto.SplitHint;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SplitKeyEstimator implements ISplitKeyEstimator {
    private final Supplier<Long> nanoSource;
    private final double expectedLatencyNanos;
    private final long trackWindowLengthInNano;
    private final long minRecords;
    private final NavigableMap<Long, RecordingWindowSlot> trackedKeySlots = new ConcurrentSkipListMap<>();
    private final NavigableMap<Long, SplitHint> splitHints = new ConcurrentSkipListMap<>();
    private final Function<ByteString, ByteString> toSplitKey;
    private final boolean trackConcurrentLoad;

    public SplitKeyEstimator(Duration expectedLatency, int windowSize, boolean trackConcurrentLoad) {
        this(System::nanoTime, expectedLatency, windowSize, trackConcurrentLoad, k -> k);
    }

    public SplitKeyEstimator(Duration expectedLatency, int windowSize, boolean trackConcurrentLoad,
                             Function<ByteString, ByteString> toSplitKey) {
        this(System::nanoTime, expectedLatency, windowSize, trackConcurrentLoad, toSplitKey);
    }

    public SplitKeyEstimator(Supplier<Long> nanoSource, Duration expectedLatency, int windowSize,
                             boolean trackConcurrentLoad) {
        this(nanoSource, expectedLatency, windowSize, trackConcurrentLoad, k -> k);
    }

    public SplitKeyEstimator(Supplier<Long> nanoSource,
                             Duration expectedLatency,
                             int windowSize,
                             boolean trackConcurrentLoad,
                             Function<ByteString, ByteString> toSplitKey) {
        Preconditions.checkArgument(0 < windowSize, "Window size must be positive");
        Preconditions.checkArgument(expectedLatency.toNanos() < Duration.ofSeconds(windowSize).toNanos(),
            "Expected latency must be shorter than windowSize");
        this.nanoSource = nanoSource;
        this.expectedLatencyNanos = expectedLatency.toNanos();
        this.toSplitKey = toSplitKey;
        this.trackWindowLengthInNano = Duration.ofSeconds(windowSize).toNanos();
        this.trackConcurrentLoad = trackConcurrentLoad;
        this.minRecords = trackWindowLengthInNano / expectedLatency.toNanos() / 2;
    }

    @Override
    public SplitHint estimate() {
        long currentSlot = getSlot();
        trackedKeySlots.headMap(currentSlot - 2).clear();
        splitHints.headMap(currentSlot).clear();
        return splitHints.computeIfAbsent(currentSlot, n -> doEstimate(n - 1));
    }

    private SplitHint doEstimate(long slot) {
        RecordingWindowSlot keysSlot = trackedKeySlots.get(slot);
        if (keysSlot == null) {
            return SplitHint.getDefaultInstance();
        }
        int records = keysSlot.records.get();
        double totalLatency = keysSlot.totalLatency.get();
        double avgLatency = totalLatency / records;
        SplitHint.Builder hintBuilder = SplitHint.newBuilder().setLoad(avgLatency / expectedLatencyNanos);
        // start estimating only when there is enough records tracked in the window if the load on range is concurrently happened
        if ((!trackConcurrentLoad || records > minRecords) && avgLatency > expectedLatencyNanos) {
            long loadSum = 0;
            long halfTotal = keysSlot.totalKVLatency.get() / 2;
            NavigableMap<ByteString, AtomicLong> slotDistro = new TreeMap<>(unsignedLexicographicalComparator());
            slotDistro.putAll(keysSlot.loadDistribution);
            for (Map.Entry<ByteString, AtomicLong> e : slotDistro.entrySet()) {
                if (e.getValue().get() >= halfTotal) {
                    hintBuilder.setSplitKey(toSplitKey.apply(e.getKey()));
                    break;
                } else {
                    loadSum += e.getValue().get();
                    if (loadSum >= halfTotal) {
                        hintBuilder.setSplitKey(toSplitKey.apply(e.getKey()));
                        break;
                    }
                }
            }
        }
        return hintBuilder.build();
    }

    private long getSlot() {
        long nano = nanoSource.get();
        return nano / trackWindowLengthInNano;
    }

    @Override
    public ILoadRecorder start() {
        return new LoadRecorder();
    }

    private static class RecordingWindowSlot {
        final AtomicInteger records = new AtomicInteger();
        final AtomicLong totalKVLatency = new AtomicLong();
        final AtomicLong totalLatency = new AtomicLong();
        final Map<ByteString, AtomicLong> loadDistribution = new ConcurrentHashMap<>();

        void record(Map<ByteString, Long> keyLoads, long totalLatencyNanos) {
            keyLoads.forEach((key, latencyNanos) -> {
                totalKVLatency.addAndGet(latencyNanos);
                loadDistribution.computeIfAbsent(key, k -> new AtomicLong()).addAndGet(latencyNanos);
            });
            totalLatency.addAndGet(totalLatencyNanos);
            records.incrementAndGet();
        }
    }

    private class LoadRecorder implements ILoadRecorder {
        private final long startNanos = nanoSource.get();
        Map<ByteString, Long> loadDistribution = new HashMap<>();

        @Override
        public void record(ByteString key, long latencyNanos) {
            loadDistribution.compute(key, (k, v) -> v == null ? latencyNanos : v + latencyNanos);
        }

        @Override
        public void stop() {
            long currentSlot = nanoSource.get() / trackWindowLengthInNano;
            long mySlot = startNanos / trackWindowLengthInNano;
            if (mySlot < currentSlot) {
                // mySlot has passed, add record to current slot
                mySlot = currentSlot;
            }
            trackedKeySlots.headMap(mySlot - 1).clear();
            trackedKeySlots.computeIfAbsent(mySlot, k -> new RecordingWindowSlot())
                .record(loadDistribution, nanoSource.get() - startNanos);
        }
    }
}
