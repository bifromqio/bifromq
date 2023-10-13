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

package com.baidu.bifromq.basekv.store.range.estimator;

import com.baidu.bifromq.basekv.proto.SplitHint;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import java.lang.ref.Cleaner;
import java.time.Duration;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class AbstractSplitKeyEstimator implements ISplitKeyEstimator {
    private record CleanableState(Meter meter) implements Runnable {

        @Override
        public void run() {
            Metrics.globalRegistry.remove(meter);
        }
    }

    private static final Cleaner CLEANER = Cleaner.create();
    private final Supplier<Long> nanoSource;
    private final long windowSizeNanos;
    private final NavigableMap<Long, RecordingWindowSlot> trackedKeySlots = new ConcurrentSkipListMap<>();
    private final NavigableMap<Long, SplitHint> splitHints = new ConcurrentSkipListMap<>();
    private final Function<ByteString, ByteString> toSplitKey;
    private final AtomicDouble ioDensity = new AtomicDouble();
    private final AtomicLong ioNanos = new AtomicLong();
    private final Gauge ioDensityGuage;
    private final Gauge ioNanosGauge;
    private final Gauge avgLatencyNanosGauge;

    protected AbstractSplitKeyEstimator(Supplier<Long> nanoSource,
                                        int windowSizeSeconds,
                                        Function<ByteString, ByteString> toSplitKey,
                                        String... tags) {
        Preconditions.checkArgument(0 <= windowSizeSeconds, "Window size must be positive");
        this.nanoSource = nanoSource;
        this.windowSizeNanos = Duration.ofSeconds(windowSizeSeconds).toNanos();
        this.toSplitKey = toSplitKey;
        this.ioDensityGuage = Gauge.builder("basekv.load.est.iodensity", ioDensity::get)
            .tags(tags)
            .register(Metrics.globalRegistry);
        this.ioNanosGauge = Gauge.builder("basekv.load.est.iolatency", ioNanos::get)
            .tags(tags)
            .register(Metrics.globalRegistry);
        this.avgLatencyNanosGauge = Gauge.builder("basekv.load.est.avglatency", ioNanos::get)
            .tags(tags)
            .register(Metrics.globalRegistry);
        CLEANER.register(this, new CleanableState(ioDensityGuage));
        CLEANER.register(this, new CleanableState(ioNanosGauge));
        CLEANER.register(this, new CleanableState(avgLatencyNanosGauge));
    }

    @Override
    public ILoadRecorder start() {
        return new LoadRecorder(nanoSource, this::onRecord);
    }

    private void onRecord(LoadRecorder recorder) {
        long startNanos = recorder.startNanos();
        int kvIOs = recorder.getKVIOs();
        long kvNanos = recorder.getKVIONanos();
        Map<ByteString, Long> loadDistribution = recorder.keyDistribution();
        long now = nanoSource.get();
        long currentSlot = now / windowSizeNanos;
        long mySlot = startNanos / windowSizeNanos;
        trackedKeySlots.headMap(currentSlot - 1).clear();
        if (mySlot < currentSlot) {
            // cross window slot
            long slotBegin = currentSlot * windowSizeNanos;
            trackedKeySlots.computeIfAbsent(currentSlot, k -> new RecordingWindowSlot(toSplitKey))
                .record(loadDistribution, kvIOs, kvNanos, now - slotBegin);

            if (mySlot + 1 < currentSlot) {
                trackedKeySlots.computeIfAbsent(currentSlot - 1, k -> new RecordingWindowSlot(toSplitKey))
                    .record(loadDistribution, kvIOs, kvNanos, windowSizeNanos);
            } else {
                trackedKeySlots.computeIfAbsent(currentSlot - 1, k -> new RecordingWindowSlot(toSplitKey))
                    .record(loadDistribution, kvIOs, kvNanos, slotBegin - startNanos);
            }
            // re-estimate
            splitHints.put(currentSlot - 1, doEstimate(currentSlot - 1));
        } else {
            // still in same window slot
            trackedKeySlots.computeIfAbsent(currentSlot, k -> new RecordingWindowSlot(toSplitKey))
                .record(loadDistribution, kvIOs, kvNanos, now - startNanos);
        }
    }

    private SplitHint doEstimate(long slot) {
        RecordingWindowSlot windowSlot = trackedKeySlots.get(slot);
        if (windowSlot == null) {
            ioDensity.set(0);
            ioNanos.set(0);
            return SplitHint.getDefaultInstance();
        }
        ioDensity.set(windowSlot.ioDensity());
        ioNanos.set(windowSlot.ioLatencyNanos());
        return doEstimate(windowSlot);
    }

    protected abstract SplitHint doEstimate(RecordingWindowSlot windowSlot);

    private long getSlot() {
        long nano = nanoSource.get();
        return nano / windowSizeNanos;
    }

    @Override
    public SplitHint estimate() {
        long currentSlot = getSlot();
        trackedKeySlots.headMap(currentSlot - 2).clear();
        splitHints.headMap(currentSlot).clear();
        return splitHints.computeIfAbsent(currentSlot, n -> doEstimate(n - 1));
    }
}
