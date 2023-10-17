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
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import java.util.function.Supplier;

public class SplitKeyEstimator implements ISplitKeyEstimator {
    private final Supplier<Long> nanoSource;
    private final long windowSizeNanos;
    private final NavigableMap<Long, LoadRecordWindow> trackedKeySlots = new ConcurrentSkipListMap<>();
    private final NavigableMap<Long, SplitHint> recentLoadHints = new ConcurrentSkipListMap<>();
    private final Function<ByteString, ByteString> toSplitKey;

    public SplitKeyEstimator(int windowSizeNanos) {
        this(windowSizeNanos, k -> k);
    }

    public SplitKeyEstimator(int windowSizeSeconds, Function<ByteString, ByteString> toSplitKey) {
        this(System::nanoTime, windowSizeSeconds, toSplitKey);
    }

    public SplitKeyEstimator(Supplier<Long> nanoSource,
                             int windowSizeSeconds,
                             Function<ByteString, ByteString> toSplitKey) {
        Preconditions.checkArgument(0 <= windowSizeSeconds, "Window size must be positive");
        this.nanoSource = nanoSource;
        this.windowSizeNanos = Duration.ofSeconds(windowSizeSeconds).toNanos();
        this.toSplitKey = toSplitKey;
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
            trackedKeySlots.computeIfAbsent(currentSlot, k -> new LoadRecordWindow(toSplitKey))
                .record(loadDistribution, kvIOs, kvNanos, now - slotBegin);

            if (mySlot + 1 < currentSlot) {
                trackedKeySlots.computeIfAbsent(currentSlot - 1, k -> new LoadRecordWindow(toSplitKey))
                    .record(loadDistribution, kvIOs, kvNanos, windowSizeNanos);
            } else {
                trackedKeySlots.computeIfAbsent(currentSlot - 1, k -> new LoadRecordWindow(toSplitKey))
                    .record(loadDistribution, kvIOs, kvNanos, slotBegin - startNanos);
            }
            // re-estimate
            recentLoadHints.put(currentSlot - 1, doEstimate(currentSlot - 1));
        } else {
            // still in same window slot
            trackedKeySlots.computeIfAbsent(currentSlot, k -> new LoadRecordWindow(toSplitKey))
                .record(loadDistribution, kvIOs, kvNanos, now - startNanos);
        }
    }

    private SplitHint doEstimate(long slot) {
        LoadRecordWindow loadHint = trackedKeySlots.get(slot);
        if (loadHint == null) {
            return SplitHint.getDefaultInstance();
        }
        SplitHint.Builder hintBuilder = SplitHint.newBuilder();
        hintBuilder.setIoDensity(loadHint.ioDensity());
        hintBuilder.setIoLatencyNanos(loadHint.ioLatencyNanos());
        hintBuilder.setAvgLatency(loadHint.avgLatencyNanos());
        loadHint.estimateSplitKey().ifPresent(hintBuilder::setSplitKey);
        return hintBuilder.build();
    }

    private long getSlot() {
        long nano = nanoSource.get();
        return nano / windowSizeNanos;
    }

    @Override
    public SplitHint estimate() {
        long currentSlot = getSlot();
        trackedKeySlots.headMap(currentSlot - 2).clear();
        recentLoadHints.headMap(currentSlot).clear();
        return recentLoadHints.computeIfAbsent(currentSlot, n -> doEstimate(n - 1));
    }
}
