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

package com.baidu.bifromq.dist.worker;

import static com.google.protobuf.ByteString.unsignedLexicographicalComparator;

import com.baidu.bifromq.basekv.proto.LoadHint;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.jctools.maps.NonBlockingHashMap;

@Slf4j
public final class LoadEstimator implements ILoadEstimator {
    private final Supplier<Long> nanoSource;
    private final double maxUnitLoad;
    private final double splitKeyEstThreshold;
    private final long trackWindowLengthInNano;
    private final long trackWindowSize;
    private final NavigableMap<Long, Map<ByteString, AtomicInteger>> trackedKeySlots = new ConcurrentSkipListMap<>();
    private final NavigableMap<Long, LoadHint> hints = new ConcurrentSkipListMap<>();

    public LoadEstimator(long maxUnitLoad, double splitKeyEstThreshold, int trackWindow) {
        this(System::nanoTime, maxUnitLoad, splitKeyEstThreshold, trackWindow);
    }

    public LoadEstimator(Supplier<Long> nanoSource, long maxUnitLoad, double splitKeyEstThreshold, int trackSeconds) {
        Preconditions.checkArgument(0 < maxUnitLoad, "Max unit load must be positive");
        Preconditions.checkArgument(0 < splitKeyEstThreshold && splitKeyEstThreshold < 1,
            "SplitKey estimation threshold must be between 0.0 and 1.0");
        Preconditions.checkArgument(0 < trackSeconds, "Track seconds must be positive");
        this.nanoSource = nanoSource;
        this.maxUnitLoad = maxUnitLoad;
        this.splitKeyEstThreshold = splitKeyEstThreshold;
        this.trackWindowSize = trackSeconds;
        this.trackWindowLengthInNano = Duration.ofSeconds(trackSeconds).toNanos();
    }

    @Override
    public void track(ByteString key, int unitLoad) {
        long slot = getSlot();
        trackedKeySlots.headMap(slot - 1).clear();
        Map<ByteString, AtomicInteger> keySlot = trackedKeySlots.computeIfAbsent(slot,
            k -> {
                ConcurrentMap<ByteString, AtomicInteger> value = new ConcurrentHashMap<>();
                value.put(ByteString.EMPTY, new AtomicInteger());
                return value;
            });
        keySlot.get(ByteString.EMPTY).addAndGet(unitLoad);
        keySlot.computeIfAbsent(key, k -> new AtomicInteger()).addAndGet(unitLoad);
    }

    @Override
    public LoadHint estimate() {
        long currentSlot = getSlot();
        trackedKeySlots.headMap(currentSlot - 2).clear();
        hints.headMap(currentSlot).clear();
        return hints.computeIfAbsent(currentSlot, n -> doEstimate(n - 1));
    }

    private LoadHint doEstimate(long slot) {
        Map<ByteString, AtomicInteger> keysSlot = trackedKeySlots.get(slot);
        if (keysSlot == null) {
            return LoadHint.newBuilder().setLoad(0.0).build();
        }
        long totalUnitLoad = keysSlot.get(ByteString.EMPTY).get();
        long avgUnitLoad = totalUnitLoad / trackWindowSize;
        double loadPercent = Math.min(1, avgUnitLoad / maxUnitLoad);
        LoadHint.Builder loadHintBuilder = LoadHint.newBuilder().setLoad(loadPercent);
        if (loadPercent > splitKeyEstThreshold) {
            long loadSum = 0;
            long halfTotal = totalUnitLoad / 2;
            NavigableMap<ByteString, AtomicInteger> slotDistro = new TreeMap<>(unsignedLexicographicalComparator());
            slotDistro.putAll(keysSlot);
            slotDistro.remove(ByteString.EMPTY);
            for (Map.Entry<ByteString, AtomicInteger> e : slotDistro.entrySet()) {
                if (e.getValue().get() >= halfTotal) {
                    loadHintBuilder.setSplitKey(e.getKey());
                    break;
                } else {
                    loadSum += e.getValue().get();
                    if (loadSum >= halfTotal) {
                        loadHintBuilder.setSplitKey(e.getKey());
                        break;
                    }
                }
            }
        }
        return loadHintBuilder.build();
    }

    private long getSlot() {
        long nano = nanoSource.get();
        return nano / trackWindowLengthInNano;
    }
}
