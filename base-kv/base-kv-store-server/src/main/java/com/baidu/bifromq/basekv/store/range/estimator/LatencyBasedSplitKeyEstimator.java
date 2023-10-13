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
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LatencyBasedSplitKeyEstimator extends AbstractSplitKeyEstimator {
    private final double expectedLatencyNanos;
    private final long minRecords;

    public LatencyBasedSplitKeyEstimator(Duration expectedLatency,
                                         int windowSize,
                                         String... tags) {
        this(System::nanoTime, expectedLatency, windowSize, k -> k, tags);
    }

    public LatencyBasedSplitKeyEstimator(Duration expectedLatency,
                                         int windowSize,
                                         Function<ByteString, ByteString> toSplitKey,
                                         String... tags) {
        this(System::nanoTime, expectedLatency, windowSize, toSplitKey, tags);
    }

    public LatencyBasedSplitKeyEstimator(Supplier<Long> nanoSource, Duration expectedLatency, int windowSize,
                                         String... tags) {
        this(nanoSource, expectedLatency, windowSize, k -> k, tags);
    }

    public LatencyBasedSplitKeyEstimator(Supplier<Long> nanoSource,
                                         Duration expectedLatency,
                                         int windowSize,
                                         Function<ByteString, ByteString> toSplitKey,
                                         String... tags) {
        super(nanoSource, windowSize, toSplitKey, tags);
        Preconditions.checkArgument(expectedLatency.toNanos() < Duration.ofSeconds(windowSize).toNanos(),
            "Expected latency must be shorter than windowSize");
        this.expectedLatencyNanos = expectedLatency.toNanos();
        this.minRecords = Duration.ofSeconds(windowSize).toNanos() / expectedLatency.toNanos() / 2;
    }

    @Override
    protected SplitHint doEstimate(RecordingWindowSlot windowSlot) {
        int records = windowSlot.records();
        double avgLatency = windowSlot.avgLatencyNanos();
        SplitHint.Builder hintBuilder = SplitHint.newBuilder().setLoad(avgLatency / expectedLatencyNanos);
        // start estimating only when there is enough records tracked in the window if the load on range is concurrently happened
        if (records > minRecords && avgLatency > expectedLatencyNanos) {
            windowSlot.estimateSplitKey().ifPresent(hintBuilder::setSplitKey);
        }
        return hintBuilder.build();
    }
}
