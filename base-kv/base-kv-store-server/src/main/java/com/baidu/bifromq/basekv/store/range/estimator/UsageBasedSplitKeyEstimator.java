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
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UsageBasedSplitKeyEstimator extends AbstractSplitKeyEstimator {
    private static final long DEFAULT_IO_NANOS_LIMIT = 50_000; // 50 microseconds
    private static final int DEFAULT_MAX_IO_DENSITY = 30;
    private final long ioNanosLimit;
    private final int maxIODensity;

    public UsageBasedSplitKeyEstimator(Supplier<Long> nanoSource, int windowSizeSec, String... tags) {
        this(nanoSource, windowSizeSec, DEFAULT_IO_NANOS_LIMIT, DEFAULT_MAX_IO_DENSITY, k -> k, tags);
    }

    public UsageBasedSplitKeyEstimator(int windowSizeSec,
                                       long ioNanosLimit,
                                       int maxIODensity,
                                       Function<ByteString, ByteString> toSplitKey,
                                       String... tags) {
        this(System::nanoTime, windowSizeSec, ioNanosLimit, maxIODensity, toSplitKey, tags);
    }

    public UsageBasedSplitKeyEstimator(Supplier<Long> nanoSource,
                                       int windowSizeSec,
                                       long ioNanosLimit,
                                       int maxIODensity,
                                       Function<ByteString, ByteString> toSplitKey, String... tags) {
        super(nanoSource, windowSizeSec, toSplitKey, tags);
        Preconditions.checkArgument(0 <= windowSizeSec, "Window size must be positive");
        this.ioNanosLimit = ioNanosLimit;
        this.maxIODensity = maxIODensity;
    }

    @Override
    protected SplitHint doEstimate(RecordingWindowSlot windowSlot) {
        double density = windowSlot.ioDensity();
        long avgIONanos = windowSlot.ioLatencyNanos();
        SplitHint.Builder hintBuilder = SplitHint.newBuilder().setLoad(density / (double) maxIODensity);
        if (avgIONanos < ioNanosLimit && density > maxIODensity) {
            windowSlot.estimateSplitKey().ifPresent(hintBuilder::setSplitKey);
        }
        return hintBuilder.build();
    }
}
