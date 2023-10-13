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

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basekv.proto.SplitHint;
import com.baidu.bifromq.basekv.store.range.ILoadTracker;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.testng.annotations.Test;

public class LatencyBasedSplitKeyEstimatorTest extends AbstractSplitKeyEstimatorTest {

    @Override
    protected ISplitKeyEstimator create(Supplier<Long> nanoSource, int windowSizeSec) {
        return new LatencyBasedSplitKeyEstimator(nanoSource, Duration.ofMillis(100), windowSizeSec);
    }

    @Test
    public void notEnoughRecords() {
        LatencyBasedSplitKeyEstimator
            estimator = new LatencyBasedSplitKeyEstimator(nanoSource, Duration.ofMillis(100), 1);
        long now = 0L;
        for (int i = 0; i < 5; i++) {
            when(nanoSource.get()).thenReturn(now);
            ILoadTracker.ILoadRecorder recorder = estimator.start();
            for (int j = 0; j < 80; j++) {
                recorder.record(ByteString.copyFrom(new byte[] {(byte) j}), 1);
            }
            when(nanoSource.get()).thenReturn(now + Duration.ofMillis(200).toNanos());
            recorder.stop();
        }

        when(nanoSource.get()).thenReturn(now + Duration.ofMillis(1100).toNanos());
        SplitHint hint = estimator.estimate();
        assertTrue(hint.getLoad() > 0.0);
        assertFalse(hint.hasSplitKey());
    }

    @Test
    public void trackClearExpiredSlots() {
        LatencyBasedSplitKeyEstimator
            estimator = new LatencyBasedSplitKeyEstimator(nanoSource, Duration.ofMillis(100), 1);
        long now = 0L;
        for (int i = 0; i < 11; i++) {
            when(nanoSource.get()).thenReturn(now);
            ILoadTracker.ILoadRecorder recorder = estimator.start();
            recorder.record(ByteString.copyFromUtf8("Key"), Duration.ofMillis(10).toNanos());
            when(nanoSource.get()).thenReturn(now + Duration.ofMillis(200).toNanos());
            recorder.stop();
        }
        when(nanoSource.get()).thenReturn(now + Duration.ofMillis(1100).toNanos());
        SplitHint hint = estimator.estimate();
        assertTrue(hint.getLoad() > 0);
        assertTrue(hint.hasSplitKey());
        when(nanoSource.get()).thenReturn(now + Duration.ofSeconds(2).toNanos());
        hint = estimator.estimate();
        assertEquals(hint.getLoad(), 0);
        assertFalse(hint.hasSplitKey());
    }

    @Test
    public void skipCurrentWindow() {
        LatencyBasedSplitKeyEstimator
            estimator = new LatencyBasedSplitKeyEstimator(nanoSource, Duration.ofMillis(100), 1);
        long now = 0L;
        for (int i = 0; i < 11; i++) {
            when(nanoSource.get()).thenReturn(now);
            ILoadTracker.ILoadRecorder recorder = estimator.start();
            recorder.record(ByteString.copyFromUtf8("key" + i), Duration.ofMillis(100).toNanos());
            when(nanoSource.get()).thenReturn(now + Duration.ofMillis(200).toNanos());
            recorder.stop();
        }
        when(nanoSource.get()).thenReturn(now + Duration.ofMillis(900).toNanos());

        SplitHint hint = estimator.estimate();
        assertFalse(hint.hasSplitKey());

        when(nanoSource.get()).thenReturn(now + Duration.ofMillis(1001).toNanos());
        hint = estimator.estimate();
        assertTrue(hint.hasSplitKey());
    }

    @Test
    public void estimateSplitKey() {
        LatencyBasedSplitKeyEstimator
            estimator = new LatencyBasedSplitKeyEstimator(nanoSource, Duration.ofMillis(100), 1);
        long now = 0L;
        for (int i = 0; i < 11; i++) {
            when(nanoSource.get()).thenReturn(now);
            ILoadTracker.ILoadRecorder recorder = estimator.start();
            for (int j = 0; j < 80; j++) {
                recorder.record(ByteString.copyFrom(new byte[] {(byte) j}), 1);
            }
            when(nanoSource.get()).thenReturn(now + Duration.ofMillis(200).toNanos());
            recorder.stop();
        }

        when(nanoSource.get()).thenReturn(now + Duration.ofMillis(1100).toNanos());
        SplitHint hint = estimator.estimate();
        assertTrue(hint.hasSplitKey());
        assertEquals(hint.getSplitKey(), ByteString.copyFrom(new byte[] {(byte) 39}));
    }

    @Test
    public void latencyLargerThanWindowSize() {
        LatencyBasedSplitKeyEstimator
            estimator = new LatencyBasedSplitKeyEstimator(nanoSource, Duration.ofMillis(100), 1);

        long now = 0L;
        List<ILoadTracker.ILoadRecorder> recorderList = new ArrayList<>();
        for (int i = 0; i < 11; i++) {
            when(nanoSource.get()).thenReturn(now);
            ILoadTracker.ILoadRecorder recorder = estimator.start();
            recorder.record(ByteString.copyFromUtf8("Key" + i), 1);
            recorderList.add(recorder);
        }

        // advance 1 sec to start a new window
        when(nanoSource.get()).thenReturn(now + Duration.ofMillis(1001).toNanos());
        assertFalse(estimator.estimate().hasSplitKey());

        when(nanoSource.get()).thenReturn(now + Duration.ofMillis(1500).toNanos());
        recorderList.forEach(ILoadTracker.ILoadRecorder::stop);

        // advance 2 sec to start a new window
        when(nanoSource.get()).thenReturn(now + Duration.ofMillis(2001).toNanos());
        assertTrue(estimator.estimate().hasSplitKey());
    }
}
