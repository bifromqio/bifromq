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

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basekv.MockableTest;
import com.baidu.bifromq.basekv.proto.SplitHint;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class SplitKeyEstimatorTest extends MockableTest {
    @Mock
    private Supplier<Long> nanoSource;

    @Test
    public void noLoad() {
        SplitKeyEstimator estimator = new SplitKeyEstimator(Duration.ofMillis(100), 5, true);
        SplitHint hint = estimator.estimate();
        assertFalse(hint.hasSplitKey());
    }

    @Test
    public void notEnoughRecordsInConcurrentMode() {
        SplitKeyEstimator estimator = new SplitKeyEstimator(nanoSource, Duration.ofMillis(100), 1, true);
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
    public void nonConcurrentMode() {
        SplitKeyEstimator estimator = new SplitKeyEstimator(nanoSource, Duration.ofMillis(100), 1, false);
        long now = 0L;
        when(nanoSource.get()).thenReturn(now);
        ILoadTracker.ILoadRecorder recorder = estimator.start();
        for (int i = 0; i < 80; i++) {
            recorder.record(ByteString.copyFrom(new byte[] {(byte) i}), 1);
        }
        when(nanoSource.get()).thenReturn(now + Duration.ofMillis(200).toNanos());
        recorder.stop();

        when(nanoSource.get()).thenReturn(now + Duration.ofMillis(1100).toNanos());
        SplitHint hint = estimator.estimate();
        assertTrue(hint.getLoad() > 0.0);
        assertTrue(hint.hasSplitKey());
    }

    @Test
    public void hintMemoization() {
        SplitKeyEstimator estimator = new SplitKeyEstimator(nanoSource, Duration.ofMillis(100), 1, true);
        long now = 0L;
        // track enough records
        for (int i = 0; i < 11; i++) {
            when(nanoSource.get()).thenReturn(now);
            ILoadTracker.ILoadRecorder recorder = estimator.start();
            recorder.record(ByteString.copyFromUtf8("Key"), Duration.ofMillis(20).toNanos());
            when(nanoSource.get()).thenReturn(now + Duration.ofMillis(200).toNanos());
            recorder.stop();
        }
        when(nanoSource.get()).thenReturn(now + Duration.ofSeconds(1).toNanos());
        SplitHint hint1 = estimator.estimate();
        SplitHint hint2 = estimator.estimate();
        assertTrue(hint1.hasSplitKey());
        assertSame(hint1, hint2);
        when(nanoSource.get()).thenReturn(now + Duration.ofSeconds(2).toNanos());
        hint2 = estimator.estimate();
        assertNotSame(hint1, hint2);
    }

    @Test
    public void trackClearExpiredSlots() {
        SplitKeyEstimator estimator = new SplitKeyEstimator(nanoSource, Duration.ofMillis(100), 1, true);
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
        SplitKeyEstimator estimator = new SplitKeyEstimator(nanoSource, Duration.ofMillis(100), 1, true);
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
        SplitKeyEstimator estimator = new SplitKeyEstimator(nanoSource, Duration.ofMillis(100), 1, true);
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
        SplitKeyEstimator estimator = new SplitKeyEstimator(nanoSource, Duration.ofMillis(100), 1, true);

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
