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

import com.baidu.bifromq.basekv.proto.LoadHint;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.function.Supplier;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LoadEstimatorTest {
    @Mock
    private Supplier<Long> nanoSource;

    private AutoCloseable closeable;
    @BeforeMethod
    public void openMocks() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterMethod
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    public void noLoad() {
        LoadEstimator estimator = new LoadEstimator(100, 0.5, 5);
        LoadHint hint = estimator.estimate();
        assertEquals(hint.getLoad(), 0.0d);
    }

    @Test
    public void hintMemoization() {
        LoadEstimator estimator = new LoadEstimator(nanoSource, 100, 0.5, 3);
        long now = System.nanoTime();
        when(nanoSource.get()).thenReturn(now);
        LoadHint hint1 = estimator.estimate();
        LoadHint hint2 = estimator.estimate();
        assertSame(hint1, hint2);
        when(nanoSource.get()).thenReturn(now + Duration.ofSeconds(3).toNanos());
        hint2 = estimator.estimate();
        assertNotSame(hint1, hint2);
    }

    @Test
    public void trackClearExpiredSlots() {
        LoadEstimator estimator = new LoadEstimator(nanoSource, 100, 0.5, 1);
        long now = System.nanoTime();
        when(nanoSource.get()).thenReturn(now);
        estimator.track(ByteString.copyFromUtf8("Key"), 1);
        // advance 1 sec
        when(nanoSource.get()).thenReturn(now + Duration.ofSeconds(1).toNanos());
        LoadHint loadHint = estimator.estimate();
        assertTrue(loadHint.getLoad() > 0);
        when(nanoSource.get()).thenReturn(now + Duration.ofSeconds(2).toNanos());
        loadHint = estimator.estimate();
        assertEquals(loadHint.getLoad(), 0.0d);
    }

    @Test
    public void estimateSkipCurrentSecond() {
        LoadEstimator estimator = new LoadEstimator(nanoSource, 100, 0.5, 2);
        long now = System.nanoTime();
        when(nanoSource.get()).thenReturn(now);
        for (int i = 0; i < 80; i++) {
            estimator.track(ByteString.copyFrom(new byte[] {(byte) i}), 1);
        }
        LoadHint hint = estimator.estimate();
        assertEquals(hint.getLoad(), 0.0d);
        when(nanoSource.get()).thenReturn(now + Duration.ofSeconds(2).toNanos());
        hint = estimator.estimate();
        assertTrue(hint.getLoad() > 0.0d);
    }

    @Test
    public void estimateAvgLoad() {
        int trackSeconds = 2;
        long trackWindow = Duration.ofSeconds(2).toNanos();
        LoadEstimator estimator = new LoadEstimator(nanoSource, 100, 0.5, trackSeconds);
        long now = System.nanoTime();
        when(nanoSource.get()).thenReturn(now);
        for (int i = 0; i < 80; i++) {
            estimator.track(ByteString.copyFrom(new byte[] {(byte) i}), 1);
        }
        long nextSecond = now + Duration.ofSeconds(1).toNanos();
        when(nanoSource.get()).thenReturn(nextSecond);
        for (int i = 80; i < 160; i++) {
            estimator.track(ByteString.copyFrom(new byte[] {(byte) i}), 1);
        }
        when(nanoSource.get()).thenReturn(now + Duration.ofSeconds(2).toNanos());
        LoadHint hint = estimator.estimate();
        if (now / trackWindow == nextSecond / trackWindow) {
            assertEquals(hint.getLoad(), 0.8d);
            assertEquals(hint.getSplitKey(), ByteString.copyFrom(new byte[] {(byte) 79}));
        } else {
            assertEquals(hint.getLoad(), 0.4d);
            assertFalse(hint.hasSplitKey());
        }
    }

    @Test
    public void estimateSplitKey() {
        LoadEstimator estimator = new LoadEstimator(nanoSource, 100, 0.5, 1);
        long now = System.nanoTime();
        when(nanoSource.get()).thenReturn(now);
        for (int i = 0; i < 80; i++) {
            estimator.track(ByteString.copyFrom(new byte[] {(byte) i}), 1);
        }
        when(nanoSource.get()).thenReturn(now + Duration.ofSeconds(1).toNanos());
        LoadHint hint = estimator.estimate();
        assertEquals(hint.getLoad(), 0.8d);
        assertEquals(hint.getSplitKey(), ByteString.copyFrom(new byte[] {(byte) 39}));
    }
}
