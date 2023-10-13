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

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;

import com.baidu.bifromq.basekv.MockableTest;
import com.baidu.bifromq.basekv.proto.SplitHint;
import com.baidu.bifromq.basekv.store.range.ILoadTracker;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Metrics;
import java.time.Duration;
import java.util.function.Supplier;
import org.mockito.Mock;
import org.testng.annotations.Test;

public abstract class AbstractSplitKeyEstimatorTest extends MockableTest {
    @Mock
    protected Supplier<Long> nanoSource;


    protected abstract ISplitKeyEstimator create(Supplier<Long> nanoSource, int windowSizeSec);

    @Test
    public void estimateSplitKey() {
        AbstractSplitKeyEstimator testEstimator = new AbstractSplitKeyEstimator(System::nanoTime, 1, k -> k) {
            @Override
            protected SplitHint doEstimate(RecordingWindowSlot windowSlot) {
                return SplitHint.getDefaultInstance();
            }
        };
        assertFalse(Metrics.globalRegistry.find("basekv.load.est.iodensity").gauges().isEmpty());
        assertFalse(Metrics.globalRegistry.find("basekv.load.est.iolatency").gauges().isEmpty());
        assertFalse(Metrics.globalRegistry.find("basekv.load.est.avglatency").gauges().isEmpty());
        testEstimator = null;
        await().until(() -> {
            System.gc();
            return Metrics.globalRegistry.find("basekv.load.est.iodensity").gauges().isEmpty() &&
                Metrics.globalRegistry.find("basekv.load.est.iolatency").gauges().isEmpty() &&
                Metrics.globalRegistry.find("basekv.load.est.avglatency").gauges().isEmpty();
        });
    }

    @Test
    public void noLoad() {
        ISplitKeyEstimator estimator = create(System::nanoTime, 5);
        SplitHint hint = estimator.estimate();
        assertFalse(hint.hasSplitKey());
    }

    @Test
    public void hintMemoization() {
        ISplitKeyEstimator estimator = create(nanoSource, 5);
        long now = 0L;
        // track enough records
        for (int i = 0; i < 11; i++) {
            when(nanoSource.get()).thenReturn(now);
            ILoadTracker.ILoadRecorder recorder = estimator.start();
            recorder.record(ByteString.copyFromUtf8("Key"), 10_000);
            when(nanoSource.get()).thenReturn(now + 10_000);
            recorder.stop();
        }
        when(nanoSource.get()).thenReturn(now + Duration.ofSeconds(6).toNanos());
        SplitHint hint1 = estimator.estimate();
        SplitHint hint2 = estimator.estimate();
        assertFalse(hint1.hasSplitKey());
        assertSame(hint1, hint2);
        when(nanoSource.get()).thenReturn(now + Duration.ofSeconds(11).toNanos());
        hint2 = estimator.estimate();
        assertNotSame(hint1, hint2);
    }
}
