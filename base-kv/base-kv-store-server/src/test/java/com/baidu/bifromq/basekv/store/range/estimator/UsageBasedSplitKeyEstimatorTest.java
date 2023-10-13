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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basekv.proto.SplitHint;
import com.google.protobuf.ByteString;
import java.util.function.Supplier;
import org.testng.annotations.Test;

public class UsageBasedSplitKeyEstimatorTest extends AbstractSplitKeyEstimatorTest {
    @Override
    protected ISplitKeyEstimator create(Supplier<Long> nanoSource, int windowSizeSec) {
        return new UsageBasedSplitKeyEstimator(nanoSource, windowSizeSec);
    }

    @Test
    public void loadEstimation() {
        when(nanoSource.get()).thenReturn(0L);
        UsageBasedSplitKeyEstimator estimator = new UsageBasedSplitKeyEstimator(nanoSource, 5);
        RecordingWindowSlot windowSlot = new RecordingWindowSlot();
        LoadRecorder recorder = new LoadRecorder(nanoSource,
            rec -> windowSlot.record(rec.keyDistribution(), rec.getKVIOs(), rec.getKVIONanos(),
                nanoSource.get() - rec.startNanos()));

        recorder.record(10);
        recorder.record(20);
        recorder.stop();
        when(nanoSource.get()).thenReturn(30L);
        SplitHint hint = estimator.doEstimate(windowSlot);
        assertTrue(hint.getLoad() > 0);
    }

    @Test
    public void estimateSplitKey() {
        when(nanoSource.get()).thenReturn(0L);
        UsageBasedSplitKeyEstimator estimator = new UsageBasedSplitKeyEstimator(nanoSource, 5, 20, 5, k -> k);
        RecordingWindowSlot windowSlot = new RecordingWindowSlot();
        LoadRecorder recorder = new LoadRecorder(nanoSource,
            rec -> windowSlot.record(rec.keyDistribution(), rec.getKVIOs(), rec.getKVIONanos(),
                nanoSource.get() - rec.startNanos()));

        recorder.record(ByteString.copyFromUtf8("Key1"), 10);
        recorder.record(ByteString.copyFromUtf8("Key2"), 10);
        recorder.record(ByteString.copyFromUtf8("Key3"), 10);
        recorder.record(ByteString.copyFromUtf8("Key4"), 10);
        recorder.record(ByteString.copyFromUtf8("Key5"), 10);
        recorder.record(ByteString.copyFromUtf8("Key6"), 10);
        when(nanoSource.get()).thenReturn(30L);
        recorder.stop();
        SplitHint hint = estimator.doEstimate(windowSlot);
        assertTrue(hint.getLoad() > 0);
        assertTrue(hint.hasSplitKey());
    }

    @Test
    public void skipEstimateSplitKeyWhenIONanosIsHigh() {
        when(nanoSource.get()).thenReturn(0L);
        UsageBasedSplitKeyEstimator estimator = new UsageBasedSplitKeyEstimator(nanoSource, 5, 20, 5, k -> k);
        RecordingWindowSlot windowSlot = new RecordingWindowSlot();
        LoadRecorder recorder = new LoadRecorder(nanoSource,
            rec -> windowSlot.record(rec.keyDistribution(), rec.getKVIOs(), rec.getKVIONanos(),
                nanoSource.get() - rec.startNanos()));

        recorder.record(ByteString.copyFromUtf8("Key1"), 20);
        recorder.record(ByteString.copyFromUtf8("Key2"), 20);
        recorder.record(ByteString.copyFromUtf8("Key3"), 20);
        recorder.record(ByteString.copyFromUtf8("Key4"), 30);
        recorder.record(ByteString.copyFromUtf8("Key5"), 30);
        recorder.record(ByteString.copyFromUtf8("Key6"), 30);
        recorder.stop();
        when(nanoSource.get()).thenReturn(30L);
        SplitHint hint = estimator.doEstimate(windowSlot);
        assertTrue(hint.getLoad() > 0);
        assertFalse(hint.hasSplitKey());
    }
}
