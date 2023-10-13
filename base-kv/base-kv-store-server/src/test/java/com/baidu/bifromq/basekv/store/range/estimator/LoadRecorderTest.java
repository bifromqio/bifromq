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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basekv.MockableTest;
import com.google.protobuf.ByteString;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class LoadRecorderTest extends MockableTest {
    @Mock
    private Supplier<Long> nanoSource;

    @Mock
    private Consumer<LoadRecorder> onStop;

    @Test
    public void initState() {
        when(nanoSource.get()).thenReturn(10L);
        LoadRecorder recorder = new LoadRecorder(nanoSource, onStop);
        assertEquals(recorder.getKVIONanos(), 0);
        assertEquals(recorder.getKVIOs(), 0);
        assertEquals(recorder.startNanos(), 10L);
        assertTrue(recorder.keyDistribution().isEmpty());
        recorder.stop();
        ArgumentCaptor<LoadRecorder> captor = ArgumentCaptor.forClass(LoadRecorder.class);
        verify(onStop).accept(captor.capture());
        assertEquals(captor.getValue(), recorder);
    }

    @Test
    public void record() {
        when(nanoSource.get()).thenReturn(10L);
        LoadRecorder recorder = new LoadRecorder(nanoSource, onStop);
        recorder.record(1);
        recorder.record(ByteString.copyFromUtf8("key1"), 1L);
        recorder.record(ByteString.copyFromUtf8("key1"), 2L);
        recorder.record(ByteString.copyFromUtf8("key2"), 1L);

        assertEquals(recorder.getKVIONanos(), 5);
        assertEquals(recorder.getKVIOs(), 4);
        assertEquals(recorder.startNanos(), 10L);
        assertEquals(recorder.keyDistribution().size(), 2);
        recorder.stop();
    }
}
