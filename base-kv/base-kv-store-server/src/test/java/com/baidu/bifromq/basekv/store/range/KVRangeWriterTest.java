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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basekv.localengine.IKVEngine;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.mockito.Mock;

public class KVRangeWriterTest {
    @Mock
    private IKVEngine engine;
    @Mock
    private IKVRangeMetadataAccessor metadataAccessor;
    private KVRangeStateAccessor accessor = new KVRangeStateAccessor();
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
    public void update() {
        KVRangeId rangeId = KVRangeIdUtil.generate();
        when(metadataAccessor.dataBoundId()).thenReturn(1);
        when(metadataAccessor.version()).thenReturn(0L);
        when(engine.startBatch()).thenReturn(1);
        KVRangeWriter rangeWriter = new KVRangeWriter(rangeId, metadataAccessor, engine, accessor.mutator());

        rangeWriter.resetVer(2);
        rangeWriter.close();
        verify(engine).endBatch(1);
        verify(metadataAccessor, times(2)).refresh();
    }
}
