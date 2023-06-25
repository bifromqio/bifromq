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

import static com.baidu.bifromq.basekv.Constants.EMPTY_RANGE;
import static com.baidu.bifromq.basekv.Constants.FULL_RANGE;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.lastAppliedIndexKey;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.rangeKey;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.stateKey;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.verKey;
import static com.baidu.bifromq.basekv.store.util.KVUtil.toByteString;
import static com.baidu.bifromq.basekv.store.util.KVUtil.toByteStringNativeOrder;
import static org.testng.AssertJUnit.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basekv.localengine.IKVEngine;
import com.baidu.bifromq.basekv.localengine.IKVEngineIterator;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.Range;
import com.baidu.bifromq.basekv.proto.State;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.protobuf.ByteString;
import java.util.Optional;

import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.mockito.Mock;

public class KVRangeReaderTest {
    @Mock
    private IKVEngine engine;
    @Mock
    private IKVEngineIterator engineIterator;
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
    public void init() {
        KVRangeId id = KVRangeIdUtil.generate();
        when(engine.get(anyString(), any(ByteString.class))).thenReturn(Optional.empty());
        when(engine.registerKeyRange(anyString(), any(ByteString.class), any(ByteString.class)))
            .thenReturn(1);
        KVRangeReader rangeReader = new KVRangeReader(id, engine, accessor.refresher());
        assertEquals(-1L, rangeReader.ver());
        assertEquals(-1L, rangeReader.lastAppliedIndex());
        assertEquals(State.StateType.Normal, rangeReader.state().getType());
        assertEquals(EMPTY_RANGE, rangeReader.kvReader().range());
    }

    @Test
    public void refresh() {
        KVRangeId id = KVRangeIdUtil.generate();
        when(engine.newIterator(1)).thenReturn(engineIterator);
        when(engine.get(anyString(), any(ByteString.class))).thenReturn(Optional.empty());
        when(engine.registerKeyRange(anyString(), any(ByteString.class), any(ByteString.class)))
            .thenReturn(1);

        KVRangeReader rangeReader = new KVRangeReader(id, engine, accessor.refresher());

        int keyRangeId = 2;
        long ver = 10;
        Range range = FULL_RANGE;
        Range dataBound = KVRangeKeys.dataBound(range);
        long lastAppliedIndex = 10;
        State state = State.newBuilder().setType(State.StateType.Normal).build();

        when(engine.registerKeyRange(IKVEngine.DEFAULT_NS, dataBound.getStartKey(), dataBound.getEndKey()))
            .thenReturn(keyRangeId);
        when(engine.get(IKVEngine.DEFAULT_NS, verKey(id))).thenReturn(Optional.of(toByteStringNativeOrder(ver)));
        when(engine.get(IKVEngine.DEFAULT_NS, rangeKey(id))).thenReturn(Optional.of(range.toByteString()));
        when(engine.get(IKVEngine.DEFAULT_NS, lastAppliedIndexKey(id)))
            .thenReturn(Optional.of(toByteString(lastAppliedIndex)));
        when(engine.get(IKVEngine.DEFAULT_NS, stateKey(id))).thenReturn(Optional.of(state.toByteString()));

        accessor.mutator().run(() -> {
        });

        rangeReader.refresh();

        assertEquals(10, rangeReader.ver());
        assertEquals(10, rangeReader.lastAppliedIndex());
        assertEquals(State.StateType.Normal, rangeReader.state().getType());
        assertEquals(FULL_RANGE, rangeReader.kvReader().range());
        verify(engine, times(2)).newIterator(2);
        verify(engineIterator, times(2)).close();
    }
}
