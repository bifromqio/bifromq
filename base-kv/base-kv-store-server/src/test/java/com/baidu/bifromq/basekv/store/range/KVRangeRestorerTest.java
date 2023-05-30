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

import static com.baidu.bifromq.basekv.Constants.FULL_RANGE;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.dataBound;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.dataKey;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.lastAppliedIndexKey;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.rangeKey;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.stateKey;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.verKey;
import static com.baidu.bifromq.basekv.store.util.KVUtil.toByteString;
import static com.baidu.bifromq.basekv.store.util.KVUtil.toByteStringNativeOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basekv.localengine.IKVEngine;
import com.baidu.bifromq.basekv.proto.KVPair;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeSnapshot;
import com.baidu.bifromq.basekv.proto.State;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KVRangeRestorerTest {
    @Mock
    IKVRangeMetadataAccessor metadata;
    @Mock
    IKVEngine kvEngine;

    KVRangeStateAccessor accessor = new KVRangeStateAccessor();

    @Test
    public void restore() {
        KVRangeId rangeId = KVRangeIdUtil.generate();
        int batchId = 1;
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(rangeId)
            .setVer(0)
            .setLastAppliedIndex(10)
            .setRange(FULL_RANGE)
            .setState(State.newBuilder().setType(State.StateType.Normal).build())
            .setCheckpointId("CheckPoint")
            .build();
        ByteString key = ByteString.copyFromUtf8("Key");
        ByteString val = ByteString.copyFromUtf8("Val");

        when(kvEngine.startBatch()).thenReturn(batchId);
        when(metadata.dataBound()).thenReturn(dataBound(FULL_RANGE));

        KVRangeRestorer restorer = new KVRangeRestorer(snapshot, metadata, kvEngine, accessor.mutator());
        restorer.add(KVPair.newBuilder().setKey(key).setValue(val).build());
        restorer.close();

        verify(metadata, times(2)).refresh();
        verify(kvEngine).put(batchId, IKVEngine.DEFAULT_NS, verKey(rangeId),
            toByteStringNativeOrder(snapshot.getVer()));
        verify(kvEngine).put(batchId, IKVEngine.DEFAULT_NS, lastAppliedIndexKey(rangeId),
            toByteString(snapshot.getLastAppliedIndex()));
        verify(kvEngine).put(batchId, IKVEngine.DEFAULT_NS, rangeKey(rangeId), snapshot.getRange().toByteString());
        verify(kvEngine).put(batchId, IKVEngine.DEFAULT_NS, stateKey(rangeId), snapshot.getState().toByteString());
        verify(kvEngine).clearSubRange(batchId, metadata.dataBoundId(),
            metadata.dataBound().getStartKey(),
            metadata.dataBound().getEndKey());
        kvEngine.put(batchId, IKVEngine.DEFAULT_NS, dataKey(key), val);

        verify(kvEngine).endBatch(batchId);
    }
}
