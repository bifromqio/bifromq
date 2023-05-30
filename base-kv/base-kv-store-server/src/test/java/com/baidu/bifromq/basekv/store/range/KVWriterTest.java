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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basekv.localengine.IKVEngine;
import com.baidu.bifromq.basekv.proto.Range;
import com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KVWriterTest {
    @Mock
    private IKVEngine engine;
    @Mock
    private IKVRangeMetadataAccessor metadata;

    @Test
    public void write() {
        int batchId = 1;
        int dataBoundId = 2;
        when(metadata.dataBoundId()).thenReturn(dataBoundId);
        when(metadata.range()).thenReturn(FULL_RANGE);
        KVWriter writer = new KVWriter(batchId, metadata, engine);

        // delete
        ByteString delKey = ByteString.copyFromUtf8("delKey");
        writer.delete(delKey);
        verify(engine).delete(batchId, dataBoundId, KVRangeKeys.dataKey(delKey));

        // insert
        ByteString insKey = ByteString.copyFromUtf8("insertKey");
        ByteString insValue = ByteString.copyFromUtf8("insertValue");
        writer.insert(insKey, insValue);
        verify(engine).insert(batchId, dataBoundId, KVRangeKeys.dataKey(insKey), insValue);

        // put
        ByteString putKey = ByteString.copyFromUtf8("putKey");
        ByteString putValue = ByteString.copyFromUtf8("putValue");
        writer.put(putKey, putValue);
        verify(engine).put(batchId, dataBoundId, KVRangeKeys.dataKey(putKey), putValue);

        // delete range
        Range delRange = Range.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setStartKey(ByteString.copyFromUtf8("z"))
            .build();
        Range bound = KVRangeKeys.dataBound(delRange);
        writer.deleteRange(delRange);
        verify(engine).clearSubRange(batchId, dataBoundId, bound.getStartKey(), bound.getEndKey());
    }
}
