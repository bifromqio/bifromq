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

import com.baidu.bifromq.basekv.localengine.IKVEngine;
import com.baidu.bifromq.basekv.proto.Range;
import com.baidu.bifromq.basekv.store.api.IKVWriter;
import com.baidu.bifromq.basekv.utils.KeyRangeUtil;
import com.google.protobuf.ByteString;

public class KVWriter implements IKVWriter {
    private final int batchId;
    private final IKVEngine kvEngine;
    private final IKVRangeMetadataAccessor metadata;
    private final int keyRangeId;

    public KVWriter(int batchId, IKVRangeMetadataAccessor metadata, IKVEngine kvEngine) {
        this.batchId = batchId;
        this.kvEngine = kvEngine;
        this.keyRangeId = metadata.dataBoundId();
        this.metadata = metadata;
    }


    @Override
    public void delete(ByteString key) {
        assert KeyRangeUtil.inRange(key, metadata.range());
        kvEngine.delete(batchId, keyRangeId, KVRangeKeys.dataKey(key));
    }

    @Override
    public void deleteRange(Range range) {
        assert KeyRangeUtil.contains(range, metadata.range());
        Range bound = KVRangeKeys.dataBound(range);
        kvEngine.clearSubRange(batchId, keyRangeId, bound.getStartKey(), bound.getEndKey());
    }

    @Override
    public void insert(ByteString key, ByteString value) {
        assert KeyRangeUtil.inRange(key, metadata.range());
        kvEngine.insert(batchId, keyRangeId, KVRangeKeys.dataKey(key), value);
    }

    @Override
    public void put(ByteString key, ByteString value) {
        assert KeyRangeUtil.inRange(key, metadata.range());
        kvEngine.put(batchId, keyRangeId, KVRangeKeys.dataKey(key), value);
    }
}
