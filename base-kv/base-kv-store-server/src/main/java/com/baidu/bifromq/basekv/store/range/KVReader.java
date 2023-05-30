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

import static com.baidu.bifromq.basekv.utils.KeyRangeUtil.contains;
import static com.baidu.bifromq.basekv.utils.KeyRangeUtil.inRange;

import com.baidu.bifromq.basekv.localengine.IKVEngine;
import com.baidu.bifromq.basekv.localengine.IKVEngineIterator;
import com.baidu.bifromq.basekv.proto.Range;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.google.protobuf.ByteString;
import java.util.Optional;
import java.util.function.Supplier;

class KVReader implements IKVReader {
    private final IKVRangeMetadataAccessor metadata;
    private final IKVEngine engine;
    private final Supplier<IKVEngineIterator[]> dataIterator;

    KVReader(IKVRangeMetadataAccessor metadata,
             IKVEngine engine,
             Supplier<IKVEngineIterator[]> dataIterator) {
        this.metadata = metadata;
        this.engine = engine;
        this.dataIterator = dataIterator;
    }

    @Override
    public Range range() {
        return metadata.range();
    }

    @Override
    public long size(Range range) {
        assert contains(range, range());
        Range bound = KVRangeKeys.dataBound(range);
        return engine.size(IKVEngine.DEFAULT_NS, bound.getStartKey(), bound.getEndKey());
    }

    @Override
    public boolean exist(ByteString key) {
        assert inRange(key, range());
        ByteString dataKey = KVRangeKeys.dataKey(key);
        IKVEngineIterator itr = dataIterator.get()[0];
        itr.seek(dataKey);
        return itr.isValid() && itr.key().equals(dataKey);
    }

    @Override
    public Optional<ByteString> get(ByteString key) {
        assert inRange(key, range());
        ByteString dataKey = KVRangeKeys.dataKey(key);
        IKVEngineIterator itr = dataIterator.get()[0];
        itr.seek(dataKey);
        if (!itr.isValid() || !itr.key().equals(dataKey)) {
            return Optional.empty();
        } else {
            return Optional.of(itr.value());
        }
    }

    @Override
    public IKVIterator iterator() {
        return new KVRangeIterator(() -> dataIterator.get()[1]);
    }
}
