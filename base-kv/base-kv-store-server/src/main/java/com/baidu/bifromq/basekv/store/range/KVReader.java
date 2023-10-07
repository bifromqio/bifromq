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

import static com.baidu.bifromq.basekv.store.range.TrackableKVOperation.KEY_ITR_GET;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.inRange;

import com.baidu.bifromq.basekv.localengine.IKVSpaceIterator;
import com.baidu.bifromq.basekv.localengine.IKVSpaceReader;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.google.protobuf.ByteString;
import java.util.Optional;

public class KVReader implements IKVReader {
    private final IKVSpaceReader keyRange;
    private final IKVSpaceIterator keyRangeIterator;
    private final IKVRangeReader kvRangeReader;
    private final ILoadTracker loadTracker;

    KVReader(IKVSpaceReader keyRange, IKVRangeReader reader, ILoadTracker loadTracker) {
        this.keyRange = keyRange;
        this.keyRangeIterator = keyRange.newIterator();
        this.kvRangeReader = reader;
        this.loadTracker = loadTracker;
    }

    @Override
    public Boundary boundary() {
        return kvRangeReader.boundary();
    }

    @Override
    public long size(Boundary range) {
        assert inRange(range, boundary());
        return kvRangeReader.size(range);
    }

    @Override
    public boolean exist(ByteString key) {
        assert inRange(key, boundary());
        boolean exists = keyRange.exist(key);
        loadTracker.track(key, KEY_ITR_GET);
        return exists;
    }

    @Override
    public Optional<ByteString> get(ByteString key) {
        assert inRange(key, boundary());
        Optional<ByteString> val = keyRange.get(key);
        loadTracker.track(key, KEY_ITR_GET);
        return val;
    }

    @Override
    public IKVIterator iterator() {
        return new KVRangeIterator(loadTracker, keyRangeIterator);
    }

    @Override
    public void refresh() {
        keyRangeIterator.refresh();
    }
}
