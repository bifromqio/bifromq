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

import static com.baidu.bifromq.basekv.store.range.TrackableKVOperation.KEY_DEL;
import static com.baidu.bifromq.basekv.store.range.TrackableKVOperation.KEY_INSERT;
import static com.baidu.bifromq.basekv.store.range.TrackableKVOperation.KEY_PUT;

import com.baidu.bifromq.basekv.localengine.IKVSpaceWriter;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.store.api.IKVWriter;
import com.google.protobuf.ByteString;

public class KVWriter implements IKVWriter {
    private final IKVSpaceWriter writer;
    private final ILoadTracker loadTracker;

    public KVWriter(IKVSpaceWriter writer, ILoadTracker loadTracker) {
        this.writer = writer;
        this.loadTracker = loadTracker;
    }


    @Override
    public void delete(ByteString key) {
        writer.delete(key);
        loadTracker.track(key, KEY_DEL);
    }

    @Override
    public void clear(Boundary boundary) {
        writer.clear(boundary);
        if (boundary.hasStartKey()) {
            loadTracker.track(boundary.getStartKey(), KEY_DEL);
        }
        if (boundary.hasEndKey()) {
            loadTracker.track(boundary.getEndKey(), KEY_DEL);
        }
    }

    @Override
    public void insert(ByteString key, ByteString value) {
        writer.insert(key, value);
        loadTracker.track(key, KEY_INSERT);
    }

    @Override
    public void put(ByteString key, ByteString value) {
        writer.put(key, value);
        loadTracker.track(key, KEY_PUT);
    }
}
