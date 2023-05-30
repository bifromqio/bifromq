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

import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.dataKey;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.userKey;

import com.baidu.bifromq.basekv.localengine.IKVEngineIterator;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.google.protobuf.ByteString;
import java.util.function.Supplier;

class KVRangeIterator implements IKVIterator {
    private final Supplier<IKVEngineIterator> engineIterator;

    KVRangeIterator(Supplier<IKVEngineIterator> engineIterator) {
        this.engineIterator = engineIterator;
    }

    @Override
    public ByteString key() {
        return userKey(engineIterator.get().key());
    }

    @Override
    public ByteString value() {
        return engineIterator.get().value();
    }

    @Override
    public boolean isValid() {
        return engineIterator.get().isValid();
    }

    @Override
    public void next() {
        engineIterator.get().next();
    }

    @Override
    public void prev() {
        engineIterator.get().prev();
    }

    @Override
    public void seekToFirst() {
        engineIterator.get().seekToFirst();
    }

    @Override
    public void seekToLast() {
        engineIterator.get().seekToLast();
    }

    @Override
    public void seek(ByteString key) {
        engineIterator.get().seek(dataKey(key));
    }

    @Override
    public void seekForPrev(ByteString key) {
        engineIterator.get().seekForPrev(dataKey(key));
    }
}
