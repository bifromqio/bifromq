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
import static com.baidu.bifromq.basekv.store.range.TrackableKVOperation.KEY_ITR_GET;
import static com.baidu.bifromq.basekv.store.range.TrackableKVOperation.KEY_ITR_SEEK;

import com.baidu.bifromq.basekv.localengine.IKVEngineIterator;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.google.protobuf.ByteString;
import java.util.function.Supplier;

class KVRangeIterator implements IKVIterator {
    private final ILoadTracker loadTracker;
    private final Supplier<IKVEngineIterator> engineIterator;
    private final Runnable onClose;
    private ByteString currentKey;

    KVRangeIterator(ILoadTracker loadTracker, Supplier<IKVEngineIterator> engineIterator) {
        this(loadTracker, engineIterator, () -> {
        });
    }

    KVRangeIterator(ILoadTracker loadTracker, Supplier<IKVEngineIterator> engineIterator, Runnable onClose) {
        this.loadTracker = loadTracker;
        this.engineIterator = engineIterator;
        this.onClose = onClose;
    }

    @Override
    public ByteString key() {
        assert currentKey != null : "current key should not be null";
        return currentKey;
    }

    @Override
    public ByteString value() {
        assert currentKey != null : "current key should not be null";
        return engineIterator.get().value();
    }

    @Override
    public boolean isValid() {
        return engineIterator.get().isValid();
    }

    @Override
    public void next() {
        engineIterator.get().next();
        loadCurrentKey();
    }

    @Override
    public void prev() {
        engineIterator.get().prev();
        loadCurrentKey();
    }

    @Override
    public void seekToFirst() {
        engineIterator.get().seekToFirst();
        loadCurrentKey();
    }

    @Override
    public void seekToLast() {
        engineIterator.get().seekToLast();
        loadCurrentKey();
    }

    @Override
    public void seek(ByteString key) {
        engineIterator.get().seek(dataKey(key));
        loadTracker.track(key, KEY_ITR_SEEK);
        loadCurrentKey();
    }

    @Override
    public void seekForPrev(ByteString key) {
        engineIterator.get().seekForPrev(dataKey(key));
        loadTracker.track(key, KEY_ITR_SEEK);
        loadCurrentKey();
    }

    @Override
    public void close() throws Exception {
        this.onClose.run();
    }

    private void loadCurrentKey() {
        if (engineIterator.get().isValid()) {
            currentKey = userKey(engineIterator.get().key());
            loadTracker.track(key(), KEY_ITR_GET);
        } else {
            currentKey = null;
        }
    }
}
