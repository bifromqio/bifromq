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
import static com.baidu.bifromq.basekv.store.range.TrackableKVOperation.KEY_ITR_SEEK;

import com.baidu.bifromq.basekv.localengine.IKVSpaceIterator;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.google.protobuf.ByteString;

class KVRangeIterator implements IKVIterator {
    private final ILoadTracker loadTracker;
    private final IKVSpaceIterator rangeIterator;
    private ByteString currentKey;

    KVRangeIterator(ILoadTracker loadTracker, IKVSpaceIterator rangeIterator) {
        this.loadTracker = loadTracker;
        this.rangeIterator = rangeIterator;
    }

    @Override
    public ByteString key() {
        assert currentKey != null : "current key should not be null";
        return currentKey;
    }

    @Override
    public ByteString value() {
        assert currentKey != null : "current key should not be null";
        return rangeIterator.value();
    }

    @Override
    public boolean isValid() {
        return rangeIterator.isValid();
    }

    @Override
    public void next() {
        rangeIterator.next();
        loadCurrentKey();
    }

    @Override
    public void prev() {
        rangeIterator.prev();
        loadCurrentKey();
    }

    @Override
    public void seekToFirst() {
        rangeIterator.seekToFirst();
        loadCurrentKey();
    }

    @Override
    public void seekToLast() {
        rangeIterator.seekToLast();
        loadCurrentKey();
    }

    @Override
    public void seek(ByteString key) {
        rangeIterator.seek(key);
        loadTracker.track(key, KEY_ITR_SEEK);
        loadCurrentKey();
    }

    @Override
    public void seekForPrev(ByteString key) {
        rangeIterator.seekForPrev(key);
        loadTracker.track(key, KEY_ITR_SEEK);
        loadCurrentKey();
    }

    private void loadCurrentKey() {
        if (rangeIterator.isValid()) {
            currentKey = rangeIterator.key();
            loadTracker.track(key(), KEY_ITR_GET);
        } else {
            currentKey = null;
        }
    }
}
