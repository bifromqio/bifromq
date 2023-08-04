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

import com.baidu.bifromq.basekv.proto.KVRangeSnapshot;
import com.baidu.bifromq.basekv.proto.Range;
import com.baidu.bifromq.basekv.proto.State;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import io.reactivex.rxjava3.core.Observable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

/**
 * The KVRange State Machine
 */
public interface IKVRangeState {
    @AllArgsConstructor
    @EqualsAndHashCode
    class KVRangeMeta {
        final long ver;
        final State state;
        final Range range;
    }

    /**
     * Make a checkpoint of current state and return a descriptor
     *
     * @return
     */
    KVRangeSnapshot checkpoint();

    /**
     * Check if the given checkpoint exists
     *
     * @param checkpoint
     * @return
     */
    boolean hasCheckpoint(KVRangeSnapshot checkpoint);

    /**
     * Open an iterator for accessing the checkpoint data
     *
     * @param checkpoint
     * @return
     */
    IKVIterator open(KVRangeSnapshot checkpoint);

    /**
     * Borrow a reader for query the latest data. The borrowed the reader must be released once finished use
     *
     * @return
     */
    IKVRangeReader borrow();

    /**
     * Release a borrowed reader, so it will be available for other borrower
     *
     * @param reader
     */
    void returnBorrowed(IKVRangeReader reader);

    IKVRangeReader getReader(boolean trackingLoad);

    IKVRangeWriter getWriter(boolean trackingLoad);

    Observable<KVRangeMeta> metadata();

    /**
     * Get a state restorer
     *
     * @param checkpoint
     * @return
     */
    IKVRangeRestorer reset(KVRangeSnapshot checkpoint);

    void destroy(boolean includeData);
}
