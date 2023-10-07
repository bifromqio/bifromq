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

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeSnapshot;
import com.baidu.bifromq.basekv.proto.State;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import io.reactivex.rxjava3.core.Observable;

public interface IKVRange extends IKVRangeReader {
    record KVRangeMeta(long ver, State state, Boundary boundary) {
    }


    /**
     * Get the observable of metadata
     *
     * @return the observable
     */
    Observable<KVRangeMeta> metadata();

    /**
     * Make a checkpoint of current state and return a descriptor
     *
     * @return the descriptor of the checkpoint
     */
    KVRangeSnapshot checkpoint();

    /**
     * Check if the given checkpoint exists
     *
     * @param checkpoint the descriptor
     * @return bool
     */
    boolean hasCheckpoint(KVRangeSnapshot checkpoint);

    /**
     * Open an iterator for accessing the checkpoint data
     *
     * @param checkpoint the descriptor
     * @return the checkpoint reader
     */
    IKVRangeReader open(KVRangeSnapshot checkpoint);

    IKVReader borrowDataReader();

    void returnDataReader(IKVReader borrowed);

    /**
     * Get a writer for updating the range
     *
     * @return the range writer
     */
    IKVRangeWriter<?> toWriter();

    IKVReseter toReseter(KVRangeSnapshot snapshot);

    void destroy();
}
