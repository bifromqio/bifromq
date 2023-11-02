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

package com.baidu.bifromq.basekv.localengine;

import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface IKVSpace extends IKVSpaceReader {
    Observable<Map<ByteString, ByteString>> metadata();

    KVSpaceDescriptor describe();

    /**
     * Trigger a flush if underlying storage engine support
     *
     * @return the flush start nanos time
     */
    CompletableFuture<Long> flush();

    /**
     * Destroy the range, after destroy all data and associated resources will be cleared and released. The range object
     * will transit to destroyed state
     */
    void destroy();

    /**
     * Make a checkpoint of the current range state
     *
     * @return global unique id of the checkpoint
     */
    String checkpoint();

    /**
     * Open a readonly range object to access the checkpoint state. When the returned range object is garbage-collected
     * the associated checkpoint will be cleaned as well, except for the latest checkpoint. So the caller should keep a
     * strong reference to the checkpoint if it's still useful.
     *
     * @param checkpointId the checkpoint id
     * @return the range object for accessing the checkpoint
     */
    Optional<IKVSpaceCheckpoint> open(String checkpointId);

    /**
     * Get a writer to update range state
     *
     * @return the writer object
     */
    IKVSpaceWriter toWriter();
}
