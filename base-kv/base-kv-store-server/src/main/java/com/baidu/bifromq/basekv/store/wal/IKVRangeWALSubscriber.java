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

package com.baidu.bifromq.basekv.store.wal;

import com.baidu.bifromq.basekv.proto.KVRangeSnapshot;
import com.baidu.bifromq.basekv.raft.proto.LogEntry;
import java.util.concurrent.CompletableFuture;

public interface IKVRangeWALSubscriber {
    default void onSubscribe(IKVRangeWALSubscription subscription) {

    }

    CompletableFuture<Void> apply(LogEntry log);

    /**
     * Install snapshot to kv range asynchronously and the returned snapshot will be used for WAL compaction
     *
     * @param requested the snapshot requested to be installed
     * @param leader    the leader
     * @return future for the installation
     */
    CompletableFuture<KVRangeSnapshot> install(KVRangeSnapshot requested, String leader);
}
