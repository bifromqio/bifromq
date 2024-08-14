/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.basekv.store;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ROCoProcOutput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public interface IKVRangeStore {
    String clusterId();

    String id();

    boolean isStarted();

    void start(IStoreMessenger messenger);

    void stop();

    /**
     * Bootstrap a KVRange on current store.
     *
     * @return future true if success or false if the range is already hosted by current store
     */
    CompletableFuture<Boolean> bootstrap(KVRangeId rangeId, Boundary boundary);

    boolean isHosting(KVRangeId rangeId);

    /**
     * Recover unwritable ranges hosted in current store
     *
     * @return
     */
    CompletionStage<Void> recover();

    Observable<KVRangeStoreDescriptor> describe();

    /**
     * Transfer the leadership of specified KVRange to other KVRangeStore
     *
     * @param id the id of the KVRange
     * @return
     */
    CompletionStage<Void> transferLeadership(long ver, KVRangeId id, String newLeader);

    /**
     * Change a KVRange's config, this method must be invoked on the leader of specified KVRange, and there is no
     * un-stabilized range settings
     *
     * @param id
     * @return
     */
    CompletionStage<Void> changeReplicaConfig(long ver, KVRangeId id, Set<String> newVoters, Set<String> newLearners);

    CompletionStage<Void> split(long ver, KVRangeId id, ByteString splitKey);

    CompletionStage<Void> merge(long ver, KVRangeId mergerId, KVRangeId mergeeId);

    CompletionStage<Boolean> exist(long ver, KVRangeId id, ByteString key, boolean linearized);

    CompletionStage<Optional<ByteString>> get(long ver, KVRangeId id, ByteString key, boolean linearized);

    CompletionStage<ROCoProcOutput> queryCoProc(long ver, KVRangeId id, ROCoProcInput query, boolean linearized);

    CompletionStage<ByteString> put(long ver, KVRangeId id, ByteString key, ByteString value);

    CompletionStage<ByteString> delete(long ver, KVRangeId id, ByteString key);

    CompletionStage<RWCoProcOutput> mutateCoProc(long ver, KVRangeId id, RWCoProcInput mutate);
}
