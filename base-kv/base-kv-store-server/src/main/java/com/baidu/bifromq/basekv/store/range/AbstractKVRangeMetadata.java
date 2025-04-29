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

package com.baidu.bifromq.basekv.store.range;

import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.METADATA_LAST_APPLIED_INDEX_BYTES;

import com.baidu.bifromq.basekv.localengine.IKVSpaceMetadata;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.State;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.store.api.IKVRangeMetadata;
import com.baidu.bifromq.basekv.store.util.KVUtil;
import com.google.protobuf.ByteString;
import lombok.SneakyThrows;

abstract class AbstractKVRangeMetadata implements IKVRangeMetadata {
    protected final KVRangeId id;
    private final IKVSpaceMetadata keyRangeMetadata;

    AbstractKVRangeMetadata(KVRangeId id, IKVSpaceMetadata keyRangeMetadata) {
        this.id = id;
        this.keyRangeMetadata = keyRangeMetadata;
    }

    @Override
    public final KVRangeId id() {
        return id;
    }

    protected long version(ByteString versionBytes) {
        if (versionBytes != null) {
            return KVUtil.toLongNativeOrder(versionBytes);
        }
        return -1L;
    }

    @SneakyThrows
    protected State state(ByteString stateBytes) {
        if (stateBytes != null) {
            return State.parseFrom(stateBytes);
        }
        return State.newBuilder().setType(State.StateType.NoUse).build();
    }

    @Override
    public final long lastAppliedIndex() {
        return keyRangeMetadata.metadata(METADATA_LAST_APPLIED_INDEX_BYTES).map(KVUtil::toLong).orElse(-1L);
    }

    @SneakyThrows
    protected ClusterConfig clusterConfig(ByteString clusterConfigBytes) {
        if (clusterConfigBytes != null) {
            return ClusterConfig.parseFrom(clusterConfigBytes);
        }
        return ClusterConfig.getDefaultInstance();
    }

    @SneakyThrows
    protected Boundary boundary(ByteString boundaryBytes) {
        if (boundaryBytes != null) {
            return Boundary.parseFrom(boundaryBytes);
        }
        return Boundary.getDefaultInstance();
    }

    @Override
    public final long size() {
        return keyRangeMetadata.size();
    }

    @Override
    public final long size(Boundary boundary) {
        return keyRangeMetadata.size(boundary);
    }
}
