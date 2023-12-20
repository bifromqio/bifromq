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
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.METADATA_RANGE_BOUND_BYTES;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.METADATA_STATE_BYTES;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.METADATA_VER_BYTES;
import static com.baidu.bifromq.basekv.store.util.VerUtil.bump;

import com.baidu.bifromq.basekv.localengine.IKVSpaceMetadata;
import com.baidu.bifromq.basekv.localengine.IKVSpaceMetadataUpdatable;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.State;
import com.baidu.bifromq.basekv.store.util.KVUtil;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;

abstract class AbstractKVRangeMetadataUpdatable<T extends AbstractKVRangeMetadataUpdatable<T>>
    extends AbstractKVRangeMetadata implements IKVRangeMetadataUpdatable<T> {
    protected final KVRangeId id;

    AbstractKVRangeMetadataUpdatable(IKVSpaceMetadata keyRangeMetadata) {
        super(keyRangeMetadata);
        this.id = KVRangeIdUtil.fromString(keyRangeMetadata.id());
    }

    @Override
    public final T bumpVer(boolean toOdd) {
        resetVer(bump(version(), toOdd));
        return thisT();
    }

    @Override
    public final T resetVer(long ver) {
        keyRangeWriter().metadata(METADATA_VER_BYTES, KVUtil.toByteStringNativeOrder(ver));
        return thisT();
    }

    @Override
    public final T lastAppliedIndex(long lastAppliedIndex) {
        keyRangeWriter().metadata(METADATA_LAST_APPLIED_INDEX_BYTES, KVUtil.toByteString(lastAppliedIndex));
        return thisT();
    }

    @Override
    public final T boundary(Boundary boundary) {
        keyRangeWriter().metadata(METADATA_RANGE_BOUND_BYTES, boundary.toByteString());
        return thisT();
    }

    @Override
    public final T state(State state) {
        keyRangeWriter().metadata(METADATA_STATE_BYTES, state.toByteString());
        return thisT();
    }

    @SuppressWarnings("unchecked")
    private T thisT() {
        return (T) this;
    }

    protected abstract IKVSpaceMetadataUpdatable<?> keyRangeWriter();
}
