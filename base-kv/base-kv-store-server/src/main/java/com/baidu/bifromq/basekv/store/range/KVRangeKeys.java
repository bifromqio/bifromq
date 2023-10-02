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

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.baidu.bifromq.basekv.localengine.proto.KeyBoundary;
import com.baidu.bifromq.basekv.proto.Range;
import com.google.protobuf.ByteString;

public class KVRangeKeys {
    public static final ByteString METADATA_VER_BYTES = unsafeWrap(new byte[] {0x00});
    public static final ByteString METADATA_RANGE_BOUND_BYTES = unsafeWrap(new byte[] {0x01});
    public static final ByteString METADATA_LAST_APPLIED_INDEX_BYTES = unsafeWrap(new byte[] {0x02});
    public static final ByteString METADATA_STATE_BYTES = unsafeWrap(new byte[] {0x03});

    public static KeyBoundary toBoundary(Range range) {
        // TODO: unify range and KeyRangeBoundary
        KeyBoundary.Builder boundaryBuilder = KeyBoundary.newBuilder();
        if (range.hasStartKey()) {
            boundaryBuilder.setStartKey(range.getStartKey());
        }
        if (range.hasEndKey()) {
            boundaryBuilder.setEndKey(range.getEndKey());
        }
        return boundaryBuilder.build();
    }

    public static Range toRange(KeyBoundary boundary) {
        // TODO: unify range and KeyRangeBoundary
        Range.Builder rangeBuilder = Range.newBuilder();
        if (boundary.hasStartKey()) {
            rangeBuilder.setStartKey(boundary.getStartKey());
        }
        if (boundary.hasEndKey()) {
            rangeBuilder.setEndKey(boundary.getEndKey());
        }
        return rangeBuilder.build();
    }
}
