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

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.Range;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class KVRangeKeys {
    private static final ByteString METADATA_VER_BYTES = unsafeWrap(new byte[] {0x00});
    private static final ByteString METADATA_RANGE_BOUND_BYTES = unsafeWrap(new byte[] {0x01});
    private static final ByteString METADATA_LAST_APPLIED_INDEX_BYTES = unsafeWrap(new byte[] {0x02});
    private static final ByteString METADATA_STATE_BYTES = unsafeWrap(new byte[] {0x03});
    private static final ByteString DATA_PREFIX = unsafeWrap(new byte[] {(byte) 0xFE});
    private static final ByteString DATA_SECTION_START = unsafeWrap(new byte[] {(byte) 0xFE});
    private static final ByteString DATA_SECTION_END = unsafeWrap(new byte[] {(byte) 0xFF});
    private static final LoadingCache<KVRangeId, Map<ByteString, ByteString>> CACHED_KEYS = Caffeine.newBuilder()
        .maximumSize(100)
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .build(kvRangeId -> {
            Map<ByteString, ByteString> keys = new HashMap<>();
            keys.put(METADATA_VER_BYTES, unsafeWrap(
                ByteBuffer.allocate(METADATA_VER_BYTES.size() + 2 * Long.BYTES)
                    .put(METADATA_VER_BYTES.toByteArray())
                    .putLong(kvRangeId.getEpoch())
                    .putLong(kvRangeId.getId())
                    .array()
            ));
            keys.put(METADATA_RANGE_BOUND_BYTES, unsafeWrap(
                ByteBuffer.allocate(METADATA_RANGE_BOUND_BYTES.size() + 2 * Long.BYTES)
                    .put(METADATA_RANGE_BOUND_BYTES.toByteArray())
                    .putLong(kvRangeId.getEpoch())
                    .putLong(kvRangeId.getId())
                    .array()
            ));
            keys.put(METADATA_LAST_APPLIED_INDEX_BYTES, unsafeWrap(
                ByteBuffer.allocate(METADATA_LAST_APPLIED_INDEX_BYTES.size() + 2 * Long.BYTES)
                    .put(METADATA_LAST_APPLIED_INDEX_BYTES.toByteArray())
                    .putLong(kvRangeId.getEpoch())
                    .putLong(kvRangeId.getId())
                    .array()
            ));
            keys.put(METADATA_STATE_BYTES, unsafeWrap(
                ByteBuffer.allocate(METADATA_STATE_BYTES.size() + 2 * Long.BYTES)
                    .put(METADATA_STATE_BYTES.toByteArray())
                    .putLong(kvRangeId.getEpoch())
                    .putLong(kvRangeId.getId())
                    .array()
            ));
            return keys;
        });

    public static ByteString verKey(KVRangeId kvRangeId) {
        return CACHED_KEYS.get(kvRangeId).get(METADATA_VER_BYTES);
    }

    public static ByteString rangeKey(KVRangeId kvRangeId) {
        return CACHED_KEYS.get(kvRangeId).get(METADATA_RANGE_BOUND_BYTES);
    }

    public static ByteString lastAppliedIndexKey(KVRangeId kvRangeId) {
        return CACHED_KEYS.get(kvRangeId).get(METADATA_LAST_APPLIED_INDEX_BYTES);
    }

    public static ByteString stateKey(KVRangeId kvRangeId) {
        return CACHED_KEYS.get(kvRangeId).get(METADATA_STATE_BYTES);
    }

    public static ByteString dataKey(ByteString userKey) {
        return DATA_PREFIX.concat(userKey);
    }

    public static ByteString userKey(ByteString dataKey) {
        assert dataKey.startsWith(DATA_PREFIX);
        return dataKey.substring(DATA_PREFIX.size());
    }

    public static Range dataBound(Range bound) {
        ByteString startKey = bound.hasStartKey() ? dataKey(bound.getStartKey()) : DATA_SECTION_START;
        ByteString endKey = bound.hasEndKey() ? dataKey(bound.getEndKey()) : DATA_SECTION_END;
        return Range.newBuilder().setStartKey(startKey).setEndKey(endKey).build();
    }
}
