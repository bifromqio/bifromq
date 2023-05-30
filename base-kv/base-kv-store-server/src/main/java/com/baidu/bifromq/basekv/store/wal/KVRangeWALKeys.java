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

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.baidu.bifromq.basekv.localengine.RangeUtil;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.util.KVUtil;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class KVRangeWALKeys {
    public static final ByteString KEY_PREFIX_CURRENT_VOTING_BYTES = unsafeWrap(new byte[] {(byte) 0});
    public static final ByteString KEY_PREFIX_LATEST_SNAPSHOT_BYTES = unsafeWrap(new byte[] {(byte) 1});
    public static final ByteString KEY_PREFIX_CONFIG_ENTRY_INDEXES_BYTES = unsafeWrap(new byte[] {(byte) 2});
    public static final ByteString KEY_PREFIX_CURRENT_TERM_BYTES = unsafeWrap(new byte[] {(byte) 3});
    public static final ByteString KEY_PREFIX_LOG_ENTRIES_INCAR = unsafeWrap(new byte[] {(byte) 4});
    public static final ByteString KEY_PREFIX_LOG_ENTRIES_BYTES = unsafeWrap(new byte[] {(byte) 5});
    private static final Map<KVRangeId, Map<ByteString, ByteString>> CACHED_KEYS = new ConcurrentHashMap<>();

    public static void uncache(KVRangeId kvRangeId) {
        CACHED_KEYS.remove(kvRangeId);
    }

    public static Map<ByteString, ByteString> cache(KVRangeId kvRangeId) {
        return CACHED_KEYS.computeIfAbsent(kvRangeId, id -> {
            Map<ByteString, ByteString> keys = new HashMap<>();
            keys.put(KEY_PREFIX_CURRENT_TERM_BYTES, unsafeWrap(ByteBuffer.allocate(2 * Long.BYTES + 1)
                .putLong(kvRangeId.getEpoch())
                .putLong(kvRangeId.getId())
                .put(KEY_PREFIX_CURRENT_TERM_BYTES.asReadOnlyByteBuffer())
                .array()));
            keys.put(KEY_PREFIX_CURRENT_VOTING_BYTES, unsafeWrap(ByteBuffer.allocate(2 * Long.BYTES + 1)
                .putLong(kvRangeId.getEpoch())
                .putLong(kvRangeId.getId())
                .put(KEY_PREFIX_CURRENT_VOTING_BYTES.asReadOnlyByteBuffer())
                .array()));
            keys.put(KEY_PREFIX_LATEST_SNAPSHOT_BYTES, unsafeWrap(ByteBuffer.allocate(2 * Long.BYTES + 1)
                .putLong(kvRangeId.getEpoch())
                .putLong(kvRangeId.getId())
                .put(KEY_PREFIX_LATEST_SNAPSHOT_BYTES.asReadOnlyByteBuffer())
                .array()));
            keys.put(KEY_PREFIX_CONFIG_ENTRY_INDEXES_BYTES, unsafeWrap(ByteBuffer.allocate(2 * Long.BYTES + 1)
                .putLong(kvRangeId.getEpoch())
                .putLong(kvRangeId.getId())
                .put(KEY_PREFIX_CONFIG_ENTRY_INDEXES_BYTES.asReadOnlyByteBuffer())
                .array()));
            keys.put(KEY_PREFIX_LOG_ENTRIES_INCAR, unsafeWrap(ByteBuffer.allocate(2 * Long.BYTES + 1)
                .putLong(kvRangeId.getEpoch())
                .putLong(kvRangeId.getId())
                .put(KEY_PREFIX_LOG_ENTRIES_INCAR.asReadOnlyByteBuffer())
                .array()));
            keys.put(KEY_PREFIX_LOG_ENTRIES_BYTES, unsafeWrap(ByteBuffer.allocate(2 * Long.BYTES + 1)
                .putLong(kvRangeId.getEpoch())
                .putLong(kvRangeId.getId())
                .put(KEY_PREFIX_LOG_ENTRIES_BYTES.asReadOnlyByteBuffer())
                .array()));
            return Collections.unmodifiableMap(keys);
        });
    }

    public static ByteString walStartKey(KVRangeId kvRangeId) {
        return KVUtil.toByteString(kvRangeId);
    }

    public static ByteString walEndKey(KVRangeId kvRangeId) {
        return RangeUtil.upperBound(KVUtil.toByteString(kvRangeId));
    }

    public static ByteString currentTermKey(KVRangeId kvRangeId) {
        return cache(kvRangeId).get(KEY_PREFIX_CURRENT_TERM_BYTES);
    }

    public static ByteString currentVotingKey(KVRangeId kvRangeId) {
        return cache(kvRangeId).get(KEY_PREFIX_CURRENT_VOTING_BYTES);
    }

    public static ByteString latestSnapshotKey(KVRangeId kvRangeId) {
        return cache(kvRangeId).get(KEY_PREFIX_LATEST_SNAPSHOT_BYTES);
    }

    public static ByteString configEntriesKeyPrefix(KVRangeId kvRangeId) {
        return cache(kvRangeId).get(KEY_PREFIX_CONFIG_ENTRY_INDEXES_BYTES);
    }

    public static ByteString configEntriesKeyPrefixInfix(KVRangeId kvRangeId, int logEntriesKeyInfix) {
        return configEntriesKeyPrefix(kvRangeId).concat(KVUtil.toByteString(logEntriesKeyInfix));
    }

    public static ByteString configEntriesKey(KVRangeId kvRangeId, int logEntriesKeyInfix, long index) {
        return configEntriesKeyPrefixInfix(kvRangeId, logEntriesKeyInfix).concat(KVUtil.toByteString(index));
    }

    public static ByteString logEntriesKeyPrefix(KVRangeId kvRangeId) {
        return cache(kvRangeId).get(KEY_PREFIX_LOG_ENTRIES_BYTES);
    }

    public static ByteString logEntriesKeyInfix(KVRangeId kvRangeId) {
        return cache(kvRangeId).get(KEY_PREFIX_LOG_ENTRIES_INCAR);
    }

    public static ByteString logEntriesKeyPrefixInfix(KVRangeId kvRangeId, int logEntriesKeyInfix) {
        return logEntriesKeyPrefix(kvRangeId).concat(KVUtil.toByteString(logEntriesKeyInfix));
    }

    public static ByteString logEntryKey(KVRangeId kvRangeId, int logEntriesKeyInfix, long logIndex) {
        return logEntriesKeyPrefixInfix(kvRangeId, logEntriesKeyInfix).concat(KVUtil.toByteString(logIndex));
    }

    public static long parseLogIndex(ByteString logEntryKey) {
        return logEntryKey.asReadOnlyByteBuffer().getLong(1 + 2 * Long.BYTES + Integer.BYTES);
    }

    public static KVRangeId parseKVRangeId(ByteString walKey) {
        long m = walKey.asReadOnlyByteBuffer().getLong();
        long l = walKey.asReadOnlyByteBuffer().getLong(Long.BYTES);
        return KVRangeId.newBuilder().setEpoch(m).setId(l).build();
    }
}
