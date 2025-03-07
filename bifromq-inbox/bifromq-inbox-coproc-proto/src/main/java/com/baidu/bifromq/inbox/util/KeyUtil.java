/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.inbox.util;

import static com.baidu.bifromq.util.BSUtil.toByteString;
import static com.baidu.bifromq.util.BSUtil.toInt;
import static com.baidu.bifromq.util.BSUtil.toLong;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.nio.charset.StandardCharsets;

public class KeyUtil {
    // the first byte indicating the version of KV schema for storing inbox data
    public static final ByteString SCHEMA_VER = ByteString.copyFrom(new byte[] {0x01});
    private static final ByteString QOS0INBOX_SIGN = ByteString.copyFrom(new byte[] {0x00});
    private static final ByteString SEND_BUFFER_SIGN = ByteString.copyFrom(new byte[] {0x01});
    private static final ByteString UPPER_BOUND = ByteString.copyFrom(new byte[] {(byte) 0xFF});

    private static final int MAX_BUCKETS = 0xFF; // 256
    private static final int BUCKET_BYTES =
        (MAX_BUCKETS <= 0xFF) ? 1 : (MAX_BUCKETS <= 0xFFFF) ? 2 : (MAX_BUCKETS <= 0xFFFFFF) ? 3 : 4;

    private static int tenantIdLength(ByteString key) {
        return toInt(key.substring(SCHEMA_VER.size(), SCHEMA_VER.size() + Integer.BYTES));
    }

    private static int inboxIdLength(ByteString key, int tenantPrefixLength) {
        return toInt(
            key.substring(tenantPrefixLength + BUCKET_BYTES, tenantPrefixLength + BUCKET_BYTES + Integer.BYTES));
    }

    public static ByteString tenantPrefix(String tenantId) {
        // tenantPrefix: <SCHEMA_VER><tenantIdLength><tenantId>
        ByteString tenantIdBS = UnsafeByteOperations.unsafeWrap(tenantId.getBytes(StandardCharsets.UTF_8));
        return SCHEMA_VER.concat(toByteString(tenantIdBS.size())).concat(tenantIdBS);
    }

    private static int tenantPrefixLength(ByteString key) {
        // tenantPrefix: <SCHEMA_VER><tenantIdLength><tenantId>
        return SCHEMA_VER.size() + Integer.BYTES + tenantIdLength(key);
    }

    private static int inboxBucketPrefixLength(ByteString key) {
        // inboxBucketPrefix: <TENANT_PREFIX><BUCKET_BYTES>
        int tenantPrefixLength = tenantPrefixLength(key);
        return tenantPrefixLength + BUCKET_BYTES;
    }

    public static ByteString inboxBucketPrefix(String tenantId, String inboxId) {
        // inboxBucketPrefix: <TENANT_PREFIX><BUCKET_BYTES>
        return tenantPrefix(tenantId).concat(bucket(inboxId));
    }

    public static ByteString inboxPrefix(String tenantId, String inboxId) {
        // inboxPrefix: <TENANT_PREFIX><BUCKET_BYTES><inboxIdLength><inboxId>
        ByteString inboxIdBS = UnsafeByteOperations.unsafeWrap(inboxId.getBytes(StandardCharsets.UTF_8));
        return inboxBucketPrefix(tenantId, inboxId).concat(toByteString(inboxIdBS.size())).concat(inboxIdBS);
    }

    private static ByteString bucket(String inboxId) {
        int hash = inboxId.hashCode();
        byte bucket = (byte) ((hash ^ (hash >>> 16)) & MAX_BUCKETS);
        return UnsafeByteOperations.unsafeWrap(new byte[] {bucket});
    }

    private static int inboxPrefixLength(ByteString key) {
        // inboxPrefix: <TENANT_PREFIX><BUCKET_BYTES><inboxIdLength><inboxId>
        int tenantPrefixLength = tenantPrefixLength(key);
        int inboxIdLen = inboxIdLength(key, tenantPrefixLength);
        return tenantPrefixLength + BUCKET_BYTES + Integer.BYTES + inboxIdLen;
    }

    public static ByteString inboxKeyPrefix(String tenantId, String inboxId, long incarnation) {
        // inboxKeyPrefix: <INBOX_PREFIX><incarnation>
        return inboxPrefix(tenantId, inboxId).concat(toByteString(incarnation));
    }

    private static int inboxKeyPrefixLength(ByteString key) {
        // inboxKeyPrefix: <INBOX_PREFIX><incarnation>
        return inboxPrefixLength(key) + Long.BYTES;
    }

    public static ByteString inboxKeyUpperBound(ByteString metadataKey) {
        return metadataKey.concat(UPPER_BOUND);
    }

    public static boolean isInboxKey(ByteString key) {
        int size = key.size();
        if (size < SCHEMA_VER.size() + Integer.BYTES) {
            return false;
        }
        int tenantPrefixLength = tenantPrefixLength(key);
        if (size <= tenantPrefixLength + BUCKET_BYTES + Integer.BYTES) {
            return false;
        }
        int inboxIdLen = inboxIdLength(key, tenantPrefixLength);
        return size >= tenantPrefixLength + BUCKET_BYTES + Integer.BYTES + inboxIdLen;
    }

    public static boolean isMetadataKey(ByteString key) {
        return key.size() == inboxKeyPrefixLength(key);
    }

    public static boolean hasInboxKeyPrefix(ByteString key) {
        if (key.size() > SCHEMA_VER.size() + Integer.BYTES) {
            int tenantPrefixLength = tenantPrefixLength(key);
            if (key.size() > tenantPrefixLength + BUCKET_BYTES + Integer.BYTES) {
                int inboxPrefixLength = inboxPrefixLength(key);
                if (key.size() >= inboxPrefixLength + Long.BYTES) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isQoS0MessageKey(ByteString key) {
        // QOS0 MessageKey: <INBOX_KEY_PREFIX><QOS0INBOX_SIGN><SEQ>
        int inboxKeyPrefixLen = inboxKeyPrefixLength(key);
        return key.size() > inboxKeyPrefixLen && key.byteAt(inboxKeyPrefixLen) == QOS0INBOX_SIGN.byteAt(0);
    }

    public static boolean isQoS0MessageKey(ByteString key, ByteString inboxKeyPrefix) {
        // QOS0 MessageKey: <INBOX_KEY_PREFIX><QOS0INBOX_SIGN><SEQ>
        return isQoS0MessageKey(key) && key.startsWith(inboxKeyPrefix);
    }

    public static boolean isBufferMessageKey(ByteString key) {
        // QOS0 MessageKey: <INBOX_KEY_PREFIX><QOS1INBOX_SIGN><SEQ>
        int inboxKeyPrefixLen = inboxKeyPrefixLength(key);
        return key.size() > inboxKeyPrefixLen && key.byteAt(inboxKeyPrefixLen) == SEND_BUFFER_SIGN.byteAt(0);
    }

    public static boolean isBufferMessageKey(ByteString key, ByteString inboxKeyPrefix) {
        // QOS0 MessageKey: <INBOX_KEY_PREFIX><QOS1INBOX_SIGN><SEQ>
        return isBufferMessageKey(key) && key.startsWith(inboxKeyPrefix);
    }

    public static String parseTenantId(ByteString metadataKey) {
        int tenantIdLength = tenantIdLength(metadataKey);
        int startAt = SCHEMA_VER.size() + Integer.BYTES;
        return metadataKey.substring(startAt, startAt + tenantIdLength).toStringUtf8();
    }

    public static String parseInboxId(ByteString metadataKey) {
        int tenantPrefixLength = tenantPrefixLength(metadataKey);
        int inboxIdLen = inboxIdLength(metadataKey, tenantPrefixLength);
        return metadataKey.substring(tenantPrefixLength + BUCKET_BYTES + Integer.BYTES,
            tenantPrefixLength + BUCKET_BYTES + Integer.BYTES + inboxIdLen).toStringUtf8();
    }

    public static long parseIncarnation(ByteString inboxKey) {
        inboxKey = parseInboxKeyPrefix(inboxKey);
        return toLong(inboxKey.substring(inboxPrefixLength(inboxKey)));
    }

    public static ByteString parseInboxBucketPrefix(ByteString inboxKey) {
        return inboxKey.substring(0, inboxBucketPrefixLength(inboxKey));
    }

    public static ByteString parseInboxPrefix(ByteString inboxKey) {
        return inboxKey.substring(0, inboxPrefixLength(inboxKey));
    }

    public static ByteString parseInboxKeyPrefix(ByteString inboxKey) {
        return inboxKey.substring(0, inboxKeyPrefixLength(inboxKey));
    }

    public static ByteString qos0InboxPrefix(ByteString inboxKeyPrefix) {
        return inboxKeyPrefix.concat(QOS0INBOX_SIGN);
    }

    public static ByteString qos0InboxMsgKey(ByteString inboxKeyPrefix, long seq) {
        return qos0InboxPrefix(inboxKeyPrefix).concat(toByteString(seq));
    }

    public static ByteString sendBufferPrefix(ByteString inboxKeyPrefix) {
        return inboxKeyPrefix.concat(SEND_BUFFER_SIGN);
    }

    public static ByteString bufferMsgKey(ByteString inboxKeyPrefix, long seq) {
        return sendBufferPrefix(inboxKeyPrefix).concat(toByteString(seq));
    }

    public static long parseSeq(ByteString inboxKeyPrefix, ByteString inboxMsgKey) {
        // QOS0 MessageKey: <INBOX_KEY_PREFIX><QOS0INBOX_SIGN><SEQ>
        // QOS1 MessageKey: <INBOX_KEY_PREFIX><QOS1INBOX_SIGN><SEQ>
        return inboxMsgKey.substring(inboxKeyPrefix.size() + 1).asReadOnlyByteBuffer().getLong();
    }
}
