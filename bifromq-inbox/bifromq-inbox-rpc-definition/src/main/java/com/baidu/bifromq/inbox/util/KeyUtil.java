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

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class KeyUtil {
    // the first byte indicating the version of KV schema for storing inbox data
    public static final ByteString SCHEMA_VER = ByteString.copyFrom(new byte[] {0x00});
    private static final ByteString QOS0INBOX_SIGN = ByteString.copyFrom(new byte[] {0x00});
    private static final ByteString SEND_BUFFER_SIGN = ByteString.copyFrom(new byte[] {0x01});
    private static final ByteString UPPER_BOUND = ByteString.copyFrom(new byte[] {(byte) 0xFF});

    private static int tenantIdLength(ByteString key) {
        return toInt(key.substring(SCHEMA_VER.size(), SCHEMA_VER.size() + Integer.BYTES));
    }

    private static int inboxIdLength(ByteString key, int tenantPrefixLength) {
        return toInt(key.substring(tenantPrefixLength, tenantPrefixLength + Integer.BYTES));
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

    public static ByteString inboxPrefix(String tenantId, String inboxId) {
        // inboxPrefix: <TENANT_PREFIX><inboxIdLength><inboxId>
        ByteString inboxIdBS = UnsafeByteOperations.unsafeWrap(inboxId.getBytes(StandardCharsets.UTF_8));
        return tenantPrefix(tenantId).concat(toByteString(inboxIdBS.size())).concat(inboxIdBS);
    }

    private static int inboxPrefixLength(ByteString key) {
        // inboxPrefix: <TENANT_PREFIX><inboxIdLength><inboxId>
        int tenantPrefixLength = tenantPrefixLength(key);
        int inboxIdLen = inboxIdLength(key, tenantPrefixLength);
        return tenantPrefixLength + Integer.BYTES + inboxIdLen;
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
        if (size <= tenantPrefixLength + Integer.BYTES) {
            return false;
        }
        int inboxIdLen = inboxIdLength(key, tenantPrefixLength);
        return size >= tenantPrefixLength + Integer.BYTES + inboxIdLen;
    }

    public static boolean isMetadataKey(ByteString key) {
        return key.size() == inboxKeyPrefixLength(key);
    }

    public static boolean hasInboxKeyPrefix(ByteString key) {
        if (key.size() > SCHEMA_VER.size() + Integer.BYTES) {
            int tenantPrefixLength = tenantPrefixLength(key);
            if (key.size() > tenantPrefixLength + Integer.BYTES) {
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
        return metadataKey.substring(tenantPrefixLength + Integer.BYTES,
            tenantPrefixLength + Integer.BYTES + inboxIdLen).toStringUtf8();
    }

    public static long parseIncarnation(ByteString inboxKey) {
        inboxKey = parseInboxKeyPrefix(inboxKey);
        return toLong(inboxKey.substring(inboxPrefixLength(inboxKey)));
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

    private static ByteString toByteString(int i) {
        return UnsafeByteOperations.unsafeWrap(toBytes(i));
    }

    private static ByteString toByteString(long l) {
        return UnsafeByteOperations.unsafeWrap(toBytes(l));
    }

    private static byte[] toBytes(long l) {
        return ByteBuffer.allocate(Long.BYTES).putLong(l).array();
    }

    private static byte[] toBytes(int i) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(i).array();
    }

    private static long toLong(ByteString b) {
        assert b.size() == Long.BYTES;
        ByteBuffer buffer = b.asReadOnlyByteBuffer();
        return buffer.getLong();
    }

    private static int toInt(ByteString b) {
        assert b.size() == Integer.BYTES;
        ByteBuffer buffer = b.asReadOnlyByteBuffer();
        return buffer.getInt();
    }
}
