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

import com.baidu.bifromq.inbox.storage.proto.InboxMessage;
import com.baidu.bifromq.type.TopicMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class KeyUtil {
    private static final ByteString SCHEMA_VER = ByteString.copyFrom(new byte[] {0x00});
    private static final ByteString QOS0INBOX_SIGN = ByteString.copyFrom(new byte[] {0x00});
    private static final ByteString QOS1INBOX_SIGN = ByteString.copyFrom(new byte[] {0x01});
    private static final ByteString QOS2INBOX_SIGN = ByteString.copyFrom(new byte[] {0x11});
    private static final ByteString QOS2INDEX = ByteString.copyFrom(new byte[] {0x00});
    private static final ByteString QOS2MSG = ByteString.copyFrom(new byte[] {0x01});

    private static int tenantIdLength(ByteString key) {
        return toInt(key.substring(0, Integer.BYTES));
    }

    private static int inboxIdLength(ByteString key) {
        return inboxIdLength(key, tenantIdLength(key));
    }

    private static int inboxIdLength(ByteString key, int tenantIdLength) {
        return toInt(key.substring(Integer.BYTES + tenantIdLength, tenantIdLength + 2 * Integer.BYTES));
    }

    public static ByteString tenantPrefix(String tenantId) {
        ByteString tenantIdBS = UnsafeByteOperations.unsafeWrap(tenantId.getBytes(StandardCharsets.UTF_8));
        return toByteString(tenantIdBS.size()).concat(tenantIdBS);
    }

    public static ByteString inboxPrefix(String tenantId, String inboxId) {
        // inboxId format: <tenantIdLength><tenantId><inboxIdLength><inboxId>
        ByteString tenantIdBS = UnsafeByteOperations.unsafeWrap(tenantId.getBytes(StandardCharsets.UTF_8));
        ByteString inboxIdBS = UnsafeByteOperations.unsafeWrap(inboxId.getBytes(StandardCharsets.UTF_8));
        return toByteString(tenantIdBS.size())
            .concat(tenantIdBS)
            .concat(toByteString(inboxIdBS.size()))
            .concat(inboxIdBS);
    }

    private static int inboxPrefixLength(ByteString key) {
        int tenantIdLen = tenantIdLength(key);
        int inboxIdLen = inboxIdLength(key, tenantIdLen);
        return Integer.BYTES + tenantIdLen + Integer.BYTES + inboxIdLen;
    }

    public static ByteString inboxKeyPrefix(String tenantId, String inboxId, long incarnation) {
        return inboxPrefix(tenantId, inboxId).concat(toByteString(incarnation));
    }

    private static int inboxKeyPrefixLength(ByteString key) {
        int tenantIdLen = tenantIdLength(key);
        int inboxIdLen = inboxIdLength(key, tenantIdLen);
        return Integer.BYTES + tenantIdLen + Integer.BYTES + inboxIdLen + Long.BYTES;
    }

    public static boolean isInboxKey(ByteString key) {
        int size = key.size();
        if (size < Integer.BYTES) {
            return false;
        }
        int tenantIdLen = tenantIdLength(key);
        if (size <= tenantIdLen + Integer.BYTES) {
            return false;
        }
        int inboxIdLen = inboxIdLength(key, tenantIdLen);
        return size >= tenantIdLen + inboxIdLen + 2 * Integer.BYTES;
    }

    public static boolean isMetadataKey(ByteString key) {
        return key.size() == inboxKeyPrefixLength(key);
    }

    public static boolean isQoS0MessageKey(ByteString key) {
        int inboxKeyPrefixLen = inboxKeyPrefixLength(key);
        return key.size() > inboxKeyPrefixLen && key.byteAt(inboxKeyPrefixLen) == QOS0INBOX_SIGN.byteAt(0);
    }

    public static boolean isQoS0MessageKey(ByteString key, ByteString inboxKeyPrefix) {
        return isQoS0MessageKey(key) && key.startsWith(inboxKeyPrefix);
    }

    public static boolean isQoS1MessageKey(ByteString key) {
        int inboxKeyPrefixLen = inboxKeyPrefixLength(key);
        return key.size() > inboxKeyPrefixLen && key.byteAt(inboxKeyPrefixLen) == QOS1INBOX_SIGN.byteAt(0);
    }

    public static boolean isQoS1MessageKey(ByteString key, ByteString inboxKeyPrefix) {
        return isQoS1MessageKey(key) && key.startsWith(inboxKeyPrefix);
    }

    public static boolean isQoS2MessageIndexKey(ByteString key) {
        int inboxKeyPrefixLen = inboxKeyPrefixLength(key);
        return key.size() > inboxKeyPrefixLen + 2 &&
            key.byteAt(inboxKeyPrefixLen) == QOS2INBOX_SIGN.byteAt(0) &&
            key.byteAt(inboxKeyPrefixLen + 1) == QOS2INDEX.byteAt(0);
    }

    public static boolean isQoS2MessageIndexKey(ByteString key, ByteString inboxKeyPrefix) {
        return isQoS2MessageIndexKey(key) && key.startsWith(inboxKeyPrefix);
    }

    public static String parseTenantId(ByteString metadataKey) {
        int tenantIdLength = toInt(metadataKey.substring(0, Integer.BYTES));
        return metadataKey.substring(Integer.BYTES, Integer.BYTES + tenantIdLength).toStringUtf8();
    }

    public static String parseInboxId(ByteString metadataKey) {
        int tenantIdLen = tenantIdLength(metadataKey);
        int inboxIdLen = inboxIdLength(metadataKey, tenantIdLen);
        return metadataKey.substring(2 * Integer.BYTES + tenantIdLen,
            2 * Integer.BYTES + tenantIdLen + inboxIdLen).toStringUtf8();
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

    public static ByteString qos1InboxPrefix(ByteString inboxKeyPrefix) {
        return inboxKeyPrefix.concat(QOS1INBOX_SIGN);
    }

    public static ByteString qos1InboxMsgKey(ByteString inboxKeyPrefix, long seq) {
        return qos1InboxPrefix(inboxKeyPrefix).concat(toByteString(seq));
    }

    public static ByteString qos2InboxPrefix(ByteString inboxKeyPrefix) {
        return inboxKeyPrefix.concat(QOS2INBOX_SIGN);
    }

    public static ByteString qos2InboxIndex(ByteString inboxKeyPrefix, long seq) {
        return qos2InboxPrefix(inboxKeyPrefix).concat(QOS2INDEX).concat(toByteString(seq));
    }

    public static ByteString qos2InboxIndexPrefix(ByteString inboxKeyPrefix) {
        return qos2InboxPrefix(inboxKeyPrefix).concat(QOS2INDEX);
    }

    public static ByteString qos2InboxMsgKey(ByteString inboxKeyPrefix, ByteString qos2MsgKey) {
        return qos2InboxPrefix(inboxKeyPrefix).concat(QOS2MSG).concat(qos2MsgKey);
    }

    public static ByteString buildQoS2MsgKey(InboxMessage message) {
        TopicMessage userMsg = message.getMsg();
        return userMsg.getPublisher().toByteString().concat(toByteString(userMsg.getMessage().getMessageId()));
    }

    public static long parseSeq(ByteString inboxKeyPrefix, ByteString inboxMsgKey) {
        return inboxMsgKey.substring(inboxKeyPrefix.size() + 1).asReadOnlyByteBuffer().getLong();
    }

    public static long parseQoS2Index(ByteString inboxKeyPrefix, ByteString qos2InboxIndexKey) {
        return qos2InboxIndexKey.substring(inboxKeyPrefix.size() + 2).asReadOnlyByteBuffer().getLong();
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
