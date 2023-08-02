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

package com.baidu.bifromq.inbox.util;

import com.baidu.bifromq.inbox.storage.proto.InboxMessage;
import com.baidu.bifromq.type.TopicMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class KeyUtil {
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

    private static int scopedInboxIdLength(ByteString key) {
        int tenantIdLen = tenantIdLength(key);
        int inboxIdLen = inboxIdLength(key, tenantIdLen);
        return Integer.BYTES + tenantIdLen + Integer.BYTES + inboxIdLen;
    }

    private static int inboxIdLength(ByteString key, int tenantIdLength) {
        return toInt(key.substring(Integer.BYTES + tenantIdLength, tenantIdLength + 2 * Integer.BYTES));
    }

    public static ByteString tenantPrefix(String tenantId) {
        ByteString tenantIdBS = UnsafeByteOperations.unsafeWrap(tenantId.getBytes(StandardCharsets.UTF_8));
        return toByteString(tenantIdBS.size()).concat(tenantIdBS);
    }

    public static ByteString scopedInboxId(String tenantId, String inboxId) {
        // scopedInboxId format: <tenantIdLength><tenantId><inboxIdLength><inboxId>
        ByteString tenantIdBS = UnsafeByteOperations.unsafeWrap(tenantId.getBytes(StandardCharsets.UTF_8));
        ByteString inboxIdBS = UnsafeByteOperations.unsafeWrap(inboxId.getBytes(StandardCharsets.UTF_8));
        return toByteString(tenantIdBS.size())
            .concat(tenantIdBS)
            .concat(toByteString(inboxIdBS.size()))
            .concat(inboxIdBS);
    }

    public static boolean isInboxMetadataKey(ByteString key) {
        int tenantIdLen = tenantIdLength(key);
        return key.size() == tenantIdLen + inboxIdLength(key, tenantIdLen) + 2 * Integer.BYTES;
    }

    public static boolean isQoS0MessageKey(ByteString key) {
        int scopedInboxIdLen = scopedInboxIdLength(key);
        return key.size() > scopedInboxIdLen && key.byteAt(scopedInboxIdLen) == QOS0INBOX_SIGN.byteAt(0);
    }

    public static boolean isQoS0MessageKey(ByteString key, ByteString scopedInboxId) {
        return key.startsWith(scopedInboxId) && isQoS0MessageKey(key);
    }

    public static boolean isQoS1MessageKey(ByteString key) {
        int scopedInboxIdLen = scopedInboxIdLength(key);
        return key.size() > scopedInboxIdLen && key.byteAt(scopedInboxIdLen) == QOS1INBOX_SIGN.byteAt(0);
    }

    public static boolean isQoS1MessageKey(ByteString key, ByteString scopedInboxId) {
        return parseScopedInboxId(key).equals(scopedInboxId) && isQoS1MessageKey(key);
    }

    public static boolean isQoS2MessageIndexKey(ByteString key) {
        int scopedInboxIdLen = scopedInboxIdLength(key);
        return key.size() > scopedInboxIdLen + 2 &&
            key.byteAt(scopedInboxIdLen) == QOS2INBOX_SIGN.byteAt(0) &&
            key.byteAt(scopedInboxIdLen + 1) == QOS2INDEX.byteAt(0);
    }

    public static boolean isQoS2MessageIndexKey(ByteString key, ByteString scopedInboxId) {
        return parseScopedInboxId(key).equals(scopedInboxId) && isQoS2MessageIndexKey(key);
    }

    public static boolean isQoS2QueueMessageKey(ByteString key) {
        int scopedInboxIdLen = scopedInboxIdLength(key);
        return key.size() > scopedInboxIdLen + 2 &&
            key.byteAt(scopedInboxIdLen) == QOS2INBOX_SIGN.byteAt(0);
    }

    public static String parseTenantId(ByteString scopedInboxId) {
        int tenantIdLength = toInt(scopedInboxId.substring(0, Integer.BYTES));
        return scopedInboxId.substring(Integer.BYTES, Integer.BYTES + tenantIdLength).toStringUtf8();
    }

    public static String parseInboxId(ByteString scopedInboxId) {
        int tenantIdLen = tenantIdLength(scopedInboxId);
        int inboxIdLen = inboxIdLength(scopedInboxId, tenantIdLen);
        return scopedInboxId.substring(2 * Integer.BYTES + tenantIdLen,
            2 * Integer.BYTES + tenantIdLen + inboxIdLen).toStringUtf8();
    }

    public static ByteString parseScopedInboxId(ByteString key) {
        int tenantIdLen = tenantIdLength(key);
        int inboxIdLen = inboxIdLength(key, tenantIdLen);
        return key.substring(0, tenantIdLen + inboxIdLen + 2 * Integer.BYTES);
    }

    public static ByteString qos0InboxPrefix(ByteString scopedInboxId) {
        return scopedInboxId.concat(QOS0INBOX_SIGN);
    }

    public static ByteString qos0InboxMsgKey(ByteString scopedInboxId, long seq) {
        return qos0InboxPrefix(scopedInboxId).concat(toByteString(seq));
    }

    public static ByteString qos1InboxPrefix(ByteString scopedInboxId) {
        return scopedInboxId.concat(QOS1INBOX_SIGN);
    }

    public static ByteString qos1InboxMsgKey(ByteString scopedInboxId, long seq) {
        return qos1InboxPrefix(scopedInboxId).concat(toByteString(seq));
    }

    public static ByteString qos2InboxPrefix(ByteString scopedInboxId) {
        return scopedInboxId.concat(QOS2INBOX_SIGN);
    }

    public static ByteString qos2InboxIndex(ByteString scopedInboxId, long seq) {
        return qos2InboxPrefix(scopedInboxId).concat(QOS2INDEX).concat(toByteString(seq));
    }

    public static ByteString qos2InboxIndexPrefix(ByteString scopedInboxId) {
        return qos2InboxPrefix(scopedInboxId).concat(QOS2INDEX);
    }

    public static ByteString qos2InboxMsgKey(ByteString scopedInboxId, ByteString msgKey) {
        return qos2InboxPrefix(scopedInboxId).concat(QOS2MSG).concat(msgKey);
    }

    public static ByteString buildMsgKey(InboxMessage message) {
        TopicMessage userMsg = message.getMsg();
        return userMsg.getPublisher().toByteString().concat(toByteString(userMsg.getMessage().getMessageId()));
    }

    public static long parseSeq(ByteString scopedInboxId, ByteString inboxMsgKey) {
        return inboxMsgKey.substring(scopedInboxId.size() + 1).asReadOnlyByteBuffer().getLong();
    }

    public static long parseQoS2Index(ByteString scopedInboxId, ByteString qos2InboxIndexKey) {
        return qos2InboxIndexKey.substring(scopedInboxId.size() + 2).asReadOnlyByteBuffer().getLong();
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

    private static int toInt(ByteString b) {
        assert b.size() == Integer.BYTES;
        ByteBuffer buffer = b.asReadOnlyByteBuffer();
        return buffer.getInt();
    }

    private static long toLong(ByteString b) {
        assert b.size() == Long.BYTES;
        ByteBuffer buffer = b.asReadOnlyByteBuffer();
        return buffer.getLong();
    }
}
