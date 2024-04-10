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

package com.baidu.bifromq.mqtt.utils;

public class MessageIdUtil {

    private static final long FLUSH_FLAG_MASK = 0x80000000L;
    private static final long MESSAGE_SEQ_MASK = 0x7FFFFFFFL;
    private static final int INTEGER_BITS = 32;

    public static long syncWindowSequence(long nowMillis, long syncWindowIntervalMillis) {
        return nowMillis / syncWindowIntervalMillis;
    }

    public static long messageId(long syncWindowSequence, long messageSequence) {
        return (syncWindowSequence << INTEGER_BITS) | messageSequence;
    }

    public static long messageId(long syncWindowSequence, long messageSequence, boolean drainFlag) {
        return (syncWindowSequence << INTEGER_BITS) | messageSequence | (drainFlag ? FLUSH_FLAG_MASK : 0);
    }

    public static boolean isFlushFlagSet(long messageId) {
        return (messageId & FLUSH_FLAG_MASK) != 0;
    }

    public static long syncWindowSequence(long messageId) {
        return messageId >> INTEGER_BITS;
    }

    public static long messageSequence(long messageId) {
        return messageId & MESSAGE_SEQ_MASK;
    }

    public static boolean isSuccessive(long messageId, long successorMessageId) {
        if (messageId + 1 == successorMessageId) {
            return true;
        }
        long syncWindowSequence = syncWindowSequence(messageId);
        long successorSyncWindowSequence = syncWindowSequence(successorMessageId);
        if (successorSyncWindowSequence < syncWindowSequence) {
            return false;
        }
        if (syncWindowSequence == successorSyncWindowSequence ||
            syncWindowSequence + 1 == successorSyncWindowSequence) {
            return (messageSequence(messageId) + 1) % FLUSH_FLAG_MASK == messageSequence(successorMessageId);
        }
        return messageSequence(successorMessageId) == 0;
    }

    public static long previousMessageId(long messageId) {
        long syncWindowSequence = syncWindowSequence(messageId);
        long messageSequence = messageSequence(messageId);
        if (messageSequence == 0) {
            return messageId(syncWindowSequence - 1, MESSAGE_SEQ_MASK);
        }
        return messageId - 1;
    }
}
