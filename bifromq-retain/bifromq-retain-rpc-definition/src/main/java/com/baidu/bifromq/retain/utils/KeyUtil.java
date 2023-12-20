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

package com.baidu.bifromq.retain.utils;

import static com.baidu.bifromq.retain.utils.TopicUtil.NUL;
import static com.baidu.bifromq.retain.utils.TopicUtil.escape;
import static com.baidu.bifromq.retain.utils.TopicUtil.parse;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class KeyUtil {
    public static ByteString tenantNS(String tenantId) {
        ByteString tenantIdBS = unsafeWrap(tenantId.getBytes(StandardCharsets.UTF_8));
        return toByteString(tenantIdBS.size()).concat(tenantIdBS);
    }

    public static ByteString retainKey(ByteString tenantNS, String topic) {
        return tenantNS.concat(unsafeWrap(new byte[] {(byte) parse(topic, false).size()}))
            .concat(copyFromUtf8(escape(topic)));
    }

    public static ByteString retainKeyPrefix(ByteString tenantNS, List<String> topicFilterLevels) {
        ByteString prefix = ByteString.empty();
        byte leastLevels = 0;
        for (int i = 0; i < topicFilterLevels.size(); i++) {
            String tfl = topicFilterLevels.get(i);
            if ("+".equals(tfl)) {
                leastLevels++;
                break;
            }
            if ("#".equals(tfl)) {
                break;
            }
            leastLevels++;
            prefix = prefix.concat(copyFromUtf8(tfl));
            if (i + 1 < topicFilterLevels.size()) {
                switch (topicFilterLevels.get(i + 1)) {
                    case "+", "#" -> {
                    }
                    default -> prefix = prefix.concat(copyFromUtf8(NUL));
                }
            }
        }
        return tenantNS.concat(unsafeWrap(new byte[] {leastLevels})).concat(prefix);
    }

    public static boolean isTenantNS(ByteString key) {
        int tenantIdLength = toInt(key.substring(0, Integer.BYTES));
        return key.size() == Integer.BYTES + tenantIdLength;
    }

    public static String parseTenantId(ByteString key) {
        int tenantIdLength = toInt(key.substring(0, Integer.BYTES));
        return key.substring(Integer.BYTES, Integer.BYTES + tenantIdLength).toStringUtf8();
    }

    public static ByteString parseTenantNS(ByteString key) {
        int tenantIdLength = toInt(key.substring(0, Integer.BYTES));
        return key.substring(0, Integer.BYTES + tenantIdLength);
    }

    public static List<String> parseTopic(ByteString retainKey) {
        int tenantIdLength = toInt(retainKey.substring(0, Integer.BYTES));
        String escapedTopic = retainKey.substring(Integer.BYTES + tenantIdLength + 1).toStringUtf8();
        return parse(escapedTopic, true);
    }

    private static ByteString toByteString(int i) {
        return unsafeWrap(toBytes(i));
    }

    private static byte[] toBytes(int i) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(i).array();
    }

    private static int toInt(ByteString b) {
        assert b.size() == Integer.BYTES;
        ByteBuffer buffer = b.asReadOnlyByteBuffer();
        return buffer.getInt();
    }
}
