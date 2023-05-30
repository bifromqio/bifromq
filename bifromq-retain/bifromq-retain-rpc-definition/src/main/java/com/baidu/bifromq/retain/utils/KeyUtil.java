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

package com.baidu.bifromq.retain.utils;

import static com.baidu.bifromq.retain.utils.TopicUtil.escape;
import static com.baidu.bifromq.retain.utils.TopicUtil.parse;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class KeyUtil {
    public static ByteString trafficNS(String trafficId) {
        ByteString trafficIdBS = unsafeWrap(trafficId.getBytes(StandardCharsets.UTF_8));
        return toByteString(trafficIdBS.size()).concat(trafficIdBS);
    }

    public static ByteString retainKey(ByteString trafficNS, String topic) {
        return trafficNS.concat(unsafeWrap(new byte[] {(byte) parse(topic, false).size()}))
            .concat(copyFromUtf8(escape(topic)));
    }

    public static ByteString retainKeyPrefix(ByteString trafficNS, List<String> topicFilterLevels) {
        ByteString prefix = ByteString.empty();
        byte leastLevels = 0;
        for (String tfl : topicFilterLevels) {
            if ("+".equals(tfl)) {
                leastLevels++;
                break;
            }
            if ("#".equals(tfl)) {
                break;
            }
            leastLevels++;
            prefix = prefix.concat(copyFromUtf8(tfl));
        }
        return trafficNS.concat(unsafeWrap(new byte[] {leastLevels})).concat(prefix);
    }

    public static boolean isTrafficNS(ByteString key) {
        int trafficIdLength = toInt(key.substring(0, Integer.BYTES));
        return key.size() == Integer.BYTES + trafficIdLength;
    }

    public static ByteString parseTrafficNS(ByteString key) {
        int trafficIdLength = toInt(key.substring(0, Integer.BYTES));
        return key.substring(0, Integer.BYTES + trafficIdLength);
    }

    public static List<String> parseTopic(ByteString retainKey) {
        int trafficIdLength = toInt(retainKey.substring(0, Integer.BYTES));
        String escapedTopic = retainKey.substring(Integer.BYTES + trafficIdLength + 1).toStringUtf8();
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
