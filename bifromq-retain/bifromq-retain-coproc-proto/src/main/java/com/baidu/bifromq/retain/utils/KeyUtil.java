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

import static com.baidu.bifromq.util.TopicConst.MULTI_WILDCARD;
import static com.baidu.bifromq.util.TopicConst.NUL;
import static com.baidu.bifromq.util.TopicConst.SINGLE_WILDCARD;
import static com.baidu.bifromq.util.TopicUtil.escape;
import static com.baidu.bifromq.util.TopicUtil.fastJoin;
import static com.baidu.bifromq.util.TopicUtil.parse;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class KeyUtil {
    // the first byte indicating the version of KV schema for storing inbox data
    public static final ByteString SCHEMA_VER = ByteString.copyFrom(new byte[] {0x00});

    private static final int TENANT_ID_PREFIX_LENGTH = SCHEMA_VER.size() + Integer.BYTES;

    public static ByteString tenantNS(String tenantId) {
        ByteString tenantIdBS = unsafeWrap(tenantId.getBytes(StandardCharsets.UTF_8));
        return SCHEMA_VER.concat(toByteString(tenantIdBS.size())).concat(tenantIdBS);
    }

    public static ByteString retainKey(String tenantId, String topic) {
        return retainKey(tenantNS(tenantId), topic);
    }

    public static ByteString retainKey(ByteString tenantNS, String topic) {
        return tenantNS.concat(toByteString((short) parse(topic, false).size()))
            .concat(copyFromUtf8(escape(topic)));
    }

    public static List<String> filterPrefix(List<String> filterLevels) {
        int firstWildcard = filterLevels.indexOf(SINGLE_WILDCARD);
        if (firstWildcard == -1) {
            assert filterLevels.get(filterLevels.size() - 1).equals(MULTI_WILDCARD);
            firstWildcard = filterLevels.size() - 1;
        }
        return filterLevels.subList(0, firstWildcard);
    }

    public static ByteString retainKeyPrefix(String tenantId, short levels, List<String> filterPrefix) {
        return tenantNS(tenantId)
            .concat(toByteString(levels))
            .concat(copyFromUtf8(fastJoin(NUL, filterPrefix)));
    }

    public static ByteString retainKeyPrefix(ByteString tenantNS, List<String> topicFilterLevels) {
        ByteString prefix = ByteString.empty();
        short leastLevels = 0;
        boolean singleLevelWildcard = false;
        for (int i = 0; i < topicFilterLevels.size(); i++) {
            String tfl = topicFilterLevels.get(i);
            if ("+".equals(tfl)) {
                leastLevels++;
                singleLevelWildcard = true;
                continue;
            }
            if ("#".equals(tfl)) {
                break;
            }
            leastLevels++;
            if (!singleLevelWildcard) {
                prefix = prefix.concat(copyFromUtf8(tfl));
            }
            if (i + 1 < topicFilterLevels.size()) {
                if (!topicFilterLevels.get(i + 1).equals("#") && !singleLevelWildcard) {
                    prefix = prefix.concat(copyFromUtf8(NUL));
                }
            }
        }
        return tenantNS.concat(toByteString(leastLevels)).concat(prefix);
    }

    public static boolean isTenantNS(ByteString key) {
        return key.size() == TENANT_ID_PREFIX_LENGTH + tenantIdLength(key);
    }

    public static String parseTenantId(ByteString key) {
        return key.substring(TENANT_ID_PREFIX_LENGTH, TENANT_ID_PREFIX_LENGTH + tenantIdLength(key)).toStringUtf8();
    }

    public static ByteString parseTenantNS(ByteString key) {
        return key.substring(0, TENANT_ID_PREFIX_LENGTH + tenantIdLength(key));
    }

    private static int tenantIdLength(ByteString key) {
        return toInt(key.substring(SCHEMA_VER.size(), TENANT_ID_PREFIX_LENGTH));
    }

    public static List<String> parseTopic(ByteString retainKey) {
        String escapedTopic =
            retainKey.substring(TENANT_ID_PREFIX_LENGTH + tenantIdLength(retainKey) + Short.BYTES).toStringUtf8();
        return parse(escapedTopic, true);
    }

    static ByteString toByteString(int i) {
        return unsafeWrap(toBytes(i));
    }

    static ByteString toByteString(short s) {
        return unsafeWrap(toBytes(s));
    }

    static byte[] toBytes(short s) {
        return ByteBuffer.allocate(Short.BYTES).putShort(s).array();
    }

    static byte[] toBytes(int i) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(i).array();
    }

    static int toInt(ByteString b) {
        assert b.size() == Integer.BYTES;
        ByteBuffer buffer = b.asReadOnlyByteBuffer();
        return buffer.getInt();
    }
}
