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

import static com.baidu.bifromq.util.BSUtil.toByteString;
import static com.baidu.bifromq.util.BSUtil.toInt;
import static com.baidu.bifromq.util.BSUtil.toShort;
import static com.baidu.bifromq.util.TopicConst.MULTI_WILDCARD;
import static com.baidu.bifromq.util.TopicConst.SINGLE_WILDCARD;
import static com.baidu.bifromq.util.TopicUtil.escape;
import static com.baidu.bifromq.util.TopicUtil.parse;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.google.protobuf.ByteString;
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

    private static ByteString retainKey(ByteString tenantNS, String topic) {
        List<String> topicLevels = parse(topic, false);
        short levels = (short) topicLevels.size();
        return tenantNS.concat(toByteString(levels))
            .concat(LevelHash.hash(topicLevels))
            .concat(copyFromUtf8(escape(topic)));
    }

    public static List<String> filterPrefix(List<String> filterLevels) {
        int firstWildcard = filterLevels.indexOf(SINGLE_WILDCARD);
        if (firstWildcard == -1) {
            if (filterLevels.get(filterLevels.size() - 1).equals(MULTI_WILDCARD)) {
                firstWildcard = filterLevels.size() - 1;
                return filterLevels.subList(0, firstWildcard);
            }
            return filterLevels;
        }
        return filterLevels.subList(0, firstWildcard);
    }

    public static ByteString retainKeyPrefix(String tenantId, short levels, List<String> filterPrefix) {
        return tenantNS(tenantId)
            .concat(toByteString(levels))
            .concat(LevelHash.hash(filterPrefix));
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

    public static short parseLevel(ByteString key) {
        assert !isTenantNS(key);
        int startIdx = TENANT_ID_PREFIX_LENGTH + tenantIdLength(key);
        return parseLevel(key, startIdx);
    }

    private static short parseLevel(ByteString key, int startIdx) {
        return toShort(key.substring(startIdx, startIdx + Short.BYTES));
    }

    public static ByteString parseLevelHash(ByteString key) {
        assert !isTenantNS(key);
        int tenantIdLength = tenantIdLength(key);
        short levels = parseLevel(key, TENANT_ID_PREFIX_LENGTH + tenantIdLength);
        int startIdx = TENANT_ID_PREFIX_LENGTH + tenantIdLength + Short.BYTES;
        return key.substring(startIdx, startIdx + levels);
    }

    private static int tenantIdLength(ByteString key) {
        return toInt(key.substring(SCHEMA_VER.size(), TENANT_ID_PREFIX_LENGTH));
    }

    public static List<String> parseTopic(ByteString retainKey) {
        int tenantIdLength = tenantIdLength(retainKey);
        short levels = toShort(retainKey.substring(TENANT_ID_PREFIX_LENGTH + tenantIdLength,
            TENANT_ID_PREFIX_LENGTH + tenantIdLength + Short.BYTES));
        String escapedTopic = retainKey.substring(TENANT_ID_PREFIX_LENGTH + tenantIdLength + Short.BYTES + levels)
            .toStringUtf8();
        return parse(escapedTopic, true);
    }

}
