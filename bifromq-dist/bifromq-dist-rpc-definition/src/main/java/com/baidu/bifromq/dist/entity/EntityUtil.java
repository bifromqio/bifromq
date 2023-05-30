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

package com.baidu.bifromq.dist.entity;

import static com.baidu.bifromq.dist.util.TopicUtil.NUL;
import static com.baidu.bifromq.dist.util.TopicUtil.escape;
import static com.baidu.bifromq.dist.util.TopicUtil.isNormalTopicFilter;
import static com.baidu.bifromq.dist.util.TopicUtil.parseSharedTopic;
import static com.baidu.bifromq.dist.util.TopicUtil.unescape;
import static com.google.protobuf.ByteString.copyFromUtf8;

import com.baidu.bifromq.dist.rpc.proto.MatchRecord;
import com.baidu.bifromq.dist.util.TopicUtil;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class EntityUtil {
    private static final ByteString INFIX_SUBINFO_INFIX = copyFromUtf8("0");
    private static final ByteString INFIX_MATCH_RECORD_INFIX = copyFromUtf8("1");
    private static final ByteString INFIX_UPPERBOUND_INFIX = copyFromUtf8("2");
    private static final ByteString FLAG_NORMAL = copyFromUtf8("0");
    private static final ByteString FLAG_UNORDERD_SHARE = copyFromUtf8("1");
    private static final ByteString FLAG_ORDERD_SHARE = copyFromUtf8("2");

    public static String toQualifiedInboxId(int inboxBrokerId, String inboxId, String inboxGroupKey) {
        String scoped = inboxBrokerId + NUL + inboxId + NUL + inboxGroupKey;
        return Base64.getEncoder().encodeToString(scoped.getBytes(StandardCharsets.UTF_8));
    }

    public static int parseSubBroker(String scopedInboxId) {
        scopedInboxId = new String(Base64.getDecoder().decode(scopedInboxId), StandardCharsets.UTF_8);
        int splitIdx = scopedInboxId.indexOf(NUL);
        return Integer.parseInt(scopedInboxId.substring(0, splitIdx));
    }

    public static String parseInboxId(String scopedInboxId) {
        scopedInboxId = new String(Base64.getDecoder().decode(scopedInboxId), StandardCharsets.UTF_8);
        int splitIdx = scopedInboxId.indexOf(NUL);
        return scopedInboxId.substring(splitIdx + 1, scopedInboxId.lastIndexOf(NUL));
    }

    public static ByteString trafficPrefix(String trafficId) {
        return copyFromUtf8(trafficId + NUL);
    }

    public static ByteString trafficUpperBound(String trafficId) {
        return trafficPrefix(trafficId).concat(INFIX_UPPERBOUND_INFIX);
    }

    public static ByteString subInfoKeyPrefix(String trafficId) {
        return trafficPrefix(trafficId).concat(INFIX_SUBINFO_INFIX);
    }

    public static ByteString subInfoKey(String trafficId, String scopedInboxId) {
        return subInfoKeyPrefix(trafficId).concat(copyFromUtf8(scopedInboxId));
    }

    public static boolean isSubInfoKey(ByteString rawKey) {
        String keyStr = rawKey.toStringUtf8();
        int firstSplit = keyStr.indexOf(NUL);
        return keyStr.substring(firstSplit + 1, firstSplit + 2).equals(INFIX_SUBINFO_INFIX.toStringUtf8());
    }

    public static String parseTrafficId(ByteString rawKey) {
        String keyStr = rawKey.toStringUtf8();
        int firstSplit = keyStr.indexOf(NUL);
        String trafficId = keyStr.substring(0, firstSplit);
        return trafficId;
    }

    public static String parseTrafficId(String rawKeyUtf8) {
        int firstSplit = rawKeyUtf8.indexOf(NUL);
        String trafficId = rawKeyUtf8.substring(0, firstSplit);
        return trafficId;
    }

    public static Inbox parseInbox(ByteString subInfoKey) {
        String subInfoKeyStr = subInfoKey.toStringUtf8();
        int firstSplit = subInfoKeyStr.indexOf(NUL);
        String scopedInboxId = subInfoKeyStr.substring(firstSplit + 2);
        return new Inbox(scopedInboxId);
    }

    public static Matching parseMatchRecord(ByteString matchRecordKey, ByteString matchRecordValue) {
        // <trafficId><NUL><1><ESCAPED_TOPIC_FILTER><NUL><FLAG><SCOPED_INBOX|SHARE_GROUP>
        String matchRecordKeyStr = matchRecordKey.toStringUtf8();
        int lastSplit = matchRecordKeyStr.lastIndexOf(NUL);
        char flag = matchRecordKeyStr.charAt(lastSplit + 1);
        try {
            MatchRecord matchRecord = MatchRecord.parseFrom(matchRecordValue);
            switch (flag) {
                case '0':
                    assert matchRecord.hasNormal();
                    String scopedInbox = matchRecordKeyStr.substring(lastSplit + 2);
                    return new NormalMatching(matchRecordKey, scopedInbox, matchRecord.getNormal());
                case '1':
                case '2':
                default:
                    assert matchRecord.hasGroup();
                    String group = matchRecordKeyStr.substring(lastSplit + 2);
                    return new GroupMatching(matchRecordKey, group, flag == '2', matchRecord.getGroup().getEntryMap());
            }
        } catch (Exception e) {
            throw new IllegalStateException("Unable to parse matching record", e);
        }
    }

    public static ByteString matchRecordKeyPrefix(String trafficId) {
        return trafficPrefix(trafficId).concat(INFIX_MATCH_RECORD_INFIX);
    }

    public static ByteString matchRecordKey(String trafficId, String topicFilter, String qInboxId) {
        if (isNormalTopicFilter(topicFilter)) {
            return matchRecordKeyPrefix(trafficId)
                .concat(copyFromUtf8(escape(topicFilter) + NUL))
                .concat(FLAG_NORMAL)
                .concat(copyFromUtf8(qInboxId));
        } else {
            TopicUtil.SharedTopicFilter stf = parseSharedTopic(topicFilter);
            return matchRecordKeyPrefix(trafficId)
                .concat(copyFromUtf8(escape(stf.topicFilter) + NUL))
                .concat(stf.ordered ? FLAG_ORDERD_SHARE : FLAG_UNORDERD_SHARE)
                .concat(copyFromUtf8(stf.shareGroup));
        }
    }

    public static ByteString matchRecordTopicFilterPrefix(String trafficId, String escapedTopicFilter) {
        return matchRecordKeyPrefix(trafficId).concat(copyFromUtf8(escapedTopicFilter + NUL));
    }

    public static String parseTopicFilter(String matchRecordKeyStr) {
        // <trafficId><NUL><1><ESCAPED_TOPIC_FILTER><NUL><FLAG><SCOPED_INBOX|SHARE_GROUP>
        int firstSplit = matchRecordKeyStr.indexOf(NUL);
        int lastSplit = matchRecordKeyStr.lastIndexOf(NUL);
        return unescape(matchRecordKeyStr.substring(firstSplit + 2, lastSplit));
    }
}
