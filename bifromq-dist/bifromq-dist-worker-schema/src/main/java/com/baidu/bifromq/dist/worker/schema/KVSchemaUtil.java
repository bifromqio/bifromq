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

package com.baidu.bifromq.dist.worker.schema;

import static com.baidu.bifromq.dist.worker.schema.RouteDetail.RouteType.NormalReceiver;
import static com.baidu.bifromq.dist.worker.schema.RouteDetail.RouteType.OrderedReceiverGroup;
import static com.baidu.bifromq.dist.worker.schema.RouteDetail.RouteType.UnorderedReceiverGroup;
import static com.baidu.bifromq.util.BSUtil.toByteString;
import static com.baidu.bifromq.util.BSUtil.toShort;
import static com.baidu.bifromq.util.TopicConst.DELIMITER;
import static com.baidu.bifromq.util.TopicConst.NUL;
import static com.baidu.bifromq.util.TopicUtil.escape;
import static com.baidu.bifromq.util.TopicUtil.isNormalTopicFilter;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.baidu.bifromq.dist.rpc.proto.MatchRoute;
import com.baidu.bifromq.dist.rpc.proto.RouteGroup;
import com.baidu.bifromq.util.BSUtil;
import com.google.protobuf.ByteString;

/**
 * Utility for working with the data stored in dist worker.
 */
public class KVSchemaUtil {
    public static final ByteString SCHEMA_VER = ByteString.copyFrom(new byte[] {0x00});
    private static final int MAX_RECEIVER_BUCKETS = 0xFF; // one byte
    private static final byte FLAG_NORMAL = 0x01;
    private static final byte FLAG_UNORDERED = 0x02;
    private static final byte FLAG_ORDERED = 0x03;
    private static final ByteString SEPARATOR_BYTES = ByteString.copyFrom(new byte[] {0x00, 0x00});
    private static final ByteString FLAG_NORMAL_VAL = ByteString.copyFrom(new byte[] {FLAG_NORMAL});
    private static final ByteString FLAG_UNORDERED_VAL = ByteString.copyFrom(new byte[] {FLAG_UNORDERED});
    private static final ByteString FLAG_ORDERED_VAL = ByteString.copyFrom(new byte[] {FLAG_ORDERED});

    public static String toReceiverUrl(MatchRoute route) {
        return toReceiverUrl(route.getBrokerId(), route.getReceiverId(), route.getDelivererKey());
    }

    public static String toReceiverUrl(int subBrokerId, String receiverId, String delivererKey) {
        return subBrokerId + NUL + receiverId + NUL + delivererKey;
    }

    public static Receiver parseReceiver(String receiverUrl) {
        String[] parts = receiverUrl.split(NUL);
        return new Receiver(Integer.parseInt(parts[0]), parts[1], parts[2]);
    }

    public static Matching buildMatchRoute(ByteString routeKey, ByteString routeValue) {
        RouteDetail routeDetail = parseRouteDetail(routeKey);
        try {
            if (routeDetail.type() == NormalReceiver) {
                return new NormalMatching(routeDetail, BSUtil.toLong(routeValue));
            }
            return new GroupMatching(routeDetail, RouteGroup.parseFrom(routeValue).getMembersMap());
        } catch (Exception e) {
            throw new IllegalStateException("Unable to parse matching record", e);
        }
    }

    public static ByteString tenantBeginKey(String tenantId) {
        ByteString tenantIdBytes = copyFromUtf8(tenantId);
        return SCHEMA_VER.concat(toByteString((short) tenantIdBytes.size()).concat(tenantIdBytes));
    }

    public static ByteString tenantRouteStartKey(String tenantId, String escapedTopicFilter) {
        assert !escapedTopicFilter.contains(DELIMITER);
        return tenantBeginKey(tenantId).concat(copyFromUtf8(escapedTopicFilter)).concat(SEPARATOR_BYTES);
    }

    public static ByteString tenantRouteBucketStartKey(String tenantId, String escapedTopicFilter, byte bucket) {
        return tenantRouteStartKey(tenantId, escapedTopicFilter).concat(unsafeWrap(new byte[] {bucket}));
    }

    public static ByteString toNormalRouteKey(String tenantId, String topicFilter, String receiverUrl) {
        assert isNormalTopicFilter(topicFilter);
        return tenantRouteBucketStartKey(tenantId, escape(topicFilter), bucket(receiverUrl))
            .concat(FLAG_NORMAL_VAL)
            .concat(toReceiverBytes(receiverUrl));
    }

    public static ByteString toGroupRouteKey(String tenantId, String topicFilter) {
        assert !isNormalTopicFilter(topicFilter);
        SharedTopicFilter stf = SharedTopicFilter.from(topicFilter);
        return tenantRouteBucketStartKey(tenantId, escape(stf.topicFilter), bucket(stf.shareGroup))
            .concat(stf.ordered ? FLAG_ORDERED_VAL : FLAG_UNORDERED_VAL)
            .concat(toReceiverBytes(stf.shareGroup));
    }

    public static RouteDetail parseRouteDetail(ByteString routeKey) {
        // <VER><LENGTH_PREFIX_TENANT_ID><ESCAPED_TOPIC_FILTER><SEP><BUCKET_BYTE><FLAG_BYTE><LENGTH_SUFFIX_RECEIVER_BYTES>
        short tenantIdLen = tenantIdLen(routeKey);
        int tenantIdStartIdx = SCHEMA_VER.size() + Short.BYTES;
        int escapedTopicFilterStartIdx = tenantIdStartIdx + tenantIdLen;
        int receiverBytesLen = receiverBytesLen(routeKey);
        int receiverBytesStartIdx = routeKey.size() - Short.BYTES - receiverBytesLen;
        int receiverBytesEndIdx = routeKey.size() - Short.BYTES;
        int flagByteIdx = receiverBytesStartIdx - 1;
        int separatorBytesIdx = flagByteIdx - 1 - SEPARATOR_BYTES.size();
        String receiverInfo = routeKey.substring(receiverBytesStartIdx, receiverBytesEndIdx).toStringUtf8();
        byte flag = routeKey.byteAt(flagByteIdx);

        String tenantId = routeKey.substring(tenantIdStartIdx, escapedTopicFilterStartIdx).toStringUtf8();
        String escapedTopicFilter =
            routeKey.substring(escapedTopicFilterStartIdx, separatorBytesIdx).toStringUtf8();
        switch (flag) {
            case FLAG_NORMAL -> {
                return new RouteDetail(tenantId, escapedTopicFilter, NormalReceiver, receiverInfo);
            }
            case FLAG_UNORDERED -> {
                return new RouteDetail(tenantId, escapedTopicFilter, UnorderedReceiverGroup, receiverInfo);
            }
            case FLAG_ORDERED -> {
                return new RouteDetail(tenantId, escapedTopicFilter, OrderedReceiverGroup, receiverInfo);
            }
            default -> throw new UnsupportedOperationException("Unknown route type: " + flag);
        }
    }

    private static short tenantIdLen(ByteString routeKey) {
        return toShort(routeKey.substring(SCHEMA_VER.size(), SCHEMA_VER.size() + Short.BYTES));
    }

    private static short receiverBytesLen(ByteString routeKey) {
        return toShort(routeKey.substring(routeKey.size() - Short.BYTES));
    }

    private static ByteString toReceiverBytes(String receiver) {
        ByteString b = copyFromUtf8(receiver);
        return b.concat(toByteString((short) b.size()));
    }

    private static byte bucket(String receiver) {
        int hash = receiver.hashCode();
        return (byte) ((hash ^ (hash >>> 16)) & MAX_RECEIVER_BUCKETS);
    }

    public record Receiver(int subBrokerId, String receiverId, String delivererKey) {

    }
}
