/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.inbox.store.spi;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static com.baidu.bifromq.inbox.store.schema.KVSchemaUtil.inboxBucketStartKeyPrefix;
import static com.baidu.bifromq.inbox.store.schema.KVSchemaUtil.inboxStartKeyPrefix;
import static com.baidu.bifromq.inbox.store.schema.KVSchemaUtil.tenantBeginKeyPrefix;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.google.protobuf.ByteString;

/**
 * Utility for generating valid split keys for inbox store.
 */
public class SplitKey {
    /**
     * The key boundary of the tenant.
     *
     * @param tenantId the tenant id
     * @return the boundary
     */
    public static Boundary tenantBoundary(String tenantId) {
        ByteString tenantBeginKey = tenantBeginKeyPrefix(tenantId);
        return toBoundary(tenantBeginKey, upperBound(tenantBeginKey));
    }

    /**
     * The boundary of the inbox bucket.
     *
     * @param tenantId the tenant id
     * @param bucket   the bucket number
     * @return the boundary
     */
    public static Boundary inboxBucketBoundary(String tenantId, byte bucket) {
        ByteString inboxBucketStartKey = inboxBucketStartKeyPrefix(tenantId, bucket);
        return toBoundary(inboxBucketStartKey, upperBound(inboxBucketStartKey));
    }

    /**
     * The boundary of the inbox.
     *
     * @param tenantId the tenant id
     * @param inboxId  the inbox id
     * @return the boundary
     */
    public static Boundary inboxBoundary(String tenantId, String inboxId) {
        ByteString inboxStartKeyPrefix = inboxStartKeyPrefix(tenantId, inboxId);
        return toBoundary(inboxStartKeyPrefix, upperBound(inboxStartKeyPrefix));
    }
}
