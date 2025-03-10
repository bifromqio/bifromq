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

package com.baidu.bifromq.dist.worker.spi;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static com.baidu.bifromq.dist.worker.schema.KVSchemaUtil.tenantRouteStartKey;
import static com.baidu.bifromq.dist.worker.schema.KVSchemaUtil.tenantBeginKey;
import static com.baidu.bifromq.util.TopicUtil.escape;
import static com.baidu.bifromq.util.TopicUtil.isNormalTopicFilter;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.dist.worker.schema.SharedTopicFilter;
import com.google.protobuf.ByteString;

/**
 * Utility for generating valid split key for dist worker.
 */
public final class SplitKey {
    /**
     * The key boundary of the tenant.
     *
     * @param tenantId the tenant id
     * @return the boundary
     */
    public static Boundary tenantBoundary(String tenantId) {
        ByteString tenantBeginKey = tenantBeginKey(tenantId);
        return toBoundary(tenantBeginKey, upperBound(tenantBeginKey));
    }

    /**
     * The boundary of the route range for given topic filter.
     *
     * @param tenantId    the tenant id
     * @param topicFilter the topic filter
     * @return the boundary
     */
    public static Boundary routeBoundary(String tenantId, String topicFilter) {
        ByteString routeStartKey;
        if (isNormalTopicFilter(topicFilter)) {
            routeStartKey = tenantRouteStartKey(tenantId, escape(topicFilter));
        } else {
            SharedTopicFilter stf = SharedTopicFilter.from(topicFilter);
            routeStartKey = tenantRouteStartKey(tenantId, escape(stf.topicFilter));
        }
        return toBoundary(routeStartKey, upperBound(routeStartKey));
    }
}
