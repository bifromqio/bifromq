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

package com.baidu.bifromq.retain.store;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.intersect;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.isNULLRange;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static com.baidu.bifromq.metrics.TenantMetric.MqttRetainNumGauge;
import static com.baidu.bifromq.metrics.TenantMetric.MqttRetainSpaceGauge;
import static com.baidu.bifromq.retain.store.schema.KVSchemaUtil.tenantBeginKey;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.metrics.ITenantMeter;
import com.google.protobuf.ByteString;
import java.util.concurrent.atomic.AtomicLong;

public class TenantRetainedSet {
    private final AtomicLong topicCount = new AtomicLong();
    private final String tenantId;
    private final String[] tags;

    public TenantRetainedSet(String tenantId, IKVReader reader, String... tags) {
        this.tenantId = tenantId;
        this.tags = tags;
        ITenantMeter.gauging(tenantId, MqttRetainSpaceGauge, () -> {
            ByteString tenantBeginKey = tenantBeginKey(tenantId);
            Boundary tenantBoundary =
                intersect(toBoundary(tenantBeginKey, upperBound(tenantBeginKey)), reader.boundary());
            if (isNULLRange(tenantBoundary)) {
                return 0L;
            }
            return reader.size(tenantBoundary);
        }, tags);
        ITenantMeter.gauging(tenantId, MqttRetainNumGauge, topicCount::get, tags);
    }

    public long incrementTopicCount(int delta) {
        return topicCount.addAndGet(delta);
    }

    void destroy() {
        ITenantMeter.stopGauging(tenantId, MqttRetainSpaceGauge, tags);
        ITenantMeter.stopGauging(tenantId, MqttRetainNumGauge, tags);
    }
}
