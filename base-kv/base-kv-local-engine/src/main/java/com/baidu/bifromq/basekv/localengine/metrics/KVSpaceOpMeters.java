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

package com.baidu.bifromq.basekv.localengine.metrics;

import static com.baidu.bifromq.basekv.localengine.metrics.KVSpaceMeters.getTimer;

import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

public class KVSpaceOpMeters {
    public final Timer metadataCallTimer;
    public final Timer sizeCallTimer;
    public final Timer existCallTimer;
    public final Timer getCallTimer;
    public final Timer iterNewCallTimer;
    public final Timer iterSeekCallTimer;
    public final Timer iterSeekForPrevCallTimer;
    public final Timer iterSeekToFirstCallTimer;
    public final Timer iterSeekToLastCallTimer;
    public final Timer iterNextCallTimer;
    public final Timer iterPrevCallTimer;
    public final Timer iterRefreshTimer;
    public final Timer batchWriteCallTimer;

    public KVSpaceOpMeters(String id, Tags tags) {
        metadataCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "metadata"));
        sizeCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "size"));
        existCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "exist"));
        getCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "get"));
        iterNewCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "newitr"));
        iterSeekCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "seek"));
        iterSeekForPrevCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "pseek"));
        iterSeekToFirstCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "fseek"));
        iterSeekToLastCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "lseek"));
        iterNextCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "next"));
        iterPrevCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "prev"));
        iterRefreshTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "refresh"));
        batchWriteCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "bwrite"));
    }

    public void close() {
        metadataCallTimer.close();
        sizeCallTimer.close();
        existCallTimer.close();
        getCallTimer.close();
        iterNewCallTimer.close();
        iterSeekCallTimer.close();
        iterSeekForPrevCallTimer.close();
        iterSeekToFirstCallTimer.close();
        iterSeekToLastCallTimer.close();
        iterNextCallTimer.close();
        iterPrevCallTimer.close();
        iterRefreshTimer.close();
        batchWriteCallTimer.close();
    }
}
