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

package com.baidu.bifromq.basekv.localengine.metrics;

import io.micrometer.core.instrument.Meter;

public enum KVSpaceMetric {
    BlockCache("basekv.le.rocksdb.mem.blockcache", Meter.Type.GAUGE),
    TableReader("basekv.le.rocksdb.mem.tablereader", Meter.Type.GAUGE),
    MemTable("basekv.le.rocksdb.mem.memtable", Meter.Type.GAUGE),
    PinnedMem("basekv.le.rocksdb.mem.pinned", Meter.Type.GAUGE),
    CheckpointNumGauge("basekv.le.active.checkpoints", Meter.Type.GAUGE),
    CheckpointTimer("basekv.le.rocksdb.checkpoint.time", Meter.Type.TIMER),
    CompactionCounter("basekv.le.rocksdb.compaction.count", Meter.Type.COUNTER),
    CompactionTimer("basekv.le.rocksdb.compaction.time", Meter.Type.TIMER),
    TotalKeysGauge("basekv.le.rocksdb.compaction.keys", Meter.Type.GAUGE),
    TotalTombstoneKeysGauge("basekv.le.rocksdb.compaction.delkeys", Meter.Type.GAUGE),
    TotalTombstoneRangesGauge("basekv.le.rocksdb.compaction.delranges", Meter.Type.GAUGE),
    FlushTimer("basekv.le.rocksdb.flush.time", Meter.Type.TIMER),
    CallTimer("basekv.le.call.time", Meter.Type.TIMER);

    public final String metricName;
    public final Meter.Type meterType;

    KVSpaceMetric(String metricName, Meter.Type meterType) {
        this.metricName = metricName;
        this.meterType = meterType;
    }
}
