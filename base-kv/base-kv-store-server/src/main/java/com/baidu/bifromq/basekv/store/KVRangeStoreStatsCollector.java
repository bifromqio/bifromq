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

package com.baidu.bifromq.basekv.store;

import com.baidu.bifromq.basekv.localengine.rocksdb.RocksDBCPableKVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.rocksdb.RocksDBWALableKVEngineConfigurator;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.basekv.store.stats.StatsCollector;
import com.baidu.bifromq.basekv.store.util.ProcessUtil;
import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executor;

class KVRangeStoreStatsCollector extends StatsCollector {
    private final KVRangeStoreOptions opt;

    KVRangeStoreStatsCollector(KVRangeStoreOptions opt, Duration interval, Executor executor) {
        super(interval, executor);
        this.opt = opt;
        tick();
    }

    @Override
    protected void scrap(Map<String, Double> stats) {
        if (opt.getDataEngineConfigurator() instanceof RocksDBCPableKVEngineConfigurator conf) {
            File dbRootDir = new File(conf.dbRootDir());
            stats.put("db.usable", (double) dbRootDir.getUsableSpace());
            stats.put("db.total", (double) dbRootDir.getTotalSpace());
        }
        if (opt.getWalEngineConfigurator() instanceof RocksDBWALableKVEngineConfigurator conf) {
            File dbRootDir = new File(conf.dbRootDir());
            stats.put("wal.usable", (double) dbRootDir.getUsableSpace());
            stats.put("wal.total", (double) dbRootDir.getTotalSpace());
        }
        stats.put("cpu.usage", ProcessUtil.cpuLoad());
    }
}
