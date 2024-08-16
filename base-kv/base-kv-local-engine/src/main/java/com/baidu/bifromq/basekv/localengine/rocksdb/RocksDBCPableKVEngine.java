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

package com.baidu.bifromq.basekv.localengine.rocksdb;

import com.baidu.bifromq.basekv.localengine.KVEngineException;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.io.File;
import java.nio.file.Files;

public class RocksDBCPableKVEngine
    extends
    RocksDBKVEngine<RocksDBCPableKVEngine, RocksDBCPableKVSpace, RocksDBCPableKVEngineConfigurator> {
    private final File cpRootDir;
    private final RocksDBCPableKVEngineConfigurator configurator;
    private MetricManager metricManager;

    public RocksDBCPableKVEngine(String overrideIdentity,
                                 RocksDBCPableKVEngineConfigurator configurator) {
        super(overrideIdentity, configurator);
        cpRootDir = new File(configurator.dbCheckpointRootDir());
        this.configurator = configurator;
        try {
            Files.createDirectories(cpRootDir.getAbsoluteFile().toPath());
        } catch (Throwable e) {
            throw new KVEngineException("Failed to create checkpoint root folder", e);
        }
    }

    @Override
    protected RocksDBCPableKVSpace buildKVSpace(String spaceId,
                                                RocksDBCPableKVEngineConfigurator configurator,
                                                Runnable onDestroy,
                                                String... tags) {
        return new RocksDBCPableKVSpace(spaceId, configurator, this, onDestroy, tags);
    }

    @Override
    protected void doStart(String... metricTags) {
        super.doStart(metricTags);
        metricManager = new MetricManager(metricTags);
    }

    @Override
    protected void doStop() {
        metricManager.close();
        super.doStop();
    }

    private class MetricManager {
        private final Gauge checkpointTotalSpaceGauge;
        private final Gauge checkpointsUsableSpaceGauge;

        MetricManager(String... metricTags) {
            Tags tags = Tags.of(metricTags);
            checkpointTotalSpaceGauge =
                Gauge.builder("basekv.le.rocksdb.total.checkpoints", cpRootDir::getTotalSpace)
                    .tags(tags)
                    .register(Metrics.globalRegistry);
            checkpointsUsableSpaceGauge = Gauge.builder("basekv.le.rocksdb.usable.checkpoints",
                    cpRootDir::getUsableSpace)
                .tags(tags)
                .register(Metrics.globalRegistry);
        }

        void close() {
            Metrics.globalRegistry.remove(checkpointTotalSpaceGauge);
            Metrics.globalRegistry.remove(checkpointsUsableSpaceGauge);
        }
    }
}
