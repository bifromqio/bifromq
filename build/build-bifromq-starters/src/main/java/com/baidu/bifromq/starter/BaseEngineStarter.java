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

package com.baidu.bifromq.starter;

import com.baidu.bifromq.basekv.localengine.InMemoryKVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.KVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.RocksDBKVEngineConfigurator;
import com.baidu.bifromq.starter.config.StarterConfig;
import com.baidu.bifromq.starter.config.standalone.model.InMemEngineConfig;
import com.baidu.bifromq.starter.config.standalone.model.RocksDBEngineConfig;
import com.baidu.bifromq.starter.config.standalone.model.StorageEngineConfig;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BaseEngineStarter<T extends StarterConfig> extends BaseStarter<T> {
    public static final String USER_DIR_PROP = "user.dir";
    public static final String DATA_DIR_PROP = "DATA_DIR";

    protected KVEngineConfigurator<?> buildEngineConf(StorageEngineConfig config, String name) {
        if (config instanceof InMemEngineConfig) {
            return InMemoryKVEngineConfigurator.builder()
                .gcInterval(config.getGcIntervalInSec())
                .build();
        } else {
            Path dataRootDir;
            Path dataCheckpointRootDir;
            RocksDBEngineConfig rocksDBConfig = (RocksDBEngineConfig) config;
            if (Paths.get(rocksDBConfig.getDataPathRoot()).isAbsolute()) {
                dataRootDir = Paths.get(rocksDBConfig.getDataPathRoot(), name);
                dataCheckpointRootDir =
                    Paths.get(rocksDBConfig.getDataPathRoot(), name + "_cp");
            } else {
                String userDir = System.getProperty(USER_DIR_PROP);
                String dataDir = System.getProperty(DATA_DIR_PROP, userDir);
                dataRootDir = Paths.get(dataDir, rocksDBConfig.getDataPathRoot(), name);
                dataCheckpointRootDir =
                    Paths.get(dataDir, rocksDBConfig.getDataPathRoot(), name + "_cp");
            }
            return RocksDBKVEngineConfigurator.builder()
                .dbRootDir(dataRootDir.toString())
                .dbCheckpointRootDir(dataCheckpointRootDir.toString())
                .gcInterval(config.getGcIntervalInSec())
                .compactMinTombstoneKeys(rocksDBConfig.getCompactMinTombstoneKeys())
                .compactTombstonePercent(rocksDBConfig.getCompactTombstonePercent())
                .build();
        }
    }

}
