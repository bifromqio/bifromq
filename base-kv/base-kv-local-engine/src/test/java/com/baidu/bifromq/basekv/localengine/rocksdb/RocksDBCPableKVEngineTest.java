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

package com.baidu.bifromq.basekv.localengine.rocksdb;

import com.baidu.bifromq.basekv.localengine.ICPableKVSpace;
import com.baidu.bifromq.basekv.localengine.IKVEngine;
import java.nio.file.Paths;
import lombok.SneakyThrows;

public class RocksDBCPableKVEngineTest extends AbstractRocksDBKVEngine2Test {
    private RocksDBCPableKVEngineConfigurator configurator;

    @SneakyThrows
    @Override
    protected void beforeStart() {
        super.beforeStart();
        String DB_NAME = "testDB";
        String DB_CHECKPOINT_DIR = "testDB_cp";
        configurator = RocksDBCPableKVEngineConfigurator.builder()
            .dbRootDir(Paths.get(dbRootDir.toString(), DB_NAME).toString())
            .dbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR).toString())
            .build();
    }

    @SneakyThrows
    @Override
    protected IKVEngine<? extends ICPableKVSpace> newEngine() {
        return new RocksDBCPableKVEngine(null, configurator);
    }
}
