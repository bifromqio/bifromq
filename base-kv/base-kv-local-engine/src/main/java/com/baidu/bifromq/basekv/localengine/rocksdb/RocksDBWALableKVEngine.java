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

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

public class RocksDBWALableKVEngine
    extends RocksDBKVEngine<RocksDBWALableKVEngine, RocksDBWALableKVSpace, RocksDBWALableKVEngineConfigurator> {
    private final RocksDBWALableKVEngineConfigurator configurator;

    public RocksDBWALableKVEngine(String overrideIdentity, RocksDBWALableKVEngineConfigurator configurator) {
        super(overrideIdentity, configurator);
        this.configurator = configurator;
    }

    @Override
    protected RocksDBWALableKVSpace buildKVSpace(String spaceId, ColumnFamilyDescriptor cfDesc,
                                                 ColumnFamilyHandle cfHandle, RocksDB db, Runnable onDestroy,
                                                 String... tags) {
        return new RocksDBWALableKVSpace(spaceId, cfDesc, cfHandle, db, configurator, this, onDestroy, tags);
    }
}
