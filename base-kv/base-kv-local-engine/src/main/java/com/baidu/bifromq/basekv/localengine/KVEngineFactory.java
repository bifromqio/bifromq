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

package com.baidu.bifromq.basekv.localengine;

import com.baidu.bifromq.basekv.localengine.memory.InMemKVEngine;
import com.baidu.bifromq.basekv.localengine.memory.InMemKVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.rocksdb.RocksDBKVEngine;
import com.baidu.bifromq.basekv.localengine.rocksdb.RocksDBKVEngineConfigurator;

public class KVEngineFactory {
    public static IKVEngine create(String overrideIdentity,
                                   KVEngineConfigurator<?> configurator) {
        if (configurator instanceof InMemKVEngineConfigurator) {
            return new InMemKVEngine(overrideIdentity, (InMemKVEngineConfigurator) configurator);
        }
        if (configurator instanceof RocksDBKVEngineConfigurator) {
            return new RocksDBKVEngine(overrideIdentity, (RocksDBKVEngineConfigurator) configurator);
        }
        throw new UnsupportedOperationException();
    }
}
