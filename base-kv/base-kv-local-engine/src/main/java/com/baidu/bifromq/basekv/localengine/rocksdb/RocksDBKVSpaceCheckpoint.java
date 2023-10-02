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

import static com.baidu.bifromq.basekv.localengine.IKVEngine.DEFAULT_NS;
import static com.baidu.bifromq.basekv.localengine.rocksdb.Keys.toMetaKey;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.baidu.bifromq.basekv.localengine.ISyncContext;
import com.baidu.bifromq.basekv.localengine.KVEngineException;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Tags;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

@Slf4j
class RocksDBKVSpaceCheckpoint extends RocksDBKVSpaceReader {
    private final String cpId;
    private final DBOptions dbOptions;
    private final RocksDB roDB;
    private final ColumnFamilyDescriptor cfDesc;
    private final ColumnFamilyHandle cfHandle;

    RocksDBKVSpaceCheckpoint(String id,
                             String cpId,
                             File cpDir,
                             RocksDBKVEngineConfigurator configurator,
                             String... tags) {
        super(id, Tags.of(tags));
        this.cpId = cpId;
        try {
            dbOptions = configurator.config();
            cfDesc = new ColumnFamilyDescriptor(id.getBytes(), configurator.config(id));

            List<ColumnFamilyDescriptor> cfDescs = new ArrayList<>();
            cfDescs.add(new ColumnFamilyDescriptor(DEFAULT_NS.getBytes()));
            cfDescs.add(cfDesc);
            List<ColumnFamilyHandle> handles = new ArrayList<>();
            roDB = RocksDB.openReadOnly(dbOptions, cpDir.getAbsolutePath(), cfDescs, handles);

            cfHandle = handles.get(1);
        } catch (RocksDBException e) {
            throw new KVEngineException("Failed to open checkpoint", e);
        }
    }

    String cpId() {
        return cpId;
    }

    @Override
    protected Optional<ByteString> doMetadata(ByteString metaKey) {
        try {
            byte[] valBytes = roDB.get(cfHandle(), toMetaKey(metaKey));
            if (valBytes == null) {
                return Optional.empty();
            }
            return Optional.of(unsafeWrap(valBytes));
        } catch (RocksDBException e) {
            throw new KVEngineException("Failed to read metadata", e);
        }
    }

    void close() {
        super.close();
        roDB.destroyColumnFamilyHandle(cfHandle);
        cfDesc.getOptions().close();
        roDB.close();
        dbOptions.close();
    }

    @Override
    protected RocksDB db() {
        return roDB;
    }

    @Override
    protected ColumnFamilyHandle cfHandle() {
        return cfHandle;
    }

    @Override
    protected ISyncContext.IRefresher newRefresher() {
        return new ISyncContext.IRefresher() {

            @Override
            public void runIfNeeded(Runnable runnable) {
                // no need to do any refresh, since it's readonly
            }

            @Override
            public <T> T call(Supplier<T> supplier) {
                return supplier.get();
            }
        };
    }
}
