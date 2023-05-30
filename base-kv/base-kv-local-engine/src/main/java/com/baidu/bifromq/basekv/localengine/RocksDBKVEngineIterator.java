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

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.google.protobuf.ByteString;
import java.lang.ref.Cleaner;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.Snapshot;

@Slf4j
class RocksDBKVEngineIterator implements IKVEngineIterator {
    private static final Cleaner CLEANER = Cleaner.create();

    private static class NativeState implements Runnable {
        private final RocksIterator itr;
        private final ReadOptions readOptions;

        private NativeState(RocksIterator itr, ReadOptions readOptions) {
            this.itr = itr;
            this.readOptions = readOptions;
        }

        @Override
        public void run() {
            itr.close();
            readOptions.close();
        }
    }

    private final RocksIterator rocksIterator;
    private final Cleaner.Cleanable onClose;

    RocksDBKVEngineIterator(RocksDB db, ColumnFamilyHandle cfHandle,
                            @Nullable ByteString start, @Nullable ByteString end) {
        this(db, null, cfHandle, start, end);
    }

    RocksDBKVEngineIterator(RocksDB db,
                            Snapshot snapshot,
                            ColumnFamilyHandle cfHandle,
                            @Nullable ByteString start,
                            @Nullable ByteString end) {
        ReadOptions readOptions = new ReadOptions();
        if (snapshot != null) {
            readOptions.setSnapshot(snapshot);
        }
        if (start != null) {
            readOptions.setIterateLowerBound(new Slice(start.toByteArray()));
        }
        if (end != null) {
            readOptions.setIterateUpperBound(new Slice(end.toByteArray()));
        }
        rocksIterator = db.newIterator(cfHandle, readOptions);
        onClose = CLEANER.register(this, new NativeState(rocksIterator, readOptions));
    }

    @Override
    public ByteString key() {
        return unsafeWrap(rocksIterator.key());
    }

    @Override
    public ByteString value() {
        return unsafeWrap(rocksIterator.value());
    }

    @Override
    public boolean isValid() {
        return rocksIterator.isValid();
    }

    @Override
    public void next() {
        rocksIterator.next();
    }

    @Override
    public void prev() {
        rocksIterator.prev();
    }

    @Override
    public void seekToFirst() {
        rocksIterator.seekToFirst();
    }

    @Override
    public void seekToLast() {
        rocksIterator.seekToLast();
    }

    @Override
    public void seek(ByteString target) {
        rocksIterator.seek(target.toByteArray());
    }

    @Override
    public void seekForPrev(ByteString target) {
        rocksIterator.seekForPrev(target.toByteArray());
    }

    @Override
    public void refresh() {
        try {
            rocksIterator.refresh();
        } catch (Throwable e) {
            throw new KVEngineException("Unable to refresh iterator", e);
        }
    }

    @Override
    public void close() {
        onClose.clean();
    }
}
