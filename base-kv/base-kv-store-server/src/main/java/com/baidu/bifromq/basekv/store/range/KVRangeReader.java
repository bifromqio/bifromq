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

package com.baidu.bifromq.basekv.store.range;

import com.baidu.bifromq.basekv.localengine.IKVEngine;
import com.baidu.bifromq.basekv.localengine.IKVEngineIterator;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.Range;
import com.baidu.bifromq.basekv.proto.State;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class KVRangeReader implements IKVRangeReader {
    private static final Cleaner CLEANER = Cleaner.create();

    private record CleanableState(KVRangeMetadataAccessor metadataAccessor) implements Runnable {

        @Override
        public void run() {
            metadataAccessor.close();
        }
    }

    private final ILoadTracker loadTracker;
    private final IKVEngine kvEngine;
    private final KVRangeMetadataAccessor metadata;
    private final AtomicReference<IKVEngineIterator[]> engineIterator = new AtomicReference<>();
    private final KVRangeStateAccessor.KVRangeReaderRefresher refresher;
    private final Cleaner.Cleanable onClose;

    KVRangeReader(KVRangeId rangeId,
                  IKVEngine engine,
                  KVRangeStateAccessor.KVRangeReaderRefresher refresher,
                  ILoadTracker loadTracker) {
        this.kvEngine = engine;
        this.refresher = refresher;
        this.loadTracker = loadTracker;
        refresher.lock();
        try {
            this.metadata = new KVRangeMetadataAccessor(rangeId, engine);
            engineIterator.set(new IKVEngineIterator[] {
                engine.newIterator(metadata.dataBoundId()),
                engine.newIterator(metadata.dataBoundId())
            });
        } finally {
            refresher.unlock();
        }
        onClose = CLEANER.register(this, new CleanableState(metadata));
    }

    @Override
    public long ver() {
        return metadata.version();
    }

    @Override
    public State state() {
        return metadata.state();
    }

    @Override
    public long lastAppliedIndex() {
        return metadata.lastAppliedIndex();
    }

    @Override
    public IKVReader kvReader() {
        return new KVReader(metadata, kvEngine, engineIterator::get, loadTracker);
    }

    @Override
    public void refresh() {
        Range orig = metadata.dataBound();
        refresher.run(() -> {
            metadata.refresh();
            Range now = metadata.dataBound();
            if (!orig.equals(now)) {
                for (IKVEngineIterator itr : engineIterator.getAndSet(
                    new IKVEngineIterator[] {
                        kvEngine.newIterator(metadata.dataBoundId()),
                        kvEngine.newIterator(metadata.dataBoundId())
                    })) {
                    itr.close();
                }
            } else {
                for (IKVEngineIterator itr : engineIterator.get()) {
                    itr.refresh();
                }
            }
        });
    }

    @Override
    public void close() {
        onClose.clean();
    }
}
