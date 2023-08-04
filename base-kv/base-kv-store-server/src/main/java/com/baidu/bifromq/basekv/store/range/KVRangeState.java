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
import com.baidu.bifromq.basekv.proto.KVRangeSnapshot;
import com.baidu.bifromq.basekv.proto.Range;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import io.reactivex.rxjava3.core.Observable;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class KVRangeState implements IKVRangeState {
    private static final ILoadTracker DUMMY_TRACKER = (key, loadUnits) -> {
    };
    private final KVRangeId rangeId;
    private final IKVEngine kvEngine;
    private final KVRangeMetadataAccessor metadata;
    private final KVRangeStateAccessor accessor;
    private final ConcurrentLinkedQueue<IKVRangeReader> sharedReaders = new ConcurrentLinkedQueue<>();
    private final ILoadTracker loadTracker;

    KVRangeState(KVRangeSnapshot snapshot, IKVEngine kvEngine, ILoadTracker loadTracker) {
        this(snapshot.getId(), kvEngine, loadTracker);
        reset(snapshot).close();
    }

    KVRangeState(KVRangeId id, IKVEngine kvEngine, ILoadTracker loadTracker) {
        this.rangeId = id;
        this.kvEngine = kvEngine;
        this.loadTracker = loadTracker;
        this.metadata = new KVRangeMetadataAccessor(rangeId, kvEngine);
        this.accessor = new KVRangeStateAccessor();
    }

    @Override
    public KVRangeSnapshot checkpoint() {
        String checkpointId = kvEngine.checkpoint();
        KVRangeMetadataAccessor metadata = new KVRangeMetadataAccessor(rangeId, kvEngine, checkpointId);
        KVRangeSnapshot.Builder builder = KVRangeSnapshot.newBuilder()
            .setVer(metadata.version())
            .setId(rangeId)
            .setCheckpointId(checkpointId)
            .setLastAppliedIndex(metadata.lastAppliedIndex())
            .setState(metadata.state())
            .setRange(metadata.range());
        metadata.close();
        return builder.build();
    }

    @Override
    public boolean hasCheckpoint(KVRangeSnapshot checkpoint) {
        assert checkpoint.getId().equals(rangeId);
        return checkpoint.hasCheckpointId() && kvEngine.hasCheckpoint(checkpoint.getCheckpointId());
    }

    @Override
    public IKVIterator open(KVRangeSnapshot checkpoint) {
        assert hasCheckpoint(checkpoint);
        KVRangeMetadataAccessor metadata =
            new KVRangeMetadataAccessor(checkpoint.getId(), kvEngine, checkpoint.getCheckpointId());
        Range dataBound = metadata.dataBound();
        IKVEngineIterator dataIterator = kvEngine.newIterator(checkpoint.getCheckpointId(),
            metadata.dataBoundId(), dataBound.getStartKey(), dataBound.getEndKey());
        return new KVRangeIterator(DUMMY_TRACKER, () -> dataIterator, () -> {
            dataIterator.close();
            metadata.close();
        });
    }

    @SneakyThrows
    @Override
    public IKVRangeReader borrow() {
        IKVRangeReader reader = sharedReaders.poll();
        if (reader == null) {
            return new KVRangeReader(rangeId, kvEngine, accessor.refresher(), loadTracker);
        }
        reader.refresh();
        return reader;
    }

    @Override
    public void returnBorrowed(IKVRangeReader reader) {
        sharedReaders.add(reader);
    }

    @Override
    public IKVRangeReader getReader(boolean trackingLoad) {
        return new KVRangeReader(rangeId, kvEngine, accessor.refresher(), trackingLoad ? loadTracker : DUMMY_TRACKER);
    }

    @Override
    public IKVRangeWriter getWriter(boolean trackingLoad) {
        return new KVRangeWriter(rangeId, metadata, kvEngine, accessor.mutator(),
            trackingLoad ? loadTracker : DUMMY_TRACKER);
    }

    @Override
    public Observable<KVRangeMeta> metadata() {
        return metadata.source();
    }

    @Override
    public IKVRangeRestorer reset(KVRangeSnapshot checkpoint) {
        assert rangeId.equals(checkpoint.getId());
        return new KVRangeRestorer(checkpoint, metadata, kvEngine, accessor.mutator());
    }

    @Override
    public void destroy(boolean includeData) {
        metadata.destroy(includeData);
    }
}
