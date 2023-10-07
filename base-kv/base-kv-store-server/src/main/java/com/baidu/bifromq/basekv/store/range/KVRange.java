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

import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.METADATA_RANGE_BOUND_BYTES;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.METADATA_STATE_BYTES;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.METADATA_VER_BYTES;

import com.baidu.bifromq.basekv.localengine.IKVSpace;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeSnapshot;
import com.baidu.bifromq.basekv.proto.State;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.api.IKVWriter;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.Getter;
import lombok.SneakyThrows;

public class KVRange extends AbstractKVRangeMetadata implements IKVRange {
    @Getter
    private final IKVSpace kvSpace;
    private final ILoadTracker loadTracker;
    private final ConcurrentLinkedQueue<IKVReader> sharedDataReaders = new ConcurrentLinkedQueue<>();

    KVRange(IKVSpace kvSpace, ILoadTracker loadTracker) {
        super(kvSpace);
        this.kvSpace = kvSpace;
        this.loadTracker = loadTracker;
    }

    @Override
    public Observable<KVRangeMeta> metadata() {
        return kvSpace.metadata().map(metadataMap -> {
            long version = version(metadataMap.get(METADATA_VER_BYTES));
            State state = state(metadataMap.get(METADATA_STATE_BYTES));
            Boundary boundary = boundary(metadataMap.get(METADATA_RANGE_BOUND_BYTES));
            return new KVRangeMeta(version, state, boundary);
        }).distinctUntilChanged();
    }

    @Override
    public KVRangeSnapshot checkpoint() {
        String checkpointId = kvSpace.checkpoint();
        IKVRangeReader kvRangeCheckpoint = new KVRangeCheckpoint(kvSpace.open(checkpointId).get());
        KVRangeSnapshot.Builder builder = KVRangeSnapshot.newBuilder()
            .setVer(kvRangeCheckpoint.version())
            .setId(id)
            .setCheckpointId(checkpointId)
            .setLastAppliedIndex(kvRangeCheckpoint.lastAppliedIndex())
            .setState(kvRangeCheckpoint.state())
            .setBoundary(kvRangeCheckpoint.boundary());
        return builder.build();
    }

    @Override
    public boolean hasCheckpoint(KVRangeSnapshot checkpoint) {
        assert checkpoint.getId().equals(id);
        return checkpoint.hasCheckpointId() && kvSpace.open(checkpoint.getCheckpointId()).isPresent();
    }

    @Override
    public IKVRangeReader open(KVRangeSnapshot checkpoint) {
        return new KVRangeCheckpoint(kvSpace.open(checkpoint.getCheckpointId()).get());
    }

    @SneakyThrows
    @Override
    public final IKVReader borrowDataReader() {
        IKVReader reader = sharedDataReaders.poll();
        if (reader == null) {
            return newDataReader();
        }
        reader.refresh();
        return reader;
    }

    @Override
    public final void returnDataReader(IKVReader borrowed) {
        sharedDataReaders.add(borrowed);
    }

    @Override
    public IKVReader newDataReader() {
        return new KVReader(kvSpace, this, loadTracker);
    }

    @Override
    public IKVRangeWriter<?> toWriter() {
        return new KVRangeWriter(kvSpace.toWriter(), loadTracker);
    }

    @Override
    public IKVReseter toReseter(KVRangeSnapshot snapshot) {
        IKVRangeWriter<?> rangeWriter = toWriter();
        IKVWriter kvWriter = rangeWriter
            .resetVer(snapshot.getVer())
            .lastAppliedIndex(snapshot.getLastAppliedIndex())
            .state(snapshot.getState())
            .boundary(snapshot.getBoundary())
            .kvWriter();
        kvWriter.clear(boundary());
        return new IKVReseter() {
            @Override
            public void put(ByteString key, ByteString value) {
                kvWriter.put(key, value);
            }

            @Override
            public IKVRange abort() {
                rangeWriter.abort();
                return KVRange.this;
            }

            @Override
            public IKVRange done() {
                rangeWriter.done();
                return KVRange.this;
            }
        };
    }

    @Override
    public void destroy() {
        kvSpace.destroy();
    }
}
