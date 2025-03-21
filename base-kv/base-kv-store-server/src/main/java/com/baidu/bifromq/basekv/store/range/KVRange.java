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

package com.baidu.bifromq.basekv.store.range;

import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.METADATA_RANGE_BOUND_BYTES;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.METADATA_STATE_BYTES;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.METADATA_VER_BYTES;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.NULL_BOUNDARY;

import com.baidu.bifromq.basekv.localengine.ICPableKVSpace;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeSnapshot;
import com.baidu.bifromq.basekv.proto.State;
import com.baidu.bifromq.basekv.store.api.IKVCloseableReader;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.api.IKVWriter;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.Getter;
import lombok.SneakyThrows;

public class KVRange extends AbstractKVRangeMetadata implements IKVRange {
    @Getter
    private final ICPableKVSpace kvSpace;
    private final ConcurrentLinkedQueue<IKVCloseableReader> sharedDataReaders = new ConcurrentLinkedQueue<>();
    private final BehaviorSubject<KVRangeMeta> metaSubject;

    public KVRange(ICPableKVSpace kvSpace) {
        super(kvSpace);
        this.kvSpace = kvSpace;
        metaSubject = BehaviorSubject.createDefault(
            new IKVRange.KVRangeMeta(-1L, State.newBuilder().setType(State.StateType.NoUse).build(), NULL_BOUNDARY));
        kvSpace.metadata()
            .map(metadataMap -> {
                long version = version(metadataMap.get(METADATA_VER_BYTES));
                State state = state(metadataMap.get(METADATA_STATE_BYTES));
                Boundary boundary = boundary(metadataMap.get(METADATA_RANGE_BOUND_BYTES));
                return new IKVRange.KVRangeMeta(version, state, boundary);
            })
            .subscribe(metaSubject);
    }

    public KVRange(ICPableKVSpace kvSpace, KVRangeSnapshot snapshot) {
        this(kvSpace);
        toReseter(snapshot).done();
    }

    @Override
    public final long version() {
        return metaSubject.getValue().ver();
    }

    @Override
    public final State state() {
        return metaSubject.getValue().state();
    }

    @Override
    public final Boundary boundary() {
        return metaSubject.getValue().boundary();
    }

    @Override
    public Observable<KVRangeMeta> metadata() {
        return metaSubject;
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
    public IKVRangeCheckpointReader open(KVRangeSnapshot checkpoint) {
        return new KVRangeCheckpoint(kvSpace.open(checkpoint.getCheckpointId()).get());
    }

    @SneakyThrows
    @Override
    public final IKVReader borrowDataReader() {
        IKVReader reader = sharedDataReaders.poll();
        if (reader == null) {
            return newDataReader();
        }
        return reader;
    }

    @Override
    public final void returnDataReader(IKVReader borrowed) {
        sharedDataReaders.add((IKVCloseableReader) borrowed);
    }

    @Override
    public IKVCloseableReader newDataReader() {
        return new KVReader(kvSpace, this);
    }

    @Override
    public IKVRangeWriter<?> toWriter() {
        return new KVRangeWriter(kvSpace.toWriter());
    }

    @Override
    public IKVRangeWriter<?> toWriter(IKVLoadRecorder recorder) {
        return new LoadRecordableKVRangeWriter(kvSpace.toWriter(), recorder);
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
    public void close() {
        IKVCloseableReader reader;
        while ((reader = sharedDataReaders.poll()) != null) {
            reader.close();
        }
        metaSubject.onComplete();
    }

    @Override
    public void destroy() {
        kvSpace.destroy();
    }
}
