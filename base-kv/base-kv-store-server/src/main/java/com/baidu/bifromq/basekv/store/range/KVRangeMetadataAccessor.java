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

import static com.baidu.bifromq.basekv.Constants.EMPTY_RANGE;
import static com.baidu.bifromq.basekv.localengine.IKVEngine.DEFAULT_NS;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.lastAppliedIndexKey;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.rangeKey;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.stateKey;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.verKey;

import com.baidu.bifromq.basekv.localengine.IKVEngine;
import com.baidu.bifromq.basekv.localengine.IKVEngineIterator;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.Range;
import com.baidu.bifromq.basekv.proto.State;
import com.baidu.bifromq.basekv.store.exception.KVRangeStoreException;
import com.baidu.bifromq.basekv.store.util.KVUtil;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.util.Optional;
import java.util.function.Function;

class KVRangeMetadataAccessor implements IKVRangeMetadataAccessor {
    private final KVRangeId rangeId;
    private final IKVEngine kvEngine;
    private final Function<ByteString, Optional<ByteString>> getter;
    private final BehaviorSubject<IKVRangeState.KVRangeMeta> metaSubject = BehaviorSubject.create();
    private volatile int dataBoundId;
    private volatile long ver;
    private volatile State state;
    private volatile Range range;
    private volatile Range dataBound;

    KVRangeMetadataAccessor(KVRangeId rangeId, IKVEngine kvEngine) {
        this(rangeId, kvEngine, key -> kvEngine.get(IKVEngine.DEFAULT_NS, key));
    }

    KVRangeMetadataAccessor(KVRangeId rangeId, IKVEngine kvEngine, String checkpointId) {
        this(rangeId, kvEngine, key -> kvEngine.get(checkpointId, IKVEngine.DEFAULT_NS, key));
    }

    private KVRangeMetadataAccessor(KVRangeId rangeId, IKVEngine kvEngine,
                                    Function<ByteString, Optional<ByteString>> getter) {
        this.rangeId = rangeId;
        this.kvEngine = kvEngine;
        this.getter = getter;
        this.ver = readVersion();
        this.state = readState();
        this.range = readRange();
        this.dataBound = KVRangeKeys.dataBound(range);
        this.dataBoundId =
            kvEngine.registerKeyRange(IKVEngine.DEFAULT_NS, dataBound.getStartKey(), dataBound.getEndKey());
        if (ver > -1) {
            this.metaSubject.onNext(new IKVRangeState.KVRangeMeta(ver, state, range));
        }
    }

    public long version() {
        return ver;
    }

    private long readVersion() {
        try {
            return getter.apply(KVRangeKeys.verKey(rangeId)).map(KVUtil::toLongNativeOrder).orElse(-1L);
        } catch (Throwable e) {
            throw new KVRangeStoreException("Unable to parse version data", e);
        }
    }

    public long lastAppliedIndex() {
        return getter.apply(KVRangeKeys.lastAppliedIndexKey(rangeId)).map(KVUtil::toLong).orElse(-1L);
    }

    public Range range() {
        return range;
    }

    private Range readRange() {
        try {
            Optional<ByteString> rangeData = getter.apply(KVRangeKeys.rangeKey(rangeId));
            if (rangeData.isPresent()) {
                return Range.parseFrom(rangeData.get());
            }
            return EMPTY_RANGE;
        } catch (Throwable e) {
            throw new KVRangeStoreException("Unable to parse range data", e);
        }
    }

    public int dataBoundId() {
        return dataBoundId;
    }

    public Range dataBound() {
        return dataBound;
    }

    public State state() {
        return state;
    }

    public Observable<IKVRangeState.KVRangeMeta> source() {
        return metaSubject;
    }

    private State readState() {
        try {
            Optional<ByteString> stateData = getter.apply(KVRangeKeys.stateKey(rangeId));
            if (stateData.isPresent()) {
                return State.parseFrom(stateData.get());
            }
            return State.newBuilder().setType(State.StateType.Normal).build();
        } catch (Throwable e) {
            throw new KVRangeStoreException("Unable to parse state data", e);
        }
    }

    public void refresh() {
        long currentVer = readVersion();
        State currentState = readState();
        if (ver < currentVer || !state.equals(currentState)) {
            if (ver <= 0 || currentVer % 2 == 1) {
                range = readRange();
                dataBound = KVRangeKeys.dataBound(range);
                kvEngine.unregisterKeyRange(dataBoundId);
                dataBoundId =
                    kvEngine.registerKeyRange(IKVEngine.DEFAULT_NS, dataBound.getStartKey(), dataBound.getEndKey());
            }
            state = readState();
            ver = currentVer;
            this.metaSubject.onNext(new IKVRangeState.KVRangeMeta(ver, state, range));
        }
    }

    public void destroy(boolean includeData) {
        refresh();
        int batchId = kvEngine.startBatch();
        kvEngine.delete(batchId, DEFAULT_NS, verKey(rangeId));
        kvEngine.delete(batchId, DEFAULT_NS, rangeKey(rangeId));
        kvEngine.delete(batchId, DEFAULT_NS, lastAppliedIndexKey(rangeId));
        kvEngine.delete(batchId, DEFAULT_NS, stateKey(rangeId));
        if (includeData && !range.equals(EMPTY_RANGE)) {
            try (IKVEngineIterator itr = kvEngine.newIterator(dataBoundId)) {
                for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                    kvEngine.delete(batchId, dataBoundId, itr.key());
                }
            }
        }
        kvEngine.endBatch(batchId);
        kvEngine.unregisterKeyRange(dataBoundId);
        metaSubject.onComplete();
    }
}
