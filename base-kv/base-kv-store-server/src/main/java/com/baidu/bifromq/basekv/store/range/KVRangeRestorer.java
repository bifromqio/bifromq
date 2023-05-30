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

import static com.baidu.bifromq.basekv.localengine.IKVEngine.DEFAULT_NS;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.dataKey;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.lastAppliedIndexKey;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.rangeKey;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.stateKey;
import static com.baidu.bifromq.basekv.store.range.KVRangeKeys.verKey;
import static com.baidu.bifromq.basekv.store.util.KVUtil.toByteString;

import com.baidu.bifromq.basekv.localengine.IKVEngine;
import com.baidu.bifromq.basekv.proto.KVPair;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeSnapshot;
import com.baidu.bifromq.basekv.store.util.KVUtil;

public class KVRangeRestorer implements IKVRangeRestorer {
    private final IKVEngine kvEngine;
    private final IKVRangeMetadataAccessor metadata;
    private final KVRangeStateAccessor.KVRangeWriterMutator mutator;
    private final int batchId;

    public KVRangeRestorer(KVRangeSnapshot checkpoint,
                           IKVRangeMetadataAccessor metadata,
                           IKVEngine kvEngine,
                           KVRangeStateAccessor.KVRangeWriterMutator mutator) {
        this.kvEngine = kvEngine;
        this.metadata = metadata;
        this.mutator = mutator;
        metadata.refresh();
        this.batchId = kvEngine.startBatch();
        KVRangeId rangeId = checkpoint.getId();
        kvEngine.put(batchId, DEFAULT_NS, verKey(rangeId),
            KVUtil.toByteStringNativeOrder(checkpoint.getVer()));
        kvEngine.put(batchId, DEFAULT_NS, lastAppliedIndexKey(rangeId), toByteString(checkpoint.getLastAppliedIndex()));
        kvEngine.put(batchId, DEFAULT_NS, rangeKey(rangeId), checkpoint.getRange().toByteString());
        kvEngine.put(batchId, DEFAULT_NS, stateKey(rangeId), checkpoint.getState().toByteString());
        kvEngine.clearSubRange(batchId, metadata.dataBoundId(),
            metadata.dataBound().getStartKey(),
            metadata.dataBound().getEndKey());
    }

    @Override
    public void add(KVPair kvPair) {
        kvEngine.put(batchId, DEFAULT_NS, dataKey(kvPair.getKey()), kvPair.getValue());
    }

    @Override
    public void abort() {
        kvEngine.abortBatch(batchId);
    }

    @Override
    public int size() {
        return kvEngine.batchSize(batchId);
    }

    @Override
    public void close() {
        mutator.run(() -> {
            kvEngine.endBatch(batchId);
            metadata.refresh();
        });
    }
}
