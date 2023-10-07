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

package com.baidu.bifromq.basekv.localengine.memory;

import com.baidu.bifromq.basekv.localengine.IKVSpaceIterator;
import com.baidu.bifromq.basekv.localengine.IKVSpaceMetadataUpdatable;
import com.baidu.bifromq.basekv.localengine.IKVSpaceWriter;
import com.baidu.bifromq.basekv.localengine.ISyncContext;
import com.baidu.bifromq.basekv.localengine.KVEngineException;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Tags;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InMemKVSpaceWriter extends InMemKVSpaceReader implements IKVSpaceWriter {
    private final Map<ByteString, ByteString> metadataMap;
    private final ConcurrentSkipListMap<ByteString, ByteString> rangeData;
    private final InMemKVEngine engine;
    private final InMemKVSpaceWriterHelper helper;

    InMemKVSpaceWriter(String id,
                       Map<ByteString, ByteString> metadataMap,
                       ConcurrentSkipListMap<ByteString, ByteString> rangeData,
                       InMemKVEngine engine,
                       ISyncContext syncContext,
                       Consumer<Boolean> afterWrite,
                       String... tags) {
        this(id, metadataMap, rangeData, engine, syncContext, new InMemKVSpaceWriterHelper(), afterWrite, tags);
    }

    InMemKVSpaceWriter(String id,
                       Map<ByteString, ByteString> metadataMap,
                       ConcurrentSkipListMap<ByteString, ByteString> rangeData,
                       InMemKVEngine engine,
                       ISyncContext syncContext,
                       InMemKVSpaceWriterHelper writerHelper,
                       Consumer<Boolean> afterWrite,
                       String... tags) {
        super(id, Tags.of(tags));
        this.metadataMap = metadataMap;
        this.rangeData = rangeData;
        this.engine = engine;
        this.helper = writerHelper;
        writerHelper.addMutators(id, metadataMap, rangeData, syncContext.mutator());
        writerHelper.addAfterWriteCallback(id, afterWrite);
    }


    @Override
    public IKVSpaceWriter metadata(ByteString metaKey, ByteString metaValue) {
        helper.metadata(id, metaKey, metaValue);
        return this;
    }

    @Override
    public IKVSpaceWriter insert(ByteString key, ByteString value) {
        helper.insert(id, key, value);
        return this;
    }

    @Override
    public IKVSpaceWriter put(ByteString key, ByteString value) {
        helper.put(id, key, value);
        return this;
    }

    @Override
    public IKVSpaceWriter delete(ByteString key) {
        helper.delete(id, key);
        return this;
    }

    @Override
    public IKVSpaceWriter clear() {
        helper.clear(id, Boundary.getDefaultInstance());
        return this;
    }

    @Override
    public IKVSpaceWriter clear(Boundary boundary) {
        helper.clear(id, boundary);
        return this;
    }

    @Override
    public IKVSpaceMetadataUpdatable<?> migrateTo(String targetRangeId, Boundary boundary) {
        InMemKVSpace toKeyRange = (InMemKVSpace) engine.createIfMissing(targetRangeId);
        try {
            // move data
            try (IKVSpaceIterator itr = newIterator(boundary)) {
                for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                    helper.put(toKeyRange.id(), itr.key(), itr.value());
                }
            }
            // clear moved data in left range
            helper.clear(id, boundary);
            return toKeyRange.toWriter(helper);
        } catch (Throwable e) {
            throw new KVEngineException("Delete range in batch failed", e);
        }
    }

    @Override
    public IKVSpaceMetadataUpdatable<?> migrateFrom(String fromRangeId, Boundary boundary) {
        InMemKVSpace fromKeyRange = (InMemKVSpace) engine.createIfMissing(fromRangeId);
        helper.addMutators(fromKeyRange.id,
            fromKeyRange.metadataMap(),
            fromKeyRange.rangeData(),
            fromKeyRange.syncContext().mutator());

        try {
            // move data
            try (IKVSpaceIterator itr = fromKeyRange.newIterator(boundary)) {
                for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                    helper.put(id, itr.key(), itr.value());
                }
            }
            // clear moved data in right range
            helper.clear(fromKeyRange.id(), boundary);
            return fromKeyRange.toWriter(helper);
        } catch (Throwable e) {
            throw new KVEngineException("Delete range in batch failed", e);
        }
    }

    @Override
    public void done() {
        helper.done();
    }

    @Override
    public void abort() {
        helper.abort();

    }

    @Override
    public int count() {
        return helper.count();
    }

    @Override
    protected Map<ByteString, ByteString> metadataMap() {
        return metadataMap;
    }

    @Override
    protected ConcurrentSkipListMap<ByteString, ByteString> rangeData() {
        return rangeData;
    }
}
