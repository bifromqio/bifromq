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

import static com.google.protobuf.ByteString.unsignedLexicographicalComparator;

import com.baidu.bifromq.basekv.localengine.IKVSpace;
import com.baidu.bifromq.basekv.localengine.IKVSpaceReader;
import com.baidu.bifromq.basekv.localengine.IKVSpaceWriter;
import com.baidu.bifromq.basekv.localengine.ISyncContext;
import com.baidu.bifromq.basekv.localengine.KVSpaceDescriptor;
import com.baidu.bifromq.basekv.localengine.SyncContext;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Tags;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InMemKVSpace extends InMemKVSpaceReader implements IKVSpace {
    protected final String id;
    private final InMemKVEngine engine;
    private final Map<ByteString, ByteString> metadataMap = new ConcurrentHashMap<>();
    private final ConcurrentSkipListMap<ByteString, ByteString> rangeData =
        new ConcurrentSkipListMap<>(unsignedLexicographicalComparator());
    private final BehaviorSubject<Map<ByteString, ByteString>> metadataSubject = BehaviorSubject.create();
    private final Cache<String, InMemKVSpaceCheckpoint> checkpoints;
    private final ISyncContext syncContext = new SyncContext();
    private final ISyncContext.IRefresher metadataRefresher = syncContext.refresher();
    private final Runnable onDestroy;
    private volatile InMemKVSpaceCheckpoint latestCheckpoint;

    protected InMemKVSpace(String id,
                           InMemKVEngineConfigurator configurator,
                           InMemKVEngine engine,
                           Runnable onDestroy,
                           String... tags) {
        super(id, Tags.of(tags).and("from", "kvspace"));
        this.id = id;
        this.engine = engine;
        this.onDestroy = onDestroy;
        checkpoints = Caffeine.newBuilder().weakValues().build();
    }

    ISyncContext syncContext() {
        return syncContext;
    }

    @Override
    protected Map<ByteString, ByteString> metadataMap() {
        return metadataRefresher.call(() -> metadataMap);
    }

    @Override
    protected ConcurrentSkipListMap<ByteString, ByteString> rangeData() {
        return rangeData;
    }


    @Override
    public Observable<Map<ByteString, ByteString>> metadata() {
        return metadataSubject;
    }

    @Override
    public KVSpaceDescriptor describe() {
        return new KVSpaceDescriptor(id, collectStats());
    }

    private Map<String, Double> collectStats() {
        Map<String, Double> stats = new HashMap<>();
        stats.put("size", (double) size());
        // TODO: more stats
        return stats;
    }

    @Override
    public CompletableFuture<Long> flush() {
        return CompletableFuture.completedFuture(System.nanoTime());
    }

    @Override
    public void destroy() {
        metadataSubject.onComplete();
        onDestroy.run();
    }

    @Override
    public String checkpoint() {
        synchronized (this) {
            return metadataRefresher.call(() -> {
                String cpId = UUID.randomUUID().toString();
                latestCheckpoint = new InMemKVSpaceCheckpoint(id, cpId, new HashMap<>(metadataMap), rangeData.clone());
                checkpoints.put(cpId, latestCheckpoint);
                return cpId;
            });
        }
    }

    @Override
    public Optional<String> latestCheckpoint() {
        synchronized (this) {
            return Optional.ofNullable(latestCheckpoint == null ? null : latestCheckpoint.cpId());
        }
    }

    @Override
    public Optional<IKVSpaceReader> open(String checkpointId) {
        return Optional.ofNullable(checkpoints.getIfPresent(checkpointId));
    }

    @Override
    public IKVSpaceWriter toWriter() {
        return new InMemKVSpaceWriter(id, metadataMap, rangeData, engine, syncContext,
            metadataUpdated -> {
                if (metadataUpdated) {
                    this.loadMetadata();
                }
            });
    }

    IKVSpaceWriter toWriter(InMemKVSpaceWriterHelper helper) {
        return new InMemKVSpaceWriter(id, metadataMap, rangeData, engine, syncContext, helper,
            metadataUpdated -> {
                if (metadataUpdated) {
                    this.loadMetadata();
                }
            });
    }

    private void loadMetadata() {
        metadataRefresher.runIfNeeded(() -> {
            if (!metadataMap.isEmpty()) {
                metadataSubject.onNext(Collections.unmodifiableMap(new HashMap<>(metadataMap)));
            }
        });
    }
}
