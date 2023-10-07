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

package com.baidu.bifromq.basekv.store.wal;

import static com.baidu.bifromq.basekv.store.wal.KVRangeWALKeys.KEY_LATEST_SNAPSHOT_BYTES;

import com.baidu.bifromq.basekv.localengine.IKVEngine;
import com.baidu.bifromq.basekv.localengine.IKVSpace;
import com.baidu.bifromq.basekv.localengine.KVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.KVEngineFactory;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.raft.IRaftStateStore;
import com.baidu.bifromq.basekv.raft.proto.Snapshot;
import com.baidu.bifromq.basekv.store.exception.KVRangeStoreException;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.extern.slf4j.Slf4j;

/**
 * store engine
 */
@NotThreadSafe
@Slf4j
public class KVRangeWALStorageEngine implements IKVRangeWALStoreEngine {
    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
    private final Map<KVRangeId, KVRangeWALStore> instances = Maps.newConcurrentMap();
    private final IKVEngine kvEngine;

    public KVRangeWALStorageEngine(String clusterId, String overrideIdentity, KVEngineConfigurator<?> configurator) {
        kvEngine = KVEngineFactory.create(overrideIdentity, configurator);
    }

    @Override
    public void stop() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            try {
                log.debug("Stopping WALStoreEngine[{}]", kvEngine.id());
                instances.values().forEach(KVRangeWALStore::stop);
                kvEngine.stop();
                state.set(State.STOPPED);
            } catch (Throwable e) {
                log.warn("Failed to stop wal engine", e);
            } finally {
                state.set(State.TERMINATED);
            }
        }
    }

    @Override
    public void start() {
        if (state.compareAndSet(State.INIT, State.STARTING)) {
            try {
                kvEngine.start("storeId", id(), "type", "wal");
                loadExisting();
                state.set(State.STARTED);
            } catch (Throwable e) {
                state.set(State.TERMINATED);
                throw new KVRangeStoreException("Failed to start wal engine", e);
            }
        }
    }

    @Override
    public Set<KVRangeId> allKVRangeIds() {
        checkState();
        return Sets.newHashSet(instances.keySet());
    }

    @Override
    public String id() {
        return kvEngine.id();
    }

    @Override
    public IRaftStateStore newRaftStateStorage(KVRangeId kvRangeId, Snapshot initSnapshot) {
        checkState();
        instances.computeIfAbsent(kvRangeId, id -> {
            IKVSpace kvSpace = kvEngine.createIfMissing(KVRangeIdUtil.toString(id));
            kvSpace.toWriter().put(KEY_LATEST_SNAPSHOT_BYTES, initSnapshot.toByteString())
                .done();
            kvSpace.flush().join();
            return new KVRangeWALStore(kvEngine.id(), kvRangeId, kvSpace);
        });
        return instances.get(kvRangeId);
    }

    @Override
    public boolean has(KVRangeId kvRangeId) {
        checkState();
        return instances.containsKey(kvRangeId);
    }

    @Override
    public IRaftStateStore get(KVRangeId kvRangeId) {
        checkState();
        return instances.get(kvRangeId);
    }

    @Override
    public long storageSize(KVRangeId kvRangeId) {
        checkState();
        return instances.get(kvRangeId).size();
    }

    @Override
    public void destroy(KVRangeId rangeId) {
        checkState();
        instances.computeIfPresent(rangeId, (k, v) -> {
            try {
                log.debug("Destroy range wal storage: storeId={}, rangeId={}", id(), KVRangeIdUtil.toString(rangeId));
                v.destroy();
            } catch (Throwable e) {
                log.error("Failed to destroy KVRangeWALStorage[{}]", KVRangeIdUtil.toString(rangeId), e);
            }
            return null;
        });
    }

    private void checkState() {
        Preconditions.checkState(state.get() == State.STARTED, "Not started");
    }

    private void loadExisting() {
        kvEngine.ranges().forEach((String id, IKVSpace kvSpace) -> {
            KVRangeId kvRangeId = KVRangeIdUtil.fromString(id);
            instances.put(kvRangeId, new KVRangeWALStore(kvEngine.id(), kvRangeId, kvSpace));
            log.debug("WAL loaded: kvRangeId={}", KVRangeIdUtil.toString(kvRangeId));

        });
    }

    private enum State {
        INIT, STARTING, STARTED, STOPPING, STOPPED, TERMINATED
    }
}
