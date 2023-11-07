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

package com.baidu.bifromq.basecrdt.core.internal;

import static com.baidu.bifromq.basecrdt.core.internal.ProtoUtils.toByteString;
import static java.lang.Long.toUnsignedString;

import com.baidu.bifromq.logger.FormatableLogger;
import com.baidu.bifromq.basecrdt.core.api.CRDTEngineOptions;
import com.baidu.bifromq.basecrdt.core.api.CRDTURI;
import com.baidu.bifromq.basecrdt.core.api.ICRDTEngine;
import com.baidu.bifromq.basecrdt.core.api.ICRDTOperation;
import com.baidu.bifromq.basecrdt.core.api.ICausalCRDT;
import com.baidu.bifromq.basecrdt.core.exception.CRDTNotFoundException;
import com.baidu.bifromq.basecrdt.proto.Replacement;
import com.baidu.bifromq.basecrdt.proto.Replica;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;

public final class InMemCRDTEngine implements ICRDTEngine {

    private enum State {
        CREATED, RUNNING, STOPPING, SHUTDOWN
    }

    private static final Logger log = FormatableLogger.getLogger(InMemCRDTEngine.class);

    private final long id;
    private final AtomicReference<State> status = new AtomicReference<>(State.CREATED);

    private final AtomicLong seqNo = new AtomicLong();
    private final Map<String, CausalCRDTInflater<?, ?, ?>> uriToCRDTInflater = Maps.newConcurrentMap();
    private final ScheduledExecutorService inflationExecutor;
    private final CRDTEngineOptions options;

    public InMemCRDTEngine(CRDTEngineOptions options) {
        this.options = options;
        this.id = options.id();
        inflationExecutor = options.inflationExecutor() != null ?
            options.inflationExecutor() : SharedInflationExecutor.getInstance();
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public Iterator<Replica> hosting() {
        checkServingState();
        return Iterators.transform(uriToCRDTInflater.values().iterator(), CausalCRDTInflater::id);
    }

    @Override
    public Replica host(String crdtURI) {
        return host(crdtURI, replicaAddr(id, seqNo.incrementAndGet()));
    }

    @Override
    public Replica host(String crdtURI, ByteString replicaId) {
        checkServingState();
        CRDTURI.checkURI(crdtURI);
        CausalCRDTInflater<?, ?, ?> crdt = uriToCRDTInflater.computeIfAbsent(crdtURI, key -> {
            Replica replica = Replica.newBuilder().setUri(crdtURI).setId(replicaId).build();
            IReplicaStateLattice lattice = new InMemReplicaStateLattice(replica,
                options.orHistoryExpireTime(), options.maxCompactionTime());

            return switch (CRDTURI.parseType(crdtURI)) {
                case aworset ->
                    new AWORSetInflater(id, replica, lattice, inflationExecutor, options.inflationInterval());
                case rworset ->
                    new RWORSetInflater(id, replica, lattice, inflationExecutor, options.inflationInterval());
                case ormap -> new ORMapInflater(id, replica, lattice, inflationExecutor, options.inflationInterval());
                case cctr -> new CCounterInflater(id, replica, lattice, inflationExecutor, options.inflationInterval());
                case dwflag -> new DWFlagInflater(id, replica, lattice, inflationExecutor, options.inflationInterval());
                case ewflag -> new EWFlagInflater(id, replica, lattice, inflationExecutor, options.inflationInterval());
                case mvreg -> new MVRegInflater(id, replica, lattice, inflationExecutor, options.inflationInterval());
            };
        });
        if (!crdt.id().getId().equals(replicaId)) {
            log.warn("Replica[{}] already host", crdt.id());
        }
        return crdt.id();
    }

    @Override
    public CompletableFuture<Void> stopHosting(String crdtURI) {
        checkServingState();
        CRDTURI.checkURI(crdtURI);
        CausalCRDTInflater<?, ?, ?> inflater = uriToCRDTInflater.remove(crdtURI);
        if (inflater != null) {
            return inflater.stop();
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <O extends ICRDTOperation, T extends ICausalCRDT<O>> Optional<T> get(String crdtURI) {
        checkServingState();
        CausalCRDTInflater<?, ?, ?> inflater = uriToCRDTInflater.get(crdtURI);
        if (inflater != null) {
            return Optional.of((T) inflater.getCRDT());
        }
        return Optional.empty();
    }

    @Override
    public Optional<Map<ByteString, NavigableMap<Long, Long>>> latticeEvents(String crdtURI) {
        checkServingState();
        CausalCRDTInflater<?, ?, ?> inflater = uriToCRDTInflater.get(crdtURI);
        if (inflater != null) {
            return Optional.of(inflater.latticeEvents());
        }
        return Optional.empty();
    }

    @Override
    public Optional<Map<ByteString, NavigableMap<Long, Long>>> historyEvents(String crdtURI) {
        checkServingState();
        CausalCRDTInflater<?, ?, ?> inflater = uriToCRDTInflater.get(crdtURI);
        if (inflater != null) {
            return Optional.of(inflater.historyEvents());
        }
        return Optional.empty();

    }

    @Override
    public CompletableFuture<Void> join(String crdtURI, Iterable<Replacement> delta) {
        checkServingState();
        CausalCRDTInflater<?, ?, ?> crdt = uriToCRDTInflater.get(crdtURI);
        if (crdt != null) {
            return crdt.join(delta);
        }
        return CompletableFuture.failedFuture(new CRDTNotFoundException());
    }

    @Override
    public CompletableFuture<Optional<Iterable<Replacement>>>
    delta(String crdtURI, Map<ByteString, NavigableMap<Long, Long>> coveredLatticeEvents,
          Map<ByteString, NavigableMap<Long, Long>> coveredHistoryEvents, int maxEvents) {
        checkServingState();
        CausalCRDTInflater<?, ?, ?> crdt = uriToCRDTInflater.get(crdtURI);
        if (crdt != null) {
            return crdt.delta(coveredLatticeEvents, coveredHistoryEvents, maxEvents);
        }
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public void start() {
        if (status.compareAndSet(State.CREATED, State.RUNNING)) {
            log.debug("Start CRDTEngine[{}]", toUnsignedString(id));
        }
    }

    @Override
    public void stop() {
        if (status.compareAndSet(State.RUNNING, State.STOPPING)) {
            log.debug("Shutting down CRDTEngine[{}]", toUnsignedString(id));
            log.debug("Stop all hosted CRDTs[{}]", toUnsignedString(id));
            stopAllCRDTs();
            uriToCRDTInflater.clear();
            status.set(State.SHUTDOWN);
            log.debug("CRDTEngine[{}] is terminated", toUnsignedString(id));
        }
    }

    private void stopAllCRDTs() {
        List<CompletableFuture<Void>> stopFutures = new ArrayList<>();
        uriToCRDTInflater.forEach((uri, inflater) -> stopFutures.add(inflater.stop()));
        CompletableFuture.allOf(stopFutures.toArray(new CompletableFuture[] {})).join();
    }

    private static ByteString replicaAddr(long hostId, long rId) {
        return toByteString(hostId).concat(Varint.encodeLong(rId));
    }

    private void checkServingState() {
        Preconditions.checkState(status.get() == State.RUNNING, "Not started");
    }
}
