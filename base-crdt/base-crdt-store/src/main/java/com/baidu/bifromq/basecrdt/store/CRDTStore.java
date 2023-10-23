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

package com.baidu.bifromq.basecrdt.store;

import static com.baidu.bifromq.basecrdt.util.Formatter.toStringifiable;
import static java.lang.Long.toUnsignedString;

import com.baidu.bifromq.basecrdt.core.api.ICRDTEngine;
import com.baidu.bifromq.basecrdt.core.api.ICRDTOperation;
import com.baidu.bifromq.basecrdt.core.api.ICausalCRDT;
import com.baidu.bifromq.basecrdt.proto.Replica;
import com.baidu.bifromq.basecrdt.store.compressor.Compressor;
import com.baidu.bifromq.basecrdt.store.proto.AckMessage;
import com.baidu.bifromq.basecrdt.store.proto.CRDTStoreMessage;
import com.baidu.bifromq.basecrdt.store.proto.DeltaMessage;
import com.baidu.bifromq.basecrdt.store.proto.MessagePayload;
import com.baidu.bifromq.basecrdt.util.Formatter;
import com.baidu.bifromq.logger.FormatableLogger;
import com.baidu.bifromq.logger.LogFormatter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;

class CRDTStore implements ICRDTStore {
    static {
        LogFormatter.setStringifier(Replica.class, Formatter::toString);
        LogFormatter.setStringifier(DeltaMessage.class, Formatter::toString);
        LogFormatter.setStringifier(AckMessage.class, Formatter::toString);
    }

    private class MetricManager {
        final Gauge objectNumGauge;


        MetricManager(Tags tags) {
            objectNumGauge = Gauge.builder("basecrdt.objectnum", CRDTStore.this,
                    r -> r.antiEntropyByURI.values().size())
                .tags(tags)
                .register(Metrics.globalRegistry);

        }

        void close() {
            Metrics.globalRegistry.removeByPreFilterId(objectNumGauge.getId());
        }
    }

    private enum State {
        INIT, STARTING, STARTED, STOPPING, STOPPED
    }

    private static final Logger log = FormatableLogger.getLogger(CRDTStore.class);
    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
    private final CRDTStoreOptions options;
    private final ICRDTEngine engine;
    private final Subject<CRDTStoreMessage> storeMsgPublisher = PublishSubject.<CRDTStoreMessage>create()
        .toSerialized();
    private final Map<String, AntiEntropyManager> antiEntropyByURI = Maps.newConcurrentMap();
    private final CompositeDisposable disposable = new CompositeDisposable();
    private final ScheduledExecutorService storeExecutor;
    private final Compressor compressor;
    private final MetricManager metricManager;

    public CRDTStore(CRDTStoreOptions options) {
        this.options = options;
        engine = ICRDTEngine.newInstance(options.engineOptions);
        storeExecutor = options.storeExecutor() != null ?
            options.storeExecutor() : SharedAntiEntropyExecutor.getInstance();
        metricManager = new MetricManager(Tags.of("store.id", toUnsignedString(engine.id())));
        compressor = Compressor.newInstance(options.compressAlgorithm);
    }

    @Override
    public long id() {
        return engine.id();
    }

    @Override
    public Replica host(String crdtURI) {
        checkState();
        return engine.host(crdtURI);
    }

    @Override
    public Replica host(String crdtURI, ByteString replicaId) {
        checkState();
        return engine.host(crdtURI, replicaId);
    }

    @Override
    public CompletableFuture<Void> stopHosting(String uri) {
        checkState();
        antiEntropyByURI.computeIfPresent(uri, (k, v) -> {
            v.stop();
            return null;
        });
        return engine.stopHosting(uri);
    }

    @Override
    public Iterator<Replica> hosting() {
        checkState();
        return engine.hosting();
    }

    @Override
    public <O extends ICRDTOperation, T extends ICausalCRDT<O>> Optional<T> get(String uri) {
        checkState();
        return engine.get(uri);
    }

    @Override
    public void join(String uri, ByteString localAddr, Set<ByteString> cluster) {
        checkState();
        // make sure local address is a cluster member
        cluster.add(localAddr);
        // ensure localAddr is a member
        Optional<ICausalCRDT<ICRDTOperation>> crdt = engine.get(uri);
        if (crdt.isEmpty()) {
            throw new IllegalArgumentException("CRDT not found");
        }
        antiEntropyByURI.compute(uri, (k, v) -> {
            if (v == null) {
                log.debug("Replica[{}] bind to address[{}]", crdt.get().id(), toStringifiable(localAddr));
                v = new AntiEntropyManager(crdt.get(), localAddr, engine, storeExecutor,
                    storeMsgPublisher, options.maxEventsInDelta, compressor);
            } else if (!v.getLocalAddr().equals(localAddr)) {
                log.debug("Replica[{}] relocate to new address[{}] from address[{}]",
                    crdt.get().id(), toStringifiable(localAddr), toStringifiable(v.getLocalAddr()));
                v.stop();
                v = new AntiEntropyManager(crdt.get(), localAddr, engine, storeExecutor,
                    storeMsgPublisher, options.maxEventsInDelta, compressor);
            }
            return v;
        }).setCluster(cluster);
    }

    @Override
    public Optional<ByteString> localAddr(String uri) {
        AntiEntropyManager aeMgr = antiEntropyByURI.get(uri);
        if (aeMgr != null) {
            return Optional.of(aeMgr.getLocalAddr());
        }
        return Optional.empty();
    }

    @Override
    public Optional<Set<ByteString>> cluster(String uri) {
        AntiEntropyManager aeMgr = antiEntropyByURI.get(uri);
        if (aeMgr != null) {
            return Optional.of(aeMgr.cluster());
        }
        return Optional.empty();
    }

    @Override
    public void sync(String uri, ByteString peerAddr) {
        antiEntropyByURI.computeIfPresent(uri, (k, v) -> {
            v.resetIfNeighbor(peerAddr);
            return v;
        });
    }

    @Override
    public Observable<CRDTStoreMessage> storeMessages() {
        checkState();
        return storeMsgPublisher;
    }

    @Override
    public void start(Observable<CRDTStoreMessage> replicaMessages) {
        if (state.compareAndSet(State.INIT, State.STARTING)) {
            disposable.add(replicaMessages
                .subscribeOn(Schedulers.computation())
                .subscribe(msg -> {
                    if (started()) {
                        handleStoreMessage(msg);
                    }
                }));
            engine.start();
            state.set(State.STARTED);
            log.debug("Started CRDTStore[{}]", toUnsignedString(engine.id()));
        } else {
            log.warn("Start more than one time");
        }
    }

    @Override
    public void stop() {
        if (state.compareAndSet(State.STARTED, State.STOPPED)) {
            log.debug("Stop CRDTStore[{}]", toUnsignedString(engine.id()));
            antiEntropyByURI.forEach((uri, aaMgr) -> aaMgr.stop());
            antiEntropyByURI.clear();
            metricManager.close();
            disposable.dispose();
            engine.stop();
            state.set(State.STOPPED);
        }
    }

    private void handleStoreMessage(CRDTStoreMessage msg) {
        AntiEntropyManager antiEntropyMgr = antiEntropyByURI.get(msg.getUri());
        if (antiEntropyMgr != null) {
            log.trace("Anti-entropy manager of crdt[{}] bind to addr[{}], receive message from addr[{}]:\n{}",
                msg.getUri(), toStringifiable(msg.getReceiver()), toStringifiable(msg.getSender()), msg);
            MessagePayload payload = MessagePayloadUtil.decompress(compressor, msg);
            switch (payload.getMsgTypeCase()) {
                case DELTA -> antiEntropyMgr.join(payload.getDelta(), msg.getSender());
                case ACK -> antiEntropyMgr.receive(payload.getAck(), msg.getSender());
            }
        } else {
            log.debug("No anti-entropy manager of crdt[{}] bind to addr[{}], ignore the message from addr[{}]",
                msg.getUri(), toStringifiable(msg.getReceiver()), toStringifiable(msg.getSender()));
        }
    }

    private boolean started() {
        return state.get() == State.STARTED;
    }

    private void checkState() {
        Preconditions.checkState(started(), "Not started");
    }
}
