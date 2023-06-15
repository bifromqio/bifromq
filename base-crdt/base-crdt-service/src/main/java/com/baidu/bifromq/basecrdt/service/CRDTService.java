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

package com.baidu.bifromq.basecrdt.service;

import static java.lang.Long.toUnsignedString;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.core.api.ICausalCRDT;
import com.baidu.bifromq.basecrdt.proto.Replica;
import com.baidu.bifromq.basecrdt.store.ICRDTStore;
import com.baidu.bifromq.basecrdt.store.proto.CRDTStoreMessage;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CRDTService implements ICRDTService {

    private enum State {
        INIT, STARTING, STARTED, STOPPING, SHUTDOWN
    }

    private final ICRDTStore store;
    private IAgentHost agentHost;
    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
    private final Map<String, CRDTContext> hostedCRDT = Maps.newConcurrentMap(); // key is the uri of crdt
    private final Subject<CRDTStoreMessage> incomingStoreMessages;
    private final ExecutorService executor = newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setNameFormat("crdt-service-scheduler")
        .build());
    private final Scheduler scheduler = Schedulers.from(executor);


    public CRDTService(CRDTServiceOptions options) {
        store = ICRDTStore.newInstance(options.storeOptions);
        incomingStoreMessages = PublishSubject.<CRDTStoreMessage>create().toSerialized();
    }

    @Override
    public long id() {
        return store.id();
    }

    @Override
    public Replica host(String uri) {
        checkState();
        CRDTContext crdtContext = hostedCRDT.computeIfAbsent(uri,
            k -> new CRDTContext(k, store, agentHost, scheduler, incomingStoreMessages));
        return crdtContext.id();
    }

    @Override
    public Optional<ICausalCRDT> get(String uri) {
        checkState();
        return Optional.ofNullable(hostedCRDT.get(uri).crdt());
    }

    @Override
    public CompletableFuture<Void> stopHosting(String uri) {
        checkState();
        assert hostedCRDT.containsKey(uri);
        return stopHostingInternal(uri);
    }

    @Override
    public Observable<Set<Replica>> aliveReplicas(String uri) {
        checkState();
        return hostedCRDT.get(uri).aliveReplicas();
    }

    @Override
    public boolean isStarted() {
        return state.get() == State.STARTED;
    }

    private CompletableFuture<Void> stopHostingInternal(String uri) {
        return hostedCRDT.remove(uri).close();
    }

    @Override
    public void start(IAgentHost agentHost) {
        if (state.compareAndSet(State.INIT, State.STARTING)) {
            this.agentHost = agentHost;
            store.start(incomingStoreMessages);
            state.set(State.STARTED);
            log.debug("Started CRDT service[{}]", toUnsignedString(store.id()));
        }
    }

    @Override
    public void stop() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            log.info("Stopping CRDT service[{}]", id());
            log.debug("Stop hosting CRDTs");
            CompletableFuture.allOf(hostedCRDT.values()
                    .stream()
                    .map(CRDTContext::close)
                    .toArray(CompletableFuture[]::new))
                .join();
            log.debug("Stopping CRDT store");
            store.stop();
            log.info("CRDT service[{}] stopped", id());
            executor.shutdown();
            state.set(State.SHUTDOWN);
        }
    }

    private void checkState() {
        Preconditions.checkState(state.get() == State.STARTED, "Not started");
    }
}
