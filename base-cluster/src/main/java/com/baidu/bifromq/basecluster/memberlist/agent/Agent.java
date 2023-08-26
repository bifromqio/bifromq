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

package com.baidu.bifromq.basecluster.memberlist.agent;

import static com.baidu.bifromq.basecrdt.core.api.CausalCRDTType.mvreg;
import static java.util.Collections.emptyMap;

import com.baidu.bifromq.basecluster.agent.proto.AgentMemberAddr;
import com.baidu.bifromq.basecluster.agent.proto.AgentMemberMetadata;
import com.baidu.bifromq.basecluster.membership.proto.HostEndpoint;
import com.baidu.bifromq.basecrdt.core.api.IORMap;
import com.baidu.bifromq.basecrdt.core.api.ORMapOperation;
import com.baidu.bifromq.basecrdt.proto.Replica;
import com.baidu.bifromq.basecrdt.store.ICRDTStore;
import com.google.common.collect.Sets;
import com.google.protobuf.AbstractMessageLite;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class Agent implements IAgent {
    private enum State {
        JOINED, QUITTING, QUITED
    }

    private final ReadWriteLock quitLock = new ReentrantReadWriteLock();
    private final String agentId;
    private final HostEndpoint hostEndpoint;
    private final AtomicReference<State> state = new AtomicReference<>(State.JOINED);
    private final IAgentMessenger messenger;
    private final Scheduler scheduler;
    private final ICRDTStore store;
    private final IAgentHostProvider hostProvider;
    private final IORMap agentCRDT;
    private final Map<AgentMemberAddr, AgentMember> localMemberRegistry = new ConcurrentHashMap<>();
    private final BehaviorSubject<Map<AgentMemberAddr, AgentMemberMetadata>> agentMembersSubject =
        BehaviorSubject.createDefault(emptyMap());
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final Gauge memberNumGauge;
    private volatile Set<HostEndpoint> currentEndpoints = new HashSet<>();

    public Agent(String agentId,
                 HostEndpoint hostEndpoint,
                 IAgentMessenger messenger,
                 Scheduler scheduler,
                 ICRDTStore store,
                 IAgentHostProvider hostProvider) {
        this.agentId = agentId;
        this.hostEndpoint = hostEndpoint;
        this.messenger = messenger;
        this.scheduler = scheduler;
        this.store = store;
        this.hostProvider = hostProvider;
        Replica replica = store.host(CRDTUtil.toAgentURI(agentId), hostEndpoint.toByteString());
        agentCRDT = (IORMap) store.get(replica.getUri()).get();
        disposables.add(agentCRDT.inflation()
            .observeOn(scheduler)
            .subscribe(this::sync));
        disposables.add(hostProvider.getHostEndpoints()
            .observeOn(scheduler)
            .subscribe(this::handleHostEndpointsUpdate));
        memberNumGauge = Gauge.builder("basecluster.agent.members", () -> agentMembersSubject.getValue().size())
            .tags("id", agentId)
            .tags("local", hostEndpoint.getAddress() + ":" + hostEndpoint.getPort())
            .register(Metrics.globalRegistry);
    }

    @Override
    public String id() {
        return agentId;
    }

    @Override
    public HostEndpoint endpoint() {
        return hostEndpoint;
    }

    @Override
    public Observable<Map<AgentMemberAddr, AgentMemberMetadata>> membership() {
        return agentMembersSubject;
    }

    @Override
    public IAgentMember register(String memberName) {
        return runIfJoined(() -> {
            AgentMemberAddr memberAddr = AgentMemberAddr.newBuilder()
                .setName(memberName)
                .setEndpoint(hostEndpoint)
                .build();
            return localMemberRegistry.computeIfAbsent(memberAddr,
                k -> new AgentMember(memberAddr, agentCRDT, messenger, scheduler,
                    () -> agentMembersSubject.getValue().keySet()));
        });
    }

    @Override
    public CompletableFuture<Void> deregister(IAgentMember member) {
        return runIfJoined(() -> {
            if (localMemberRegistry.remove(member.address(), member)) {
                return ((AgentMember) member).destroy();
            }
            return CompletableFuture.completedFuture(null);
        });
    }

    public CompletableFuture<Void> quit() {
        Lock writeLock = quitLock.writeLock();
        try {
            writeLock.lock();
            if (state.compareAndSet(State.JOINED, State.QUITTING)) {
                // stop react to host update and inflation
                return CompletableFuture.allOf(localMemberRegistry.values().stream()
                        .map(AgentMember::destroy)
                        .toArray(CompletableFuture[]::new))
                    .thenCompose(v -> {
                        disposables.dispose();
                        agentMembersSubject.onComplete();
                        return store.stopHosting(CRDTUtil.toAgentURI(agentId));
                    })
                    .whenComplete((v, e) -> state.set(State.QUITED));
            } else if (state.get() == State.QUITTING) {
                return CompletableFuture.failedFuture(new IllegalStateException("quit has started"));
            } else {
                return CompletableFuture.completedFuture(null);
            }
        } finally {
            writeLock.unlock();
            Metrics.globalRegistry.remove(memberNumGauge);
        }
    }

    private void sync(long ts) {
        skipRunIfNotJoined(() -> {
            Map<AgentMemberAddr, AgentMemberMetadata> agentMembersCRDT = CRDTUtil.toAgentMemberMap(agentCRDT);
            Map<AgentMemberAddr, AgentMemberMetadata> agentMembersLocal = new HashMap<>();
            localMemberRegistry.values().forEach(member -> agentMembersLocal.put(member.address(), member.metadata()));
            for (AgentMemberAddr memberAddr : Sets.difference(agentMembersCRDT.keySet(), agentMembersLocal.keySet())) {
                if (memberAddr.getEndpoint().equals(hostEndpoint)) {
                    // obsolete member
                    agentCRDT.execute(ORMapOperation.remove(memberAddr.toByteString()).of(mvreg));
                }
            }
            agentMembersCRDT.putAll(agentMembersLocal);
            agentMembersSubject.onNext(agentMembersCRDT);
        });
    }

    private void handleHostEndpointsUpdate(Set<HostEndpoint> endpoints) {
        skipRunIfNotJoined(() -> {
            Set<HostEndpoint> newEndpoints = Sets.newHashSet(endpoints);
            newEndpoints.add(hostEndpoint);
            Set<HostEndpoint> leftHosts = Sets.difference(currentEndpoints, newEndpoints);
            // drop members on left hosts
            Map<AgentMemberAddr, AgentMemberMetadata> agentMemberMap = CRDTUtil.toAgentMemberMap(agentCRDT);
            for (AgentMemberAddr memberAddr : agentMemberMap.keySet()) {
                if (leftHosts.contains(memberAddr.getEndpoint())) {
                    agentCRDT.execute(ORMapOperation.remove(memberAddr.toByteString()).of(mvreg));
                }
            }
            // update landscape
            currentEndpoints = newEndpoints;
            store.join(agentCRDT.id().getUri(), hostEndpoint.toByteString(),
                currentEndpoints.stream().map(AbstractMessageLite::toByteString).collect(Collectors.toSet()));
        });
    }

    private void skipRunIfNotJoined(Runnable runnable) {
        Lock readLock = quitLock.readLock();
        try {
            readLock.lock();
            if (state.get() != State.JOINED) {
                return;
            }
            runnable.run();
        } finally {
            readLock.unlock();
        }

    }

    private <T> T runIfJoined(Supplier<T> supplier) {
        Lock readLock = quitLock.readLock();
        try {
            readLock.lock();
            if (state.get() != State.JOINED) {
                throw new IllegalArgumentException("Agent has quit");
            }
            return supplier.get();
        } finally {
            readLock.unlock();
        }
    }
}
