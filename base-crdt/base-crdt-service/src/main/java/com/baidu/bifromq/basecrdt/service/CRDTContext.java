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

import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecluster.agent.proto.AgentMemberAddr;
import com.baidu.bifromq.basecluster.memberlist.agent.IAgent;
import com.baidu.bifromq.basecluster.memberlist.agent.IAgentMember;
import com.baidu.bifromq.basecrdt.core.api.ICausalCRDT;
import com.baidu.bifromq.basecrdt.proto.Replica;
import com.baidu.bifromq.basecrdt.store.ICRDTStore;
import com.baidu.bifromq.basecrdt.store.proto.CRDTStoreMessage;
import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.subjects.Subject;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class CRDTContext {
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final AgentMemberAddr endpoint;
    private final ReadWriteLock shutdownLock = new ReentrantReadWriteLock();
    private final Replica replica;
    private final ICausalCRDT crdt;
    private final ICRDTStore store;
    private final IAgent replicaAgent;
    private final IAgentMember replicaAgentMember;
    private final Subject<CRDTStoreMessage> storeMsgSubject;
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final CompletableFuture<Void> quitSignal = new CompletableFuture<>();

    CRDTContext(String uri,
                ICRDTStore store,
                IAgentHost host,
                Scheduler scheduler,
                Subject<CRDTStoreMessage> storeMsgSubject) {
        replica = store.host(uri);
        replicaAgent = host.host(replica.getUri());
        endpoint = AgentMemberAddr.newBuilder()
            .setName(AgentUtil.toAgentMemberName(replica))
            .setEndpoint(replicaAgent.endpoint())
            .build();
        this.store = store;
        this.crdt = store.get(uri).get();
        this.replicaAgentMember = replicaAgent.register(endpoint.getName());
        this.storeMsgSubject = storeMsgSubject;
        disposables.add(replicaAgent.membership()
            .observeOn(scheduler)
            .subscribe(withLock(shutdownLock.readLock(), agentMembers -> {
                if (stopped.get()) {
                    return;
                }
                Set<ByteString> peers = agentMembers.keySet().stream()
                    .map(AbstractMessageLite::toByteString)
                    .collect(Collectors.toSet());
                store.join(replica.getUri(), endpoint.toByteString(), peers);
            })));
        disposables.add(replicaAgentMember.receive()
            .observeOn(scheduler)
            .subscribe(withLock(shutdownLock.readLock(), agentMessage -> {
                if (stopped.get()) {
                    return;
                }
                try {
                    this.storeMsgSubject.onNext(CRDTStoreMessage.parseFrom(agentMessage.getPayload()));
                } catch (InvalidProtocolBufferException e) {
                    log.error("Unable to parse crdt store message from agent message", e);
                }
            })));
        disposables.add(store.storeMessages()
            .filter(msg -> msg.getSender().equals(endpoint.toByteString()))
            .observeOn(scheduler)
            .subscribe(withLock(shutdownLock.readLock(), msg -> {
                if (stopped.get()) {
                    return;
                }
                AgentMemberAddr target = AgentMemberAddr.parseFrom(msg.getReceiver());
                replicaAgentMember.send(target, msg.toByteString(), true)
                    .whenComplete((v, e) -> {
                        if (e != null) {
                            log.debug("Failed to send store message, uri={}, sender={}, receiver={}",
                                msg.getUri(), target.getName(), endpoint.getName(), e);
                        } else {
                            if (log.isTraceEnabled()) {
                                log.trace("Sent store message, uri={}, sender={}, receiver={}, msg={}",
                                    msg.getUri(), target.getName(), endpoint.getName(), msg);
                            }
                        }
                    });
            })));
    }

    Replica id() {
        return replica;
    }

    ICausalCRDT crdt() {
        return crdt;
    }

    Observable<Set<Replica>> aliveReplicas() {
        return replicaAgent.membership()
            .map(agentMembers -> agentMembers.keySet().stream()
                .map(agentMemberAddr -> AgentUtil.toReplica(agentMemberAddr.getName()))
                .collect(Collectors.toSet()));
    }

    CompletableFuture<Void> close() {
        Lock lock = shutdownLock.writeLock();
        try {
            lock.lock();
            if (stopped.compareAndSet(false, true)) {
                disposables.dispose();
                replicaAgent.deregister(replicaAgentMember)
                    .thenCompose(v -> store.stopHosting(replica.getUri()))
                    .whenComplete((v, e) -> {
                        if (e != null) {
                            log.warn("Error during close", e);
                        }
                        quitSignal.complete(null);
                    });
            }
            return quitSignal;
        } finally {
            lock.unlock();
        }
    }

    private <T> Consumer<T> withLock(Lock lock, Consumer<T> consumer) {
        return (T value) -> {
            try {
                lock.lock();
                consumer.accept(value);
            } finally {
                lock.unlock();
            }
        };
    }
}
