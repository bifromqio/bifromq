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

package com.baidu.bifromq.basecluster.memberlist;

import static com.baidu.bifromq.basecluster.memberlist.CRDTUtil.AGENT_HOST_MAP_URI;
import static com.baidu.bifromq.basecluster.memberlist.CRDTUtil.getHostMember;
import static com.baidu.bifromq.basecluster.memberlist.CRDTUtil.iterate;
import static com.baidu.bifromq.basecrdt.core.api.CausalCRDTType.mvreg;

import com.baidu.bifromq.basecluster.memberlist.agent.Agent;
import com.baidu.bifromq.basecluster.memberlist.agent.AgentHostProvider;
import com.baidu.bifromq.basecluster.memberlist.agent.AgentMessenger;
import com.baidu.bifromq.basecluster.memberlist.agent.IAgent;
import com.baidu.bifromq.basecluster.membership.proto.Doubt;
import com.baidu.bifromq.basecluster.membership.proto.Fail;
import com.baidu.bifromq.basecluster.membership.proto.HostEndpoint;
import com.baidu.bifromq.basecluster.membership.proto.HostMember;
import com.baidu.bifromq.basecluster.membership.proto.Join;
import com.baidu.bifromq.basecluster.membership.proto.Quit;
import com.baidu.bifromq.basecluster.messenger.IMessenger;
import com.baidu.bifromq.basecluster.proto.ClusterMessage;
import com.baidu.bifromq.basecrdt.core.api.IORMap;
import com.baidu.bifromq.basecrdt.core.api.MVRegOperation;
import com.baidu.bifromq.basecrdt.core.api.ORMapOperation;
import com.baidu.bifromq.basecrdt.store.ICRDTStore;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.AbstractMessageLite;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HostMemberList implements IHostMemberList {
    private enum State {
        JOINED, QUITTING, QUITED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.JOINED);
    private final IMessenger messenger;
    private final Scheduler scheduler;
    private final ICRDTStore store;
    private final IHostAddressResolver addressResolver;

    private final BehaviorSubject<Map<HostEndpoint, HostMember>> membershipSubject = BehaviorSubject.createDefault(
        new ConcurrentHashMap<>());
    private final Map<String, Agent> agentMap = new ConcurrentHashMap<>();
    private final IORMap hostListCRDT;
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final MetricManager metricManager;
    private final String[] tags;
    private volatile HostMember local;

    public HostMemberList(String bindAddr,
                          int port,
                          IMessenger messenger,
                          Scheduler scheduler,
                          ICRDTStore store,
                          IHostAddressResolver addressResolver,
                          String... tags) {
        this.messenger = messenger;
        this.scheduler = scheduler;
        this.store = store;
        this.addressResolver = addressResolver;
        this.tags = tags;
        // setup an ORMap for syncing host list
        store.host(AGENT_HOST_MAP_URI);
        assert store.get(AGENT_HOST_MAP_URI).isPresent();
        hostListCRDT = (IORMap) store.get(AGENT_HOST_MAP_URI).get();
        local = HostMember.newBuilder()
            .setEndpoint(HostEndpoint.newBuilder()
                .setId(hostListCRDT.id().getId())
                .setAddress(bindAddr)
                .setPort(port)
                .build())
            .setIncarnation(0)
            .build();
        join(local);
        disposables.add(hostListCRDT.inflation().observeOn(scheduler).subscribe(this::sync));
        disposables.add(messenger.receive()
            .map(m -> m.value().message)
            .observeOn(scheduler)
            .subscribe(this::handleMessage));
        metricManager = new MetricManager();
    }

    @Override
    public HostMember local() {
        return local;
    }

    private boolean join(HostMember member) {
        if (isZombie(member.getEndpoint())) {
            // never join zombie
            return false;
        }
        synchronized (this) {
            // add it to the list
            boolean joined = addMember(member);
            if (joined) {
                // add it into crdt
                hostListCRDT.execute(ORMapOperation.update(member.getEndpoint().toByteString())
                    .with(MVRegOperation.write(member.toByteString())));
                // update crdt landscape
                store.join(AGENT_HOST_MAP_URI, local.getEndpoint().toByteString(),
                    currentMembers().keySet().stream()
                        .map(AbstractMessageLite::toByteString)
                        .collect(Collectors.toSet()));
            }
            return joined;
        }
    }

    private void drop(HostEndpoint memberEndpoint, int incarnation) {
        synchronized (this) {
            boolean removed = removeMember(memberEndpoint, incarnation);
            // remove it from crdt if any
            hostListCRDT.execute(ORMapOperation.remove(memberEndpoint.toByteString()).of(mvreg));
            if (removed) {
                // update crdt landscape
                store.join(AGENT_HOST_MAP_URI, local.getEndpoint().toByteString(),
                    currentMembers().keySet().stream()
                        .map(AbstractMessageLite::toByteString)
                        .collect(Collectors.toSet()));
            }
        }
    }

    @Override
    public boolean isZombie(HostEndpoint endpoint) {
        return !endpoint.getId().equals(local.getEndpoint().getId()) &&
            endpoint.getAddress().equals(local.getEndpoint().getAddress()) &&
            endpoint.getPort() == local.getEndpoint().getPort();
    }

    private InetSocketAddress getMemberAddress(HostEndpoint endpoint) {
        Map<HostEndpoint, HostMember> aliveHostList = membershipSubject.getValue();
        return !aliveHostList.containsKey(endpoint) ? null : addressResolver.resolve(endpoint);
    }

    @Override
    public CompletableFuture<Void> stop() {
        if (state.compareAndSet(State.JOINED, State.QUITTING)) {
            synchronized (this) {
                return CompletableFuture.allOf(agentMap.values().stream()
                        .map(Agent::quit).toArray(CompletableFuture[]::new))
                    .exceptionally(e -> null)
                    .thenCompose(v -> {
                        disposables.dispose();
                        // remove self from alive host list
                        removeMember(local.getEndpoint(), local.getIncarnation());
                        // delete from crdt and wait for
                        return hostListCRDT.execute(
                                ORMapOperation.remove(local.getEndpoint().toByteString()).of(mvreg))
                            .exceptionally(e -> null);
                    })
                    .thenCompose(v1 -> {
                        ClusterMessage quit = ClusterMessage.newBuilder()
                            .setQuit(Quit.newBuilder()
                                .setEndpoint(local.getEndpoint())
                                .setIncarnation(local.getIncarnation())
                                .build())
                            .build();
                        return messenger.spread(quit)
                            .handle((v, e) -> null);
                    })
                    .thenCompose(v -> store.stopHosting(AGENT_HOST_MAP_URI))
                    .whenComplete((v, e) -> {
                        membershipSubject.onComplete();
                        metricManager.close();
                        state.set(State.QUITED);
                    });
            }
        } else if (state.get() == State.QUITTING) {
            return CompletableFuture.failedFuture(new IllegalStateException("quit has started"));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public Observable<Map<HostEndpoint, Integer>> members() {
        return membershipSubject.map(hostMap -> Maps.transformValues(hostMap, HostMember::getIncarnation));
    }

    private void renew(int atLeastIncarnation) {
        synchronized (this) {
            local = local.toBuilder().setIncarnation(Math.max(local.getIncarnation(), atLeastIncarnation) + 1).build();
            join(local);
        }
    }

    @Override
    public IAgent host(String agentId) {
        checkState();
        synchronized (this) {
            if (!local.getAgentIdList().contains(agentId)) {
                agentMap.put(agentId, new Agent(agentId,
                    local.getEndpoint(),
                    new AgentMessenger(agentId, this::getMemberAddress, messenger),
                    scheduler,
                    store,
                    new AgentHostProvider(agentId, membershipSubject),
                    tags));
                local = local.toBuilder().setIncarnation(local.getIncarnation() + 1).addAgentId(agentId).build();
                join(local);
            }
            return agentMap.get(agentId);
        }
    }

    @Override
    public CompletableFuture<Void> stopHosting(String agentId) {
        checkState();
        Agent agent = agentMap.remove(agentId);
        if (agent != null) {
            return agent.quit().whenComplete((v, e) -> {
                synchronized (this) {
                    local = local.toBuilder()
                        .setIncarnation(local.getIncarnation() + 1)
                        .clearAgentId()
                        .addAllAgentId(agentMap.keySet())
                        .build();
                }
                join(local);
            });
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Set<String> agents() {
        return Sets.newHashSet(local.getAgentIdList());
    }

    private Map<HostEndpoint, HostMember> currentMembers() {
        return membershipSubject.getValue();
    }

    private boolean addMember(HostMember member) {
        Map<HostEndpoint, HostMember> members = Maps.newHashMap(membershipSubject.getValue());
        boolean joined = members.compute(member.getEndpoint(), (k, v) -> {
            if (v == null) {
                return member;
            } else {
                return member.getIncarnation() > v.getIncarnation() ? member : v;
            }
        }) == member;
        if (joined) {
            membershipSubject.onNext(members);
        }
        return joined;
    }

    private boolean removeMember(HostEndpoint endpoint, int incarnation) {
        Map<HostEndpoint, HostMember> members = Maps.newHashMap(membershipSubject.getValue());
        AtomicBoolean removed = new AtomicBoolean();
        members.computeIfPresent(endpoint, (k, v) -> {
            if (v.getIncarnation() <= incarnation) {
                removed.set(true);
                return null;
            } else {
                return v;
            }
        });
        if (removed.get()) {
            membershipSubject.onNext(members);
        }
        return removed.get();
    }

    private void handleMessage(ClusterMessage message) {
        if (state.get() != State.JOINED) {
            return;
        }
        switch (message.getClusterMessageTypeCase()) {
            case JOIN -> handleJoin(message.getJoin());
            case QUIT -> handleQuit(message.getQuit());
            case FAIL -> handleFail(message.getFail());
            case DOUBT -> handleDoubt(message.getDoubt());
        }
    }

    private void handleJoin(Join join) {
        HostMember joinMember = join.getMember();
        if (!join.hasExpectedHost() || join.getExpectedHost().equals(local.getEndpoint())) {
            if (join(joinMember)) {
                if (join.hasExpectedHost()) {
                    // send back a join to prove I'm still alive
                    messenger.send(ClusterMessage.newBuilder()
                        .setJoin(Join.newBuilder().setMember(local).build())
                        .build(), getMemberAddress(joinMember.getEndpoint()), true);
                }
            }
        } else {
            clearZombie(join.getExpectedHost());
        }
    }

    private void handleFail(Fail fail) {
        HostEndpoint failedEndpoint = fail.getEndpoint();
        if (failedEndpoint.equals(local.getEndpoint())) {
            if (fail.getIncarnation() >= local.getIncarnation()) {
                // I'm declared dead by someone, refute it
                log.debug("Renew[{}] to refute failure report", local);
                renew(fail.getIncarnation());
                messenger.spread(ClusterMessage.newBuilder()
                    .setJoin(Join.newBuilder().setMember(local).build())
                    .build());
            }
        } else if (isZombie(failedEndpoint)) {
            clearZombie(failedEndpoint);
        } else {
            drop(failedEndpoint, fail.getIncarnation());
        }
    }

    private void handleQuit(Quit quit) {
        HostEndpoint quitEndpoint = quit.getEndpoint();
        if (!quitEndpoint.equals(local.getEndpoint()) && !isZombie(quitEndpoint)) {
            drop(quitEndpoint, quit.getIncarnation());
        }
    }

    private void handleDoubt(Doubt doubt) {
        if (doubt.getEndpoint().equals(local.getEndpoint()) && doubt.getIncarnation() >= local.getIncarnation()) {
            // I'm suspected, refute it
            log.debug("Member[{}] refutes the death suspicion from reporter[{}]", local, doubt.getReporter());
            renew(doubt.getIncarnation());
            messenger.spread(ClusterMessage.newBuilder()
                .setJoin(Join.newBuilder().setMember(local).build())
                .build());
        }
    }

    private void clearZombie(HostEndpoint zombieEndpoint) {
        // drop zombie if any, and broadcast a quit on behalf of it
        drop(zombieEndpoint, Integer.MAX_VALUE);
        messenger.spread(ClusterMessage.newBuilder()
            .setQuit(Quit.newBuilder().setEndpoint(zombieEndpoint).setIncarnation(Integer.MAX_VALUE).build())
            .build());
    }

    private void sync(long ts) {
        if (state.get() != State.JOINED) {
            return;
        }
        // keep myself reporting via memberlist crdt
        Optional<HostMember> localMemberInCRDT = getHostMember(hostListCRDT, local.getEndpoint());
        if (localMemberInCRDT.isEmpty() || localMemberInCRDT.get().getIncarnation() > local.getIncarnation()) {
            renew(localMemberInCRDT.orElse(local).getIncarnation());
        }

        // update alive list with members known from memberlist crdt, and remove the zombie hosts if found
        Iterator<HostMember> itr = iterate(hostListCRDT);
        while (itr.hasNext()) {
            HostMember observed = itr.next();
            if (observed.getEndpoint().equals(local.getEndpoint())) {
                continue;
            }
            if (isZombie(observed.getEndpoint())) {
                // the zombie host found in crdt
                clearZombie(observed.getEndpoint());
            } else {
                // join the observed if needed
                join(observed);
            }
        }
    }

    private void checkState() {
        Preconditions.checkState(state.get() == State.JOINED);
    }

    private class MetricManager {

        private final Set<Meter> meters = new HashSet<>();

        MetricManager() {
            Tags metricTags = Tags.of(tags);
            meters.add(Gauge.builder("basecluster.crdt.agentcluster.count", agentMap, Map::size)
                .tags(metricTags)
                .register(Metrics.globalRegistry));
            meters.add(Gauge.builder("basecluster.crdt.hostcluster.size", membershipSubject, a -> a.getValue().size())
                .tags(metricTags)
                .register(Metrics.globalRegistry));
        }

        void close() {
            meters.forEach(meter -> Metrics.globalRegistry.removeByPreFilterId(meter.getId()));
        }
    }
}
