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

package com.baidu.bifromq.basecluster;

import static com.github.benmanes.caffeine.cache.Scheduler.systemScheduler;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

import com.baidu.bifromq.basecluster.fd.FailureDetector;
import com.baidu.bifromq.basecluster.fd.IFailureDetector;
import com.baidu.bifromq.basecluster.fd.IProbingTarget;
import com.baidu.bifromq.basecluster.memberlist.AutoDropper;
import com.baidu.bifromq.basecluster.memberlist.AutoHealer;
import com.baidu.bifromq.basecluster.memberlist.AutoSeeder;
import com.baidu.bifromq.basecluster.memberlist.HostMemberList;
import com.baidu.bifromq.basecluster.memberlist.IHostMemberList;
import com.baidu.bifromq.basecluster.memberlist.MemberSelector;
import com.baidu.bifromq.basecluster.memberlist.agent.IAgent;
import com.baidu.bifromq.basecluster.membership.proto.HostEndpoint;
import com.baidu.bifromq.basecluster.messenger.IMessenger;
import com.baidu.bifromq.basecluster.messenger.Messenger;
import com.baidu.bifromq.basecluster.messenger.MessengerOptions;
import com.baidu.bifromq.basecluster.proto.ClusterMessage;
import com.baidu.bifromq.basecrdt.store.ICRDTStore;
import com.baidu.bifromq.basecrdt.store.proto.CRDTStoreMessage;
import com.baidu.bifromq.baseenv.EnvProvider;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class AgentHost implements IAgentHost {
    private enum State {
        INIT, STARTING, STARTED, STOPPING, SHUTDOWN
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
    private final AgentHostOptions options;
    private final ICRDTStore store;
    private final IMessenger messenger;
    private final IHostMemberList memberList;
    private final MemberSelector memberSelector;
    private final AutoHealer healer;
    private final AutoSeeder seeder;
    private final AutoDropper deadDropper;
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final LoadingCache<HostEndpoint, InetSocketAddress> hostAddressCache;
    private final String[] tags;

    AgentHost(AgentHostOptions options) {
        checkArgument(!Strings.isNullOrEmpty(options.addr()) && !"0.0.0.0".equals(options.addr()),
            "Invalid bind address");
        checkArgument(Strings.isNullOrEmpty(options.clusterDomainName()) ||
            !Strings.isNullOrEmpty(options.clusterDomainName()) && options.port() > 0, "Invalid port number");
        this.options = options.toBuilder().build();
        this.store = ICRDTStore.newInstance(options.crdtStoreOptions());
        MessengerOptions messengerOptions = new MessengerOptions();
        messengerOptions.maxFanout(options.gossipFanout())
            .maxFanoutGossips(options.gossipFanoutPerPeriod())
            .maxHealthScore(options.awarenessMaxMultiplier())
            .retransmitMultiplier(options.retransmitMultiplier())
            .spreadPeriod(options.gossipPeriod())
            .transporterOptions()
            .mtu(options.udpPacketLimit())
            .tcpTransportOptions()
            .maxChannelsPerHost(options.maxChannelsPerHost())
            .idleTimeoutInSec(options.idleTimeoutInSec())
            .connTimeoutInMS(options.connTimeoutInMS());
        Scheduler scheduler = Schedulers.from(newSingleThreadScheduledExecutor(
            EnvProvider.INSTANCE.newThreadFactory("agent-host-scheduler", true)));
        this.messenger = Messenger.builder()
            .bindAddr(new InetSocketAddress(options.addr(), options.port()))
            .serverSslContext(options.serverSslContext())
            .clientSslContext(options.clientSslContext())
            .env(options.env())
            .opts(messengerOptions)
            .scheduler(scheduler)
            .build();
        hostAddressCache = Caffeine.newBuilder()
            .scheduler(systemScheduler())
            .expireAfterAccess(Duration.ofMinutes(5))
            .build(hostEndpoint -> new InetSocketAddress(hostEndpoint.getAddress(), hostEndpoint.getPort()));
        this.store.start(messenger.receive()
            .filter(m -> m.value().message.hasCrdtStoreMessage())
            .map(m -> m.value().message.getCrdtStoreMessage()));
        tags = new String[] {
            "env", options.env(), "local", options.addr() + ":" + messenger.bindAddress().getPort()
        };
        this.memberList = new HostMemberList(options.addr(), messenger.bindAddress().getPort(),
            messenger, scheduler, store, hostAddressCache::get, tags);
        IFailureDetector failureDetector = FailureDetector.builder()
            .local(new IProbingTarget() {
                @Override
                public ByteString id() {
                    return memberList.local().getEndpoint().toByteString();
                }

                @Override
                public InetSocketAddress addr() {
                    return messenger.bindAddress();
                }
            })
            .baseProbeInterval(options.baseProbeInterval())
            .baseProbeTimeout(options.baseProbeTimeout())
            .indirectProbes(options.indirectProbes())
            .worstHealthScore(options.awarenessMaxMultiplier())
            .messenger(messenger)
            .scheduler(scheduler)
            .build();
        healer = new AutoHealer(messenger, scheduler, memberList, hostAddressCache::get, options.autoHealingTimeout(),
            options.autoHealingInterval(), tags);
        seeder = new AutoSeeder(messenger, scheduler, memberList, hostAddressCache::get, options.joinTimeout(),
            Duration.ofSeconds(options.joinRetryInSec()), tags);
        deadDropper = new AutoDropper(messenger, scheduler, memberList, failureDetector, hostAddressCache::get,
            options.suspicionMultiplier(), options.suspicionMaxTimeoutMultiplier(), tags);
        memberSelector = new MemberSelector(memberList, scheduler, hostAddressCache::get);
        disposables.add(store.storeMessages().subscribe(this::sendCRDTStoreMessage));
    }

    @Override
    public HostEndpoint local() {
        return memberList.local().getEndpoint();
    }

    @Override
    public CompletableFuture<Void> join(Set<InetSocketAddress> seeds) {
        return seeder.join(seeds);
    }

    @Override
    public Observable<Set<HostEndpoint>> cluster() {
        return memberList.members().map(Map::keySet);
    }

    @Override
    public IAgent host(String agentId) {
        Preconditions.checkState(state.get() == State.STARTED);
        return memberList.host(agentId);
    }

    @Override
    public CompletableFuture<Void> stopHosting(String agentId) {
        Preconditions.checkState(state.get() == State.STARTED);
        return memberList.stopHosting(agentId);
    }

    @Override
    public Observable<Set<HostEndpoint>> membership() {
        return memberList.members().map(Map::keySet);
    }

    @Override
    public void start() {
        if (state.compareAndSet(State.INIT, State.STARTING)) {
            messenger.start(memberSelector);
            deadDropper.start();
            if (!Strings.isNullOrEmpty(options.clusterDomainName())) {
                if (options.port() > 0) {
                    seeder.join(options.clusterDomainName(), options.port());
                } else {
                    throw new IllegalArgumentException(
                        "Port number must be explicitly specified if cluster domain enabled");
                }
            }
            state.set(State.STARTED);
        }
    }

    @Override
    public void shutdown() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            healer.stop();
            seeder.stop();
            deadDropper.stop();
            memberList.stop()
                .exceptionally(e -> null)
                .thenCompose(v -> {
                    store.stop();
                    return messenger.shutdown();
                })
                .whenComplete((v, e) -> {
                    memberSelector.stop();
                    disposables.dispose();
                    state.set(State.SHUTDOWN);
                }).join();
        }
    }

    private void sendCRDTStoreMessage(CRDTStoreMessage storeMsg) {
        ClusterMessage msg = ClusterMessage.newBuilder().setCrdtStoreMessage(storeMsg).build();
        try {
            HostEndpoint endpoint = HostEndpoint.parseFrom(storeMsg.getReceiver());
            messenger.send(msg, hostAddressCache.get(endpoint), false);
        } catch (Exception e) {
            log.error("Target Host[{}] not found:\n{}", storeMsg.getReceiver(), storeMsg);
        }
    }
}
