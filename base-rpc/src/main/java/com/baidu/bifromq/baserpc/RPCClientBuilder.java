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

package com.baidu.bifromq.baserpc;

import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.baserpc.interceptor.TenantAwareClientInterceptor;
import com.baidu.bifromq.baserpc.loadbalancer.IUpdateListener;
import com.baidu.bifromq.baserpc.loadbalancer.TrafficDirectiveLoadBalancerProvider;
import com.baidu.bifromq.baserpc.nameresolver.TrafficGovernorNameResolverProvider;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficDirector;
import com.baidu.bifromq.baserpc.utils.BehaviorSubject;
import com.baidu.bifromq.baserpc.utils.NettyUtil;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Channel;
import io.grpc.ConnectivityState;
import io.grpc.LoadBalancerProvider;
import io.grpc.LoadBalancerRegistry;
import io.grpc.ManagedChannel;
import io.grpc.netty.LocalInProcNettyChannelBuilder;
import io.grpc.netty.NegotiationType;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.reactivex.rxjava3.core.Observable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.NonNull;

public final class RPCClientBuilder {
    private BluePrint bluePrint;
    private Executor executor;
    private ICRDTService crdtService;
    private EventLoopGroup eventLoopGroup;
    private long keepAliveInSec;
    private long idleTimeoutInSec;
    private SslContext sslContext;

    RPCClientBuilder() {
    }

    public RPCClientBuilder bluePrint(@NonNull BluePrint bluePrint) {
        this.bluePrint = bluePrint;
        return this;
    }

    public RPCClientBuilder executor(Executor executor) {
        this.executor = executor;
        return this;
    }

    public RPCClientBuilder crdtService(@NonNull ICRDTService crdtService) {
        this.crdtService = crdtService;
        return this;
    }

    public RPCClientBuilder eventLoopGroup(EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
        return this;
    }

    public RPCClientBuilder keepAliveInSec(long keepAliveInSec) {
        this.keepAliveInSec = keepAliveInSec;
        return this;
    }

    public RPCClientBuilder idleTimeoutInSec(long idleTimeoutInSec) {
        this.idleTimeoutInSec = idleTimeoutInSec;
        return this;
    }

    public RPCClientBuilder sslContext(SslContext sslContext) {
        if (sslContext != null) {
            Preconditions.checkArgument(sslContext.isClient(), "Client auth must be enabled");
        }
        this.sslContext = sslContext;
        return this;
    }

    public RPCClient build() {
        final String serviceUniqueName = bluePrint.serviceDescriptor().getName();
        RPCClient.ChannelHolder channelHolder = new RPCClient.ChannelHolder() {
            private final ManagedChannel internalChannel;
            private final BehaviorSubject<IUpdateListener.IServerSelector> serverSelectorSubject =
                BehaviorSubject.create();
            private final BehaviorSubject<IRPCClient.ConnState> connStateSubject = BehaviorSubject.create();
            private final Observable<Map<String, Map<String, String>>> serverListSubject;
            private final LoadBalancerProvider loadBalancerProvider;
            private final Executor rpcExecutor;
            private final boolean needShutdownExecutor;

            {
                this.needShutdownExecutor = executor == null;
                if (needShutdownExecutor) {
                    int threadNum = Math.max(EnvProvider.INSTANCE.availableProcessors(), 1);
                    rpcExecutor = ExecutorServiceMetrics
                        .monitor(Metrics.globalRegistry, new ThreadPoolExecutor(threadNum, threadNum,
                                0L, TimeUnit.MILLISECONDS,
                                new LinkedTransferQueue<>(),
                                EnvProvider.INSTANCE.newThreadFactory(serviceUniqueName + "_client-executor", true)),
                            serviceUniqueName + "_client-executor");
                } else {
                    rpcExecutor = executor;
                }
                loadBalancerProvider =
                    new TrafficDirectiveLoadBalancerProvider(bluePrint, serverSelectorSubject::onNext);
                IRPCServiceTrafficDirector trafficDirector = IRPCServiceTrafficDirector
                    .newInstance(serviceUniqueName, crdtService);
                serverListSubject = trafficDirector.serverList()
                    .map(sl -> sl.stream().collect(Collectors.toMap(s -> s.id, s -> s.attrs)));

                LoadBalancerRegistry.getDefaultRegistry().register(loadBalancerProvider);

                TrafficGovernorNameResolverProvider.register(serviceUniqueName, trafficDirector);

                LocalInProcNettyChannelBuilder internalChannelBuilder = LocalInProcNettyChannelBuilder
                    .forTarget(TrafficGovernorNameResolverProvider.SCHEME + "://" + serviceUniqueName)
                    .keepAliveTime(keepAliveInSec <= 0 ? 600 : keepAliveInSec, TimeUnit.SECONDS)
                    .keepAliveWithoutCalls(true)
                    .idleTimeout(idleTimeoutInSec <= 0 ? (365 * 24 * 3600) : idleTimeoutInSec, TimeUnit.SECONDS)
                    .defaultLoadBalancingPolicy(loadBalancerProvider.getPolicyName())
                    .executor(rpcExecutor);
                if (sslContext != null) {
                    internalChannelBuilder
                        .negotiationType(NegotiationType.TLS)
                        .intercept(new TenantAwareClientInterceptor())
                        .sslContext(sslContext)
                        .overrideAuthority(serviceUniqueName);
                } else {
                    internalChannelBuilder
                        .negotiationType(NegotiationType.PLAINTEXT)
                        .intercept(new TenantAwareClientInterceptor());
                }
                if (eventLoopGroup != null) {
                    internalChannelBuilder.eventLoopGroup(eventLoopGroup)
                        .channelType(NettyUtil.determineSocketChannelClass(eventLoopGroup));
                }
                internalChannel = internalChannelBuilder.build();
                ConnStateListener connStateListener = (server, connState) ->
                    connStateSubject.onNext(IRPCClient.ConnState.values()[connState.ordinal()]);
                startStateListener(connStateListener);
            }

            @Override
            public Executor rpcExecutor() {
                return rpcExecutor;
            }

            @Override
            public Channel channel() {
                return internalChannel;
            }

            @Override
            public Observable<IRPCClient.ConnState> connState() {
                return connStateSubject;
            }

            @Override
            public Observable<Map<String, Map<String, String>>> serverList() {
                return serverListSubject;
            }

            @Override
            public Observable<IUpdateListener.IServerSelector> serverSelectorObservable() {
                return serverSelectorSubject;
            }

            @Override
            public boolean shutdown(long timeout, TimeUnit unit) {
                if (internalChannel.isShutdown()) {
                    return true;
                }
                long nsLeft = TimeUnit.NANOSECONDS.convert(timeout, unit);
                long timeoutNS = nsLeft;
                LoadBalancerRegistry.getDefaultRegistry().deregister(loadBalancerProvider);
                boolean result;
                try {
                    long start = System.nanoTime();
                    internalChannel.shutdownNow();
                    result = internalChannel.awaitTermination(timeout / 2, unit);
                    nsLeft -= System.nanoTime() - start;
                } catch (InterruptedException e) {
                    result = internalChannel.isTerminated();
                }
                if (needShutdownExecutor) {
                    ExecutorService executorService = (ExecutorService) rpcExecutor;
                    result &= MoreExecutors.shutdownAndAwaitTermination(executorService,
                        Math.max(timeoutNS / 2, nsLeft), TimeUnit.NANOSECONDS);
                }
                serverSelectorSubject.onComplete();
                connStateSubject.onComplete();
                return result;
            }

            private void startStateListener(ConnStateListener connStateListener) {
                ConnectivityState currentState = this.internalChannel.getState(true);
                connStateListener.onChange(serviceUniqueName, currentState);
                if (currentState != ConnectivityState.SHUTDOWN) {
                    this.internalChannel.notifyWhenStateChanged(currentState,
                        () -> startStateListener(connStateListener));
                }
            }
        };
        return new RPCClient(serviceUniqueName, bluePrint, channelHolder);
    }
}

