/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.baserpc.client;

import static com.baidu.bifromq.baserpc.utils.NettyUtil.determineSocketChannelClass;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.baserpc.BluePrint;
import com.baidu.bifromq.baserpc.client.interceptor.TenantAwareClientInterceptor;
import com.baidu.bifromq.baserpc.client.loadbalancer.IServerSelector;
import com.baidu.bifromq.baserpc.client.loadbalancer.TrafficDirectiveLoadBalancerProvider;
import com.baidu.bifromq.baserpc.client.nameresolver.TrafficGovernorNameResolverProvider;
import com.baidu.bifromq.baserpc.client.util.BehaviorSubject;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceLandscape;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
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
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * The builder for creating RPC client.
 */
@Accessors(fluent = true)
@Setter
@NoArgsConstructor(access = AccessLevel.PACKAGE)
public final class RPCClientBuilder {
    private IRPCServiceTrafficService trafficService;
    private BluePrint bluePrint;
    private Executor executor;
    private EventLoopGroup eventLoopGroup;
    private long keepAliveInSec;
    private long idleTimeoutInSec;
    private SslContext sslContext;

    public RPCClientBuilder sslContext(SslContext sslContext) {
        if (sslContext != null) {
            Preconditions.checkArgument(sslContext.isClient(), "Client auth must be enabled");
        }
        this.sslContext = sslContext;
        return this;
    }

    public IRPCClient build() {
        final String serviceUniqueName = bluePrint.serviceDescriptor().getName();
        IChannelHolder channelHolder = new IChannelHolder() {
            private final ManagedChannel internalChannel;
            private final BehaviorSubject<IServerSelector> serverSelectorSubject = BehaviorSubject.create();
            private final BehaviorSubject<IRPCClient.ConnState> connStateSubject = BehaviorSubject.create();
            // key: server id, value: server attributes
            private final Observable<Map<String, Map<String, String>>> serverListSubject;
            private final LoadBalancerProvider loadBalancerProvider;
            private final Executor rpcExecutor;

            {
                IRPCServiceLandscape serviceLandscape = trafficService.getServiceLandscape(serviceUniqueName);
                rpcExecutor = executor;
                loadBalancerProvider =
                    new TrafficDirectiveLoadBalancerProvider(bluePrint, serverSelectorSubject::onNext);
                serverListSubject = serviceLandscape.serverEndpoints()
                    .map(sl -> sl.stream().collect(Collectors.toMap(s -> s.id(), s -> s.attrs())));

                LoadBalancerRegistry.getDefaultRegistry().register(loadBalancerProvider);

                TrafficGovernorNameResolverProvider.register(serviceUniqueName, serviceLandscape);

                LocalInProcNettyChannelBuilder internalChannelBuilder = LocalInProcNettyChannelBuilder
                    .forTarget(TrafficGovernorNameResolverProvider.SCHEME + "://" + serviceUniqueName)
                    .keepAliveTime(keepAliveInSec <= 0 ? 600 : keepAliveInSec, TimeUnit.SECONDS)
                    .keepAliveWithoutCalls(true)
                    .idleTimeout(idleTimeoutInSec <= 0 ? (365 * 24 * 3600) : idleTimeoutInSec, TimeUnit.SECONDS)
                    .maxInboundMessageSize(Integer.MAX_VALUE)
                    .defaultLoadBalancingPolicy(loadBalancerProvider.getPolicyName())
                    .executor(rpcExecutor);
                if (sslContext != null) {
                    internalChannelBuilder
                        .negotiationType(NegotiationType.TLS)
                        .intercept(new TenantAwareClientInterceptor())
                        .sslContext(sslContext);
                } else {
                    internalChannelBuilder
                        .negotiationType(NegotiationType.PLAINTEXT)
                        .intercept(new TenantAwareClientInterceptor());
                }
                if (eventLoopGroup != null) {
                    internalChannelBuilder.eventLoopGroup(eventLoopGroup)
                        .channelType(determineSocketChannelClass(eventLoopGroup));
                }
                internalChannel = internalChannelBuilder.build();
                ConnStateListener connStateListener = (server, connState) ->
                    connStateSubject.onNext(IRPCClient.ConnState.values()[connState.ordinal()]);
                startStateListener(connStateListener);
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
            public Observable<IServerSelector> serverSelectorObservable() {
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
        return new RPCClient(bluePrint, channelHolder);
    }
}

