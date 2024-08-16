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

package com.baidu.bifromq.baserpc;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.baserpc.interceptor.TenantAwareServerInterceptor;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceServerRegister;
import com.baidu.bifromq.baserpc.utils.NettyUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallExecutorSupplier;
import io.grpc.ServerInterceptors;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.micrometer.core.instrument.binder.netty4.NettyEventExecutorMetrics;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class RPCServer implements IRPCServer {
    public interface ExecutorSupplier {
        @NonNull Executor getExecutor(MethodDescriptor<?, ?> call);
    }

    private enum State {
        INIT, STARTING, STARTED, STOPPING, STOPPED, FATAL_FAILURE
    }

    private final AtomicReference<State>
        state = new AtomicReference<>(State.INIT);

    private final String id;
    private final Executor defaultExecutor;
    private final Map<ServerServiceDefinition, BluePrint> serviceDefinitions;
    private final Map<String, Map<String, String>> serviceMetadata = new HashMap<>();
    private final Map<String, IRPCServiceServerRegister> serverRegisters = new HashMap<>();
    private final Server inProcServer;
    private final Server interProcServer;

    private final boolean needShutdownExecutor;

    RPCServer(RPCServerBuilder builder) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(builder.host) && !"0.0.0.0".equals(builder.host),
            "Invalid host");
        Preconditions.checkArgument(!builder.serviceDefinitions.isEmpty(), "No service defined");
        this.id = builder.id;
        this.serviceDefinitions = builder.serviceDefinitions;
        needShutdownExecutor = builder.executor == null;

        if (needShutdownExecutor) {
            int threadNum = Math.max(EnvProvider.INSTANCE.availableProcessors(), 1);
            this.defaultExecutor = ExecutorServiceMetrics
                .monitor(Metrics.globalRegistry, new ThreadPoolExecutor(threadNum, threadNum,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedTransferQueue<>(),
                    EnvProvider.INSTANCE.newThreadFactory("rpc_server-executor", true)), "rpc_server-executor");
        } else {
            this.defaultExecutor = builder.executor;
        }

        ServerBuilder<?> serverBuilder = InProcServerBuilder.forName(this.id).executor(builder.executor);
        if (builder.executorSupplier != null) {
            serverBuilder.callExecutor(new ServerCallExecutorSupplier() {
                @Override
                public <ReqT, RespT> Executor getExecutor(ServerCall<ReqT, RespT> call, Metadata metadata) {
                    return builder.executorSupplier.getExecutor(call.getMethodDescriptor());
                }
            });
        }
        bindServiceToServer(serverBuilder);
        inProcServer = serverBuilder.build();


        NettyServerBuilder nettyServerBuilder = NettyServerBuilder
            .forAddress(new InetSocketAddress(builder.host, builder.port))
            .permitKeepAliveWithoutCalls(true)
            .maxInboundMessageSize(Integer.MAX_VALUE)
            .executor(builder.executor);
        if (builder.executorSupplier != null) {
            nettyServerBuilder.callExecutor(new ServerCallExecutorSupplier() {
                @Override
                public <ReqT, RespT> Executor getExecutor(ServerCall<ReqT, RespT> call, Metadata metadata) {
                    return builder.executorSupplier.getExecutor(call.getMethodDescriptor());
                }
            });
        }
        if (builder.sslContext != null) {
            nettyServerBuilder.sslContext(builder.sslContext);
        }
        if (builder.bossEventLoopGroup == null) {
            builder.bossEventLoopGroup =
                NettyUtil.createEventLoopGroup(1, EnvProvider.INSTANCE.newThreadFactory("rpc-server-boss-elg"));
            new NettyEventExecutorMetrics(builder.bossEventLoopGroup).bindTo(Metrics.globalRegistry);
        }
        if (builder.workerEventLoopGroup == null) {
            builder.workerEventLoopGroup =
                NettyUtil.createEventLoopGroup(0, EnvProvider.INSTANCE.newThreadFactory("rpc-server-worker-elg"));
            new NettyEventExecutorMetrics(builder.workerEventLoopGroup).bindTo(Metrics.globalRegistry);
        }
        // if null, GRPC managed shared eventloop group will be used
        nettyServerBuilder.bossEventLoopGroup(builder.bossEventLoopGroup)
            .workerEventLoopGroup(builder.workerEventLoopGroup)
            .channelType(NettyUtil.determineServerSocketChannelClass(builder.bossEventLoopGroup));
        bindServiceToServer(nettyServerBuilder);
        interProcServer = nettyServerBuilder.build();

        // the common name of the cert contains service unique name to which the server instance belongs
        serviceDefinitions.forEach((serviceDef, bluePrint) -> {
            String serviceName = serviceDef.getServiceDescriptor().getName();
            serverRegisters.put(serviceName, IRPCServiceServerRegister.newInstance(serviceName, builder.crdtService));
            serviceMetadata.put(serviceName, builder.serviceMetadata.getOrDefault(serviceName, emptyMap()));
        });
    }

    private void bindServiceToServer(ServerBuilder<?> builder) {
        serviceDefinitions.forEach((orig, bluePrint) -> {
            ServerServiceDefinition.Builder serverDefBuilder =
                ServerServiceDefinition.builder(orig.getServiceDescriptor().getName());
            for (ServerMethodDefinition serverMethodDef : orig.getMethods()) {
                MethodDescriptor methodDesc =
                    bluePrint.methodDesc(serverMethodDef.getMethodDescriptor().getFullMethodName());
                if (methodDesc != null) {
                    serverDefBuilder.addMethod(methodDesc, serverMethodDef.getServerCallHandler());
                } else {
                    serverDefBuilder.addMethod(serverMethodDef);
                }
            }
            ServerServiceDefinition serviceDef = serverDefBuilder.build();
            builder.addService(
                ServerInterceptors.intercept(serviceDef, new TenantAwareServerInterceptor(serviceDef, bluePrint)));
        });
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public final void start() {
        if (state.compareAndSet(State.INIT, State.STARTING)) {
            try {
                inProcServer.start();
                interProcServer.start();
                serverRegisters.forEach((serviceName, serverRegister) -> {
                    log.debug("Start server register for service: {}", serviceName);
                    serverRegister.start(id, (InetSocketAddress) interProcServer.getListenSockets().get(0), emptySet(),
                        serviceMetadata.get(serviceName));
                });
                state.set(State.STARTED);
            } catch (IOException e) {
                state.set(State.FATAL_FAILURE);
                throw new IllegalStateException("Unable to start rpc server", e);
            }
        }
    }

    @Override
    public void shutdown() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            try {
                serverRegisters.forEach((serviceName, serviceRegister) -> {
                    log.debug("Stopping service registration: {}", serviceName);
                    serviceRegister.stop();
                });
                log.debug("Stopping inter-proc server");
                shutdownInternalServer(interProcServer);
                log.debug("Stopping in-proc server");
                shutdownInternalServer(inProcServer);
            } finally {
                if (needShutdownExecutor) {
                    ExecutorService executorService = (ExecutorService) defaultExecutor;
                    executorService.shutdown();
                }
                state.set(State.STOPPED);
            }
        }
    }

    private void shutdownInternalServer(Server server) {
        try {
            server.shutdownNow();
            server.awaitTermination();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
