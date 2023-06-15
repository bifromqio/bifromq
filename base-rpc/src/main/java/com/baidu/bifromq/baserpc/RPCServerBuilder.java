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

import static com.google.common.base.Strings.isNullOrEmpty;
import static io.grpc.internal.GrpcUtil.getThreadFactory;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.baserpc.interceptor.TrafficAwareServerInterceptor;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceServerRegister;
import com.baidu.bifromq.baserpc.utils.NettyUtil;
import com.google.common.base.Preconditions;
import io.grpc.BindableService;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallExecutorSupplier;
import io.grpc.ServerInterceptors;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class RPCServerBuilder<T extends RPCServerBuilder> {
    public interface ExecutorSupplier {
        @NonNull Executor getExecutor(MethodDescriptor call);
    }

    protected ArrayList<BindableService> bindServices = new ArrayList<>();

    protected ExecutorSupplier executorSupplier;
    protected Executor defaultExecutor;

    protected String serviceUniqueName;

    protected BluePrint bluePrint;

    public T serviceUniqueName(@NonNull String serviceUniqueName) {
        this.serviceUniqueName = serviceUniqueName;
        return (T) this;
    }

    public T bindService(@NonNull BindableService bindService) {
        this.bindServices.add(bindService);
        return (T) this;
    }

    public T bindServices(@NonNull Collection<? extends BindableService> bindServices) {
        this.bindServices.addAll(bindServices);
        return (T) this;
    }

    public T clearBindServices() {
        this.bindServices.clear();
        return (T) this;
    }

    public T executorSupplier(ExecutorSupplier supplier) {
        this.executorSupplier = supplier;
        return (T) this;
    }

    public T defaultExecutor(Executor executor) {
        this.defaultExecutor = executor;
        return (T) this;
    }

    public T bluePrint(@NonNull BluePrint bluePrint) {
        this.bluePrint = bluePrint;
        return (T) this;
    }

    public abstract IRPCServer build();

    public abstract static class RPCServer implements IRPCServer {
        private enum State {
            INIT, STARTING, STARTED, STOPPING, STOPPED, FATAL_FAILURE
        }

        private final AtomicReference<State> state = new AtomicReference<>(State.INIT);

        protected final String serviceUniqueName;
        protected final ExecutorSupplier executorSupplier;
        protected final Executor defaultExecutor;

        protected final BluePrint bluePrint;
        protected final Collection<? extends BindableService> bindServices;
        private final boolean inProc;
        private final boolean needShutdownExecutor;

        protected RPCServer(String serviceUniqueName,
                            @NonNull Collection<? extends BindableService> bindServices,
                            @NonNull BluePrint bluePrint,
                            ExecutorSupplier executorSupplier,
                            Executor defaultExecutor,
                            boolean inProc) {
            this.serviceUniqueName = serviceUniqueName;
            this.bluePrint = bluePrint;
            this.bindServices = bindServices;
            this.inProc = inProc;
            needShutdownExecutor = defaultExecutor == null;
            this.executorSupplier = executorSupplier != null ? executorSupplier : null;

            if (needShutdownExecutor) {
                int threadNum = Math.max(Runtime.getRuntime().availableProcessors(), 1);
                this.defaultExecutor = ExecutorServiceMetrics
                    .monitor(Metrics.globalRegistry, new ThreadPoolExecutor(threadNum, threadNum,
                            0L, TimeUnit.MILLISECONDS,
                            new LinkedTransferQueue<>(),
                            getThreadFactory(serviceUniqueName + "_server-executor-%d", true)),
                        serviceUniqueName + "_server-executor");
            } else {
                this.defaultExecutor = defaultExecutor;
            }
        }

        protected abstract Server delegate();

        protected void afterStart() {
        }

        protected void beforeShutdown() {
        }

        protected void bindServiceToServer(ServerBuilder builder) {
            bindServices.forEach((service) -> {
                ServerServiceDefinition orig = service.bindService();
                ServerServiceDefinition.Builder serverDefBuilder = ServerServiceDefinition
                    .builder(orig.getServiceDescriptor().getName());
                for (ServerMethodDefinition serverMethodDef : orig.getMethods()) {
                    MethodDescriptor methodDesc =
                        bluePrint.methodDesc(serverMethodDef.getMethodDescriptor().getFullMethodName(), inProc);
                    if (methodDesc != null) {
                        serverDefBuilder.addMethod(methodDesc, serverMethodDef.getServerCallHandler());
                    } else {
                        serverDefBuilder.addMethod(serverMethodDef);
                    }
                }
                ServerServiceDefinition serviceDef = serverDefBuilder.build();
                builder.addService(ServerInterceptors
                    .intercept(serviceDef, new TrafficAwareServerInterceptor(serviceUniqueName, serviceDef))
                );
            });
        }

        @Override
        public String serviceUniqueName() {
            return serviceUniqueName;
        }

        @Override
        public final void start() {
            if (state.compareAndSet(State.INIT, State.STARTING)) {
                try {
                    delegate().start();
                    afterStart();
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
                    delegate().shutdownNow();
                    delegate().awaitTermination();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    if (needShutdownExecutor) {
                        ExecutorService executorService = (ExecutorService) defaultExecutor;
                        executorService.shutdown();
                    }
                    state.set(State.STOPPED);
                }
            }
        }
    }

    static class InterProcRPCServer extends RPCServer {
        private final String id;
        private final Server server;
        private final IRPCServiceServerRegister serverRegister;

        @Builder
        InterProcRPCServer(String serviceUniqueName,
                           String id,
                           String host,
                           int port,
                           ICRDTService crdtService,
                           SslContext sslContext,
                           EventLoopGroup bossEventLoopGroup,
                           EventLoopGroup workerEventLoopGroup,
                           BluePrint bluePrint,
                           Collection<? extends BindableService> bindServices,
                           ExecutorSupplier executorSupplier,
                           Executor executor) {
            super(serviceUniqueName, bindServices, bluePrint, executorSupplier, executor, false);
            try {
                this.id = id;
                NettyServerBuilder nettyServerBuilder = NettyServerBuilder
                    .forAddress(new InetSocketAddress(host, port))
                    .permitKeepAliveWithoutCalls(true)
                    .executor(defaultExecutor);
                if (this.executorSupplier != null) {
                    nettyServerBuilder.callExecutor(new ServerCallExecutorSupplier() {
                        @Nullable
                        @Override
                        public <ReqT, RespT> Executor getExecutor(ServerCall<ReqT, RespT> call, Metadata metadata) {
                            return executorSupplier.getExecutor(call.getMethodDescriptor());
                        }
                    });
                }
                if (sslContext != null) {
                    nettyServerBuilder.sslContext(sslContext);
                }
                if (bossEventLoopGroup == null) {
                    bossEventLoopGroup = NettyUtil.createEventLoopGroup(1);
                }
                if (workerEventLoopGroup == null) {
                    workerEventLoopGroup = NettyUtil.createEventLoopGroup();
                }
                // if null, GRPC managed shared eventloop group will be used
                nettyServerBuilder.bossEventLoopGroup(bossEventLoopGroup)
                    .workerEventLoopGroup(workerEventLoopGroup)
                    .channelType(NettyUtil.determineServerSocketChannelClass(bossEventLoopGroup));
                bindServiceToServer(nettyServerBuilder);
                // the common name of the cert contains service unique name to which the server instance belongs
                serverRegister = IRPCServiceServerRegister.newInstance(serviceUniqueName, crdtService);
                server = nettyServerBuilder.build();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected Server delegate() {
            return server;
        }

        @Override
        protected void afterStart() {
            serverRegister.start(id, (InetSocketAddress) server.getListenSockets().get(0), emptySet(), emptyMap());
        }

        @Override
        protected void beforeShutdown() {
            serverRegister.stop();
        }

        @Override
        public String id() {
            return id;
        }
    }

    public static final class InProcServerBuilder extends RPCServerBuilder<InProcServerBuilder> {
        @Override
        public IRPCServer build() {
            Preconditions.checkArgument(!isNullOrEmpty(serviceUniqueName));
            Preconditions.checkArgument(!bindServices.isEmpty());
            return new RPCServer(serviceUniqueName, bindServices, bluePrint, executorSupplier, defaultExecutor, true) {
                @Override
                public String id() {
                    // only one server instance in proc mode, so use service unique name as its id
                    return serviceUniqueName;
                }

                private final Server server;

                {
                    ServerBuilder builder = InProcessServerBuilder.forName(serviceUniqueName).executor(defaultExecutor);
                    if (executorSupplier != null) {
                        builder.callExecutor(new ServerCallExecutorSupplier() {
                            @Nullable
                            @Override
                            public <ReqT, RespT> Executor getExecutor(ServerCall<ReqT, RespT> call, Metadata metadata) {
                                return executorSupplier.getExecutor(call.getMethodDescriptor());
                            }
                        });
                    }
                    bindServiceToServer(builder);
                    server = builder.build();
                }

                @Override
                protected Server delegate() {
                    return server;
                }
            };
        }
    }

    public abstract static class InterProcServerBuilder<T extends InterProcServerBuilder<?>>
        extends RPCServerBuilder<T> {
        protected String id;
        protected String host;
        protected int port = 0;
        protected ICRDTService crdtService;
        protected EventLoopGroup bossEventLoopGroup;
        protected EventLoopGroup workerEventLoopGroup;
        protected Class<? extends ServerChannel> channelType;

        public T id(@NonNull String id) {
            this.id = id;
            return (T) this;
        }

        public T host(@NonNull String host) {
            Preconditions.checkArgument(!"0.0.0.0".equals(host), "Invalid host ip");
            this.host = host;
            return (T) this;
        }

        public T port(@NonNull Integer port) {
            this.port = port;
            return (T) this;
        }

        public T crdtService(@NonNull ICRDTService crdtService) {
            assert crdtService.isStarted();
            this.crdtService = crdtService;
            return (T) this;
        }

        public T bossEventLoopGroup(EventLoopGroup bossEventLoopGroup) {
            this.bossEventLoopGroup = bossEventLoopGroup;
            return (T) this;
        }

        public T workerEventLoopGroup(EventLoopGroup workerEventLoopGroup) {
            this.workerEventLoopGroup = workerEventLoopGroup;
            return (T) this;
        }
    }

    public static final class NonSSLServerBuilder extends InterProcServerBuilder<NonSSLServerBuilder> {
        @Override
        public IRPCServer build() {
            Preconditions.checkArgument(!isNullOrEmpty(id));
            Preconditions.checkArgument(!isNullOrEmpty(host));
            Preconditions.checkArgument(port >= 0);
            Preconditions.checkArgument(!isNullOrEmpty(serviceUniqueName));
            Preconditions.checkNotNull(crdtService, "a started crdt service must be provided");
            Preconditions.checkArgument(!bindServices.isEmpty());
            return InterProcRPCServer.builder()
                .serviceUniqueName(serviceUniqueName)
                .id(id)
                .host(host)
                .port(port)
                .bossEventLoopGroup(bossEventLoopGroup)
                .workerEventLoopGroup(workerEventLoopGroup)
                .crdtService(crdtService)
                .bluePrint(bluePrint)
                .bindServices(bindServices)
                .executorSupplier(executorSupplier)
                .executor(defaultExecutor)
                .build();
        }
    }

    public static final class SSLServerBuilder extends InterProcServerBuilder<SSLServerBuilder> {
        private @NonNull File serviceIdentityCertFile;
        private @NonNull File privateKeyFile;
        private @NonNull File trustCertsFile;

        public SSLServerBuilder serviceIdentityCertFile(@NonNull File serviceIdentityCertFile) {
            this.serviceIdentityCertFile = serviceIdentityCertFile;
            CertInfo certInfo = CertInfo.parse(serviceIdentityCertFile);
            Preconditions.checkArgument(certInfo.serverAuth, "Not server auth cert");
            return this;
        }

        public SSLServerBuilder privateKeyFile(@NonNull File privateKeyFile) {
            this.privateKeyFile = privateKeyFile;
            return this;
        }

        public SSLServerBuilder trustCertsFile(@NonNull File trustCertsFile) {
            this.trustCertsFile = trustCertsFile;
            return this;
        }

        @Override
        public IRPCServer build() {
            Preconditions.checkArgument(!isNullOrEmpty(id));
            Preconditions.checkArgument(!isNullOrEmpty(host));
            Preconditions.checkArgument(!isNullOrEmpty(serviceUniqueName));
            Preconditions.checkArgument(port >= 0);
            Preconditions.checkNotNull(serviceIdentityCertFile);
            Preconditions.checkNotNull(privateKeyFile);
            Preconditions.checkNotNull(trustCertsFile);
            Preconditions.checkNotNull(crdtService, "a started crdt service must be provided");
            Preconditions.checkArgument(!bindServices.isEmpty());
            return InterProcRPCServer.builder()
                .serviceUniqueName(serviceUniqueName)
                .id(id)
                .host(host)
                .port(port)
                .bossEventLoopGroup(bossEventLoopGroup)
                .workerEventLoopGroup(workerEventLoopGroup)
                .crdtService(crdtService)
                .executorSupplier(executorSupplier)
                .executor(defaultExecutor)
                .bindServices(bindServices)
                .sslContext(sslContext())
                .build();
        }

        private SslContext sslContext() {
            try {
                SslContextBuilder sslClientContextBuilder = GrpcSslContexts
                    .forServer(serviceIdentityCertFile, privateKeyFile);
                sslClientContextBuilder.trustManager(trustCertsFile);
                sslClientContextBuilder.clientAuth(ClientAuth.REQUIRE);
                return GrpcSslContexts.configure(sslClientContextBuilder, SslProvider.OPENSSL).build();
            } catch (Exception e) {
                throw new RuntimeException("Fail to initialize shared server SSLContext", e);
            }
        }
    }
}