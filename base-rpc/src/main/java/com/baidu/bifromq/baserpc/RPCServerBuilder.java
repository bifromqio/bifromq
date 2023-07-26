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
import com.google.common.base.Preconditions;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class RPCServerBuilder {
    public interface ExecutorSupplier {
        @NonNull Executor getExecutor(MethodDescriptor<?, ?> call);
    }

    private String id;
    private String host;
    private int port = 0;
    private ICRDTService crdtService;
    private EventLoopGroup bossEventLoopGroup;
    private EventLoopGroup workerEventLoopGroup;
    private SslContext sslContext;
    private final Map<ServerServiceDefinition, BluePrint> serviceDefinitions = new HashMap<>();
    private ExecutorSupplier executorSupplier;
    private Executor executor;

    public RPCServerBuilder id(String id) {
        this.id = id;
        return this;
    }

    public RPCServerBuilder host(String host) {
        this.host = host;
        return this;
    }

    public RPCServerBuilder port(int port) {
        Preconditions.checkArgument(port >= 0, "Port number must be non-negative");
        this.port = port;
        return this;
    }

    public RPCServerBuilder crdtService(@NonNull ICRDTService crdtService) {
        assert crdtService.isStarted();
        this.crdtService = crdtService;
        return this;
    }

    public RPCServerBuilder bossEventLoopGroup(EventLoopGroup bossEventLoopGroup) {
        this.bossEventLoopGroup = bossEventLoopGroup;
        return this;
    }

    public RPCServerBuilder workerEventLoopGroup(EventLoopGroup workerEventLoopGroup) {
        this.workerEventLoopGroup = workerEventLoopGroup;
        return this;
    }

    public RPCServerBuilder sslContext(SslContext sslContext) {
        if (sslContext != null) {
            Preconditions.checkArgument(sslContext.isServer(), "Server auth must be enabled");
        }
        this.sslContext = sslContext;
        return this;
    }

    public RPCServerBuilder bindService(@NonNull ServerServiceDefinition serviceDefinition,
                                        @NonNull BluePrint bluePrint) {
        this.serviceDefinitions.put(serviceDefinition, bluePrint);
        return this;
    }

    public RPCServerBuilder executorSupplier(ExecutorSupplier supplier) {
        this.executorSupplier = supplier;
        return this;
    }

    public RPCServerBuilder executor(Executor executor) {
        this.executor = executor;
        return this;
    }

    public IRPCServer build() {
        Preconditions.checkNotNull(crdtService, "a started crdt service must be provided");
        Preconditions.checkArgument(!serviceDefinitions.isEmpty());
        return RPCServer.builder()
            .id(id)
            .host(host)
            .port(port)
            .bossEventLoopGroup(bossEventLoopGroup)
            .workerEventLoopGroup(workerEventLoopGroup)
            .sslContext(sslContext)
            .crdtService(crdtService)
            .serviceDefinitions(serviceDefinitions)
            .executorSupplier(executorSupplier)
            .executor(executor)
            .build();
    }
}