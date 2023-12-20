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

import static com.baidu.bifromq.baserpc.RPCContext.GPID;

import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.grpc.ServerServiceDefinition;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class RPCServerBuilder {
    @Accessors(fluent = true)
    @Getter
    String id = GPID + "/" + hashCode();
    String host;
    int port = 0;
    ICRDTService crdtService;
    EventLoopGroup bossEventLoopGroup;
    EventLoopGroup workerEventLoopGroup;
    SslContext sslContext;
    final Map<ServerServiceDefinition, BluePrint> serviceDefinitions = new HashMap<>();
    final Map<String, Map<String, String>> serviceMetadata = new HashMap<>();
    RPCServer.ExecutorSupplier executorSupplier;
    Executor executor;

    public RPCServerBuilder id(String id) {
        if (!Strings.isNullOrEmpty(id)) {
            this.id = id;
        }
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
        return bindService(serviceDefinition, bluePrint, Collections.emptyMap());
    }

    public RPCServerBuilder bindService(@NonNull ServerServiceDefinition serviceDefinition,
                                        @NonNull BluePrint bluePrint,
                                        Map<String, String> metadata) {
        BluePrint oldBluePrint = this.serviceDefinitions.put(serviceDefinition, bluePrint);
        assert oldBluePrint == null;
        Map<String, String> oldMetadata =
            this.serviceMetadata.put(serviceDefinition.getServiceDescriptor().getName(), metadata);
        assert oldMetadata == null;
        return this;
    }

    public RPCServerBuilder executorSupplier(RPCServer.ExecutorSupplier supplier) {
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
        return new RPCServer(this);
    }
}