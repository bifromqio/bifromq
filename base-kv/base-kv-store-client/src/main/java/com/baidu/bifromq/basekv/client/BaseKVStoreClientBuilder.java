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

package com.baidu.bifromq.basekv.client;

import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.RPCBluePrint;
import com.baidu.bifromq.baserpc.IRPCClient;
import io.netty.channel.EventLoopGroup;
import io.reactivex.rxjava3.annotations.NonNull;
import java.io.File;
import java.util.UUID;
import java.util.concurrent.Executor;

public abstract class BaseKVStoreClientBuilder<T extends BaseKVStoreClientBuilder<T>> {
    protected final String id = UUID.randomUUID().toString();
    protected String clusterId;
    protected ICRDTService crdtService;
    protected Executor executor;
    protected int execPipelinesPerServer;
    protected int queryPipelinesPerServer;

    public T clusterId(@NonNull String clusterId) {
        this.clusterId = clusterId;
        return (T) this;
    }

    public T crdtService(@NonNull ICRDTService crdtService) {
        this.crdtService = crdtService;
        return (T) this;
    }

    public T executor(Executor executor) {
        this.executor = executor;
        return (T) this;
    }

    public T execPipelinesPerServer(int execPipelinesPerServer) {
        this.execPipelinesPerServer = execPipelinesPerServer;
        return (T) this;
    }

    public T queryPipelinesPerServer(int queryPipelinesPerServer) {
        this.queryPipelinesPerServer = queryPipelinesPerServer;
        return (T) this;
    }

    public abstract IBaseKVStoreClient build();

    public static final class InProcClientBuilder extends BaseKVStoreClientBuilder<InProcClientBuilder> {
        @Override
        public IBaseKVStoreClient build() {
            IRPCClient rpcClient = IRPCClient.builder()
                .serviceUniqueName(clusterId)
                .bluePrint(RPCBluePrint.INSTANCE)
                .executor(executor)
                .inProcChannel()
                .buildChannel()
                .build();

            return new BaseKVStoreClient(clusterId, crdtService, rpcClient,
                execPipelinesPerServer, queryPipelinesPerServer);
        }
    }

    public abstract static class InterProcClientBuilder<T extends InterProcClientBuilder<T>>
        extends BaseKVStoreClientBuilder<T> {
        protected EventLoopGroup eventLoopGroup;
        protected long keepAliveInSec;
        protected long idleTimeoutInSec;

        public T eventLoopGroup(EventLoopGroup eventLoopGroup) {
            this.eventLoopGroup = eventLoopGroup;
            return (T) this;
        }

        public T keepAliveInSec(long keepAliveInSec) {
            this.keepAliveInSec = keepAliveInSec;
            return (T) this;
        }

        public T idleTimeoutInSec(long idleTimeoutInSec) {
            this.idleTimeoutInSec = idleTimeoutInSec;
            return (T) this;
        }
    }

    public static final class NonSSLClientBuilder extends InterProcClientBuilder<NonSSLClientBuilder> {

        @Override
        public IBaseKVStoreClient build() {
            IRPCClient rpcClient = IRPCClient.builder()
                .serviceUniqueName(clusterId)
                .bluePrint(RPCBluePrint.INSTANCE)
                .executor(executor)
                .nonSSLChannel()
                .eventLoopGroup(eventLoopGroup)
                .idleTimeoutInSec(idleTimeoutInSec)
                .keepAliveInSec(keepAliveInSec)
                .crdtService(crdtService)
                .buildChannel()
                .build();

            return new BaseKVStoreClient(clusterId, crdtService, rpcClient,
                execPipelinesPerServer, queryPipelinesPerServer) {
            };
        }
    }

    public static final class SSLClientBuilder extends InterProcClientBuilder<SSLClientBuilder> {
        private File serviceIdentityCertFile;
        private File privateKeyFile;
        private File trustCertsFile;

        public SSLClientBuilder serviceIdentityCertFile(File serviceIdentityCertFile) {
            this.serviceIdentityCertFile = serviceIdentityCertFile;
            return this;
        }

        public SSLClientBuilder privateKeyFile(File privateKeyFile) {
            this.privateKeyFile = privateKeyFile;
            return this;
        }

        public SSLClientBuilder trustCertsFile(File trustCertsFile) {
            this.trustCertsFile = trustCertsFile;
            return this;
        }

        @Override
        public IBaseKVStoreClient build() {
            IRPCClient rpcClient = IRPCClient.builder()
                .serviceUniqueName(clusterId)
                .bluePrint(RPCBluePrint.INSTANCE)
                .executor(executor)
                .sslChannel()
                .serviceIdentityCertFile(serviceIdentityCertFile)
                .privateKeyFile(privateKeyFile)
                .trustCertsFile(trustCertsFile)
                .eventLoopGroup(eventLoopGroup)
                .crdtService(crdtService)
                .idleTimeoutInSec(idleTimeoutInSec)
                .keepAliveInSec(keepAliveInSec)
                .buildChannel()
                .build();


            return new BaseKVStoreClient(clusterId, crdtService, rpcClient,
                execPipelinesPerServer, queryPipelinesPerServer) {
            };
        }
    }
}
