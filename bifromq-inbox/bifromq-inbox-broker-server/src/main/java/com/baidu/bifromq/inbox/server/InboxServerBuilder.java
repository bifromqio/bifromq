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

package com.baidu.bifromq.inbox.server;

import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.baserpc.CertInfo;
import com.baidu.bifromq.baserpc.IRPCServer;
import com.baidu.bifromq.inbox.IInboxServiceBuilder;
import com.baidu.bifromq.inbox.RPCBluePrint;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.google.common.base.Preconditions;
import io.netty.channel.EventLoopGroup;
import java.io.File;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import lombok.NonNull;

public abstract class InboxServerBuilder<T extends InboxServerBuilder>
    implements IInboxServiceBuilder {
    protected ISettingProvider settingProvider;
    protected Executor ioExecutor;
    protected IBaseKVStoreClient storeClient;
    protected ScheduledExecutorService bgTaskExecutor;

    public T settingProvider(ISettingProvider settingProvider) {
        this.settingProvider = settingProvider;
        return (T) this;
    }

    public T ioExecutor(Executor ioExecutor) {
        this.ioExecutor = ioExecutor;
        return (T) this;
    }

    public T storeClient(IBaseKVStoreClient storeClient) {
        this.storeClient = storeClient;
        return (T) this;
    }

    public T bgTaskExecutor(ScheduledExecutorService bgTaskExecutor) {
        this.bgTaskExecutor = bgTaskExecutor;
        return (T) this;
    }

    public abstract IInboxServer build();

    public static final class InProcServerBuilder extends InboxServerBuilder<InProcServerBuilder> {

        @Override
        public IInboxServer build() {
            return new InboxServer(settingProvider, storeClient, bgTaskExecutor) {
                @Override
                protected IRPCServer buildRPCServer(InboxService inboxService) {
                    return IRPCServer.inProcServerBuilder()
                        .serviceUniqueName(SERVICE_NAME)
                        .defaultExecutor(ioExecutor)
                        .bluePrint(RPCBluePrint.INSTANCE)
                        .bindService(inboxService)
                        .build();
                }
            };
        }
    }

    abstract static class InterProcInboxServerBuilder<T extends InterProcInboxServerBuilder>
        extends InboxServerBuilder<T> {
        protected String id;
        protected String host;
        protected Integer port;
        protected ICRDTService crdtService;
        protected EventLoopGroup bossEventLoopGroup;
        protected EventLoopGroup workerEventLoopGroup;

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

    public static final class NonSSLServerBuilder extends InterProcInboxServerBuilder<NonSSLServerBuilder> {
        @Override
        public IInboxServer build() {
            return new InboxServer(settingProvider, storeClient, bgTaskExecutor) {
                @Override
                protected IRPCServer buildRPCServer(InboxService inboxService) {
                    return IRPCServer.nonSSLServerBuilder()
                        .serviceUniqueName(SERVICE_NAME)
                        .defaultExecutor(ioExecutor)
                        .bluePrint(RPCBluePrint.INSTANCE)
                        .bindService(inboxService)
                        .id(id)
                        .host(host)
                        .port(port)
                        .bossEventLoopGroup(bossEventLoopGroup)
                        .workerEventLoopGroup(workerEventLoopGroup)
                        .crdtService(crdtService)
                        .build();
                }
            };
        }
    }

    public static final class SSLServerBuilder extends InterProcInboxServerBuilder<SSLServerBuilder> {
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
        public IInboxServer build() {
            return new InboxServer(settingProvider, storeClient, bgTaskExecutor) {
                @Override
                protected IRPCServer buildRPCServer(InboxService inboxService) {
                    return IRPCServer.sslServerBuilder()
                        .serviceUniqueName(SERVICE_NAME)
                        .id(id)
                        .host(host)
                        .port(port)
                        .bluePrint(RPCBluePrint.INSTANCE)
                        .bindService(inboxService)
                        .defaultExecutor(ioExecutor)
                        .bossEventLoopGroup(bossEventLoopGroup)
                        .workerEventLoopGroup(workerEventLoopGroup)
                        .crdtService(crdtService)
                        .serviceIdentityCertFile(serviceIdentityCertFile)
                        .privateKeyFile(privateKeyFile)
                        .trustCertsFile(trustCertsFile)
                        .build();
                }
            };
        }
    }
}
