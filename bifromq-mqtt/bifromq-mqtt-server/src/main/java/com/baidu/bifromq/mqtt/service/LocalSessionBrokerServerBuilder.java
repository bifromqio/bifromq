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

package com.baidu.bifromq.mqtt.service;

import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.baserpc.CertInfo;
import com.baidu.bifromq.baserpc.IRPCServer;
import com.baidu.bifromq.mqtt.inbox.IMqttBrokerServiceBuilder;
import com.baidu.bifromq.mqtt.inbox.RPCBluePrint;
import com.google.common.base.Preconditions;
import io.netty.channel.EventLoopGroup;
import java.io.File;
import java.util.concurrent.Executor;
import lombok.NonNull;

abstract class LocalSessionBrokerServerBuilder<T extends LocalSessionBrokerServerBuilder>
    implements IMqttBrokerServiceBuilder {
    protected Executor executor;

    public T executor(Executor executor) {
        this.executor = executor;
        return (T) this;
    }

    public abstract ILocalSessionBrokerServer build();


    public static final class InProcBrokerBuilder extends LocalSessionBrokerServerBuilder<InProcBrokerBuilder> {
        @Override
        public ILocalSessionBrokerServer build() {
            return new LocalSessionBrokerServer() {
                @Override
                protected IRPCServer buildRPCServer(LocalSessionBrokerService service) {
                    return IRPCServer.inProcServerBuilder()
                        .serviceUniqueName(SERVICE_NAME)
                        .executor(executor)
                        .bluePrint(RPCBluePrint.INSTANCE)
                        .bindService(service)
                        .build();
                }
            };
        }
    }

    abstract static class InterProcBrokerBuilder<T extends InterProcBrokerBuilder>
        extends LocalSessionBrokerServerBuilder<T> {
        protected String serverId;
        protected String host;
        protected Integer port;
        protected ICRDTService crdtService;
        protected EventLoopGroup bossEventLoopGroup;
        protected EventLoopGroup workerEventLoopGroup;

        /**
         * The id of the server instance
         *
         * @param serverId
         * @return
         */
        public T serverId(String serverId) {
            this.serverId = serverId;
            return (T) this;
        }

        public T host(String host) {
            this.host = host;
            return (T) this;
        }

        public T port(@NonNull Integer port) {
            this.port = port;
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

        public T crdtService(ICRDTService crdtService) {
            this.crdtService = crdtService;
            return (T) this;
        }
    }

    public static final class NonSSLBrokerBuilder extends InterProcBrokerBuilder<NonSSLBrokerBuilder> {
        @Override
        public ILocalSessionBrokerServer build() {
            return new LocalSessionBrokerServer() {
                @Override
                protected IRPCServer buildRPCServer(LocalSessionBrokerService service) {
                    return IRPCServer.nonSSLServerBuilder()
                        .id(serverId)
                        .host(host)
                        .port(port)
                        .serviceUniqueName(SERVICE_NAME)
                        .executor(executor)
                        .bossEventLoopGroup(bossEventLoopGroup)
                        .workerEventLoopGroup(workerEventLoopGroup)
                        .crdtService(crdtService)
                        .bluePrint(RPCBluePrint.INSTANCE)
                        .bindService(service)
                        .build();
                }
            };
        }
    }

    public static final class SSLBrokerBuilder extends InterProcBrokerBuilder<SSLBrokerBuilder> {
        private @NonNull File serviceIdentityCertFile;
        private @NonNull File privateKeyFile;
        private @NonNull File trustCertsFile;

        public SSLBrokerBuilder serviceIdentityCertFile(@NonNull File serviceIdentityCertFile) {
            this.serviceIdentityCertFile = serviceIdentityCertFile;
            CertInfo certInfo = CertInfo.parse(serviceIdentityCertFile);
            Preconditions.checkArgument(certInfo.serverAuth, "Not server auth cert");
            return this;
        }

        public SSLBrokerBuilder privateKeyFile(@NonNull File privateKeyFile) {
            this.privateKeyFile = privateKeyFile;
            return this;
        }

        public SSLBrokerBuilder trustCertsFile(@NonNull File trustCertsFile) {
            this.trustCertsFile = trustCertsFile;
            return this;
        }

        @Override
        public ILocalSessionBrokerServer build() {
            return new LocalSessionBrokerServer() {
                @Override
                protected IRPCServer buildRPCServer(LocalSessionBrokerService service) {
                    return IRPCServer.sslServerBuilder()
                        .id(serverId)
                        .host(host)
                        .port(port)
                        .serviceUniqueName(SERVICE_NAME)
                        .executor(executor)
                        .bossEventLoopGroup(bossEventLoopGroup)
                        .workerEventLoopGroup(workerEventLoopGroup)
                        .crdtService(crdtService)
                        .bluePrint(RPCBluePrint.INSTANCE)
                        .bindService(service)
                        .serviceIdentityCertFile(serviceIdentityCertFile)
                        .privateKeyFile(privateKeyFile)
                        .trustCertsFile(trustCertsFile)
                        .build();
                }
            };
        }
    }
}
