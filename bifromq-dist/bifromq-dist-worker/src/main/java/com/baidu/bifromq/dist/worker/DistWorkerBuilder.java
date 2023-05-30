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

package com.baidu.bifromq.dist.worker;

import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.balance.option.KVRangeBalanceControllerOptions;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.server.IBaseKVStoreServer;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProcFactory;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.baseutils.PortUtil;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.inboxbroker.IInboxBrokerManager;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.google.common.base.Preconditions;
import io.netty.channel.EventLoopGroup;
import java.io.File;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import lombok.NonNull;

public abstract class DistWorkerBuilder<T extends DistWorkerBuilder> {
    protected IAgentHost agentHost;
    protected ICRDTService crdtService;
    protected IEventCollector eventCollector;
    protected IDistClient distClient;
    protected IBaseKVStoreClient storeClient;

    protected ISettingProvider settingProvider;
    protected IInboxBrokerManager inboxBrokerManager;
    protected KVRangeStoreOptions kvRangeStoreOptions;
    protected KVRangeBalanceControllerOptions balanceControllerOptions;
    protected Duration statsInterval = Duration.ofSeconds(30);
    protected Duration gcInterval = Duration.ofMinutes(5);
    protected Executor ioExecutor;
    protected Executor queryExecutor;
    protected Executor mutationExecutor;
    protected ScheduledExecutorService tickTaskExecutor;
    protected ScheduledExecutorService bgTaskExecutor;

    public T agentHost(@NonNull IAgentHost agentHost) {
        this.agentHost = agentHost;
        return (T) this;
    }

    public T crdtService(@NonNull ICRDTService crdtService) {
        this.crdtService = crdtService;
        return (T) this;
    }

    public T settingProvider(ISettingProvider settingProvider) {
        this.settingProvider = settingProvider;
        return (T) this;
    }

    public T eventCollector(IEventCollector eventCollector) {
        this.eventCollector = eventCollector;
        return (T) this;
    }

    public T distClient(@NonNull IDistClient distClient) {
        this.distClient = distClient;
        return (T) this;
    }

    public T storeClient(@NonNull IBaseKVStoreClient storeClient) {
        this.storeClient = storeClient;
        return (T) this;
    }

    public T inboxBrokerManager(IInboxBrokerManager inboxBrokerManager) {
        this.inboxBrokerManager = inboxBrokerManager;
        return (T) this;
    }

    public T kvRangeStoreOptions(KVRangeStoreOptions kvRangeStoreOptions) {
        this.kvRangeStoreOptions = kvRangeStoreOptions;
        return (T) this;
    }

    public T balanceControllerOptions(KVRangeBalanceControllerOptions balanceControllerOptions) {
        this.balanceControllerOptions = balanceControllerOptions;
        return (T) this;
    }

    public T ioExecutor(Executor rpcExecutor) {
        this.ioExecutor = rpcExecutor;
        return (T) this;
    }

    public T queryExecutor(Executor queryExecutor) {
        this.queryExecutor = queryExecutor;
        return (T) this;
    }

    public T mutationExecutor(Executor mutationExecutor) {
        this.mutationExecutor = mutationExecutor;
        return (T) this;
    }

    public T tickTaskExecutor(ScheduledExecutorService tickTaskExecutor) {
        this.tickTaskExecutor = tickTaskExecutor;
        return (T) this;
    }

    public T bgTaskExecutor(ScheduledExecutorService bgTaskExecutor) {
        this.bgTaskExecutor = bgTaskExecutor;
        return (T) this;
    }

    public T statsInterval(Duration statsInterval) {
        this.statsInterval = statsInterval;
        return (T) this;
    }

    public T gcInterval(Duration gcInterval) {
        this.gcInterval = gcInterval;
        return (T) this;
    }

    public abstract IDistWorker build();

    public static final class InProcDistWorker extends DistWorkerBuilder<InProcDistWorker> {
        @Override
        public IDistWorker build() {
            Preconditions.checkNotNull(agentHost);
            Preconditions.checkNotNull(crdtService);
            Preconditions.checkNotNull(distClient);
            Preconditions.checkNotNull(storeClient);
            return new DistWorker(agentHost,
                crdtService,
                settingProvider,
                eventCollector,
                distClient,
                storeClient,
                inboxBrokerManager,
                statsInterval,
                gcInterval,
                kvRangeStoreOptions,
                balanceControllerOptions,
                ioExecutor,
                queryExecutor,
                mutationExecutor,
                tickTaskExecutor,
                bgTaskExecutor) {
                @Override
                protected IBaseKVStoreServer buildKVStoreServer(String clusterId,
                                                                IAgentHost agentHost,
                                                                ICRDTService crdtService,
                                                                IKVRangeCoProcFactory coProcFactory,
                                                                KVRangeStoreOptions kvRangeStoreOptions,
                                                                Executor ioExecutor,
                                                                Executor queryExecutor,
                                                                Executor mutationExecutor,
                                                                ScheduledExecutorService tickTaskExecutor,
                                                                ScheduledExecutorService bgTaskExecutor) {
                    return IBaseKVStoreServer.inProcServerBuilder()
                        .clusterId(clusterId)
                        .agentHost(agentHost)
                        .crdtService(crdtService)
                        .coProcFactory(coProcFactory)
                        .ioExecutor(ioExecutor)
                        .queryExecutor(queryExecutor)
                        .mutationExecutor(mutationExecutor)
                        .tickTaskExecutor(tickTaskExecutor)
                        .bgTaskExecutor(bgTaskExecutor)
                        .storeOptions(kvRangeStoreOptions)
                        .build();
                }
            };
        }
    }

    abstract static class InterProcDistWorkerBuilder<T extends InterProcDistWorkerBuilder>
        extends DistWorkerBuilder<T> {
        protected String bindAddr;
        protected int bindPort = PortUtil.freePort();
        protected EventLoopGroup bossEventLoopGroup;
        protected EventLoopGroup workerEventLoopGroup;

        public T bindAddr(@NonNull String bindAddr) {
            this.bindAddr = bindAddr;
            return (T) this;
        }

        public T bindPort(int bindPort) {
            Preconditions.checkArgument(bindPort >= 0);
            this.bindPort = bindPort;
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

    public static final class NonSSLDistWorkerBuilder extends InterProcDistWorkerBuilder<NonSSLDistWorkerBuilder> {

        @Override
        public IDistWorker build() {
            Preconditions.checkNotNull(agentHost);
            Preconditions.checkNotNull(crdtService);
            Preconditions.checkNotNull(distClient);
            Preconditions.checkNotNull(storeClient);
            Preconditions.checkNotNull(bindAddr);
            return new DistWorker(agentHost,
                crdtService,
                settingProvider,
                eventCollector,
                distClient,
                storeClient,
                inboxBrokerManager,
                statsInterval,
                gcInterval,
                kvRangeStoreOptions,
                balanceControllerOptions,
                ioExecutor,
                queryExecutor,
                mutationExecutor,
                tickTaskExecutor,
                bgTaskExecutor) {
                @Override
                protected IBaseKVStoreServer buildKVStoreServer(String clusterId,
                                                                IAgentHost agentHost,
                                                                ICRDTService crdtService,
                                                                IKVRangeCoProcFactory coProcFactory,
                                                                KVRangeStoreOptions kvRangeStoreOptions,
                                                                Executor ioExecutor,
                                                                Executor queryExecutor,
                                                                Executor mutationExecutor,
                                                                ScheduledExecutorService tickTaskExecutor,
                                                                ScheduledExecutorService bgTaskExecutor) {
                    return IBaseKVStoreServer
                        .nonSSLServerBuilder()
                        .clusterId(clusterId)
                        .agentHost(agentHost)
                        .crdtService(crdtService)
                        .coProcFactory(coProcFactory)
                        .ioExecutor(ioExecutor)
                        .queryExecutor(queryExecutor)
                        .mutationExecutor(mutationExecutor)
                        .tickTaskExecutor(tickTaskExecutor)
                        .bgTaskExecutor(bgTaskExecutor)
                        .storeOptions(kvRangeStoreOptions)
                        .bindAddr(bindAddr)
                        .bindPort(bindPort)
                        .bossEventLoopGroup(bossEventLoopGroup)
                        .workerEventLoopGroup(workerEventLoopGroup)
                        .build();
                }
            };
        }
    }

    public static final class SSLDistWorkerBuilder extends InterProcDistWorkerBuilder<SSLDistWorkerBuilder> {
        private File serviceIdentityCertFile;

        private File privateKeyFile;

        private File trustCertsFile;

        public SSLDistWorkerBuilder serviceIdentityCertFile(File serviceIdentityFile) {
            this.serviceIdentityCertFile = serviceIdentityFile;
            return this;
        }

        public SSLDistWorkerBuilder privateKeyFile(File privateKeyFile) {
            this.privateKeyFile = privateKeyFile;
            return this;
        }

        public SSLDistWorkerBuilder trustCertsFile(File trustCertsFile) {
            this.trustCertsFile = trustCertsFile;
            return this;
        }

        @Override
        public IDistWorker build() {
            Preconditions.checkNotNull(agentHost);
            Preconditions.checkNotNull(crdtService);
            Preconditions.checkNotNull(distClient);
            Preconditions.checkNotNull(storeClient);
            Preconditions.checkNotNull(bindAddr);
            return new DistWorker(agentHost,
                crdtService,
                settingProvider,
                eventCollector,
                distClient,
                storeClient,
                inboxBrokerManager,
                statsInterval,
                gcInterval,
                kvRangeStoreOptions,
                balanceControllerOptions,
                ioExecutor,
                queryExecutor,
                mutationExecutor,
                tickTaskExecutor,
                bgTaskExecutor) {
                @Override
                protected IBaseKVStoreServer buildKVStoreServer(String clusterId,
                                                                IAgentHost agentHost,
                                                                ICRDTService crdtService,
                                                                IKVRangeCoProcFactory coProcFactory,
                                                                KVRangeStoreOptions kvRangeStoreOptions,
                                                                Executor ioExecutor,
                                                                Executor queryExecutor,
                                                                Executor mutationExecutor,
                                                                ScheduledExecutorService tickTaskExecutor,
                                                                ScheduledExecutorService bgTaskExecutor) {
                    return IBaseKVStoreServer
                        .sslServerBuilder()
                        .clusterId(clusterId)
                        .agentHost(agentHost)
                        .crdtService(crdtService)
                        .coProcFactory(coProcFactory)
                        .ioExecutor(ioExecutor)
                        .queryExecutor(queryExecutor)
                        .mutationExecutor(mutationExecutor)
                        .tickTaskExecutor(tickTaskExecutor)
                        .bgTaskExecutor(bgTaskExecutor)
                        .storeOptions(kvRangeStoreOptions)
                        .bindAddr(bindAddr)
                        .bindPort(bindPort)
                        .bossEventLoopGroup(bossEventLoopGroup)
                        .workerEventLoopGroup(workerEventLoopGroup)
                        .serviceIdentityFile(serviceIdentityCertFile)
                        .privateKeyFile(privateKeyFile)
                        .trustCertsFile(trustCertsFile)
                        .build();
                }
            };
        }
    }
}
