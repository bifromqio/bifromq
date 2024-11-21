/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.starter;

import com.baidu.bifromq.apiserver.IAPIServer;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.metaservice.IBaseKVMetaService;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.server.IDistServer;
import com.baidu.bifromq.dist.worker.IDistWorker;
import com.baidu.bifromq.inbox.server.IInboxServer;
import com.baidu.bifromq.inbox.store.IInboxStore;
import com.baidu.bifromq.mqtt.IMQTTBroker;
import com.baidu.bifromq.plugin.authprovider.AuthProviderManager;
import com.baidu.bifromq.plugin.clientbalancer.ClientBalancerManager;
import com.baidu.bifromq.plugin.eventcollector.EventCollectorManager;
import com.baidu.bifromq.plugin.resourcethrottler.ResourceThrottlerManager;
import com.baidu.bifromq.plugin.settingprovider.SettingProviderManager;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.retain.server.IRetainServer;
import com.baidu.bifromq.retain.store.IRetainStore;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import com.baidu.bifromq.sessiondict.server.ISessionDictServer;
import com.google.inject.Inject;
import io.netty.channel.EventLoopGroup;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import javax.inject.Named;
import org.pf4j.PluginManager;

public class GracefulShutdown {
    private final IAgentHost agentHost;
    private final ICRDTService crdtService;
    private final EventLoopGroup rpcClientEventLoop;
    private final ExecutorService bgTasksScheduler;

    private final IRPCServiceTrafficService trafficService;
    private final IBaseKVMetaService metaService;

    private final PluginManager pluginManager;
    private final AuthProviderManager authProviderManager;
    private final ResourceThrottlerManager resourceThrottlerManager;
    private final EventCollectorManager eventCollectorManager;
    private final SettingProviderManager settingProviderManager;
    private final ClientBalancerManager clientBalancerManager;
    private final ISubBrokerManager subBrokerManager;

    private final Optional<IMQTTBroker> mqttBrokerOpt;
    private final Optional<IAPIServer> apiServerOpt;
    private final Optional<IDistServer> distServerOpt;
    private final Optional<IDistWorker> distWorkerOpt;
    private final Optional<IInboxServer> inboxServerOpt;
    private final Optional<IInboxStore> inboxStoreOpt;
    private final Optional<IRetainServer> retainServerOpt;
    private final Optional<IRetainStore> retainStoreOpt;
    private final Optional<ISessionDictServer> sessionDictServerOpt;

    private final IDistClient distClient;
    private final IBaseKVStoreClient distWorkerClient;

    private final IBaseKVStoreClient inboxStoreClient;

    private final IRetainClient retainClient;
    private final IBaseKVStoreClient retainStoreClient;

    private final ISessionDictClient sessionDictClient;

    @Inject
    public GracefulShutdown(IAgentHost agentHost,
                            ICRDTService crdtService,
                            @Named("rpcClientEventLoop") EventLoopGroup rpcClientEventLoop,
                            @Named("bgTaskScheduler") ScheduledExecutorService bgTaskScheduler,
                            IRPCServiceTrafficService trafficService,
                            IBaseKVMetaService metaService,
                            PluginManager pluginManager,
                            AuthProviderManager authProviderManager,
                            ResourceThrottlerManager resourceThrottlerManager,
                            EventCollectorManager eventCollectorManager,
                            SettingProviderManager settingProviderManager,
                            ClientBalancerManager clientBalancerManager,
                            ISubBrokerManager subBrokerManager,
                            Optional<IMQTTBroker> mqttBrokerOpt,
                            Optional<IAPIServer> apiServerOpt,
                            Optional<IDistServer> distServerOpt,
                            Optional<IDistWorker> distWorkerOpt,
                            Optional<IInboxServer> inboxServerOpt,
                            Optional<IInboxStore> inboxStoreOpt,
                            Optional<IRetainServer> retainServerOpt,
                            Optional<IRetainStore> retainStoreOpt,
                            Optional<ISessionDictServer> sessionDictServerOpt,
                            IDistClient distClient,
                            @Named("distWorkerClient") IBaseKVStoreClient distWorkerClient,
                            @Named("inboxStoreClient") IBaseKVStoreClient inboxStoreClient,
                            IRetainClient retainClient,
                            @Named("retainStoreClient") IBaseKVStoreClient retainStoreClient,
                            ISessionDictClient sessionDictClient) {
        this.agentHost = agentHost;
        this.crdtService = crdtService;
        this.rpcClientEventLoop = rpcClientEventLoop;
        this.bgTasksScheduler = bgTaskScheduler;
        this.trafficService = trafficService;
        this.metaService = metaService;
        this.pluginManager = pluginManager;
        this.authProviderManager = authProviderManager;
        this.resourceThrottlerManager = resourceThrottlerManager;
        this.eventCollectorManager = eventCollectorManager;
        this.settingProviderManager = settingProviderManager;
        this.clientBalancerManager = clientBalancerManager;
        this.subBrokerManager = subBrokerManager;
        this.mqttBrokerOpt = mqttBrokerOpt;
        this.apiServerOpt = apiServerOpt;
        this.distServerOpt = distServerOpt;
        this.distWorkerOpt = distWorkerOpt;
        this.inboxServerOpt = inboxServerOpt;
        this.inboxStoreOpt = inboxStoreOpt;
        this.retainServerOpt = retainServerOpt;
        this.retainStoreOpt = retainStoreOpt;
        this.sessionDictServerOpt = sessionDictServerOpt;
        this.distClient = distClient;
        this.distWorkerClient = distWorkerClient;
        this.inboxStoreClient = inboxStoreClient;
        this.retainClient = retainClient;
        this.retainStoreClient = retainStoreClient;
        this.sessionDictClient = sessionDictClient;
    }

    public void shutdown() {
        mqttBrokerOpt.ifPresent(IMQTTBroker::close);
        apiServerOpt.ifPresent(IAPIServer::close);

        distClient.close();
        distServerOpt.ifPresent(IDistServer::close);

        distWorkerClient.close();
        distWorkerOpt.ifPresent(IDistWorker::close);

        inboxServerOpt.ifPresent(IInboxServer::close);

        inboxStoreClient.close();
        inboxStoreOpt.ifPresent(IInboxStore::close);

        retainClient.close();
        retainServerOpt.ifPresent(IRetainServer::close);

        retainStoreClient.close();
        retainStoreOpt.ifPresent(IRetainStore::close);

        sessionDictClient.close();
        sessionDictServerOpt.ifPresent(ISessionDictServer::close);

        metaService.close();
        trafficService.close();
        crdtService.close();
        agentHost.close();

        authProviderManager.close();
        resourceThrottlerManager.close();
        clientBalancerManager.close();
        eventCollectorManager.close();
        settingProviderManager.close();
        subBrokerManager.close();

        rpcClientEventLoop.shutdownGracefully();
        bgTasksScheduler.shutdownNow();
        pluginManager.stopPlugins();
        pluginManager.unloadPlugins();
    }
}
