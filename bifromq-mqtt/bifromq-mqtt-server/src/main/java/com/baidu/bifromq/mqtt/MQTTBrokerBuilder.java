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

package com.baidu.bifromq.mqtt;

import com.baidu.bifromq.baserpc.server.RPCServerBuilder;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.mqtt.service.ILocalDistService;
import com.baidu.bifromq.mqtt.service.ILocalSessionRegistry;
import com.baidu.bifromq.mqtt.service.ILocalTopicRouter;
import com.baidu.bifromq.mqtt.service.LocalDistService;
import com.baidu.bifromq.mqtt.service.LocalSessionRegistry;
import com.baidu.bifromq.mqtt.service.LocalTopicRouter;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.clientbalancer.IClientBalancer;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import com.bifromq.plugin.resourcethrottler.IResourceThrottler;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@Accessors(fluent = true)
@Setter
public class MQTTBrokerBuilder implements IMQTTBrokerBuilder {
    int connectTimeoutSeconds = 20;
    int connectRateLimit = 1000;
    int disconnectRate = 1000;
    long writeLimit = 512 * 1024;
    long readLimit = 512 * 1024;
    int maxBytesInMessage = 256 * 1024;
    int mqttBossELGThreads;
    int mqttWorkerELGThreads;
    RPCServerBuilder rpcServerBuilder;
    IAuthProvider authProvider;
    IClientBalancer clientBalancer;
    IResourceThrottler resourceThrottler;
    IEventCollector eventCollector;
    ISettingProvider settingProvider;
    IDistClient distClient;
    IInboxClient inboxClient;
    IRetainClient retainClient;
    ISessionDictClient sessionDictClient;

    @Setter(AccessLevel.NONE)
    ILocalSessionRegistry sessionRegistry;
    @Setter(AccessLevel.NONE)
    ILocalDistService distService;
    @Setter(AccessLevel.NONE)
    ConnListenerBuilder.TCPConnListenerBuilder tcpListenerBuilder;
    @Setter(AccessLevel.NONE)
    ConnListenerBuilder.TLSConnListenerBuilder tlsListenerBuilder;
    @Setter(AccessLevel.NONE)
    ConnListenerBuilder.WSConnListenerBuilder wsListenerBuilder;
    @Setter(AccessLevel.NONE)
    ConnListenerBuilder.WSSConnListenerBuilder wssListenerBuilder;

    @Override
    public ConnListenerBuilder.TCPConnListenerBuilder buildTcpConnListener() {
        if (tcpListenerBuilder == null) {
            tcpListenerBuilder = new ConnListenerBuilder.TCPConnListenerBuilder(this);
        }
        return tcpListenerBuilder;
    }

    @Override
    public ConnListenerBuilder.TLSConnListenerBuilder buildTLSConnListener() {
        if (tlsListenerBuilder == null) {
            tlsListenerBuilder = new ConnListenerBuilder.TLSConnListenerBuilder(this);
        }
        return tlsListenerBuilder;
    }

    @Override
    public ConnListenerBuilder.WSConnListenerBuilder buildWSConnListener() {
        if (wsListenerBuilder == null) {
            wsListenerBuilder = new ConnListenerBuilder.WSConnListenerBuilder(this);
        }
        return wsListenerBuilder;
    }

    @Override
    public ConnListenerBuilder.WSSConnListenerBuilder buildWSSConnListener() {
        if (wssListenerBuilder == null) {
            wssListenerBuilder = new ConnListenerBuilder.WSSConnListenerBuilder(this);
        }
        return wssListenerBuilder;
    }

    public MQTTBrokerBuilder distClient(IDistClient distClient) {
        this.distClient = distClient;
        sessionRegistry = new LocalSessionRegistry();
        ILocalTopicRouter router = new LocalTopicRouter(brokerId(), distClient);
        distService = new LocalDistService(brokerId(), sessionRegistry, router, distClient, resourceThrottler);
        return this;
    }

    @Override
    public String brokerId() {
        return rpcServerBuilder.id();
    }

    public IMQTTBroker build() {
        return new MQTTBroker(this);
    }
}
