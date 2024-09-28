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

import com.baidu.bifromq.basecrdt.service.ICRDTService;
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
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class AbstractMQTTBrokerBuilder<T extends AbstractMQTTBrokerBuilder<T>> implements IMQTTBrokerBuilder<T> {
    int connectTimeoutSeconds = 20;
    int connectRateLimit = 1000;
    int disconnectRate = 1000;
    int defaultKeepAliveSeconds = 5 * 60; // 5 min
    long writeLimit = 512 * 1024;
    long readLimit = 512 * 1024;
    int maxBytesInMessage = 256 * 1024;
    int mqttBossELGThreads;
    int mqttWorkerELGThreads;
    ICRDTService crdtService;
    IAuthProvider authProvider;
    IClientBalancer clientBalancer;
    IResourceThrottler resourceThrottler;
    IEventCollector eventCollector;
    ISettingProvider settingProvider;
    IDistClient distClient;
    IInboxClient inboxClient;
    IRetainClient retainClient;
    ISessionDictClient sessionDictClient;
    ILocalSessionRegistry sessionRegistry;
    ILocalDistService distService;
    ConnListenerBuilder.TCPConnListenerBuilder<T> tcpListenerBuilder;
    ConnListenerBuilder.TLSConnListenerBuilder<T> tlsListenerBuilder;
    ConnListenerBuilder.WSConnListenerBuilder<T> wsListenerBuilder;
    ConnListenerBuilder.WSSConnListenerBuilder<T> wssListenerBuilder;

    AbstractMQTTBrokerBuilder() {
    }

    @SuppressWarnings("unchecked")
    private T thisT() {
        return (T) this;
    }

    @Override
    public ConnListenerBuilder.TCPConnListenerBuilder<T> buildTcpConnListener() {
        if (tcpListenerBuilder == null) {
            tcpListenerBuilder = new ConnListenerBuilder.TCPConnListenerBuilder<>(thisT());
        }
        return tcpListenerBuilder;
    }

    @Override
    public ConnListenerBuilder.TLSConnListenerBuilder<T> buildTLSConnListener() {
        if (tlsListenerBuilder == null) {
            tlsListenerBuilder = new ConnListenerBuilder.TLSConnListenerBuilder<>(thisT());
        }
        return tlsListenerBuilder;
    }

    @Override
    public ConnListenerBuilder.WSConnListenerBuilder<T> buildWSConnListener() {
        if (wsListenerBuilder == null) {
            wsListenerBuilder = new ConnListenerBuilder.WSConnListenerBuilder<>(thisT());
        }
        return wsListenerBuilder;
    }

    @Override
    public ConnListenerBuilder.WSSConnListenerBuilder<T> buildWSSConnListener() {
        if (wssListenerBuilder == null) {
            wssListenerBuilder = new ConnListenerBuilder.WSSConnListenerBuilder<>(thisT());
        }
        return wssListenerBuilder;
    }

    public T connectTimeoutSeconds(int connectTimeoutSeconds) {
        this.connectTimeoutSeconds = connectTimeoutSeconds;
        return thisT();
    }

    public T connectRateLimit(int connectRateLimit) {
        this.connectRateLimit = connectRateLimit;
        return thisT();
    }

    public T disconnectRate(int disconnectRate) {
        this.disconnectRate = disconnectRate;
        return thisT();
    }

    public T defaultKeepAliveSeconds(int defaultKeepAliveSeconds) {
        this.defaultKeepAliveSeconds = defaultKeepAliveSeconds;
        return thisT();
    }

    public T writeLimit(long writeLimit) {
        this.writeLimit = writeLimit;
        return thisT();
    }

    public T readLimit(long readLimit) {
        this.readLimit = readLimit;
        return thisT();
    }

    public T maxBytesInMessage(int maxBytesInMessage) {
        this.maxBytesInMessage = maxBytesInMessage;
        return thisT();
    }

    public T mqttBossELGThreads(int mqttBossELGThreads) {
        this.mqttBossELGThreads = mqttBossELGThreads;
        return thisT();
    }

    public T mqttWorkerELGThreads(int mqttWorkerELGThreads) {
        this.mqttWorkerELGThreads = mqttWorkerELGThreads;
        return thisT();
    }

    public T crdtService(ICRDTService crdtService) {
        this.crdtService = crdtService;
        return thisT();
    }

    public T authProvider(IAuthProvider authProvider) {
        this.authProvider = authProvider;
        return thisT();
    }

    public T clientBalancer(IClientBalancer clientBalancer) {
        this.clientBalancer = clientBalancer;
        return thisT();
    }

    public T resourceThrottler(IResourceThrottler resourceThrottler) {
        this.resourceThrottler = resourceThrottler;
        return thisT();
    }

    public T eventCollector(IEventCollector eventCollector) {
        this.eventCollector = eventCollector;
        return thisT();
    }

    public T settingProvider(ISettingProvider settingProvider) {
        this.settingProvider = settingProvider;
        return thisT();
    }

    public T distClient(IDistClient distClient) {
        this.distClient = distClient;
        sessionRegistry = new LocalSessionRegistry();
        ILocalTopicRouter router = new LocalTopicRouter(brokerId(), distClient);
        distService = new LocalDistService(brokerId(), sessionRegistry, router, distClient, resourceThrottler);
        return thisT();
    }

    public T inboxClient(IInboxClient inboxClient) {
        this.inboxClient = inboxClient;
        return thisT();
    }

    public T retainClient(IRetainClient retainClient) {
        this.retainClient = retainClient;
        return thisT();
    }

    public T sessionDictClient(ISessionDictClient sessionDictClient) {
        this.sessionDictClient = sessionDictClient;
        return thisT();
    }

    public T tcpListenerBuilder(
        ConnListenerBuilder.TCPConnListenerBuilder<T> tcpListenerBuilder) {
        this.tcpListenerBuilder = tcpListenerBuilder;
        return thisT();
    }

    public T tlsListenerBuilder(
        ConnListenerBuilder.TLSConnListenerBuilder<T> tlsListenerBuilder) {
        this.tlsListenerBuilder = tlsListenerBuilder;
        return thisT();
    }

    public T wsListenerBuilder(
        ConnListenerBuilder.WSConnListenerBuilder<T> wsListenerBuilder) {
        this.wsListenerBuilder = wsListenerBuilder;
        return thisT();
    }

    public T wssListenerBuilder(
        ConnListenerBuilder.WSSConnListenerBuilder<T> wssListenerBuilder) {
        this.wssListenerBuilder = wssListenerBuilder;
        return thisT();
    }
}
