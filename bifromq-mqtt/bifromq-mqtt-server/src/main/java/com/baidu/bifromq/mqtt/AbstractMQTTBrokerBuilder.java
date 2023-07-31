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

package com.baidu.bifromq.mqtt;

import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxReaderClient;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import io.netty.channel.EventLoopGroup;
import java.util.concurrent.Executor;
import org.slf4j.Logger;

abstract class AbstractMQTTBrokerBuilder<T extends AbstractMQTTBrokerBuilder<T>> implements IMQTTBrokerBuilder<T> {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(AbstractMQTTBrokerBuilder.class);
    int connectTimeoutSeconds = 20;
    int connectRateLimit = 1000;
    int disconnectRate = 1000;
    int maxResendTimes = 5;
    int resendDelayMillis = 3000;
    int defaultKeepAliveSeconds = 5 * 60; // 5 min
    int qos2ConfirmWindowSeconds = 5;
    long writeLimit = 512 * 1024;
    long readLimit = 512 * 1024;
    int maxBytesInMessage = 256 * 1024;
    EventLoopGroup mqttBossGroup;
    EventLoopGroup mqttWorkerGroup;
    Executor bgTaskExecutor;
    IAuthProvider authProvider;
    IEventCollector eventCollector;
    ISettingProvider settingProvider;
    IDistClient distClient;
    IInboxReaderClient inboxClient;
    IRetainClient retainClient;
    ISessionDictClient sessionDictClient;
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

    public T maxResendTimes(int maxResendTimes) {
        this.maxResendTimes = maxResendTimes;
        return thisT();
    }

    public T resendDelayMillis(int resendDelayMillis) {
        this.resendDelayMillis = resendDelayMillis;
        return thisT();
    }

    public T defaultKeepAliveSeconds(int defaultKeepAliveSeconds) {
        this.defaultKeepAliveSeconds = defaultKeepAliveSeconds;
        return thisT();
    }

    public T qos2ConfirmWindowSeconds(int qos2ConfirmWindowSeconds) {
        this.qos2ConfirmWindowSeconds = qos2ConfirmWindowSeconds;
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

    public T mqttBossGroup(EventLoopGroup mqttBossGroup) {
        this.mqttBossGroup = mqttBossGroup;
        return thisT();
    }

    public T mqttWorkerGroup(EventLoopGroup mqttWorkerGroup) {
        this.mqttWorkerGroup = mqttWorkerGroup;
        return thisT();
    }

    public T bgTaskExecutor(Executor bgTaskExecutor) {
        this.bgTaskExecutor = bgTaskExecutor;
        return thisT();
    }

    public T authProvider(IAuthProvider authProvider) {
        this.authProvider = authProvider;
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
        return thisT();
    }

    public T inboxClient(IInboxReaderClient inboxClient) {
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
