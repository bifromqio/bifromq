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

import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxReaderClient;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.retain.client.IRetainServiceClient;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import io.netty.channel.EventLoopGroup;
import java.util.Optional;
import java.util.concurrent.Executor;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@Accessors(fluent = true)
@Setter
public final class MQTTBrokerBuilder {
    String mqttHost;
    String serverId;
    String rpcHost;
    int rpcPort;
    ICRDTService crdtService;
    EventLoopGroup rpcWorkerGroup;
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
    EventLoopGroup bossGroup;
    EventLoopGroup workerGroup;
    Executor ioExecutor;
    Executor bgTaskExecutor;
    IAuthProvider authProvider;
    IEventCollector eventCollector;
    ISettingProvider settingProvider;
    IDistClient distClient;
    IInboxReaderClient inboxClient;
    IRetainServiceClient retainClient;
    ISessionDictClient sessionDictClient;
    Optional<ConnListenerBuilder.TCPConnListenerBuilder> tcpListenerBuilder = Optional.empty();
    Optional<ConnListenerBuilder.TLSConnListenerBuilder> tlsListenerBuilder = Optional.empty();
    Optional<ConnListenerBuilder.WSConnListenerBuilder> wsListenerBuilder = Optional.empty();
    Optional<ConnListenerBuilder.WSSConnListenerBuilder> wssListenerBuilder = Optional.empty();

    public ConnListenerBuilder.TCPConnListenerBuilder buildTcpConnListener() {
        if (tcpListenerBuilder.isEmpty()) {
            tcpListenerBuilder = Optional.of(new ConnListenerBuilder.TCPConnListenerBuilder(this));
        }
        return tcpListenerBuilder.get();
    }

    public ConnListenerBuilder.TLSConnListenerBuilder buildTLSConnListener() {
        if (tlsListenerBuilder.isEmpty()) {
            tlsListenerBuilder = Optional.of(new ConnListenerBuilder.TLSConnListenerBuilder(this));
        }
        return tlsListenerBuilder.get();
    }

    public ConnListenerBuilder.WSConnListenerBuilder buildWSConnListener() {
        if (wsListenerBuilder.isEmpty()) {
            wsListenerBuilder = Optional.of(new ConnListenerBuilder.WSConnListenerBuilder(this));
        }
        return wsListenerBuilder.get();
    }

    public ConnListenerBuilder.WSSConnListenerBuilder buildWSSConnListener() {
        if (wssListenerBuilder.isEmpty()) {
            wssListenerBuilder = Optional.of(new ConnListenerBuilder.WSSConnListenerBuilder(this));
        }
        return wssListenerBuilder.get();
    }

    public IMQTTBroker build() {
        return new MQTTBroker(this);
    }
}
