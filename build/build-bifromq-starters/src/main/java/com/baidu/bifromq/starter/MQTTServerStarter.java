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

package com.baidu.bifromq.starter;


import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.baserpc.utils.NettyUtil;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxReaderClient;
import com.baidu.bifromq.mqtt.IMQTTBroker;
import com.baidu.bifromq.mqtt.MQTTBrokerBuilder;
import com.baidu.bifromq.plugin.authprovider.AuthProviderManager;
import com.baidu.bifromq.plugin.eventcollector.EventCollectorManager;
import com.baidu.bifromq.plugin.manager.BifroMQPluginManager;
import com.baidu.bifromq.plugin.settingprovider.SettingProviderManager;
import com.baidu.bifromq.retain.client.IRetainServiceClient;
import com.baidu.bifromq.sessiondict.client.ISessionDictionaryClient;
import com.baidu.bifromq.sessiondict.server.ISessionDictionaryServer;
import com.baidu.bifromq.starter.config.MQTTServerConfig;
import com.baidu.bifromq.starter.config.model.RPCClientConfig;
import com.baidu.bifromq.starter.config.model.SessionDictServerConfig;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.PluginManager;

@Slf4j
public class MQTTServerStarter extends BaseStarter<MQTTServerConfig> {
    private PluginManager pluginMgr;

    private AuthProviderManager authProviderMgr;

    private EventCollectorManager eventCollectorMgr;

    private SettingProviderManager settingProviderMgr;

    private IAgentHost agentHost;

    private ICRDTService crdtService;

    private ISessionDictionaryClient sessionDictClient;

    private ISessionDictionaryServer sessionDictServer;

    private IDistClient distClient;

    private IInboxReaderClient inboxReaderClient;

    private IRetainServiceClient retainClient;

    private IMQTTBroker mqttBroker;

    @Override
    protected void init(MQTTServerConfig config) {
        pluginMgr = new BifroMQPluginManager();
        pluginMgr.loadPlugins();
        pluginMgr.startPlugins();
        eventCollectorMgr = new EventCollectorManager(pluginMgr);

        settingProviderMgr = new SettingProviderManager(config.getSettingProviderFQN(), pluginMgr);

        authProviderMgr =
            new AuthProviderManager(config.getAuthProviderFQN(), pluginMgr, settingProviderMgr, eventCollectorMgr);

        agentHost = initAgentHost(config.getAgentHostConfig());
        log.info("Agent host started");

        crdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        crdtService.start(agentHost);
        log.info("CRDT service started");

        sessionDictClient = buildSessionDictionaryClient(config.getSessionDictClientConfig());
        sessionDictServer = buildSessionDictionaryServer(config.getSessionDictServerConfig());
        inboxReaderClient = buildInboxReaderClient(config.getInboxReaderClientConfig());
        retainClient = buildRetainServiceClient(config.getRetainServiceClientConfig());
        distClient = buildDistClient(config.getDistClientConfig());

        mqttBroker = buildMQTTBroker(config);
    }

    @Override
    protected Class<MQTTServerConfig> configClass() {
        return MQTTServerConfig.class;
    }

    public void start() {
        sessionDictServer.start();
        log.info("Session dict server started");
        mqttBroker.start();
        log.info("Mqtt server started");
        setupMetrics();
    }

    public void stop() {
        mqttBroker.shutdown();
        log.info("Mqtt server shutdown");

        distClient.stop();
        log.info("Dist client stopped");

        inboxReaderClient.stop();
        log.info("Inbox reader client stopped");

        retainClient.stop();
        log.info("Retain client stopped");

        sessionDictClient.stop();
        log.info("Session dict client stopped");
        sessionDictServer.shutdown();
        log.info("Session dict server shut down");

        crdtService.stop();
        log.info("CRDT service stopped");

        agentHost.shutdown();
        log.info("Agent host stopped");

        authProviderMgr.close();
        log.info("Auth provider manager stopped");

        eventCollectorMgr.close();
        log.info("Event collector manager stopped");

        settingProviderMgr.close();
        log.info("Setting provider manager stopped");

        pluginMgr.stopPlugins();
        pluginMgr.unloadPlugins();
    }

    public static void main(String[] args) {
        StarterRunner.run(MQTTServerStarter.class, args);
    }

    private ISessionDictionaryServer buildSessionDictionaryServer(SessionDictServerConfig config) {
        if (config.getSslContextConfig().isEnableSSL()) {
            return ISessionDictionaryServer.sslServerBuilder()
                .id(UUID.randomUUID().toString())
                .host(config.getHost())
                .port(config.getPort())
                .crdtService(crdtService)
                .executor(MoreExecutors.directExecutor())
                .trustCertsFile(loadFromConfDir(config.getSslContextConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(config.getSslContextConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(config.getSslContextConfig().getKeyFile()))
                .build();
        }
        return ISessionDictionaryServer.nonSSLServerBuilder()
            .id(UUID.randomUUID().toString())
            .host(config.getHost())
            .port(config.getPort())
            .crdtService(crdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
    }

    private ISessionDictionaryClient buildSessionDictionaryClient(RPCClientConfig config) {
        if (config.getSslContextConfig().isEnableSSL()) {
            return ISessionDictionaryClient.sslBuilder()
                .trustCertsFile(loadFromConfDir(config.getSslContextConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(config.getSslContextConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(config.getSslContextConfig().getKeyFile()))
                .crdtService(crdtService)
                .executor(MoreExecutors.directExecutor())
                .build();
        }
        return ISessionDictionaryClient.nonSSLBuilder()
            .crdtService(crdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
    }

    private IInboxReaderClient buildInboxReaderClient(RPCClientConfig config) {
        if (config.getSslContextConfig().isEnableSSL()) {
            return IInboxReaderClient.sslClientBuilder()
                .trustCertsFile(loadFromConfDir(config.getSslContextConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(config.getSslContextConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(config.getSslContextConfig().getKeyFile()))
                .crdtService(crdtService)
                .executor(MoreExecutors.directExecutor())
                .build();
        }
        return IInboxReaderClient.nonSSLClientBuilder()
            .crdtService(crdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
    }

    private IRetainServiceClient buildRetainServiceClient(RPCClientConfig config) {
        if (config.getSslContextConfig().isEnableSSL()) {
            return IRetainServiceClient.sslClientBuilder()
                .trustCertsFile(loadFromConfDir(config.getSslContextConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(config.getSslContextConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(config.getSslContextConfig().getKeyFile()))
                .crdtService(crdtService)
                .executor(MoreExecutors.directExecutor())
                .build();
        }
        return IRetainServiceClient.nonSSLClientBuilder()
            .crdtService(crdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
    }

    private IDistClient buildDistClient(RPCClientConfig config) {
        if (config.getSslContextConfig().isEnableSSL()) {
            return IDistClient.sslClientBuilder()
                .trustCertsFile(loadFromConfDir(config.getSslContextConfig().getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(config.getSslContextConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(config.getSslContextConfig().getKeyFile()))
                .crdtService(crdtService)
                .executor(MoreExecutors.directExecutor())
                .build();
        }
        return IDistClient.nonSSLClientBuilder()
            .crdtService(crdtService)
            .executor(MoreExecutors.directExecutor())
            .build();
    }

    private IMQTTBroker buildMQTTBroker(MQTTServerConfig config) {
        MQTTBrokerBuilder brokerBuilder;
        if (config.getLocalSessionServerConfig().getSslContextConfig().isEnableSSL()) {
            brokerBuilder = IMQTTBroker.sslBrokerBuilder()
                .serverId(UUID.randomUUID().toString())
                .rpcHost(config.getLocalSessionServerConfig().getHost())
                .port(config.getLocalSessionServerConfig().getPort())
                .crdtService(crdtService)
                .trustCertsFile(loadFromConfDir(config.getLocalSessionServerConfig().getSslContextConfig()
                    .getTrustCertsFile()))
                .serviceIdentityCertFile(loadFromConfDir(
                    config.getLocalSessionServerConfig().getSslContextConfig().getCertFile()))
                .privateKeyFile(loadFromConfDir(
                    config.getLocalSessionServerConfig().getSslContextConfig().getKeyFile()));
        } else {
            brokerBuilder = IMQTTBroker.nonSSLBrokerBuilder()
                .serverId(UUID.randomUUID().toString())
                .rpcHost(config.getLocalSessionServerConfig().getHost())
                .port(config.getLocalSessionServerConfig().getPort())
                .crdtService(crdtService);
        }
        brokerBuilder.host(config.getHost())
            .bossGroup(NettyUtil.createEventLoopGroup(1))
            .workerGroup(NettyUtil.createEventLoopGroup(config.getMqttWorkerThreads()))
            .ioExecutor(MoreExecutors.directExecutor())
            .authProvider(authProviderMgr)
            .eventCollector(eventCollectorMgr)
            .settingProvider(settingProviderMgr)
            .distClient(distClient)
            .inboxReader(inboxReaderClient)
            .sessionDictClient(sessionDictClient)
            .retainClient(retainClient)
            .connectTimeoutSeconds(config.getConnTimeoutSec())
            .connectRateLimit(config.getMaxConnPerSec())
            .disconnectRate(config.getMaxDisconnPerSec())
            .readLimit(config.getMaxConnBandwidth())
            .writeLimit(config.getMaxConnBandwidth())
            .defaultKeepAliveSeconds(config.getDefaultKeepAliveSec())
            .maxBytesInMessage(config.getMaxMsgByteSize())
            .maxResendTimes(config.getMaxResendTimes())
            .qos2ConfirmWindowSeconds(config.getQos2ConfirmWindowSec());
        if (config.isTcpEnabled()) {
            brokerBuilder.buildTcpConnListener()
                .port(config.getTcpPort())
                .buildListener();
        }
        if (config.isTlsEnabled()) {
            Preconditions.checkNotNull(config.getBrokerSSLCtxConfig());
            brokerBuilder.buildTLSConnListener()
                .port(config.getTlsPort())
                .sslContext(buildServerSslContext(config.getBrokerSSLCtxConfig()))
                .buildListener();
        }
        if (config.isWsEnabled()) {
            brokerBuilder.buildWSConnListener()
                .path(config.getWsPath())
                .port(config.getWsPort())
                .buildListener();
        }
        if (config.isWssEnabled()) {
            brokerBuilder.buildWSSConnListener()
                .path(config.getWsPath())
                .port(config.getWssPort())
                .sslContext(buildServerSslContext(config.getBrokerSSLCtxConfig()))
                .buildListener();
        }
        return brokerBuilder.build();
    }

}
