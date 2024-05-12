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

package com.baidu.bifromq.starter.config.standalone;

import com.baidu.bifromq.starter.config.standalone.model.ClusterConfig;
import com.baidu.bifromq.starter.config.standalone.model.RPCClientConfig;
import com.baidu.bifromq.starter.config.standalone.model.RPCServerConfig;
import com.baidu.bifromq.starter.config.standalone.model.ServerSSLContextConfig;
import com.baidu.bifromq.starter.config.standalone.model.apiserver.APIServerConfig;
import com.baidu.bifromq.starter.config.standalone.model.mqttserver.MQTTServerConfig;
import com.google.common.base.Strings;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StandaloneConfigConsolidator {
    private static final String BIND_ADDR = "BIND_ADDR";

    public static void consolidate(StandaloneConfig config) {
        consolidateClusterConfig(config);
        consolidateMQTTServerConfig(config);
        consolidateRPCClientConfig(config);
        consolidateRPCServerConfig(config);
        consolidateBaseKVClientConfig(config);
        consolidateBaseKVServerConfig(config);
        consolidateAPIServerConfig(config);
    }

    private static void consolidateClusterConfig(StandaloneConfig config) {
        ClusterConfig clusterConfig = config.getClusterConfig();
        if (clusterConfig.getHost() == null) {
            clusterConfig.setHost(resolveHost(config));
        }
        if (Strings.isNullOrEmpty(clusterConfig.getEnv())) {
            throw new IllegalArgumentException("Cluster env cannot be null or empty string");
        }
        if (!Strings.isNullOrEmpty(clusterConfig.getClusterDomainName()) && clusterConfig.getPort() == 0) {
            throw new IllegalArgumentException(
                "Port number must be specified and make sure all members use same number if seed address is resolved from domain name");
        }
    }

    private static void consolidateMQTTServerConfig(StandaloneConfig config) {
        MQTTServerConfig mqttServerConfig = config.getMqttServerConfig();
        // fill default host
        if (mqttServerConfig.getTcpListener().getHost() == null) {
            mqttServerConfig.getTcpListener().setHost("0.0.0.0");
        }
        if (mqttServerConfig.getTlsListener().getHost() == null) {
            mqttServerConfig.getTlsListener().setHost(mqttServerConfig.getTcpListener().getHost());
        }
        if (mqttServerConfig.getWsListener().getHost() == null) {
            mqttServerConfig.getWsListener().setHost(mqttServerConfig.getTcpListener().getHost());
        }
        if (mqttServerConfig.getWssListener().getHost() == null) {
            mqttServerConfig.getWssListener().setHost(mqttServerConfig.getTcpListener().getHost());
        }
        // fill self-signed certificate for ssl connection
        if ((mqttServerConfig.getWssListener().isEnable() &&
            mqttServerConfig.getWssListener().getSslConfig() == null) ||
            (mqttServerConfig.getTlsListener().isEnable() &&
                mqttServerConfig.getTlsListener().getSslConfig() == null)) {
            try {
                SelfSignedCertificate selfCert = new SelfSignedCertificate();
                ServerSSLContextConfig sslContextConfig = new ServerSSLContextConfig();
                sslContextConfig.setCertFile(selfCert.certificate().getAbsolutePath());
                sslContextConfig.setKeyFile(selfCert.privateKey().getAbsolutePath());
                if (mqttServerConfig.getTlsListener().isEnable() &&
                    mqttServerConfig.getTlsListener().getSslConfig() == null) {
                    mqttServerConfig.getTlsListener().setSslConfig(sslContextConfig);
                }
                if (mqttServerConfig.getWssListener().isEnable() &&
                    mqttServerConfig.getWssListener().getSslConfig() == null) {
                    mqttServerConfig.getWssListener().setSslConfig(sslContextConfig);
                }
            } catch (Throwable e) {
                log.warn("Unable to generate self-signed certificate, mqtt over tls or wss will be disabled", e);
                if (mqttServerConfig.getTlsListener().isEnable() &&
                    mqttServerConfig.getTlsListener().getSslConfig() == null) {
                    mqttServerConfig.getTlsListener().setEnable(false);
                }
                if (mqttServerConfig.getWssListener().isEnable() &&
                    mqttServerConfig.getWssListener().getSslConfig() == null) {
                    mqttServerConfig.getWssListener().setEnable(false);
                }
            }
        }
    }

    private static void consolidateRPCClientConfig(StandaloneConfig config) {
        // do nothing for now
    }

    private static void consolidateRPCServerConfig(StandaloneConfig config) {
        RPCServerConfig rpcServerConfig = config.getRpcServerConfig();
        // fill default host
        if (rpcServerConfig.getHost() == null) {
            rpcServerConfig.setHost(resolveHost(config));
        }
    }

    private static void consolidateBaseKVClientConfig(StandaloneConfig config) {
        RPCClientConfig rpcClientConfig = config.getBaseKVClientConfig();
        if (rpcClientConfig.getWorkerThreads() == null) {
            rpcClientConfig.setWorkerThreads(Math.max(2, Runtime.getRuntime().availableProcessors() / 8));
        }
    }

    private static void consolidateBaseKVServerConfig(StandaloneConfig config) {
        RPCServerConfig baseKVRpcServerConfig = config.getBaseKVServerConfig();
        if (baseKVRpcServerConfig.getHost() == null) {
            baseKVRpcServerConfig.setHost(resolveHost(config));
        }
        if (baseKVRpcServerConfig.getWorkerThreads() == null) {
            baseKVRpcServerConfig.setWorkerThreads(Math.max(2, Runtime.getRuntime().availableProcessors() / 2));
        }
        config.setBaseKVServerConfig(baseKVRpcServerConfig);
    }

    private static void consolidateAPIServerConfig(StandaloneConfig config) {
        APIServerConfig apiServerConfig = config.getApiServerConfig();
        if (apiServerConfig.getHost() == null) {
            apiServerConfig.setHost(resolveHost(config));
        }
    }

    @SneakyThrows
    private static String resolveHost(StandaloneConfig config) {
        String host = config.getMqttServerConfig().getTcpListener().getHost();
        if (!"0.0.0.0".equals(host)) {
            return host;
        }
        host = System.getProperty(BIND_ADDR, System.getenv(BIND_ADDR));
        if (!Strings.isNullOrEmpty(host)) {
            return host;
        }
        host = config.getRpcServerConfig().getHost();
        if (!Strings.isNullOrEmpty(host)) {
            return host;
        }
        host = config.getClusterConfig().getHost();
        if (!Strings.isNullOrEmpty(host)) {
            return host;
        }
        Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        while (networkInterfaces.hasMoreElements()) {
            NetworkInterface networkInterface = networkInterfaces.nextElement();
            Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
            while (inetAddresses.hasMoreElements()) {
                InetAddress inetAddress = inetAddresses.nextElement();
                if (!inetAddress.isLoopbackAddress() && !inetAddress.isLinkLocalAddress() &&
                    inetAddress.isSiteLocalAddress()) {
                    return inetAddress.getHostAddress();
                }
            }
        }
        throw new IllegalStateException("Unable to resolve host, please specify host in config file");
    }
}
