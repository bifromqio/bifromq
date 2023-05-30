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

package com.baidu.bifromq.starter.config.model;

import com.baidu.bifromq.starter.config.StarterConfig;
import io.netty.handler.ssl.ClientAuth;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class AgentHostConfig {
    private String env;
    private String host = StarterConfig.getHostFromSysProps();
    private int port;
    private String seedEndpoints;
    private String clusterDomainName;
    private boolean enableSSL;
    private String certFile;
    private String keyFile;
    private String trustCertsFile;

    public ServerSSLContextConfig serverSSLContextConfig() {
        ServerSSLContextConfig sslContextConfig = new ServerSSLContextConfig();
        sslContextConfig.setClientAuth(ClientAuth.REQUIRE.name());
        sslContextConfig.setTrustCertsFile(trustCertsFile);
        sslContextConfig.setCertFile(certFile);
        sslContextConfig.setKeyFile(keyFile);
        return sslContextConfig;
    }

    public ClientSSLContextConfig clientSSLContextConfig() {
        ClientSSLContextConfig sslContextConfig = new ClientSSLContextConfig();
        sslContextConfig.setTrustCertsFile(trustCertsFile);
        sslContextConfig.setCertFile(certFile);
        sslContextConfig.setKeyFile(keyFile);
        return sslContextConfig;
    }

}
