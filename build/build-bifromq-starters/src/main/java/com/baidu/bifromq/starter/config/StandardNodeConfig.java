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

package com.baidu.bifromq.starter.config;

import com.baidu.bifromq.starter.config.model.AgentHostConfig;
import com.baidu.bifromq.starter.config.model.ExecutorConfig;
import com.baidu.bifromq.starter.config.model.LocalSessionServerConfig;
import com.baidu.bifromq.starter.config.model.RPCClientConfig;
import com.baidu.bifromq.starter.config.model.ServerSSLContextConfig;
import com.baidu.bifromq.starter.config.model.SessionDictServerConfig;
import com.baidu.bifromq.starter.config.model.StoreClientConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StandardNodeConfig implements StarterConfig {
    private String host;
    private int connTimeoutSec = 20;
    private int maxConnPerSec = 1000;
    private int maxDisconnPerSec = 1000;
    private int maxMsgByteSize = 256 * 1024;
    private int maxResendTimes = 5;
    private int maxConnBandwidth = 512 * 1024;
    private int defaultKeepAliveSec = 300;
    private int qos2ConfirmWindowSec = 5;
    private int tcpPort = 1883;
    private int tlsPort = 1884;
    private int wsPort = 80;
    private int wssPort = 443;
    private String wsPath = "/mqtt";
    private boolean tcpEnabled = true;
    private boolean tlsEnabled = false;
    private boolean wsEnabled = true;
    private boolean wssEnabled = false;
    private boolean kvStoreBootstrap = false;
    private String authProviderFQN = null;
    private String settingProviderFQN = null;
    private ServerSSLContextConfig brokerSSLCtxConfig = new ServerSSLContextConfig();
    private ServerSSLContextConfig rpcServerSSLCtxConfig = new ServerSSLContextConfig();
    private RPCClientConfig rpcClientConfig = new RPCClientConfig();

    private ExecutorConfig executorConfig = new ExecutorConfig();
    private AgentHostConfig agentHostConfig;

    private StoreClientConfig distStoreClientConfig = new StoreClientConfig();
    private StoreClientConfig inboxStoreClientConfig = new StoreClientConfig();
    private StoreClientConfig retainStoreClientConfig = new StoreClientConfig();

    private LocalSessionServerConfig localSessionServerConfig = new LocalSessionServerConfig();
    private SessionDictServerConfig sessionDictServerConfig = new SessionDictServerConfig();
    private DistServerConfig distServerConfig = new DistServerConfig();
    private InboxServerConfig inboxServerConfig = new InboxServerConfig();
    private RetainServerConfig retainServerConfig = new RetainServerConfig();
    private DistWorkerConfig distWorkerConfig;
    private InboxStoreConfig inboxStoreConfig;
    private RetainStoreConfig retainStoreConfig;

    public static StandardNodeConfig build(File confFile) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            return mapper.readValue(confFile, StandardNodeConfig.class);
        } catch (IOException e) {
            throw new RuntimeException("Unable to read config file: " + confFile, e);
        }
    }
}
