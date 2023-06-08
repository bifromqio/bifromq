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

import com.baidu.bifromq.starter.config.model.ServerSSLContextConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StandaloneConfig implements StarterConfig {
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
    private ServerSSLContextConfig brokerSSLCtxConfig;
    private String authProviderFQN = null;
    private String settingProviderFQN = null;
    private DistWorkerConfig distWorkerConfig;
    private InboxStoreConfig inboxStoreConfig;
    private RetainStoreConfig retainStoreConfig;
    private int mqttWorkerThreads = Runtime.getRuntime().availableProcessors();
    private int ioClientParallelism = Math.max(2, Runtime.getRuntime().availableProcessors() / 3);
    private int ioServerParallelism = Math.max(2, Runtime.getRuntime().availableProcessors() / 3);
    private int queryThreads = Math.max(2, Runtime.getRuntime().availableProcessors() / 4);
    private int mutationThreads = 3;
    private int tickerThreads = Math.max(1, Runtime.getRuntime().availableProcessors() / 20);
    private int bgWorkerThreads = Math.max(1, Runtime.getRuntime().availableProcessors() / 20);
    public static StandaloneConfig build(File confFile) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            StandaloneConfig config = mapper.readValue(confFile, StandaloneConfig.class);
            return config;
        } catch (IOException e) {
            throw new RuntimeException("Unable to read config file: " + confFile, e);
        }
    }
}
