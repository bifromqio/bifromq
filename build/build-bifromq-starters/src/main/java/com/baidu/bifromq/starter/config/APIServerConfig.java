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
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class APIServerConfig {
    private boolean enable = false;
    private String host; // optional, if null host address will be used for listening api calls
    private int httpPort = 8090; // the listening port for http api
    private int httpsPort; // optional, if null host address will be used for listening api calls
    private int apiBossThreads = 1;
    private int apiWorkerThreads = 2;
    private ServerSSLContextConfig serverSSLCtxConfig = new ServerSSLContextConfig();
}