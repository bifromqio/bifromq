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

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StoreClientConfig {
    private String clusterName;
    @JsonAlias({"execPipelinePerStore", "execPipelinePerServer"}) // the execPipelinePerServer is deprecated
    private int execPipelinePerStore = 5;
    @JsonAlias({"queryPipelinePerStore", "queryPipelinePerServer"}) // the execPipelinePerServer is deprecated
    private int queryPipelinePerStore = 5;
    private ClientSSLContextConfig sslContextConfig = new ClientSSLContextConfig();
}
