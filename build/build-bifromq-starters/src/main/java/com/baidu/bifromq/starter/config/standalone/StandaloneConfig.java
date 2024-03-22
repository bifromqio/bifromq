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

import com.baidu.bifromq.starter.config.StarterConfig;
import com.baidu.bifromq.starter.config.standalone.model.ClusterConfig;
import com.baidu.bifromq.starter.config.standalone.model.RPCClientConfig;
import com.baidu.bifromq.starter.config.standalone.model.RPCServerConfig;
import com.baidu.bifromq.starter.config.standalone.model.StateStoreConfig;
import com.baidu.bifromq.starter.config.standalone.model.apiserver.APIServerConfig;
import com.baidu.bifromq.starter.config.standalone.model.mqttserver.MQTTServerConfig;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StandaloneConfig implements StarterConfig {
    private boolean bootstrap = false;
    private String authProviderFQN = null;
    private String resourceThrottlerFQN = null;
    private String settingProviderFQN = null;

    @JsonSetter(nulls = Nulls.SKIP)
    private ClusterConfig clusterConfig = new ClusterConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    private MQTTServerConfig mqttServerConfig = new MQTTServerConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    private RPCClientConfig rpcClientConfig = new RPCClientConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    private RPCServerConfig rpcServerConfig = new RPCServerConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    private RPCClientConfig baseKVClientConfig = new RPCClientConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    private RPCServerConfig baseKVServerConfig = new RPCServerConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    private StateStoreConfig stateStoreConfig = new StateStoreConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    private APIServerConfig apiServerConfig = new APIServerConfig();
}
