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

package com.baidu.bifromq.retain.server;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

abstract class AbstractRetainServerBuilder<T extends AbstractRetainServerBuilder<T>> implements IRetainServerBuilder {
    ISettingProvider settingProvider;
    ISubBrokerManager subBrokerManager;
    IBaseKVStoreClient retainStoreClient;
    Map<String, String> attrs = new HashMap<>();
    Set<String> defaultGroupTags = new HashSet<>();
    Executor rpcExecutor = MoreExecutors.directExecutor();

    public T subBrokerManager(ISubBrokerManager subBrokerManager) {
        this.subBrokerManager = subBrokerManager;
        return thisT();
    }

    public T retainStoreClient(IBaseKVStoreClient retainStoreClient) {
        this.retainStoreClient = retainStoreClient;
        return thisT();
    }

    public T settingProvider(ISettingProvider settingProvider) {
        this.settingProvider = settingProvider;
        return thisT();
    }

    public T attributes(Map<String, String> attrs) {
        this.attrs = attrs;
        return thisT();
    }

    public T defaultGroupTags(Set<String> defaultGroupTags) {
        this.defaultGroupTags = defaultGroupTags;
        return thisT();
    }

    public T rpcExecutor(Executor rpcExecutor) {
        this.rpcExecutor = rpcExecutor;
        return thisT();
    }

    @SuppressWarnings("unchecked")
    private T thisT() {
        return (T) this;
    }
}
