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

package com.baidu.bifromq.dist.server;

import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;

abstract class AbstractDistServerBuilder<T extends AbstractDistServerBuilder<T>> implements IDistServerBuilder {
    IBaseKVStoreClient distWorkerClient;
    ISettingProvider settingProvider;
    IEventCollector eventCollector;
    ICRDTService crdtService;
    String distCallPreSchedulerFactoryClass;

    public T distWorkerClient(IBaseKVStoreClient distWorkerClient) {
        this.distWorkerClient = distWorkerClient;
        return thisT();
    }

    public T settingProvider(ISettingProvider settingProvider) {
        this.settingProvider = settingProvider;
        return thisT();
    }

    public T eventCollector(IEventCollector eventCollector) {
        this.eventCollector = eventCollector;
        return thisT();
    }

    public T crdtService(ICRDTService crdtService) {
        this.crdtService = crdtService;
        return thisT();
    }

    public T distCallPreSchedulerFactoryClass(String distCallPreSchedulerFactoryClass) {
        this.distCallPreSchedulerFactoryClass = distCallPreSchedulerFactoryClass;
        return thisT();
    }

    @SuppressWarnings("unchecked")
    private T thisT() {
        return (T) this;
    }
}
