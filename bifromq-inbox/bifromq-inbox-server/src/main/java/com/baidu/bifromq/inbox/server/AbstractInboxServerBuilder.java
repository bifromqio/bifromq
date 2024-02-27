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

package com.baidu.bifromq.inbox.server;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.bifromq.plugin.resourcethrottler.IResourceThrottler;
import java.util.concurrent.ScheduledExecutorService;

abstract class AbstractInboxServerBuilder<T extends AbstractInboxServerBuilder<T>> implements IInboxServerBuilder {
    IEventCollector eventCollector;
    IResourceThrottler resourceThrottler;
    ISettingProvider settingProvider;
    IInboxClient inboxClient;
    IDistClient distClient;
    IRetainClient retainClient;
    IBaseKVStoreClient inboxStoreClient;
    ScheduledExecutorService bgTaskExecutor;

    AbstractInboxServerBuilder() {
    }

    public T inboxClient(IInboxClient inboxClient) {
        this.inboxClient = inboxClient;
        return thisT();
    }

    public T distClient(IDistClient distClient) {
        this.distClient = distClient;
        return thisT();
    }

    public T retainClient(IRetainClient retainClient) {
        this.retainClient = retainClient;
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

    public T resourceThrottler(IResourceThrottler resourceThrottler) {
        this.resourceThrottler = resourceThrottler;
        return thisT();
    }


    public T inboxStoreClient(IBaseKVStoreClient inboxStoreClient) {
        this.inboxStoreClient = inboxStoreClient;
        return thisT();
    }

    public T bgTaskExecutor(ScheduledExecutorService bgTaskExecutor) {
        this.bgTaskExecutor = bgTaskExecutor;
        return thisT();
    }

    @SuppressWarnings("unchecked")
    private T thisT() {
        return (T) this;
    }
}
