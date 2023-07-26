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

package com.baidu.bifromq.dist.server;

import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.dist.IDistServiceBuilder;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import java.util.concurrent.Executor;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@Accessors(fluent = true)
@Setter
public final class DistServerBuilder implements IDistServiceBuilder {
    String id;
    String host;
    int port;
    EventLoopGroup bossEventLoopGroup;
    EventLoopGroup workerEventLoopGroup;

    Executor executor;
    IBaseKVStoreClient storeClient;
    ISettingProvider settingProvider;
    IEventCollector eventCollector;
    ICRDTService crdtService;
    String distCallPreSchedulerFactoryClass;
    SslContext sslContext;

    public IDistServer build() {
        return new DistServer(this, storeClient, settingProvider, eventCollector, crdtService);
    }
}
