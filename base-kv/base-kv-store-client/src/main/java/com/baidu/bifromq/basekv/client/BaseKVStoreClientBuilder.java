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

package com.baidu.bifromq.basekv.client;

import com.baidu.bifromq.basecrdt.service.ICRDTService;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import java.util.UUID;
import java.util.concurrent.Executor;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
@Setter
public final class BaseKVStoreClientBuilder {
    final String id = UUID.randomUUID().toString();
    String clusterId;
    EventLoopGroup eventLoopGroup;
    SslContext sslContext;
    long keepAliveInSec;
    long idleTimeoutInSec;

    ICRDTService crdtService;

    Executor executor;
    int queryPipelinesPerStore;

    public IBaseKVStoreClient build() {
        return new BaseKVStoreClient(this);
    }
}
