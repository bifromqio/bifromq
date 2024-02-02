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
import com.baidu.bifromq.deliverer.IMessageDeliverer;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;

abstract class AbstractRetainServerBuilder<T extends AbstractRetainServerBuilder<T>> implements IRetainServerBuilder {
    ISubBrokerManager subBrokerManager;
    IBaseKVStoreClient retainStoreClient;

    public T subBrokerManager(ISubBrokerManager subBrokerManager) {
        this.subBrokerManager = subBrokerManager;
        return thisT();
    }

    public T retainStoreClient(IBaseKVStoreClient retainStoreClient) {
        this.retainStoreClient = retainStoreClient;
        return thisT();
    }

    @SuppressWarnings("unchecked")
    private T thisT() {
        return (T) this;
    }
}
