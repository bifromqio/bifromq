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

package com.baidu.bifromq.basekv.server;

import com.baidu.bifromq.basekv.IBaseKVMetaService;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractBaseKVStoreServerBuilder<P extends AbstractBaseKVStoreServerBuilder<P>> {
    final Map<String, BaseKVStoreServiceBuilder<P>> serviceBuilders = new HashMap<>();
    IBaseKVMetaService metaService;

    public abstract IBaseKVStoreServer build();

    protected abstract BaseKVStoreServiceBuilder<P> newService(String clusterId, boolean bootstrap, P serverBuilder);

    @SuppressWarnings("unchecked")
    public BaseKVStoreServiceBuilder<P> addService(String clusterId, boolean bootstrap) {
        assert !serviceBuilders.containsKey(clusterId);
        return newService(clusterId, bootstrap, (P) this);
    }

    @SuppressWarnings("unchecked")
    public P metaService(IBaseKVMetaService metaService) {
        this.metaService = metaService;
        return (P) this;
    }
}
