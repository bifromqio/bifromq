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

package com.baidu.bifromq.basekv.localengine.memory;

import com.baidu.bifromq.basekv.localengine.AbstractKVEngine;
import com.baidu.bifromq.basekv.localengine.IKVSpace;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class InMemKVEngine extends AbstractKVEngine {
    private final String identity;
    private final Map<String, InMemKVSpace> kvSpaceMap = new ConcurrentHashMap<>();
    private final InMemKVEngineConfigurator configurator;

    public InMemKVEngine(String overrideIdentity, InMemKVEngineConfigurator c) {
        super(overrideIdentity);
        this.configurator = c;
        if (overrideIdentity != null && !overrideIdentity.trim().isEmpty()) {
            identity = overrideIdentity;
        } else {
            identity = UUID.randomUUID().toString();
        }
    }

    @Override
    protected void doStart(String... metricTags) {

    }

    @Override
    protected void doStop() {

    }

    @Override
    public String id() {
        return identity;
    }

    @Override
    public Map<String, IKVSpace> ranges() {
        return Collections.unmodifiableMap(kvSpaceMap);
    }

    @Override
    public IKVSpace createIfMissing(String rangeId) {
        return kvSpaceMap.computeIfAbsent(rangeId,
            k -> new InMemKVSpace(k, configurator, this, () -> kvSpaceMap.remove(rangeId)));
    }
}
