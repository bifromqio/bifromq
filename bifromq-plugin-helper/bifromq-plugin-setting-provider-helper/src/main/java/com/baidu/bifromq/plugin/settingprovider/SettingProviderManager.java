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

package com.baidu.bifromq.plugin.settingprovider;

import com.baidu.bifromq.type.ClientInfo;
import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.PluginManager;

@Slf4j
public class SettingProviderManager implements ISettingProvider {
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final ISettingProvider provider;
    private final ProvideCallHelper callHelper;

    public SettingProviderManager(String settingProviderFQN, PluginManager pluginMgr) {
        this(1024, Math.max(2, Runtime.getRuntime().availableProcessors() / 4), settingProviderFQN, pluginMgr);
    }

    public SettingProviderManager(int bufferSize, String settingProviderFQN, PluginManager pluginMgr) {
        this(bufferSize, Math.max(2, Runtime.getRuntime().availableProcessors() / 4), settingProviderFQN, pluginMgr);
    }

    public SettingProviderManager(int bufferSize,
                                  int workerThreads,
                                  String settingProviderFQN,
                                  PluginManager pluginMgr) {
        assert bufferSize > 0 && ((bufferSize & (bufferSize - 1)) == 0);
        Map<String, ISettingProvider> availSettingProviders = pluginMgr.getExtensions(ISettingProvider.class).stream()
            .collect(Collectors.toMap(e -> e.getClass().getName(), e -> e));
        if (availSettingProviders.isEmpty()) {
            log.warn("No setting provider plugin available, use DEV ONLY one instead");
            provider = new DevOnlySettingProvider();
        } else {
            if (settingProviderFQN == null) {
                log.warn("Auth provider plugin type are not specified, use DEV ONLY one instead");
                provider = new DevOnlySettingProvider();
            } else {
                Preconditions.checkArgument(availSettingProviders.containsKey(settingProviderFQN),
                    String.format("Setting Provider Plugin '%s' not found", settingProviderFQN));
                log.debug("Setting provider plugin type: {}", settingProviderFQN);
                provider = availSettingProviders.get(settingProviderFQN);
            }
        }
        callHelper = new ProvideCallHelper(bufferSize, workerThreads, provider);
    }

    public <R> R provide(Setting setting, ClientInfo clientInfo) {
        assert !stopped.get();
        return callHelper.provide(setting, clientInfo);
    }

    public ISettingProvider get() {
        return provider;
    }

    @Override
    public void close() {
        if (stopped.compareAndSet(false, true)) {
            log.info("Closing setting provider manager");
            callHelper.close();
            log.info("Setting provider manager closed");
        }
    }
}
