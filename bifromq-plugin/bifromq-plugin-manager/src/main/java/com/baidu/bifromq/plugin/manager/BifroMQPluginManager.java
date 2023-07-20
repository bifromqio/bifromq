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

package com.baidu.bifromq.plugin.manager;

import lombok.extern.slf4j.Slf4j;
import org.pf4j.CompoundPluginLoader;
import org.pf4j.DefaultPluginManager;
import org.pf4j.ExtensionFactory;
import org.pf4j.PluginLoader;
import org.pf4j.PluginRuntimeException;

@Slf4j
public class BifroMQPluginManager extends DefaultPluginManager {
    @Override
    protected PluginLoader createPluginLoader() {
        return new CompoundPluginLoader()
            .add(new BifroMQDevelopmentPluginLoader(this), this::isDevelopment)
            .add(new BifroMQJarPluginLoader(this), this::isNotDevelopment)
            .add(new BifroMQDefaultPluginLoader(this), this::isNotDevelopment);
    }

    @Override
    protected ExtensionFactory createExtensionFactory() {
        return new ExtensionFactory() {
            @Override
            public <T> T create(Class<T> extensionClass) {
                try {
                    ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
                    ClassLoader targetLoader = extensionClass.getClassLoader();
                    Thread.currentThread().setContextClassLoader(targetLoader);
                    T instance = extensionClass.newInstance();
                    Thread.currentThread().setContextClassLoader(originalLoader);
                    log.debug("switch from target loader: {} to default loader: {}", targetLoader, originalLoader);
                    return instance;
                } catch (Exception e) {
                    throw new PluginRuntimeException(e);
                }
            }
        };
    }
}
