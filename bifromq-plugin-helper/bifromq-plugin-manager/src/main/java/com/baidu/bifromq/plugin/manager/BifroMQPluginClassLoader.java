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

import org.pf4j.ClassLoadingStrategy;
import org.pf4j.PluginClassLoader;
import org.pf4j.PluginDescriptor;
import org.pf4j.PluginManager;

class BifroMQPluginClassLoader extends PluginClassLoader {
    public BifroMQPluginClassLoader(PluginManager pluginManager,
                                    PluginDescriptor pluginDescriptor,
                                    ClassLoader parent) {
        super(pluginManager, pluginDescriptor, parent);
    }

    public BifroMQPluginClassLoader(PluginManager pluginManager,
                                    PluginDescriptor pluginDescriptor,
                                    ClassLoader parent,
                                    ClassLoadingStrategy classLoadingStrategy) {
        super(pluginManager, pluginDescriptor, parent, classLoadingStrategy);
    }

    @Override
    public Class<?> loadClass(String className) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(className)) {
            // if the class is provided by bifromq
            if (ProvidedPackages.isProvided(className)) {
                return getParent().loadClass(className);
            }
        }
        return super.loadClass(className);
    }
}
