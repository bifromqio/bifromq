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

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.pf4j.DefaultPluginManager;
import org.pf4j.PluginManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SettingProviderManagerTest {
    private final String tenantId = "tenantA";
    private SettingProviderManager manager;
    private PluginManager pluginManager;

    @BeforeMethod
    public void setup() {
        pluginManager = new DefaultPluginManager();
        pluginManager.loadPlugins();
        pluginManager.startPlugins();
    }

    @AfterMethod
    public void teardown() {
        pluginManager.stopPlugins();
        pluginManager.unloadPlugins();
    }

    @Test
    public void devOnlyMode() {
        manager = new SettingProviderManager(null, pluginManager);
        manager.provide(Setting.BoostModeEnabled, tenantId);
        manager.close();
    }

    @Test
    public void authPluginSpecified() {
        manager = new SettingProviderManager(SettingProviderTestStub.class.getName(), pluginManager);
        SettingProviderTestStub stub = (SettingProviderTestStub) manager.get();
        stub.setValue(Setting.MaxTopicLevels, 64);
        await().until(() -> (int) manager.provide(Setting.MaxTopicLevels, tenantId) == 64);
        manager.close();
    }

    @Test
    public void authPluginNotFound() {
        try {
            manager = new SettingProviderManager("Fake", pluginManager);
            fail();
        } catch (Throwable e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void stop() {
        manager = new SettingProviderManager(SettingProviderTestStub.class.getName(), pluginManager);
        manager.close();
    }
}
