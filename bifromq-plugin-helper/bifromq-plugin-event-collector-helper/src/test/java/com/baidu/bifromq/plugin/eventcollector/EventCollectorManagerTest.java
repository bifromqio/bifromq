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

package com.baidu.bifromq.plugin.eventcollector;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

import com.baidu.bifromq.plugin.eventcollector.mqttbroker.PingReq;
import com.baidu.bifromq.type.ClientInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pf4j.DefaultPluginManager;
import org.pf4j.PluginManager;

public class EventCollectorManagerTest {
    private PluginManager pluginManager;

    @Before
    public void setup() {
        pluginManager = new DefaultPluginManager();
        pluginManager.loadPlugins();
        pluginManager.startPlugins();
    }

    @After
    public void teardown() {
        pluginManager.stopPlugins();
        pluginManager.unloadPlugins();
    }

    @Test
    public void ReportEvent() {
        EventCollectorManager manager = new EventCollectorManager(pluginManager);
        EventCollectorTestStub collector = (EventCollectorTestStub) manager.get(EventCollectorTestStub.class.getName());
        assertEquals(0, collector.events.size());
        manager.report(ThreadLocalEventPool.getLocal(PingReq.class).clientInfo(ClientInfo.getDefaultInstance()));
        await().until(() -> collector.events.size() == 1);
    }
}
